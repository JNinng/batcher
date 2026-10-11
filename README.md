# batcher

将零散的单次请求扇入为批量操作，并把结果按位置归还各调用方的 Go 库。

典型场景：上游持续到达 `get user by id` 这类单条查询，下游只提供 `batch get users` 这类批量接口。`batcher` 在两者之间把并发到达的单条请求合并为一次批量调用，每个调用方同步等待自己那份结果，如同调用了一个普通的单条接口。

## 特性

- **同步 `Do`，按位置对位**：`Do(ctx, req)` 阻塞直到自己那份结果返回；批次函数返回与入参等长、同序的结果切片，长度不符视为整批失败。
- **先到先触发**：批大小上限、批字节上限或时间间隔（自组批起算）先到先触发 Flush；仅有数据才触发，批次函数永远不会收到空切片。
- **可配置下游并发**：默认串行单在途（下游并发 1，行为可预测）；`WithMaxConcurrency(n)` 放宽至同一时刻至多 n 个批次在途，组批与执行重叠——慢批次不再令后续批次空等，缓解队头阻塞。计数单位是批次而非条目。
- **分片 Batcher**（[sharded 子包](sharded)）：`sharded.New(route, n, factory)` 把并发请求按路由函数分发给 n 个独立组批、独立执行的分片（Redis cluster、分库分表），工厂以 shard 下标按分片选连接；内置工厂与 `NewKeyed` / `NewKeyedMap` 组合（前提「同 key ⇒ 同分片」），`Close` 级联关闭全部分片。
- **有界队列 + 负载卸载**：`WithMaxPending(n)` 限制已准入未交付的提交数（含停泊在握手上的发送方），达上限新 `Do` 立即返回 `ErrOverloaded`，避免 goroutine 无界堆积。已准入提交数恒不超过 n；并发竞争下额度尚空的提交仅可能被假阳性提前拒绝。
- **批内 key 去重**：`NewKeyed` 把同 key 的并发请求折叠为一次执行——批次函数收到去重切片（首次出现序），同 key 的 Waiter 共享结果；maxSize 仍按原始条数计数，事件双口径 `Size` / `UniqueSize`。
- **逐条结果 / 错误**：`Result[Res]{Value, Err}` 让位置对位 API 逐条表达业务失败，结果切片仍等长；`NewKeyedMap` 是 keyed-map 变体，批次函数返回 `map[K]Res`，缺 key 转单条 `ErrMissingResult`、非整批失败。
- **Fire-and-forget 入队**：`Enqueue` 提交即返回、不等待结果，写路径（日志/埋点/producer）与读路径共享同一组批与观测口径；`EnqueueFunc` 在交付点同步回调结果，回调 panic 由库 recover。
- **批级错误广播**：批次函数返回的非 nil 错误（含 panic 经 recover 转化）广播给该批全部 Waiter；库继续可用。
- **批次重试**：`WithRetry(n, backoff)` 对批次函数以 `ErrRetryable` 哨兵标记的瞬时故障整批按原请求重执行（含退避，持槽进行），至多 n 次；其他错误照常广播——不重试的默认语义不变。
- **取消不改变批次形状**：调用方取消仅解除自身等待，其 Item 保留照常执行、结果丢弃；批次的 size 与对位绝不因取消而变化。
- **优雅关闭**：`Close` 立即 flush 未满批并等待在途批次完成，正常返回即全部已入队 Waiter 已被唤醒；幂等、可并发调用。
- **旁路可观测**：类型化 `Observer` 事件、`observ.Meter` 指标、结构化日志；Observer 与 Meter 默认 Noop，日志未注入时构造期快照 `observ.DefaultLogger()`。

## 安装

```bash
go get github.com/jninng/batcher
```

要求 Go 1.26.2+。

## 快速开始

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jninng/batcher"
)

func main() {
	// 批次函数：接收整个 Batch（恒非空），返回等长结果切片。
	queryUsers := func(ctx context.Context, ids []int) ([]string, error) {
		users := make([]string, len(ids))
		for i, id := range ids {
			users[i] = fmt.Sprintf("user-%d", id)
		}
		return users, nil
	}

	b := batcher.New(queryUsers,
		batcher.WithMaxSize(10),                   // 攒够 10 条立即 Flush
		batcher.WithInterval(50*time.Millisecond), // 否则自组批起 50ms 后 Flush
	)
	defer b.Close(context.Background())

	// 多个 goroutine 并发调用 Do，各自阻塞等待自己的结果。
	res, err := b.Do(context.Background(), 42)
	if err != nil {
		panic(err)
	}
	fmt.Println(res) // user-42
}
```

完整可运行示例见 [examples/httpserver](examples/httpserver)——一个把并发 HTTP 单查扇入为下游批量查询的服务，演示两种 Flush 触发、热点 key 去重、逐条业务失败与旁路观测：

```bash
go run ./examples/httpserver
```

分片下游示例见 [examples/sharded](examples/sharded)——8 个分库独立组批/执行、工厂按分片选连接、热点分库负载不均可见：

```bash
go run ./examples/sharded
```

## 核心语义

| 语义 | 行为 |
| --- | --- |
| Flush 触发 | 批大小上限、批字节上限或时间间隔先到先触发；时间间隔自组批起点（第一条 Item 进入空批）起算，是尽力而为的软上限。空批不计时、不唤醒。字节按原始条目累计，单条即超限自成批大小 1 的孤批——不拆分、不丢弃。 |
| 下游并发 | 默认恒为 1（串行单在途）；`WithMaxConcurrency(n)` 放宽至同一时刻批次函数在途（已 Flush 未返回）的批次 ≤ n。组批与执行重叠：批次函数执行期间后续批次照常组批，成批后在执行入口排队等空闲槽位（排队中的批尚未 Flush，不计入 n）；interval 仍自各批组批起点起算，成批先于槽位空出时槽位空出即执行。n>1 时多个执行 goroutine 可并发回调 Observer / per-item 回调，实现必须并发安全、跨批事件顺序不保证。 |
| 负载卸载 | 配置 `WithMaxPending(n)` 后，已准入未交付的提交数达 n 即拒绝：新 `Do` 立即返回 `ErrOverloaded`，不入队。已准入提交数恒不超过 n；并发竞争下额度尚空的提交仅可能被假阳性提前拒绝（其他提交的拒绝尚未回滚、计数瞬时虚高），被拒调用方可退避重试。Formation 与在途中的 Item 同样占用额度：批次大小恒不超过 n，n < MaxSize 时批大小触发不可达。未配置即无界。 |
| 批内去重 | `NewKeyed(key, fn)` 在交付边界折叠同 key Item：批次函数收到去重切片（首次出现序，重复 key 只首达 Req 送达下游），`len(res) == len(unique)`，同 key 的 Waiter 共享同一条执行、各自取得一份结果。「key 相同 ⇒ 语义等价」由 key 函数的提供方声明。maxSize 按原始条数计数，重复照常占槽触发 Flush；跨批去重不做，执行期到达的重复 key 落入下一批重查；key 函数 panic 降级为批级错误、库继续可用。 |
| 批级错误 | 批次函数返回非 nil 错误、panic、或结果长度与请求不符，整批所有 Waiter 收到同一错误。单条业务失败见「逐条失败」。 |
| 批次重试 | `WithRetry(n, backoff)`：批次函数返回 `errors.Is(err, ErrRetryable)` 的错误（如 `fmt.Errorf("503: %w", ErrRetryable)`）时整批按原请求重执行至多 n 次（keyed 批含折叠），其他错误照常广播；未配置时哨兵也不启用重试。重试与退避在执行 goroutine 持槽进行、计入 `DurationSeconds` 与 `wait_seconds`，事件 `Retries` 字段与 `batcher_batch_retries_total` 可观测。退避由你实现（attempt 从 1 起），panic 或负数返回即该批失败。交互：MaxConcurrency=1 时退避把队头阻塞放大为全库停摆，建议搭配 `WithMaxConcurrency ≥ 2`；与 MaxPending 叠加时退避窗口内新提交可能集中被卸载。 |
| 逐条失败 | `Result[Res]{Value, Err}` 在位置对位 API 下逐条表达业务失败（结果切片仍等长），库不解读 Result：批级失败看 `Do` 的 error，逐条失败看 `Result.Err`。`NewKeyedMap(key, fn)` 在 NewKeyed 折叠之上回查 `map[K]Res`：缺 key 转 `Result{Err: ErrMissingResult}`（单条缺失，非整批失败），多余 key 忽略；key 函数在折叠与回查各调一次，须为纯函数。 |
| fire-and-forget | `Enqueue(ctx, req)` 提交即返回（nil = 已入队）：Item 与 `Do` 来源一视同仁地组批、执行、计入观测与准入额度，结果无人读取。ctx 只治理本次提交——取消或 Close 即返回 `ctx.Err()` / `ErrClosed` 且不入队；握手完成后再无取消语义。`EnqueueFunc` 附加 per-item 回调：交付点同步执行（先于该批 `OnBatchFlushed`）、必须快速返回、panic 由库 recover；批级失败回调收到同一 Batch Error，cb 为 nil 等价 `Enqueue`。回调内不得同步再入本 Batcher（再入提交需等槽位而槽位正被本回调所在批占用，下游并发 1 下自死锁）。 |
| 调用方取消 | `Do` 的 ctx 取消即返回 `ctx.Err()`：入队握手前取消则 Item 不入队，已入队后取消仅解除等待，Item 保留在批中照常执行、结果丢弃。批次函数运行于 batcher 级基础 ctx，不携带任何单个调用方的 deadline。 |
| Close | 先到达的 Do 立即返回 `ErrClosed`；`Close(ctx)` flush 未满批并等待在途批次完成后才返回，ctx 超时仅放弃等待（后台 flush 照常完成），返回 `ctx.Err()`。 |

## 配置选项

| 选项 | 说明 |
| --- | --- |
| `WithMaxSize(n)` | **必填**，批大小上限，必须 > 0。 |
| `WithInterval(d)` | **必填**，组批时间间隔，必须 > 0。 |
| `WithMaxPending(n)` | 已准入未交付提交数上限（负载卸载），达上限立即返回 `ErrOverloaded`；已准入提交数恒不超过 n，并发竞争下仅可能被假阳性提前拒绝；n < MaxSize 时批大小触发不可达。0 等价未设置（无界）。 |
| `WithMaxConcurrency(n)` | 下游并发上限：同一时刻批次函数在途的批次 ≤ n，组批与执行重叠、成批后排队等空闲槽位。计数单位是批次而非条目；未设置或 0 保持串行单在途（1），负值构造期 panic。 |
| `WithMaxBytes(n, size)` | 批字节上限（Flush 触发条件之三，与条数/时间并列先到先触发），`size func(Req) int` 由你声明条目字节量；按原始条目累计（keyed 批的重复照常计费），单条超限自成孤批。n 必须 > 0，size 的 Req 须与 Batcher 一致，失配构造期 panic；size panic 或返回负数 → 该批整体失败（不调批次函数）。 |
| `WithRetry(n, backoff)` | 批次级重试：仅 `ErrRetryable` 哨兵错误按原批次重执行至多 n 次（含退避，持槽进行）；其他错误照常广播。backoff panic 或负数返回 → 该批失败；n 为负、n > 0 缺 backoff、n = 0 携带 backoff → 构造期 panic（n = 0 即默认不重试）。 |
| `WithObserver(o)` | 注入 `Observer` 旁路观测，默认 `NoopObserver`。 |
| `WithMeter(m)` | 注入 `observ.Meter` 指标，默认 Noop。 |
| `WithLogger(l)` | 注入 `observ.Logger` 日志，未注入时构造期快照默认 Logger。 |

`fn` 或 key 函数为 nil、必填选项缺失或非正、`MaxPending` 为负、`MaxConcurrency` 为负、`MaxRetry` 非法（为负、>0 缺其 backoff 或 =0 携带 backoff）、`MaxBytes` 为负或其 size 函数缺失/与 Req 失配属程序员工错，构造期 panic（`New` / `NewKeyed` / `NewKeyedMap`）。

## 可观测性

**Observer**（类型化旁路事件，回调同步执行、必须快速返回：`OnBatchFlushed` 在执行 goroutine，`OnClosed` 在全部在途批次完成后的收尾段；回调可能并发——`WithMaxConcurrency(n>1)` 的多个执行 goroutine，或多个 Batcher（如路由层分片）共用同一 Observer——实现必须并发安全、跨批事件顺序不保证；回调 panic 会被库 recover；回调内不得同步再入本 Batcher）：

```go
type Observer interface {
	OnCreated(CreatedEvent)        // 构造时触发一次
	OnBatchFlushed(BatchFlushedEvent) // 每批一次：Size / UniqueSize / WaitSeconds / DurationSeconds / Retries / Err
	OnClosed(ClosedEvent)          // 全部在途批次完成后触发一次
}
```

**指标**（注入 `observ.Meter` 后启用，命名遵循 `_total` / `_seconds` / `_depth` 约定；Meter 实现不得 panic（库不做 recover）且必须并发安全——`WithMaxConcurrency(n>1)` 或路由层分片下埋点从多个 goroutine 并发发生）：

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `batcher_batch_total` | Counter | 已 flush 的批次数 |
| `batcher_batch_failed_total` | Counter | 以 Batch Error 结束的批次数 |
| `batcher_wait_seconds` | Histogram | 单条 Item 从入队到结果交付的端到端等待 |
| `batcher_batch_size` | Histogram | 每批 Item 数量 |
| `batcher_batch_duration_seconds` | Histogram | 批次函数执行耗时 |
| `batcher_queue_depth` | Gauge | 已准入未交付的提交数，含停泊在握手的发送方（批边界刷新） |
| `batcher_overloaded_total` | Counter | 被 `WithMaxPending` 卸载拒绝的提交数 |
| `batcher_batch_retries_total` | Counter | `WithRetry` 触发的重试执行次数（不含首次执行） |

## 设计文档

术语表见 [CONTEXT.md](CONTEXT.md)，设计决策见 [docs/adr](docs/adr)：

- [0001 同步 Do、按位置对位与批级错误](docs/adr/0001-sync-do-positional-api.md)
- [0002 串行单在途批次，计时器自组批起算](docs/adr/0002-serial-single-inflight.md)
- [0003 调用方取消仅解除自身等待，Item 保留照常执行](docs/adr/0003-cancel-retains-item.md)
- [0004 批内 key 去重：独立构造器 NewKeyed，折叠在交付边界](docs/adr/0004-keyed-in-batch-dedup.md)
- [0005 逐条结果表示：Result 与 keyed-map 变体](docs/adr/0005-per-item-result.md)
- [0006 Fire-and-forget 入队：Enqueue 打开写路径](docs/adr/0006-fire-and-forget-enqueue.md)
- [0007 按字节攒批：WithMaxBytes 三决策](docs/adr/0007-max-bytes-batching.md)
- [0008 下游并发上限：WithMaxConcurrency 与组批/执行流水线](docs/adr/0008-max-concurrency.md)
- [0009 分片 Batcher：独立包 sharded，工厂按分片构造](docs/adr/0009-sharded-batcher.md)
- [0010 批次重试策略：WithRetry 与 ErrRetryable 哨兵](docs/adr/0010-batch-retry.md)

`New` 不做批内去重；`NewKeyed` 提供批内 key 去重，「key 相同 ⇒ 语义等价」由 key 函数声明（ADR 0004）；`NewKeyedMap` 在其上提供 map 回查与逐条缺失表示（ADR 0005）；`Enqueue`/`EnqueueFunc` 提供 fire-and-forget 入队（ADR 0006）；`WithMaxConcurrency` 放宽下游并发上限并流水线化组批与执行（ADR 0008）；路由层在独立子包 [sharded](sharded)——`sharded.New` 把请求分发给 n 个独立分片 Batcher（ADR 0009）；`WithRetry` 以 `ErrRetryable` 哨兵启用批次级重试（ADR 0010）。
