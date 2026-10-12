# 观测旁路扩展：FlushStart、Trigger、Panic 与 ItemDiscarded

观测旁路自 v1 起仅有 OnCreated / OnBatchFlushed / OnClosed 三个事件，批次生命周期的「排队 → 执行」分界、Flush 触发条件、批次函数 panic 现场与取消后的无效下游开销均不可见。本决定一次补齐四项，全部维持旁路硬约束：回调在执行 goroutine 关键路径同步执行、必须微秒级返回、panic 由库 recover——每加一个事件都过这道关。

## 决策

一、**OnFlushStart 与批序号 Seq**：每批进入执行（已占在途槽位、批次函数调用前）触发一次，与 OnBatchFlushed 以 `Seq` 恒成对——组批期预置失败（不调批次函数）的批同样成对，trace span 不悬空。Seq 在成批处领发（Batcher 内自 1 单调递增，收尾 drain 批在 flushAndAwait 补领、正常路径不重复领取；跨 Batcher 不唯一，分片按 shard 分别配对）。n>1 下多执行 goroutine 交错回调、跨批事件顺序不保证，无序号则 start/flushed 无法配对——序号是配对的唯一可靠手段。

二、**Trigger 报告组批结束条件**：BatchFlushedEvent 新增 `Trigger`（`size` / `bytes` / `interval` / `close`）。close 一律记 Close 路径——含组批中途被 Close 截断的批（无论组批进度，交付一律因 Close）；sizeFn 失败预置错误的批记 bytes（失败发生在字节评估路径，具体错误看 Err）。早期候选文案只列 size / interval / close-drain 三值，落地补 bytes——该文案早于 WithMaxBytes 落地。

三、**Panic / Stack 字段而非独立事件**：BatchFlushedEvent 新增 `Panic any` 与 `Stack []byte`，仅批次函数 panic 降级的批非 nil——recover 原值加 recover 点调用栈（`debug.Stack`），重试下取终态那次尝试的现场。不加 OnBatchPanic 事件：panic 批本就有 flushed 事件，独立事件徒增一次分发与配对负担。key / size / backoff 函数的 panic 不捕获现场——它们是程序员工错，Err 中的错误信息（"key function panicked" 等）足以定位；堆栈诊断价值集中在下游批次函数的偶发崩溃。

四、**OnItemDiscarded 近似信号**：每条「交付时 caller ctx 已取消」的 Do 来源 Item 在交付点触发一次（先于该批 OnBatchFlushed），并配 `batcher_discarded_items_total`。两个契约口径：其一，写「ctx 已取消」不写「结果确定无人读取」——waiter 的 select 在 resCh/ctx.Done 双就绪时随机选择，个别被判丢弃的结果实际被读走；其二，排除 fire-and-forget 来源——Enqueue/EnqueueFunc 的结果本就旁路消费，无差别定义会全量误报。实现为 item 携带 caller ctx 与 waited 来源标记（Do 与 Enqueue 的 cb 同为 nil，来源只能由提交路径显式标记），交付点旁查 ctx.Err()，不改交付语义。

## Consequences

- Observer 接口新增两个方法，破坏既有实现——v0 阶段接受，Noop 与示例同步更新；事件结构体只增字段，向后兼容。
- 每批固定多一个 safeEmit 分发点（flush-start；discarded 仅取消时发生），Noop 整体跳过、回调 panic recover，旁路约束不变。
- Seq 跨 Batcher 不唯一：分片场景的事件配对按 Batcher（shard）分别进行。
- 泛型 per-item 事件（带 Req/Res 明细的请求失败 hook）依旧不列：同步 Do 下调用方已知成败，泛型化 Observer 会传染整个旁路面——等真实需求再议。
