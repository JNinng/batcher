# 下游并发上限：WithMaxConcurrency 与组批/执行流水线

ADR-0002 预留的口子（「真有吞吐瓶颈时再加 MaxConcurrency 选项」）就此落定，命名裁定 `WithMaxConcurrency(n)`——原拟 `WithInFlight` 两处含糊：缺 Max（n 是上限却读作开关）、缺名词（in-flight 什么？），计数单位歧义有误调风险（n 批 ≠ n 条，差一个批大小数量级）；MaxConcurrency 对齐 ADR-0002 原文。动机：串行单在途的队头阻塞——慢批次执行期间（如 500ms）新提交全部排队，即便下游实际支持并行批量调用。

## 前置 Benchmark 数据

扇入宽度（闭环连续 `Do` 的 client goroutine 数）× 下游单批耗时（批次函数 sleep 模拟 I/O）× 并发上限交叉（`BenchmarkMaxConcurrency`，Windows/amd64；sleep 粒度使标称 100µs 的实效延迟约 520µs，绝对值膨胀但比值与理论一致，取相对信号）：

| 形态 | 组合 | 串行（conc=1） | conc=4 | 增益 |
| --- | --- | --- | --- | --- |
| maxSize=1（最恶劣扇入，攒批不可用） | 扇入 1/16/128 并发客户端 | 恒 ≈1.9k items/s，与扇入宽度无关 | ≈7.5k items/s | ≈3.96× |
| 同上，实效延迟 1.3ms | 扇入 16/128 | 恒 ≈755 items/s | ≈2.7k items/s | ≈3.6× |
| maxSize=64、扇入 128（size 触发） | 实效延迟 ~550µs | ≈115k items/s ≈ batchSize/latency | ≈217k items/s | ≈1.9×（组批/派发开销稀释） |
| maxSize=64、扇入 16（interval 触发，Formation 受限） | 实效延迟 ~550µs | ≈6.5k items/s | ≈6.4k items/s | 无收益 |

结论：串行单在途的执行容量为 batchSize/latency，提交速率超过它即成瓶颈（吞吐钉死、与扇入宽度无关，积压随扇入线性放大等待）；`WithMaxConcurrency(n)` 把执行容量抬到 n 倍；Formation 受限（interval 触发、扇入不足以撑满批次）的流不受益——瓶颈在组批侧而非执行侧。

## 决策

一、**形式取 Option**：下游并发是交付层治理旋钮，不触及批次函数契约与批次形状——按演化原则正常叠加，无须独立构造器。二、**语义**：同一时刻批次函数在途（已 Flush 未返回）的批次 ≤ n，即下游并发 ≤ n；成批（Flush 触发）与执行开始解耦，在途已满时成批批次在执行入口排队——排队中的批尚未 Flush，不计入 n，上限因此精确等于下游并发硬界。三、**实现为统一流水线路径**：dispatcher 只组批，`launch` 先占在途槽位（计数信号量，容量 n）再派生执行 goroutine，交付、回调与观测整体迁到执行 goroutine；collect 与 execute 重叠对默认 n=1 同样成立，不设串行双轨（两条代码路径两套时序语义是持续的分裂税）。槽位占满时 dispatcher 停在 launch 处，组批随之暂停，新提交停泊在 inbox 握手上（准入口径照旧计入）。四、**默认 1**：未设置或 0 保持串行单在途，ADR-0002 的行为作为默认保留；负值构造期 panic（程序员工错惯例）。

计时器语义随之修订：ADR-0002「下一批计时器自 Formation 起算」保留，「Formation 最早始于上一批执行完毕」的隐含前提消失——Formation 与执行重叠，下一批计时器自其 Formation（第一条 Item 进入空批，哪怕上一批仍在执行）起算。「按到达时刻补偿会产生碎片小批」的论证结论不变且被强化：执行期到达的 Item 不再停泊、而是填入正在 Formation 的下一批，合并更充分；计时器仍不由到达时刻驱动，执行开始时刻 = max(成批时刻, 槽位空出)，由 `TestNextBatchStartsWhenFlushedAndSlotBothReady` 两方向钉死。

交付侧语义原样随 worker 迁移：交付序、per-item 回调先于该批 `OnBatchFlushed`、准入额度逐条递减、批边界 gauge 刷新都仍在 execute 内完成，单批内部的顺序与口径零改动。`WaitSeconds`（组批耗时）在成批处结算后随批移交——口径为首条 Item 入批 → Flush 触发，**不含槽位排队时间**，与 ADR-0002/0007 的「组批耗时」声明一致（若在执行入口结算，排队批的组批耗时会被等待时间污染，静默破坏口径）；槽位排队计入的是端到端 `batcher_wait_seconds` 直方图与执行开始延迟。跨批层面两点契约更新：n>1 时多个执行 goroutine 可并发回调，Observer 实现必须并发安全、跨批事件顺序不保证（EnqueueFunc 的 per-item 回调天然独立，无此要求）；「回调不得同步再入本 Batcher」规则保留、机制描述更新——再入提交需等槽位而槽位正被本回调所在批占用，MaxConcurrency=1 下自死锁、>1 下阻塞所在批的剩余交付。Meter 实现的并发安全要求同步写明（同 Observer 契约，n>1 下埋点多 goroutine 并发发生）。

Close 语义保持：`flushRemainder`（drain 并入末批、仅有数据才 flush）移交执行后等待全部在途 goroutine 归零——正常返回即全部 Waiter 已被唤醒、批次函数不再运行；baseCtx 在全部 worker 退出后才取消，ADR-0003 的 ctx 生命周期契约不变。queue_depth 仍为批边界刷新的近似值，n>1 时并发交付下采样可能瞬时乱序（陈旧值短暂回弹），下一批边界自校正。

## Consequences

- goroutine 形态：每批一个执行 goroutine（先占槽后派生），峰值 = n 在途 + 1 dispatcher；不引入常驻 worker 池——批次到达天然成簇，常驻池在空闲期是纯开销。
- 钉子改写：`TestSlowBatchDoesNotCompensateTimerByArrival` 的串行前提消失，改写为 `TestNextBatchStartsWhenFlushedAndSlotBothReady`；停泊窗口从「执行期」收窄为「在途槽位占满期」，`TestCancelledWhileParkedReleasesAdmissionQuota` / `TestEnqueueCancelledWhileParkedNeverReachesFn` 重构为「成批待槽占额度 + 停泊占额度」两层结构。新增 `TestMaxConcurrencyCapsDownstreamConcurrency`（重叠执行且恒 ≤ n）与 `TestMaxConcurrencyCloseWaitsAllInflightWorkers`（Close 等全部在途 worker）。
- 与 `WithMaxPending` 组合口径不变：在途批、成批待槽、停泊发送方都占准入额度，批次大小恒 ≤ maxPending；卸载判定不感知并发配置。
- 与 `WithMaxBytes` 正交（ADR-0007 预留兑现）：字节/条数/时间累计都发生在 Formation，与执行侧并发无关。
- 调参依据：执行侧瓶颈（提交速率 > batchSize/latency）才受益于调高 n；Formation 受限的流应调 interval/maxSize 而非 n。
- 批次重试（`WithRetry`，[0010](0010-batch-retry.md)）：backoff 原地 sleep 在 n=1 下把队头阻塞放大为全库停摆，Retry 建议搭配 MaxConcurrency ≥ 2——该交互已在 0010 文档化并钉死。
