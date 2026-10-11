# Fire-and-forget 入队：Enqueue 打开写路径

ADR-0001 曾明确「需要 fire-and-forget 的用户需自行包装，库不为它增加第二条提交路径」；本决定翻转为新增 `Enqueue(ctx, req) error` 及带 per-item 回调的 `EnqueueFunc`。动机是写路径场景（日志/埋点/消息队列 producer）：这些流量本就无人读取单条结果，为等结果付出的同步等待纯属浪费，而它们恰恰最需要组批摊薄下游开销。Enqueue 与 Do 共用同一条提交路径（submit：准入 → 构造 Item → 入队握手），admit 计数口径因此结构性涵盖全部提交——queue_depth、overloaded_total 与 `WithMaxPending` 卸载对 Enqueue 一视同仁。ctx 只治理本次提交：取消或 Close 即返回 `ctx.Err()` / `ErrClosed` 且不入队；握手完成即返回 nil，此后不再有取消语义——Item 与 Do 来源一视同仁地组批、执行（ADR-0003 同款：已入队不摘除），结果仍经缓冲交付通道投递后丢弃。`EnqueueFunc` 的回调在交付点同步执行，收到与 Do 相同口径的 (值, 错误)——批级失败即 Batch Error、逐条失败随 Res 自行表达（ADR-0005 届时预留的「逐条口径由 per-item 回调定义」就此落定）；必须快速返回、panic 由库 recover（与 Observer 同款旁路约束），先于该批 `OnBatchFlushed`。

## Consequences

- 写路径与读路径共享同一 Batcher、同一套组批与观测口径；`wait_seconds` 对 Enqueue 条目同样计费，含义为「入队 → 下游写出」的批量延迟。
- 回调无 ctx 参数：交付点的 caller ctx 已无意义（提交早已返回），需要取消语义的写出应在批次函数内取用 batcher 级基础 ctx。
- 回调（与 Observer 同款）不得同步再入本 Batcher：回调运行于 dispatcher goroutine，再入的 Do/Enqueue 会停泊在握手上门等同一个正在执行回调的 dispatcher，自死锁；需再入时先转到自有 goroutine 或队列再提交。
- Enqueue 条目仍分配缓冲交付通道（统一交付路径的代价，结果投递后随 Item 一起回收）；若 P2 Benchmark 显示高吞吐写路径下该分配显著，可改为 nil resCh + 交付侧分支的局部优化，不改契约。
- 回调在交付循环内逐条执行，慢回调会拉长整批交付——契约写明微秒级返回，不做超时治理（与 Observer 同策）。
- 未来 OnItemDiscarded hook 须排除 Enqueue 来源 Item——fire-and-forget 本就无人读取，无差别定义会全量误报（届时 item 需携带来源标记）。
- 按字节攒批（`WithMaxBytes`，P1 待定决策）落地后与 Enqueue 组合覆盖高吞吐写出场景。
