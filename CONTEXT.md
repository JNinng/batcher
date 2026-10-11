# Batcher

将零散的单次请求扇入为批量操作，并把结果按位置归还各调用方的库。

## Language

**Item**:
一次进入批处理器的单条请求，由一个调用方同步等待其结果。
_Avoid_: request（与下游批量接口的请求混淆）、task、entry

**Batch**:
一次交付给批次函数执行的 Item 集合，结果按位置与 Item 对位。
_Avoid_: group、chunk

**Batch Function**:
用户提供的批量执行函数，接收整个 Batch（恒非空），返回等长的结果切片；返回非 nil 错误即整批失败。
_Avoid_: handler、executor

**Formation**:
从第一条 Item 进入空批到 Flush 触发的组批阶段；时间间隔在 Formation 起点开始计时，是尽力而为的软上限。
_Avoid_: collection window、timer window

**Flush**:
将组批中的 Batch 交付给批次函数执行的动作；触发条件为批大小上限、批字节上限或时间间隔，先到先触发。仅有数据才实际触发：空批不计时、不唤醒，绝不产生对批次函数的空调用。
_Avoid_: dispatch、fire、commit

**Waiter**:
调用 Do 并阻塞等待自己那份结果返回的调用方。
_Avoid_: caller（过于泛化）、consumer

**Batch Error**:
批次函数返回的非 nil 错误，或其 panic 经 recover 转化的错误；广播给该批全部 Waiter。单条 Item 的业务失败由用户在结果类型中自行表达，与 Batch Error 无关。
_Avoid_: partial error

**Retry**:
整批按原 reqs 重执行的动作（`WithRetry(n, backoff)` 启用）：仅批次函数以哨兵 `ErrRetryable` 标记的 Batch Error 触发，至多 n 次，其他错误照常广播；重试与退避在执行 goroutine 持槽进行，keyed 批含折叠（key 函数须为纯函数）。
_Avoid_: requeue、redrive、resubmit

**In-flight Batch**:
已 Flush、批次函数尚未返回的 Batch；同一时刻至多 `WithMaxConcurrency(n)` 个（未配置即 1，串行单在途），即下游并发上限。成批后等待空闲槽位的批次尚未 Flush，不计入。

**Admission**:
提交进入 batcher 的守门动作：入口计数加一计入「已准入未交付」，超过 `WithMaxPending` 上限即拒绝并返回 `ErrOverloaded`（负载卸载），交付时释放额度。已准入提交数恒不超过上限；并发竞争下额度尚空的提交仅可能被假阳性提前拒绝（其他提交的拒绝尚未回滚、计数瞬时虚高）。
_Avoid_: rate limit（不限速率，只限同时在库提交数）、throttle、backpressure

**Fold**:
交付边界把批内同 key 的多条 Item 合并为一条执行的动作（`NewKeyed` 启用）：批次函数收到保持首次出现序的去重切片，重复 key 只首达 Req 送达下游，结果回填至全部原始位置。重复 Item 照常占槽触发 Flush，maxSize 按原始条数计数。
_Avoid_: merge、collapse、singleflight（实现手法名）

**Result**:
逐条业务失败的通用表示 `{Value, Err}`：批级失败走 `Do` 返回的 error，逐条失败由 `Result.Err` 承载，结果切片与请求恒等长。库不解读 Result，批次仍整批成败；`NewKeyedMap` 下缺 key 即 `Result{Err: ErrMissingResult}`。
_Avoid_: per-item error、partial error

**Enqueue**:
fire-and-forget 提交路径：不等待结果，ctx 只治理本次提交（准入与入队握手）；Item 与 Do 来源一视同仁地组批执行，结果无人读取（`EnqueueFunc` 可在交付点旁路回调）。
_Avoid_: submit、post、async do、send

**Shard**:
路由层 `sharded` 中独立组批、独立执行的分片 Batcher：按 Route 分发请求，各片独立计准入与并发上限、各发 Observer 事件；`Sharded.Close` 级联关闭全部分片。路由层与组批层分属不同包——组批层构造器返回同一个 `*Batcher`，路由层返回包装类型 `*Sharded`。
_Avoid_: partition、bucket、worker

**Route**:
路由函数：对每条请求直接产出分片下标 `[0, n)` 的函数，须对同一请求稳定；不做 hashFn → K comparable 中间形态，二次哈希留给 route 内做。与 key 函数组合时须满足「同 key ⇒ 同分片」，否则批内去重跨片失效，属用户责任。
_Avoid_: hash、shardFn
