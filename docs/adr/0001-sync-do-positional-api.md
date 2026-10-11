# 同步 Do、按位置对位与批级错误

> 状态：部分修订。批内 key 去重与 fire-and-forget 入队分别由 [0004](0004-keyed-in-batch-dedup.md)、[0006](0006-fire-and-forget-enqueue.md) 翻转重纳，逐条结果表示由 [0005](0005-per-item-result.md) 扩充为标准形态；其余决定继续有效。

v1 API 只提供同步的 `Do(ctx, req) (res, error)`，每个调用方阻塞等待自己那份结果，不做异步 Submit/回调变体——异步形态组合性差且易泄漏 goroutine，同步 Do 让背压自然落在调用方。批次函数签名为 `func(ctx, []Req) ([]Res, error)`，结果按位置对位，长度不符视为整批失败；非 nil 错误（含 panic 经 recover 转化的错误）作为 Batch Error 广播给该批全部 Waiter，不重试，单条业务失败由用户在 Res 中自行表达。按 key 去重（singleflight 语义）明确不在范围内，等价性判断属业务知识。

## Consequences

- 库不猜测下游批量接口的部分失败语义，只守"长度契约 + 广播"这一条。
- 需要 fire-and-forget 的用户需自行包装，库不为它增加第二条提交路径。
