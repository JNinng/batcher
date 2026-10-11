# 批次重试策略：WithRetry 与 ErrRetryable 哨兵

下游瞬时故障（如 503）会让整批 N 个请求陪葬且默认不重试——重试是调用方各自的痛点。落地为 Option `WithRetry(n, backoff)`：批次函数返回 `errors.Is(err, ErrRetryable)` 的批级错误时，整批按原 reqs 重执行至多 n 次；其他错误照常广播，不重试的默认语义不变（ADR-0001 的「不重试」在默认路径上完整保留）。

## 决策

一、**Option 形式**：重试是交付层治理旋钮，对批次函数契约与批次形状零改动（调用方只见最终结果），按演化原则正常叠加。

二、**哨兵显式选择重试**：库不猜测哪些错误值得重试——`ErrRetryable` 由批次函数以 `fmt.Errorf("...: %w", ErrRetryable)` 包装携带，`errors.Is` 判定；瞬时故障是下游语义，只有批次函数知道。未配置 WithRetry 时即便错误携带哨兵也只执行一次——哨兵本身不启用重试。

三、**重试在执行 goroutine 持槽进行**：整批重放（keyed 批含折叠，key 函数纯函数契约已覆盖重复调用）发生在 `execute` 内，重试与退避全程占用在途槽位——「已 Flush 未返回」口径连续，`WithMaxConcurrency` 的下游并发硬界不因重试而失真；`DurationSeconds` 与 `wait_seconds` 覆盖含退避的全过程（端到端等待被拉高属预期，观察者可从事件 `Retries` 字段感知）。

四、**backoff 显式函数**：`backoff(attempt) time.Duration`，attempt 从 1 起，由用户实现退避策略（线性/指数/抖动）；其 panic 与负数返回降级为批级错误（与 size 函数同款约束：运行于执行 goroutine、无人 recover 会打穿 goroutine，负值是程序错误须显式暴露），退避失败即批终止、不再重试。

五、**观测口径**：事件 `BatchFlushedEvent` 新增 `Retries int`（该批首次执行后的重试次数）；指标新增 `batcher_batch_retries_total`（重试执行次数，不含首次）；`batch_total` / `batch_failed_total` 仍按终态每批一次——重试不虚增批次数与失败数。

## Consequences

- 交互警示（文档化而非强制，保持 Option 正交）：MaxConcurrency=1 时退避在唯一槽位上原地 sleep，把队头阻塞放大为全库停摆——建议 Retry 搭配 `WithMaxConcurrency ≥ 2`；与 MaxPending 叠加时退避窗口内新提交可能集中被卸载（ErrOverloaded），钉死为 `TestRetryBackoffHoldsSlotAndShedsDuringWindow`。
- 组批期失败（sizeFn / keyFn panic 预置错误）不重试——哨兵只能由批次函数携带，组批错误一律批级失败。
- 退避不可被 Close 打断（`time.Sleep` 原地等待）：长退避会等比例推迟 Close 返回，与「Close 等待慢批次」语义一致，退避时长由用户掌控。
- 耗尽后广播最后一次错误，`batch_failed_total` 终态计一次；`Retries` 字段让观察者区分「一次慢失败」与「多次重试后失败」。计数只记实际执行的重试：退避失败（panic/负数）的尝试未执行批次函数，不计入 `Retries` 与 `batcher_batch_retries_total`。
- 构造期校验与 `WithMaxBytes` 的 n/size 配对同款：n 为负、n > 0 缺 backoff、n = 0 携带 backoff 均 panic——backoff 与 n 须成对出现，不合法组合不静默降级。
- 后续可选演化（不预支）：退避期间释放槽位（重试批次让位）、按错误分类的差异化重试次数——等真实需求。
