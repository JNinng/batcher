package batcher

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// Option 定制 Batcher 构造参数；MaxSize 与 Interval 必填。
// config 除 sizeFn 以 any 承载泛型函数（WithMaxBytes 专用，newBatcher
// 断言回具体类型）外不含泛型字段，故 Option 无需类型参数、可直接推断。
type Option func(*config)

type config struct {
	maxSize        int
	interval       time.Duration
	maxPending     int
	maxBytes       int
	sizeFn         any // func(Req) int；newBatcher 断言回具体类型
	maxConcurrency int
	maxRetries     int
	backoffFn      func(attempt int) time.Duration // 第 attempt 次重试前的退避时长
	observer       Observer
	meter          metric.Meter
	logger         *slog.Logger
}

// WithMaxSize 设置批大小上限（Flush 触发条件之一），必须 > 0。
func WithMaxSize(n int) Option {
	return func(c *config) { c.maxSize = n }
}

// WithInterval 设置组批时间间隔（Flush 触发条件之一，自组批起算的
// 尽力而为软上限），必须 > 0。
func WithInterval(d time.Duration) Option {
	return func(c *config) { c.interval = d }
}

// WithMaxPending 设置已准入未交付提交数上限，实施负载卸载：达上限后
// 新提交立即返回 ErrOverloaded，不入队、不堆积 goroutine。准入采用
// Add-then-check（原子加一后比较自身返回值），已准入未交付提交数恒
// 不超过 n；上限的软性仅在于并发竞争下的计数瞬时虚高（其他提交的
// 拒绝尚未回滚），额度尚空的提交可能被假阳性提前拒绝。Formation 与
// 在途中的 Item 同样占用额度：批次大小恒不超过 n，n < WithMaxSize 时
// size 触发不可达，Flush 全部退化为 interval 触发。未设置或 0 即无界，
// 负值属程序员工错（New 构造期 panic）。
func WithMaxPending(n int) Option {
	return func(c *config) { c.maxPending = n }
}

// WithMaxConcurrency 设置下游并发上限 n：同一时刻批次函数在途（已
// Flush 未返回）的批次 ≤ n，即下游至多 n 个批量调用并行。计数单位是
// 批次而非条目——n 批 ≠ n 条，差一个批大小数量级。未设置或 0 保持
// 串行单在途（n=1）。放宽后组批（Formation）与执行重叠：批次函数
// 执行期间后续批次照常组批，成批后若在途已满则在执行入口排队等空闲
// 槽位——排队中的批尚未 Flush，不计入 n；interval 仍自各批 Formation
// 起算，成批先于槽位空出时槽位空出即执行。并发不改变批次形状与
// 准入/观测口径；负值属程序员工错（New 构造期 panic）。
func WithMaxConcurrency(n int) Option {
	return func(c *config) { c.maxConcurrency = n }
}

// WithRetry 配置批次级重试：批次函数返回 errors.Is(err, ErrRetryable)
// 的批级错误时，整批按原 reqs 重执行（keyed 批含折叠，key 函数须为
// 纯函数），至多 n 次；其他错误照常广播——不重试的默认语义不变
// （未配置本选项时即便错误携带 ErrRetryable 也只执行一次）。
// backoff 在每次重试前以重试序号调用（首个退避为 1）并原地 sleep；
// 其 panic 与负数返回降级为批级错误（与 size 函数同款约束）。重试
// 与退避在执行 goroutine 持槽进行、计入 DurationSeconds 与
// wait_seconds（端到端等待被拉高属预期）。交互注意：MaxConcurrency=1
// 时退避把队头阻塞放大为全库停摆，建议搭配 WithMaxConcurrency ≥ 2；
// 与 WithMaxPending 叠加时退避窗口内新提交可能集中被卸载
// （ErrOverloaded）。n 必须 ≥ 0（0 即默认不重试，此时 backoff 须为
// nil）——负值、n > 0 缺失 backoff 或 n = 0 携带 backoff 属程序员
// 工错，New 构造期 panic（与 WithMaxBytes 的 n/size 配对校验同款）。
func WithRetry(n int, backoff func(attempt int) time.Duration) Option {
	return func(c *config) {
		c.maxRetries = n
		c.backoffFn = backoff
	}
}

// WithMaxBytes 设置批字节上限与条目字节量函数（Flush 触发条件之三，
// 与条数上限、时间间隔并列先到先触发）：Formation 内已累计字节达到
// n 即触发 Flush。字节按原始条目累计——keyed 批的折叠发生在交付
// 边界，重复 Item 照常占槽计费，否则单一热点条目永远凑不满 n、每批
// 白等满 interval；实际调用批次函数的是去重后切片，载荷不大于计费
// 值。单条即超限自成孤批（批大小 1）：不拆分、不丢弃。n 必须 > 0，
// size 不得为 nil，且 size 的参数类型须与 Batcher 的 Req 一致——
// Option 不携带类型参数，以上失配均在 New 构造期 panic。size 返回
// 负数属程序错误，该批以批级错误失败（负计费会让字节触发静默失效）。
func WithMaxBytes[Req any](n int, size func(Req) int) Option {
	return func(c *config) {
		c.maxBytes = n
		if size != nil {
			c.sizeFn = size
		}
	}
}

// WithObserver 注入类型化旁路观测接口，默认 Noop。
func WithObserver(o Observer) Option {
	return func(c *config) { c.observer = o }
}

// WithMeter 注入 OTel 指标 Meter；未注入即不埋点（指标句柄集为 nil，
// 提交与执行路径整体跳过打点）。仪表自构造时刻绑定 m 所属 provider
// ——构造后替换全局 MeterProvider 不追溯已建 Batcher。
func WithMeter(m metric.Meter) Option {
	return func(c *config) { c.meter = m }
}

// WithLogger 注入 *slog.Logger；未注入时构造期固定为 discard logger
// （默认静默，零输出——与未注入 Meter 不埋点同款缺省口径）。
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}
