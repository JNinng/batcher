package batcher

import "go.opentelemetry.io/otel/metric"

// metrics 汇集全部指标句柄；命名遵循 OTel/Prometheus 约定
// （snake_case、计数器带 _total、耗时带 _seconds、状态量带 _depth）。
// 核心包只有无 label 指标；带 label 聚合由用户侧 adapter 完成。
// Meter 实现不得 panic（库不 recover Meter 回调，与 Observer 契约不同）
// 且必须并发安全：WithMaxConcurrency(n>1) 或路由层分片下埋点从多个
// goroutine 并发发生（提交方 goroutine 上另有 admit/拒绝计数）。
type metrics struct {
	batchTotal      metric.Int64Counter
	batchFailed     metric.Int64Counter
	waitSeconds     metric.Float64Histogram // 每条 Item 一次：入队 → 结果交付
	batchSize       metric.Float64Histogram // 每批一次：批大小
	durationSeconds metric.Float64Histogram // 每批一次：批次函数执行耗时
	queueDepth      metric.Int64Gauge       // 已准入未交付的提交数，含停泊在握手的发送方（批边界刷新；并发交付下采样可能瞬时乱序，批边界自校正）
	overloadedTotal metric.Int64Counter     // 被 WithMaxPending 卸载拒绝的提交数
	batchRetries    metric.Int64Counter     // WithRetry 触发的重试执行次数（不含首次执行）
	discardedItems  metric.Int64Counter     // 交付时 caller ctx 已取消的 Do 来源 Item 数（结果无人读取的近似信号）
}

// 指标 bucket 口径：耗时覆盖亚毫秒到秒级，批大小覆盖 2 的幂。
var (
	durationBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.25, 0.5, 1, 2.5, 5, 10}
	sizeBuckets     = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}
)

// newMetrics 从 m 创建全部仪表。仪表自构造时刻绑定 m 的 provider
// （构造期快照语义）；创建失败的仪表为可用 noop 实现，无需门控。
func newMetrics(m metric.Meter) *metrics {
	// 同步仪表接口密封（内嵌未导出标记方法），实现只可能来自 otel：
	// 创建恒返回可用仪表，错误仅描述初始化问题（如重名/非法名），
	// 不改变后续打点行为。
	batchTotal, _ := m.Int64Counter("batcher_batch_total",
		metric.WithDescription("已 flush 的批次数"), metric.WithUnit("1"))
	batchFailed, _ := m.Int64Counter("batcher_batch_failed_total",
		metric.WithDescription("以 Batch Error 结束的批次数"), metric.WithUnit("1"))
	waitSeconds, _ := m.Float64Histogram("batcher_wait_seconds",
		metric.WithDescription("单条 Item 从入队到结果交付的端到端等待"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(durationBuckets...))
	batchSize, _ := m.Float64Histogram("batcher_batch_size",
		metric.WithDescription("每批 Item 数量"),
		metric.WithUnit("1"), metric.WithExplicitBucketBoundaries(sizeBuckets...))
	durationSeconds, _ := m.Float64Histogram("batcher_batch_duration_seconds",
		metric.WithDescription("批次函数执行耗时"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(durationBuckets...))
	queueDepth, _ := m.Int64Gauge("batcher_queue_depth",
		metric.WithDescription("已准入未交付的提交数（含停泊在握手的发送方，批边界刷新）"))
	overloadedTotal, _ := m.Int64Counter("batcher_overloaded_total",
		metric.WithDescription("被 WithMaxPending 卸载拒绝的提交数"), metric.WithUnit("1"))
	batchRetries, _ := m.Int64Counter("batcher_batch_retries_total",
		metric.WithDescription("WithRetry 触发的重试执行次数（不含首次执行）"), metric.WithUnit("1"))
	discardedItems, _ := m.Int64Counter("batcher_discarded_items_total",
		metric.WithDescription("交付时 caller ctx 已取消的 Do 来源 Item 数（结果无人读取的近似信号）"),
		metric.WithUnit("1"))
	return &metrics{
		batchTotal:      batchTotal,
		batchFailed:     batchFailed,
		waitSeconds:     waitSeconds,
		batchSize:       batchSize,
		durationSeconds: durationSeconds,
		queueDepth:      queueDepth,
		overloadedTotal: overloadedTotal,
		batchRetries:    batchRetries,
		discardedItems:  discardedItems,
	}
}
