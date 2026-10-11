package batcher

import "github.com/jninng/observ"

// metrics 汇集全部指标句柄；命名遵循 observ 约定
// （计数器带 _total、耗时带 _seconds、状态量带 _depth）。
// 核心包只有无 label 指标；带 label 聚合由用户侧 adapter 完成。
// Meter 实现不得 panic（库不 recover Meter 回调，与 Observer 契约不同）
// 且必须并发安全：WithMaxConcurrency(n>1) 或路由层分片下埋点从多个
// goroutine 并发发生（提交方 goroutine 上另有 admit/拒绝计数）。
type metrics struct {
	batchTotal      observ.Counter
	batchFailed     observ.Counter
	waitSeconds     observ.Histogram // 每条 Item 一次：入队 → 结果交付
	batchSize       observ.Histogram // 每批一次：批大小
	durationSeconds observ.Histogram // 每批一次：批次函数执行耗时
	queueDepth      observ.Gauge     // 已准入未交付的提交数，含停泊在握手的发送方（批边界刷新；并发交付下采样可能瞬时乱序，批边界自校正）
	overloadedTotal observ.Counter   // 被 WithMaxPending 卸载拒绝的提交数
	batchRetries    observ.Counter   // WithRetry 触发的重试执行次数（不含首次执行）
}

// 指标 bucket 口径：耗时覆盖亚毫秒到秒级，批大小覆盖 2 的幂。
var (
	durationBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.25, 0.5, 1, 2.5, 5, 10}
	sizeBuckets     = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}
)

func newMetrics(m observ.Meter) *metrics {
	return &metrics{
		batchTotal:      m.NewCounter("batcher_batch_total", "已 flush 的批次数"),
		batchFailed:     m.NewCounter("batcher_batch_failed_total", "以 Batch Error 结束的批次数"),
		waitSeconds:     m.NewHistogram("batcher_wait_seconds", "单条 Item 从入队到结果交付的端到端等待", durationBuckets),
		batchSize:       m.NewHistogram("batcher_batch_size", "每批 Item 数量", sizeBuckets),
		durationSeconds: m.NewHistogram("batcher_batch_duration_seconds", "批次函数执行耗时", durationBuckets),
		queueDepth:      m.NewGauge("batcher_queue_depth", "已准入未交付的提交数（含停泊在握手的发送方，批边界刷新）"),
		overloadedTotal: m.NewCounter("batcher_overloaded_total", "被 WithMaxPending 卸载拒绝的提交数"),
		batchRetries:    m.NewCounter("batcher_batch_retries_total", "WithRetry 触发的重试执行次数（不含首次执行）"),
	}
}
