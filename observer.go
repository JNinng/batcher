package batcher

import "time"

// Observer 是 batcher 的类型化旁路观测接口（默认 Noop）。
// 回调在派发 goroutine 同步执行，必须快速返回（微秒级）；
// 回调 panic 会被库 recover，观测不得改变业务执行语义。
type Observer interface {
	// OnCreated 在 New 返回前于构造方 goroutine 触发一次。
	OnCreated(CreatedEvent)
	// OnBatchFlushed 每批一次，在全部 Waiter 被唤醒后触发。
	OnBatchFlushed(BatchFlushedEvent)
	// OnClosed 在 dispatcher 退出前触发一次（含优雅 flush 完成后）。
	OnClosed(ClosedEvent)
}

// CreatedEvent 是构造事件。
type CreatedEvent struct {
	MaxSize  int
	Interval time.Duration
}

// BatchFlushedEvent 是批完成事件。WaitSeconds 为组批耗时（首条 Item
// 入批 → Flush 触发），DurationSeconds 为批次函数执行耗时，两者正交。
// Size 为原始条数（重复 Item 照常占槽）；UniqueSize 为去重后条数，
// NewKeyed 下 Size - UniqueSize 即批内重复消除量，普通 New 下两者
// 相等，折叠失败（keyFn panic）时为 0。
type BatchFlushedEvent struct {
	Size            int
	UniqueSize      int
	WaitSeconds     float64
	DurationSeconds float64
	Err             error
}

// ClosedEvent 是关闭事件。
type ClosedEvent struct{}

// noopObserver 是可比较的零大小值类型；NoopObserver 供整体跳过分发。
type noopObserver struct{}

func (noopObserver) OnCreated(CreatedEvent)           {}
func (noopObserver) OnBatchFlushed(BatchFlushedEvent) {}
func (noopObserver) OnClosed(ClosedEvent)             {}

// NoopObserver 恒不 panic、零行为。
var NoopObserver Observer = noopObserver{}
