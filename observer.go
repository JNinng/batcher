package batcher

import "time"

// FlushTrigger 是 Flush 的触发条件，由 BatchFlushedEvent.Trigger 报告。
type FlushTrigger string

const (
	// TriggerSize：批内条数达 WithMaxSize 上限。
	TriggerSize FlushTrigger = "size"
	// TriggerBytes：批内累计字节达 WithMaxBytes 上限。组批期 size 函数
	// 失败（panic / 负数返回）预置失败的批同样报 bytes——失败发生在
	// 字节评估路径，Err 已携带具体错误。
	TriggerBytes FlushTrigger = "bytes"
	// TriggerInterval：组批时间间隔到期（自 Formation 起算）。
	TriggerInterval FlushTrigger = "interval"
	// TriggerClose：Close 触发的收尾 flush（含组批中途被 Close 截断
	// 的批——无论其组批进度为何，交付一律因 Close）。
	TriggerClose FlushTrigger = "close"
)

// Observer 是 batcher 的类型化旁路观测接口（默认 Noop）。
// 回调同步执行、必须快速返回（微秒级）：OnCreated 在构造方 goroutine，
// OnFlushStart / OnItemDiscarded / OnBatchFlushed 在执行 goroutine（持槽
// 期间），OnClosed 在 dispatcher 收尾段（全部在途批次完成后）；回调
// 可能并发——WithMaxConcurrency(n>1) 的多个执行 goroutine，或多个
// Batcher（如路由层分片）共用同一 Observer 时可并发回调执行侧事件
// ——Observer 实现必须并发安全，跨批事件顺序不保证；
// 回调 panic 会被库 recover，观测不得改变业务执行语义；
// 回调内不得同步再入本 Batcher——再入提交需等槽位，而槽位正被本
// 回调所在批占用，MaxConcurrency=1 下自死锁（同 EnqueueFunc 约束）。
type Observer interface {
	// OnCreated 在 New 返回前于构造方 goroutine 触发一次。
	OnCreated(CreatedEvent)
	// OnFlushStart 每批一次，批次进入执行（已占在途槽位）时、批次
	// 函数调用前触发；组批期预置失败（不调批次函数）的批同样触发
	// ——与 OnBatchFlushed 以 Seq 恒成对，补全「排队 → 执行 → 交付」
	// 时间线。
	OnFlushStart(FlushStartEvent)
	// OnBatchFlushed 每批一次，在全部 Waiter 被唤醒后触发。
	OnBatchFlushed(BatchFlushedEvent)
	// OnItemDiscarded 每条「交付时 caller ctx 已取消」的 Do 来源 Item
	// 触发一次，在交付点、该批 OnBatchFlushed 之前。
	OnItemDiscarded(ItemDiscardedEvent)
	// OnClosed 在全部在途批次完成后、dispatcher 退出前触发一次
	// （含优雅 flush 完成后）。
	OnClosed(ClosedEvent)
}

// CreatedEvent 是构造事件。
type CreatedEvent struct {
	MaxSize  int
	Interval time.Duration
}

// FlushStartEvent 是批次开始执行事件：批次已占用在途槽位、即将（或
// 因组批期预置失败而并不）调用批次函数。与 BatchFlushedEvent 以 Seq
// 配对——每批恰一对，跨批事件顺序不保证（WithMaxConcurrency n>1
// 下交错），配对以 Seq 为准。
type FlushStartEvent struct {
	// Seq 是批序号：Batcher 内自 1 单调递增，成批时分配（跨 Batcher
	// 不唯一，分片等场景各 Batcher 独立计）。
	Seq int64
}

// ItemDiscardedEvent 旁路报告一条 Item 的结果在交付点无人读取（近似
// 信号：交付时 caller ctx 已取消——waiter 的 select 在 resCh/ctx.Done
// 双就绪时随机选择，个别被判丢弃的结果实际被读走，故事件口径是
// 「ctx 已取消」而非「结果确定无人读取」）。仅 Do 来源触发；
// Enqueue / EnqueueFunc 的结果本就旁路消费，不触发。
type ItemDiscardedEvent struct {
	// BatchSize 是该 Item 所属批的原始条数。
	BatchSize int
}

// BatchFlushedEvent 是批完成事件。WaitSeconds 为组批耗时（首条 Item
// 入批 → Flush 触发），DurationSeconds 为批次函数执行耗时（WithRetry
// 下含重试与退避的持槽全过程），两者正交。Size 为原始条数（重复
// Item 照常占槽）；UniqueSize 为去重后条数，NewKeyed 下 Size -
// UniqueSize 即批内重复消除量，普通 New 下两者相等，折叠失败（keyFn
// panic）时为 0。Retries 为该批在首次执行后的重试次数（WithRetry
// 配置；0 即无重试），重试会拉高 wait_seconds 的端到端口径。Seq 为
// 批序号（见 FlushStartEvent）；Trigger 报告 Flush 触发条件。Panic 与
// Stack 仅当本批终态错误由批次函数 panic 降级而来时非 nil——前者是
// recover 到的 panic 值，后者是捕获于 recover 点的调用栈（重试下取
// 终态那次尝试的现场）；key / size / backoff 函数的 panic 不捕获，
// 仅可从 Err 的错误信息辨识。
type BatchFlushedEvent struct {
	Size            int
	UniqueSize      int
	WaitSeconds     float64
	DurationSeconds float64
	Retries         int
	Err             error
	Seq             int64
	Trigger         FlushTrigger
	Panic           any
	Stack           []byte
}

// ClosedEvent 是关闭事件。
type ClosedEvent struct{}

// noopObserver 是可比较的零大小值类型；NoopObserver 供整体跳过分发。
type noopObserver struct{}

func (noopObserver) OnCreated(CreatedEvent)             {}
func (noopObserver) OnFlushStart(FlushStartEvent)       {}
func (noopObserver) OnBatchFlushed(BatchFlushedEvent)   {}
func (noopObserver) OnItemDiscarded(ItemDiscardedEvent) {}
func (noopObserver) OnClosed(ClosedEvent)               {}

// NoopObserver 恒不 panic、零行为。
var NoopObserver Observer = noopObserver{}
