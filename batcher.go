// Package batcher 将零散的单次请求扇入为批量操作，并把结果按位置
// 归还各调用方。
//
// 语义摘要（依据 docs/adr/）：
//   - Do 同步阻塞直到自己那份结果返回；
//   - Flush 触发条件为批大小上限或时间间隔（自组批起算），先到先触发，
//     且仅有数据才实际触发——批次函数永远不会收到空切片；
//   - 同一时刻至多一个 In-flight Batch，下游并发恒为 1；
//   - 批级错误（含批次函数 panic 降级）广播给该批全部 Waiter，不重试；
//   - 调用方取消仅解除自身等待，Item 保留照常执行；
//   - 批次函数运行于 batcher 级基础 ctx；Close 正常返回即全部已入队
//     Waiter 已被唤醒。
package batcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jninng/observ"
)

// BatchFunc 是用户提供的批量执行函数：接收整个 Batch（恒非空），
// 返回与入参等长的结果切片；返回非 nil 错误即整批失败。
type BatchFunc[Req, Res any] func(ctx context.Context, reqs []Req) ([]Res, error)

// ErrClosed 表示 Close 已开始后到达的新 Do 调用。
var ErrClosed = errors.New("batcher: closed")

// result 是单条 Item 的交付载体。
type result[Res any] struct {
	res Res
	err error
}

// item 是一条已入队的请求及其交付通道（缓冲 1，绝不阻塞派发）。
type item[Req, Res any] struct {
	req       Req
	arrivedAt time.Time
	resCh     chan result[Res]
}

// Batcher 将零散 Do 扇入为批量调用。构造后即可并发使用 Do；
// 用毕必须 Close。
type Batcher[Req, Res any] struct {
	fn       BatchFunc[Req, Res]
	maxSize  int
	interval time.Duration

	inbox    chan *item[Req, Res] // 无缓冲：执行期间新 Item 停靠在此，即待处理积压
	closing  chan struct{}        // Close 已开始，新 Do 立即 ErrClosed
	shutdown chan struct{}        // 唤醒 dispatcher：flush 后退出
	done     chan struct{}        // dispatcher 已退出

	closeOnce  sync.Once
	baseCtx    context.Context // 批次函数运行的 ctx，随 dispatcher 退出而取消
	baseCancel context.CancelFunc

	observer Observer
	metrics  *metrics // meter 为 Noop 时为 nil，整体跳过埋点
	logger   observ.Logger
	pending  atomic.Int64 // 已接收未完成 Item 数（queue_depth 口径）
}

// New 构造 Batcher 并启动 dispatcher。fn 为 nil、MaxSize/Interval
// 缺失或非正属程序员工错，构造期 panic。
func New[Req, Res any](fn BatchFunc[Req, Res], opts ...Option) *Batcher[Req, Res] {
	if fn == nil {
		panic("batcher: batch function must not be nil")
	}
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxSize <= 0 {
		panic("batcher: WithMaxSize is required and must be > 0")
	}
	if cfg.interval <= 0 {
		panic("batcher: WithInterval is required and must be > 0")
	}
	if cfg.observer == nil {
		cfg.observer = NoopObserver
	}
	if cfg.meter == nil {
		cfg.meter = observ.NoopMeter
	}
	if cfg.logger == nil {
		cfg.logger = observ.DefaultLogger() // 构造期快照语义
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &Batcher[Req, Res]{
		fn:         fn,
		maxSize:    cfg.maxSize,
		interval:   cfg.interval,
		inbox:      make(chan *item[Req, Res]),
		closing:    make(chan struct{}),
		shutdown:   make(chan struct{}),
		done:       make(chan struct{}),
		baseCtx:    ctx,
		baseCancel: cancel,
		observer:   cfg.observer,
		logger:     cfg.logger,
	}
	if cfg.meter != observ.NoopMeter { // NoopMeter 可比较，整体门控
		b.metrics = newMetrics(cfg.meter)
	}
	go b.run()
	b.emitCreated()
	b.logger.Log(slog.LevelInfo, "batcher: created",
		slog.Int("max_size", cfg.maxSize),
		slog.Duration("interval", cfg.interval),
	)
	return b
}

// Do 提交一条 Item 并阻塞等待自己那份结果。调用方 ctx 只治理本次
// 等待：取消即返回 ctx.Err()，Item 保留照常执行、结果丢弃。
// Close 开始后调用返回 ErrClosed。
func (b *Batcher[Req, Res]) Do(ctx context.Context, req Req) (Res, error) {
	var zero Res
	select {
	case <-b.closing:
		return zero, ErrClosed
	default:
	}
	it := &item[Req, Res]{
		req:       req,
		arrivedAt: time.Now(),
		resCh:     make(chan result[Res], 1),
	}
	select {
	case b.inbox <- it:
	case <-b.closing:
		return zero, ErrClosed
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	select {
	case r := <-it.resCh:
		return r.res, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Close 优雅关闭：立即 flush 未满批并等待在途批次完成；返回 nil 即
// 所有已入队 Waiter 已被唤醒、批次函数不再运行。幂等，可并发调用；
// ctx 超时仅放弃等待（后台 flush 照常完成），返回 ctx.Err()。
func (b *Batcher[Req, Res]) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		close(b.closing)
		close(b.shutdown)
	})
	select {
	case <-b.done: // 双就绪时优先成功路径：done 已闭即 Waiter 全部唤醒
		return nil
	default:
	}
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		b.logger.Log(slog.LevelWarn, "batcher: close timed out, flush continues in background",
			slog.String("error", ctx.Err().Error()),
		)
		return ctx.Err()
	}
}

// run 是 dispatcher 主循环：取首条 → 组批 → 执行，串行往复；
// shutdown 后 drain 全部积压并 flush，仅有数据才执行。
// pending 记账在接收侧完成（收到即 Add(1)，交付后 Add(-1)）：
// 无缓冲握手下发送方的增量可能滞后于整批交付，接收侧记账保证
// 先增后减的顺序，queue_depth 不出现负值。
func (b *Batcher[Req, Res]) run() {
	defer b.baseCancel()
	defer func() {
		b.emitClosed()
		b.logger.Log(slog.LevelInfo, "batcher: closed")
		close(b.done)
	}()
	for {
		var first *item[Req, Res]
		select {
		case first = <-b.inbox:
			b.pending.Add(1)
		case <-b.shutdown:
			batch := b.drain(nil)
			if len(batch) > 0 {
				b.execute(batch, time.Now())
			}
			return
		}
		formationStart := time.Now()
		batch := b.collect(first)
		if b.stopped() {
			batch = b.drain(batch)
			if len(batch) > 0 {
				b.execute(batch, formationStart)
			}
			return
		}
		b.execute(batch, formationStart)
	}
}

// collect 自首条 Item 起组批：size 达上限或 interval 到期先到先止；
// shutdown 亦立即止，交由调用方 drain。首条即满批（maxSize==1）时
// 无需计时器。
func (b *Batcher[Req, Res]) collect(first *item[Req, Res]) []*item[Req, Res] {
	batch := []*item[Req, Res]{first}
	if len(batch) >= b.maxSize {
		return batch
	}
	timer := time.NewTimer(b.interval)
	defer timer.Stop()
	for len(batch) < b.maxSize {
		select {
		case it := <-b.inbox:
			b.pending.Add(1)
			batch = append(batch, it)
		case <-timer.C:
			return batch
		case <-b.shutdown:
			return batch
		}
	}
	return batch
}

// drain 非阻塞清空 inbox 中已停靠的 Item（含停泊中的发送方握手）。
func (b *Batcher[Req, Res]) drain(batch []*item[Req, Res]) []*item[Req, Res] {
	for {
		select {
		case it := <-b.inbox:
			b.pending.Add(1)
			batch = append(batch, it)
		default:
			return batch
		}
	}
}

// execute 调用批次函数并按位置交付结果，随后记录观测。
// formationStart 用于 WaitSeconds（组批耗时）口径。
func (b *Batcher[Req, Res]) execute(batch []*item[Req, Res], formationStart time.Time) {
	reqs := make([]Req, len(batch))
	for i, it := range batch {
		reqs[i] = it.req
	}

	waitSeconds := time.Since(formationStart).Seconds() // 组批耗时：入批 → Flush 触发
	started := time.Now()
	res, err := b.callFn(reqs)
	durationSeconds := time.Since(started).Seconds()

	// 长度契约：结果与请求不等长视为整批失败。
	if err == nil && len(res) != len(reqs) {
		err = fmt.Errorf("batcher: batch function returned %d results for %d requests", len(res), len(reqs))
	}

	for i, it := range batch {
		var r Res
		if err == nil {
			r = res[i]
		}
		it.resCh <- result[Res]{res: r, err: err}
		b.pending.Add(-1)
		if b.metrics != nil {
			b.metrics.waitSeconds.Observe(time.Since(it.arrivedAt).Seconds())
		}
	}

	if b.metrics != nil {
		b.metrics.batchTotal.Inc()
		b.metrics.batchSize.Observe(float64(len(batch)))
		b.metrics.durationSeconds.Observe(durationSeconds)
		if err != nil {
			b.metrics.batchFailed.Inc()
		}
		b.metrics.queueDepth.Set(float64(b.pending.Load()))
	}
	b.emitFlushed(BatchFlushedEvent{
		Size:            len(batch),
		WaitSeconds:     waitSeconds,
		DurationSeconds: durationSeconds,
		Err:             err,
	})
}

// callFn 调用用户批次函数，panic 降级为 Batch Error，保证库继续可用。
func (b *Batcher[Req, Res]) callFn(reqs []Req) (res []Res, err error) {
	defer func() {
		if r := recover(); r != nil {
			res = nil
			err = fmt.Errorf("batcher: batch function panicked: %v", r)
		}
	}()
	return b.fn(b.baseCtx, reqs)
}

func (b *Batcher[Req, Res]) stopped() bool {
	select {
	case <-b.shutdown:
		return true
	default:
		return false
	}
}

// safeEmit 在分发点同步调用 Observer 回调；Noop 整体跳过，
// 回调 panic 一律 recover（观测是纯旁路）。
func (b *Batcher[Req, Res]) safeEmit(call func(o Observer)) {
	if b.observer == NoopObserver {
		return
	}
	defer func() { _ = recover() }()
	call(b.observer)
}

func (b *Batcher[Req, Res]) emitCreated() {
	b.safeEmit(func(o Observer) {
		o.OnCreated(CreatedEvent{MaxSize: b.maxSize, Interval: b.interval})
	})
}

func (b *Batcher[Req, Res]) emitFlushed(ev BatchFlushedEvent) {
	b.safeEmit(func(o Observer) { o.OnBatchFlushed(ev) })
}

func (b *Batcher[Req, Res]) emitClosed() {
	b.safeEmit(func(o Observer) { o.OnClosed(ClosedEvent{}) })
}
