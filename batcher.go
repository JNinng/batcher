// Package batcher 将零散的单次请求扇入为批量操作，并把结果按位置
// 归还各调用方。
//
// 语义摘要（依据 docs/adr/）：
//   - Do 同步阻塞直到自己那份结果返回；
//   - Flush 触发条件为批大小上限、批字节上限（WithMaxBytes）或时间
//     间隔（自组批起算），先到先触发，且仅有数据才实际触发——批次
//     函数永远不会收到空切片；
//   - 下游并发默认 1（串行单在途），WithMaxConcurrency(n) 放宽至
//     ≤ n：组批与执行重叠，成批后在执行入口排队等空闲槽位，缓解慢
//     下游的队头阻塞；
//   - 准入可有界（WithMaxPending）：已准入未交付的提交数达上限即负载
//     卸载，新 Do 立即返回 ErrOverloaded，避免 goroutine 无界堆积；
//   - 批内可按 key 去重（NewKeyed）：批次函数收到去重切片，同 key 的
//     Waiter 共享同一条执行；折叠发生在交付边界，重复 Item 照常占槽
//     触发 Flush；
//   - 逐条业务失败由 Result{Value, Err} 承载（New 下以 Result 作 Res、
//     NewKeyedMap 下缺 key 即 Result{Err: ErrMissingResult}），与批级
//     错误广播正交；
//   - 可 fire-and-forget 入队（Enqueue/EnqueueFunc）：不等待结果、ctx
//     只治理提交；per-item 回调在交付点同步执行，与 Observer 同款旁路
//     约束；
//   - 批级错误（含批次函数 panic 降级）广播给该批全部 Waiter，默认
//     不重试；WithRetry 下仅 errors.Is(err, ErrRetryable) 的错误按原
//     批次重试（含退避，持槽进行）；
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

// ErrOverloaded 表示负载卸载：WithMaxPending 上限已满，提交被拒绝、
// 未入队。调用方可退避后重试或直接失败。
var ErrOverloaded = errors.New("batcher: overloaded")

// ErrRetryable 是重试哨兵：批次函数返回 errors.Is(err, ErrRetryable)
// 的批级错误（如 fmt.Errorf("downstream 503: %w", ErrRetryable)）且
// 配置了 WithRetry 时，整批按原 reqs 重执行；未配置 WithRetry 或错误
// 不携带该哨兵时不重试，语义与默认一致。
var ErrRetryable = errors.New("batcher: retryable")

// delivery 是单条 Item 的交付载体。
type delivery[Res any] struct {
	res Res
	err error
}

// item 是一条已入队的请求及其交付通道（缓冲 1，绝不阻塞派发）。
// cb 为 fire-and-forget 提交的 per-item 回调，Do 来源恒为 nil。
type item[Req, Res any] struct {
	req       Req
	arrivedAt time.Time
	resCh     chan delivery[Res]
	cb        func(res Res, err error)
}

// foldFunc 把原始批折叠为按 key 去重的切片（保持首次出现序），连同
// 结果回填函数 scatter——它把批次函数对去重切片的结果按原位置展开，
// 同 key 的 Item 共享同一条结果。err 非 nil 即整批失败。
type foldFunc[Req, Res any] func(reqs []Req) (unique []Req, scatter func(res []Res) []Res, err error)

// batchJob 是一次已成批待执行的结算单元：批本体、组批耗时（首条
// Item 入批 → Flush 触发，在成批处结算，不含槽位排队时间）与组批期
// 预置失败（sizeFn panic 或负数返回，此时不调批次函数）。
// flushAndAwait → launch → execute 一路携带。
type batchJob[Req, Res any] struct {
	batch       []*item[Req, Res]
	waitSeconds float64
	presetErr   error
}

// Batcher 将零散 Do 扇入为批量调用。构造后即可并发使用 Do；
// 用毕必须 Close。
type Batcher[Req, Res any] struct {
	fn             BatchFunc[Req, Res]
	maxSize        int
	interval       time.Duration
	maxPending     int                             // 已准入未交付提交数上限；0 即无界
	fold           foldFunc[Req, Res]              // 批内 key 折叠，NewKeyed 注入；New 为 nil
	maxBytes       int                             // 批字节上限；0 即无字节上限
	sizeFn         func(Req) int                   // 条目字节量函数，maxBytes > 0 时必设
	maxConcurrency int                             // 批次函数在途批次上限（下游并发上限）；恒 ≥ 1
	maxRetries     int                             // 可重试批级错误的整批重试上限；0 即不重试
	backoffFn      func(attempt int) time.Duration // 第 attempt 次重试前的退避，maxRetries > 0 时必设

	inbox    chan *item[Req, Res] // 无缓冲：在途槽位占满期间新 Item 停靠在此；停泊发送方已过准入，计入 admitted
	closing  chan struct{}        // Close 已开始，新 Do 立即 ErrClosed
	shutdown chan struct{}        // 唤醒 dispatcher：flush 后退出
	done     chan struct{}        // dispatcher 已退出（含全部在途执行完成）

	execSlots  chan struct{}  // 在途槽位计数信号量，容量 = maxConcurrency
	workers    sync.WaitGroup // 在途执行 goroutine 计数，退出前等待归零
	closeOnce  sync.Once
	baseCtx    context.Context // 批次函数运行的 ctx，随 dispatcher 退出而取消
	baseCancel context.CancelFunc

	observer Observer
	metrics  *metrics // meter 为 Noop 时为 nil，整体跳过埋点
	logger   observ.Logger

	// admitted 记已准入未交付的提交数（准入口径，queue_depth 数据源）：
	// 提交入口原子加一，拒绝或中途退路（closing / ctx.Done）即回滚，
	// 握手成功后转由交付侧逐条递减；加一先于信道发送、递减必在其后，
	// 故恒非负。停泊在无缓冲 inbox 握手上的发送方也计入，queue_depth
	// 才反映真实积压——接收侧口径（收到才计数）批间恒 ≤ maxSize，
	// 原理上看不见这部分积压，故 gauge 取准入口径。
	admitted atomic.Int64
}

// New 构造 Batcher 并启动 dispatcher。fn 为 nil、MaxSize/Interval
// 缺失或非正、MaxPending 为负、MaxConcurrency 为负、MaxRetry 非法
// （为负、>0 缺其 backoff 或 =0 携带 backoff）、MaxBytes 为负或其
// size 函数缺失/类型与 Req 失配属程序员工错，构造期 panic。
func New[Req, Res any](fn BatchFunc[Req, Res], opts ...Option) *Batcher[Req, Res] {
	return newBatcher(fn, nil, opts...)
}

// NewKeyed 构造批内按 key 去重的 Batcher 并启动
// dispatcher：批次函数收到去重后的 []Req（保持首次出现序，重复 key
// 只首个 Req 送达下游），返回 len(res) == len(unique) 的结果切片，
// 同 key 的多 Waiter 共享同一条执行、各自取得一份结果，位置对位语义
// 对去重切片成立。「key 相同 ⇒ 语义等价」由 key 函数的提供方声明，
// 库不代猜。maxSize 仍按原始条数计数——重复 Item 照常占槽触发 Flush，
// 折叠发生在交付边界，否则单一热点 key 的流永远凑不满 maxSize 个
// unique。跨批去重不做：执行期到达的重复 key 落入下一批重查。
// key 或 fn 为 nil 属程序员工错；选项校验同 New。均为构造期 panic。
func NewKeyed[K comparable, Req, Res any](key func(Req) K, fn BatchFunc[Req, Res], opts ...Option) *Batcher[Req, Res] {
	if key == nil {
		panic("batcher: key function must not be nil")
	}
	return newBatcher(fn, foldByKey[K, Req, Res](key), opts...)
}

// newBatcher 是组批层构造器的统一装配点（返回同一个 *Batcher）。
func newBatcher[Req, Res any](fn BatchFunc[Req, Res], fold foldFunc[Req, Res], opts ...Option) *Batcher[Req, Res] {
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
	if cfg.maxPending < 0 {
		panic("batcher: WithMaxPending must not be negative")
	}
	if cfg.maxConcurrency < 0 {
		panic("batcher: WithMaxConcurrency must not be negative")
	}
	if cfg.maxConcurrency == 0 {
		cfg.maxConcurrency = 1 // 默认串行单在途（ADR-0002 的默认保留）
	}
	if cfg.maxRetries < 0 {
		panic("batcher: WithRetry must not be negative")
	}
	if cfg.maxRetries > 0 && cfg.backoffFn == nil {
		panic("batcher: WithRetry backoff function must not be nil")
	}
	if cfg.maxRetries == 0 && cfg.backoffFn != nil {
		panic("batcher: WithRetry backoff function requires retries > 0")
	}
	if cfg.maxBytes < 0 {
		panic("batcher: WithMaxBytes must not be negative")
	}
	if cfg.maxBytes > 0 && cfg.sizeFn == nil {
		panic("batcher: WithMaxBytes size function must not be nil")
	}
	var sizeFn func(Req) int
	if cfg.sizeFn != nil {
		if cfg.maxBytes <= 0 {
			panic("batcher: WithMaxBytes must be > 0")
		}
		sf, ok := cfg.sizeFn.(func(Req) int) // Option 无类型参数，失配在此显式拒绝
		if !ok {
			panic("batcher: WithMaxBytes size function does not match Batcher's Req type")
		}
		sizeFn = sf
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
		fn:             fn,
		maxSize:        cfg.maxSize,
		interval:       cfg.interval,
		maxPending:     cfg.maxPending,
		fold:           fold,
		maxBytes:       cfg.maxBytes,
		sizeFn:         sizeFn,
		maxConcurrency: cfg.maxConcurrency,
		maxRetries:     cfg.maxRetries,
		backoffFn:      cfg.backoffFn,
		inbox:          make(chan *item[Req, Res]),
		closing:        make(chan struct{}),
		shutdown:       make(chan struct{}),
		done:           make(chan struct{}),
		execSlots:      make(chan struct{}, cfg.maxConcurrency),
		baseCtx:        ctx,
		baseCancel:     cancel,
		observer:       cfg.observer,
		logger:         cfg.logger,
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

// foldByKey 生成批内折叠器：位置表 pos 在折叠时一次算好，scatter 仅
// 做下标回填、不再调 key 函数（回填发生在 callFn 的 recover 之外，
// 二次调用会重新引入 panic 面）。
func foldByKey[K comparable, Req, Res any](key func(Req) K) foldFunc[Req, Res] {
	return func(reqs []Req) (unique []Req, scatter func(res []Res) []Res, err error) {
		pos := make([]int, len(reqs)) // pos[i]：reqs[i] 在去重切片中的下标
		seen := make(map[K]int, len(reqs))
		unique = make([]Req, 0, len(reqs))
		for i, req := range reqs {
			k, kerr := callKey(key, req)
			if kerr != nil {
				return nil, nil, kerr
			}
			if j, ok := seen[k]; ok {
				pos[i] = j
				continue
			}
			pos[i] = len(unique)
			seen[k] = len(unique)
			unique = append(unique, req)
		}
		scatter = func(res []Res) []Res {
			out := make([]Res, len(reqs))
			for i := range reqs {
				out[i] = res[pos[i]]
			}
			return out
		}
		return unique, scatter, nil
	}
}

// recoverAsBatchError 供 callKey / callFn 的 defer 使用：panic 降级为
// Batch Error 形态的错误，保证库继续可用。
func recoverAsBatchError(what string, errp *error) {
	if r := recover(); r != nil {
		*errp = fmt.Errorf("batcher: %s panicked: %v", what, r)
	}
}

// callKey 调用 key 函数，panic 降级为 Batch Error。折叠运行于执行
// goroutine 且发生在 callFn 的 recover 域之外，此处不 recover 则
// panic 打穿执行 goroutine、进程崩溃。
func callKey[K comparable, Req any](key func(Req) K, req Req) (k K, err error) {
	defer recoverAsBatchError("key function", &err)
	return key(req), nil
}

// Do 提交一条 Item 并阻塞等待自己那份结果。调用方 ctx 治理提交与
// 等待两个阶段：入队握手前取消即返回 ctx.Err()、Item 不入队；已
// 入队后取消仅解除等待，Item 保留照常执行、结果丢弃。
// Close 开始后调用返回 ErrClosed；配置 WithMaxPending 且已准入未交付
// 的提交数达上限时，调用立即返回 ErrOverloaded，不入队。
func (b *Batcher[Req, Res]) Do(ctx context.Context, req Req) (Res, error) {
	var zero Res
	it, err := b.submit(ctx, req, nil)
	if err != nil {
		return zero, err
	}
	select {
	case r := <-it.resCh:
		return r.res, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Enqueue 提交一条 fire-and-forget Item：不阻塞等待结果，返回 nil 即
// 已入队，此后不再有取消语义——Item 与 Do 来源一视同仁地组批、执行、
// 计入观测与准入额度，结果无人读取。ctx 只治理本次提交（准入与入队
// 握手）：取消即返回 ctx.Err()、Item 不入队；Close 开始后返回
// ErrClosed；WithMaxPending 满时返回 ErrOverloaded。写路径（日志/
// 埋点/消息队列 producer）与读路径共享同一 Batcher 与观测口径。
func (b *Batcher[Req, Res]) Enqueue(ctx context.Context, req Req) error {
	_, err := b.submit(ctx, req, nil)
	return err
}

// EnqueueFunc 在 Enqueue 之上接受 per-item 回调：交付点同步执行，收到
// 与 Do 相同口径的结果与错误——批级失败即 Batch Error、逐条失败随
// Res 自行表达。回调必须快速返回（微秒级）、panic 由库 recover（与
// Observer 同款旁路约束），先于该批的 OnBatchFlushed。回调运行于执行
// goroutine，不得同步再入本 Batcher——再入提交需等槽位，而槽位正被
// 本回调所在批占用：MaxConcurrency=1 下自死锁，>1 下阻塞所在批的剩余
// 交付；需再入时先转到自有 goroutine 或队列。cb 为 nil 等价 Enqueue。
func (b *Batcher[Req, Res]) EnqueueFunc(ctx context.Context, req Req, cb func(res Res, err error)) error {
	_, err := b.submit(ctx, req, cb)
	return err
}

// submit 是 Do / Enqueue / EnqueueFunc 共用的提交路径：准入、构造
// Item、入队握手——admit 计数口径因此结构性涵盖全部提交。返回已入队
// 的 Item 供 Do 继续等待交付；fire-and-forget 调用方直接忽略。
func (b *Batcher[Req, Res]) submit(ctx context.Context, req Req, cb func(res Res, err error)) (*item[Req, Res], error) {
	select {
	case <-b.closing:
		return nil, ErrClosed
	default:
	}
	if err := b.admit(); err != nil {
		return nil, err
	}
	it := &item[Req, Res]{
		req:       req,
		arrivedAt: time.Now(),
		resCh:     make(chan delivery[Res], 1),
		cb:        cb,
	}
	select {
	case b.inbox <- it:
		return it, nil
	case <-b.closing:
		b.admitted.Add(-1) // 中途退路：回滚准入额度
		return nil, ErrClosed
	case <-ctx.Done():
		b.admitted.Add(-1)
		return nil, ctx.Err()
	}
}

// admit 准入一条提交：计数原子加一后比较自身返回值（Add-then-check，
// 无 check-then-act 窗口），已准入未交付提交数因此恒不超过 maxPending。
// 上限的软性仅在于并发竞争下的计数瞬时虚高（其他提交的拒绝尚未
// 回滚）：额度尚空的提交可能被假阳性提前拒绝，被拒调用方退避重试
// 即可。Do / Enqueue / EnqueueFunc 均经 submit 走此路径，计数口径
// 涵盖全部提交。
func (b *Batcher[Req, Res]) admit() error {
	if n := b.admitted.Add(1); b.maxPending > 0 && n > int64(b.maxPending) {
		b.admitted.Add(-1)
		if b.metrics != nil {
			b.metrics.overloadedTotal.Inc()
		}
		return ErrOverloaded
	}
	return nil
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

// run 是 dispatcher 主循环：取首条 → 组批 → 移交执行，串行往复。
// 组批（Formation）与执行重叠：移交后立即回到接收侧，下一批的计时器
// 自其 Formation 起算——哪怕上一批仍在执行；在途槽位占满时 dispatcher
// 停在移交处（launch 阻塞），此后新 Item 停泊在 inbox 握手上。
// shutdown 后 drain 全部积压、flush 末批，等待全部在途执行 goroutine
// 退出。Item 记账在准入口径完成（见 admitted），交付侧逐条递减。
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
		case <-b.shutdown:
			b.flushAndAwait(batchJob[Req, Res]{})
			return
		}
		formationStart := time.Now()
		batch, ferr := b.collect(first)
		// WaitSeconds 在成批处结算（口径：首条 Item 入批 → Flush 触发），
		// 槽位排队发生在结算之后，不污染组批耗时。
		job := batchJob[Req, Res]{
			batch:       batch,
			waitSeconds: time.Since(formationStart).Seconds(),
			presetErr:   ferr,
		}
		if b.stopped() {
			b.flushAndAwait(job)
			return
		}
		b.launch(job)
	}
}

// launch 把已成批的批次移交执行 goroutine：先占一个在途槽位再派生
// goroutine，「已 Flush 未返回」的批次因此恒 ≤ maxConcurrency（下游
// 并发上限的硬界）。槽位占满时在此阻塞——组批随之暂停，新提交停泊
// 在 inbox 握手上（与执行期停泊同口径计入准入额度），某批交付并释放
// 槽位后继续。组批耗时已在成批处结算、随 job 移交，不含槽位排队。
func (b *Batcher[Req, Res]) launch(job batchJob[Req, Res]) {
	b.workers.Add(1)
	b.execSlots <- struct{}{}
	go func() {
		defer b.workers.Done()
		defer func() { <-b.execSlots }() // 交付并释放槽位，唤醒排队的下一批
		b.execute(job)
	}()
}

// flushAndAwait 完成关闭路径的收尾：drain 剩余停靠 Item 并入末批
// （仅有数据才 flush——关闭 flush 无条件），移交执行后等待全部在途
// goroutine。返回即全部已入队 Waiter 已被唤醒、批次函数不再运行。
func (b *Batcher[Req, Res]) flushAndAwait(job batchJob[Req, Res]) {
	job.batch = b.drain(job.batch)
	if len(job.batch) > 0 {
		b.launch(job)
	}
	b.workers.Wait()
}

// collect 自首条 Item 起组批：条数达上限、字节达上限或 interval 到期
// 先到先止；shutdown 亦立即止，交由调用方 drain。首条即满批或即超限
// 时无需计时器。字节按原始条目累计（折叠发生在交付边界，重复 Item
// 照常占槽计费）；sizeFn panic 或负数返回时整批预置失败、不调批次
// 函数（组批运行于 dispatcher goroutine，无人 recover 会打穿
// dispatcher、进程崩溃）。drain 路径不计字节——关闭 flush 无条件。
func (b *Batcher[Req, Res]) collect(first *item[Req, Res]) ([]*item[Req, Res], error) {
	batch := []*item[Req, Res]{first}
	if len(batch) >= b.maxSize {
		return batch, nil
	}
	bytes := 0
	if b.maxBytes > 0 {
		n, err := b.sizeOf(first.req)
		if err != nil {
			return batch, err
		}
		bytes = n
		if bytes >= b.maxBytes {
			return batch, nil
		}
	}
	timer := time.NewTimer(b.interval)
	defer timer.Stop()
	for len(batch) < b.maxSize {
		select {
		case it := <-b.inbox:
			batch = append(batch, it)
			if b.maxBytes > 0 {
				n, err := b.sizeOf(it.req)
				if err != nil {
					return batch, err
				}
				bytes += n
				if bytes >= b.maxBytes {
					return batch, nil
				}
			}
		case <-timer.C:
			return batch, nil
		case <-b.shutdown:
			return batch, nil
		}
	}
	return batch, nil
}

// sizeOf 评估条目字节量，仅配置 WithMaxBytes 时被调用（未配置零开销）。
// panic 降级为批级错误（与 key 函数同款约束）；返回负数同样批级失败——
// 负计费会让字节触发静默失效。
func (b *Batcher[Req, Res]) sizeOf(req Req) (bytes int, err error) {
	defer recoverAsBatchError("size function", &err)
	n := b.sizeFn(req)
	if n < 0 {
		return 0, fmt.Errorf("batcher: size function returned negative value %d", n)
	}
	return n, nil
}

// drain 非阻塞清空 inbox 中已停靠的 Item（含停泊中的发送方握手）。
func (b *Batcher[Req, Res]) drain(batch []*item[Req, Res]) []*item[Req, Res] {
	for {
		select {
		case it := <-b.inbox:
			batch = append(batch, it)
		default:
			return batch
		}
	}
}

// execute 调用批次函数并按位置交付结果，随后记录观测。运行于执行
// goroutine（在途槽位持有者），n > 1 时多个批次可并发进入本函数，
// Observer / per-item 回调随之并发（契约要求并发安全）。
// job.waitSeconds 为组批耗时口径（首条 Item 入批 → Flush 触发，成批处
// 结算，不含槽位排队）；job.presetErr 非 nil 表示组批阶段已失败（sizeFn
// panic 或负数返回），整批直接以此错误交付、不调批次函数（UniqueSize
// 为 0，与折叠前失败口径一致）。
func (b *Batcher[Req, Res]) execute(job batchJob[Req, Res]) {
	batch := job.batch
	reqs := make([]Req, len(batch))
	for i, it := range batch {
		reqs[i] = it.req
	}

	started := time.Now()
	var res []Res
	var uniqueSize int
	var err error
	var retries int
	if job.presetErr != nil {
		err = job.presetErr // 组批阶段已失败：不调批次函数
	} else {
		res, uniqueSize, err = b.callBatch(reqs)
		// 批次级重试：仅哨兵错误按原 reqs 重执行（keyed 批含折叠），
		// 其他错误照常广播。重试与退避在执行 goroutine 持槽进行，
		// DurationSeconds / wait_seconds / 在途占用均覆盖全过程。
		for err != nil && b.maxRetries > 0 && errors.Is(err, ErrRetryable) && retries < b.maxRetries {
			if berr := b.backoffFor(retries + 1); berr != nil {
				err = berr // 退避失败替代原错误：批在此终止
				break
			}
			retries++ // 计数在退避结算后：只记实际执行的重试，退避失败不计
			if b.metrics != nil {
				b.metrics.batchRetries.Inc()
			}
			res, uniqueSize, err = b.callBatch(reqs)
		}
	}
	durationSeconds := time.Since(started).Seconds()

	for i, it := range batch {
		var r Res
		if err == nil {
			r = res[i]
		}
		it.resCh <- delivery[Res]{res: r, err: err}
		b.admitted.Add(-1) // 交付即释放准入额度
		if it.cb != nil {
			b.safeItemCallback(it.cb, r, err)
		}
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
		b.metrics.queueDepth.Set(float64(b.admitted.Load()))
	}
	b.emitFlushed(BatchFlushedEvent{
		Size:            len(batch),
		UniqueSize:      uniqueSize,
		WaitSeconds:     job.waitSeconds,
		DurationSeconds: durationSeconds,
		Retries:         retries,
		Err:             err,
	})
}

// backoffFor 结算第 attempt 次重试前的退避（用户函数，attempt 从 1
// 起）并原地 sleep。运行于执行 goroutine、持槽期间——MaxConcurrency=1
// 时退避即全库停摆（交互见 WithRetry 文档）。panic 与负数返回同款
// 降级为批级错误（与 size 函数同款约束：无人 recover 会打穿执行
// goroutine，负退避是程序错误、须显式暴露）。
func (b *Batcher[Req, Res]) backoffFor(attempt int) (err error) {
	defer recoverAsBatchError("backoff function", &err)
	if d := b.backoffFn(attempt); d > 0 {
		time.Sleep(d)
	} else if d < 0 {
		return fmt.Errorf("batcher: backoff function returned negative duration %v", d)
	}
	return nil
}

// callBatch 执行一批：keyed 批先折叠为去重切片再调用批次函数，结果
// 回填至原始位置（同 key Waiter 共享同一条结果）；长度契约（ADR-0001）
// 对去重切片成立。返回去重后条数供事件口径；折叠失败（keyFn panic）
// 时为 0。
func (b *Batcher[Req, Res]) callBatch(reqs []Req) (res []Res, uniqueSize int, err error) {
	unique := reqs
	var scatter func([]Res) []Res
	if b.fold != nil {
		unique, scatter, err = b.fold(reqs)
		if err != nil {
			return nil, 0, err
		}
	}
	res, err = b.callFn(unique)
	// 长度契约：结果与请求不等长视为整批失败。
	if err == nil && len(res) != len(unique) {
		err = fmt.Errorf("batcher: batch function returned %d results for %d requests", len(res), len(unique))
	}
	if err == nil && scatter != nil {
		res = scatter(res)
	}
	return res, len(unique), err
}

// callFn 调用用户批次函数，panic 降级为 Batch Error，保证库继续可用。
func (b *Batcher[Req, Res]) callFn(reqs []Req) (res []Res, err error) {
	defer recoverAsBatchError("batch function", &err)
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

// safeItemCallback 在交付点同步执行 fire-and-forget 的 per-item 回调；
// panic 一律 recover（与 Observer 同款旁路约束，回调不得改变交付语义）。
func (b *Batcher[Req, Res]) safeItemCallback(cb func(res Res, err error), res Res, err error) {
	defer func() { _ = recover() }()
	cb(res, err)
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
