package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/batcher"
	"github.com/jninng/observ"
)

// recordingObserver 记录全部事件（回调必须并发安全）。
type recordingObserver struct {
	mu      sync.Mutex
	created []batcher.CreatedEvent
	flushed []batcher.BatchFlushedEvent
	closed  int
}

func (o *recordingObserver) OnCreated(e batcher.CreatedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.created = append(o.created, e)
}

func (o *recordingObserver) OnBatchFlushed(e batcher.BatchFlushedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushed = append(o.flushed, e)
}

func (o *recordingObserver) OnClosed(e batcher.ClosedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed++
}

// fakeMeter 实现 observ.Meter 并记录观察值。
type fakeCounter struct {
	mu sync.Mutex
	v  float64
}

func (c *fakeCounter) Inc() { c.Add(1) }
func (c *fakeCounter) Add(v float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v += v
}
func (c *fakeCounter) Value() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

type fakeGauge struct {
	mu      sync.Mutex
	v       float64
	history []float64
}

func (g *fakeGauge) Set(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v = v
	g.history = append(g.history, v)
}
func (g *fakeGauge) Value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}
func (g *fakeGauge) Add(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v += v
}
func (g *fakeGauge) History() []float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]float64(nil), g.history...)
}

type fakeHistogram struct {
	mu     sync.Mutex
	values []float64
}

func (h *fakeHistogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.values = append(h.values, v)
}
func (h *fakeHistogram) Values() []float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]float64(nil), h.values...)
}

type fakeMeter struct {
	mu        sync.Mutex
	counters  map[string]*fakeCounter
	gauges    map[string]*fakeGauge
	histogram map[string]*fakeHistogram
}

func newFakeMeter() *fakeMeter {
	return &fakeMeter{
		counters:  map[string]*fakeCounter{},
		gauges:    map[string]*fakeGauge{},
		histogram: map[string]*fakeHistogram{},
	}
}

func (m *fakeMeter) NewCounter(name, help string) observ.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] = &fakeCounter{}
	return m.counters[name]
}

func (m *fakeMeter) NewGauge(name, help string) observ.Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = &fakeGauge{}
	return m.gauges[name]
}

func (m *fakeMeter) NewHistogram(name, help string, buckets []float64) observ.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histogram[name] = &fakeHistogram{}
	return m.histogram[name]
}

func TestObserverReceivesLifecycleAndFlushEvents(t *testing.T) {
	obs := &recordingObserver{}
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}

	interval := 500 * time.Millisecond // 放宽组批窗口；实际由 size=2 触发，无需等满
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(interval),
		batcher.WithObserver(obs),
	)
	done := make(chan struct{}, 2)
	release := launchTogether(2, func(i int) {
		defer func() { done <- struct{}{} }()
		if _, err := b.Do(context.Background(), i+1); err != nil {
			t.Errorf("Do(%d): %v", i+1, err)
		}
	})
	release()
	<-done
	<-done // size=2 已触发 flush 并交付，才可 Close
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.created) != 1 || obs.created[0].MaxSize != 2 || obs.created[0].Interval != interval {
		t.Fatalf("created events = %+v, want one {MaxSize:2 Interval:%v}", obs.created, interval)
	}
	if len(obs.flushed) != 1 {
		t.Fatalf("flushed events = %d, want 1", len(obs.flushed))
	}
	f := obs.flushed[0]
	if f.Size != 2 || f.Err != nil || f.WaitSeconds < 0 || f.DurationSeconds < 0 {
		t.Fatalf("flushed event = %+v, want {Size:2 Err:nil 非负耗时}", f)
	}
	if f.UniqueSize != f.Size {
		t.Fatalf("UniqueSize = %d, want == Size %d（普通 New 不去重）", f.UniqueSize, f.Size)
	}
	if obs.closed != 1 {
		t.Fatalf("closed events = %d, want 1", obs.closed)
	}
}

func TestObserverFlushEventCarriesBatchError(t *testing.T) {
	obs := &recordingObserver{}
	boom := errors.New("boom")
	fn := func(ctx context.Context, reqs []int) ([]int, error) { return nil, boom }

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(obs),
	)
	if _, err := b.Do(context.Background(), 1); !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want boom", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.flushed) != 1 || !errors.Is(obs.flushed[0].Err, boom) {
		t.Fatalf("flushed = %+v, want Err 携带 boom", obs.flushed)
	}
}

func TestObserverPanicIsRecovered(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(panicOnFlushObserver{}),
	)
	if res, err := b.Do(context.Background(), 3); err != nil || res != 3 {
		t.Fatalf("Do = (%d, %v), want (3, nil)：回调 panic 不得改变业务语义", res, err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type panicOnFlushObserver struct{}

func (panicOnFlushObserver) OnCreated(batcher.CreatedEvent)           {}
func (panicOnFlushObserver) OnBatchFlushed(batcher.BatchFlushedEvent) { panic("observer explodes") }
func (panicOnFlushObserver) OnClosed(batcher.ClosedEvent)             {}

func TestMetricsRecordedOnFlush(t *testing.T) {
	m := newFakeMeter()
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Second),
		batcher.WithMeter(m),
	)
	var wg sync.WaitGroup
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := b.Do(context.Background(), i); err != nil {
				t.Errorf("Do(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait() // size=2 已触发 flush 并交付，才可 Close
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if v := m.counters["batcher_batch_total"].Value(); v != 1 {
		t.Fatalf("batcher_batch_total = %v, want 1", v)
	}
	if v := m.counters["batcher_batch_failed_total"].Value(); v != 0 {
		t.Fatalf("batcher_batch_failed_total = %v, want 0", v)
	}
	if vs := m.histogram["batcher_batch_size"].Values(); len(vs) != 1 || vs[0] != 2 {
		t.Fatalf("batcher_batch_size = %v, want [2]", vs)
	}
	if vs := m.histogram["batcher_batch_duration_seconds"].Values(); len(vs) != 1 || vs[0] < 0 {
		t.Fatalf("batcher_batch_duration_seconds = %v, want one non-negative", vs)
	}
	if vs := m.histogram["batcher_wait_seconds"].Values(); len(vs) != 2 {
		t.Fatalf("batcher_wait_seconds = %v（每条 Item 一次）, want 2 次", vs)
	}
	if v := m.gauges["batcher_queue_depth"].Value(); v != 0 {
		t.Fatalf("batcher_queue_depth = %v, want 0（Close 后无积压）", v)
	}
}

func TestMetricsRecordFailure(t *testing.T) {
	m := newFakeMeter()
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return nil, errors.New("boom")
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMeter(m),
	)
	if _, err := b.Do(context.Background(), 1); err == nil {
		t.Fatal("Do err = nil, want boom")
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if v := m.counters["batcher_batch_failed_total"].Value(); v != 1 {
		t.Fatalf("batcher_batch_failed_total = %v, want 1", v)
	}
}

// recordsLogger 实现 observ.Logger 并记录输出。
type recordsLogger struct {
	mu      sync.Mutex
	records []string
}

func (l *recordsLogger) Enabled(level slog.Level) bool { return true }
func (l *recordsLogger) Log(level slog.Level, msg string, attrs ...slog.Attr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, msg)
}
func (l *recordsLogger) Records() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.records...)
}

func TestLoggerDefaultsToObservDefaultSnapshot(t *testing.T) {
	captured := &recordsLogger{}
	old := observ.SetDefaultLogger(captured)
	defer observ.SetDefaultLogger(old)

	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	recs := captured.Records()
	if len(recs) < 2 ||
		recs[0] != "batcher: created" || recs[len(recs)-1] != "batcher: closed" {
		t.Fatalf("log records = %v, want created…closed 生命周期", recs)
	}
}

type panicOnEverythingObserver struct{}

func (panicOnEverythingObserver) OnCreated(batcher.CreatedEvent) { panic("created explodes") }
func (panicOnEverythingObserver) OnBatchFlushed(batcher.BatchFlushedEvent) {
	panic("flushed explodes")
}
func (panicOnEverythingObserver) OnClosed(batcher.ClosedEvent) { panic("closed explodes") }

func TestObserverPanicOnEveryEventIsRecovered(t *testing.T) {
	fn := identityFn
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(panicOnEverythingObserver{}),
	)
	if res, err := b.Do(context.Background(), 1); err != nil || res != 1 {
		t.Fatalf("Do = (%d, %v), want (1, nil)", res, err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// 约束：WaitSeconds 是组批耗时——size 触发时远小于
// interval，interval 触发时不小于 interval。
func TestFlushEventWaitSecondsSemantics(t *testing.T) {
	t.Run("size-trigger-below-interval", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(2),
			batcher.WithInterval(10*time.Second), // size 独占触发
			batcher.WithObserver(obs),
		)
		var wg sync.WaitGroup
		for i := 1; i <= 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, err := b.Do(context.Background(), i); err != nil {
					t.Errorf("Do(%d): %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.flushed) != 1 || obs.flushed[0].WaitSeconds >= 10.0 {
			t.Fatalf("flushed = %+v, want WaitSeconds < interval（size 触发）", obs.flushed)
		}
	})

	t.Run("interval-trigger-at-least-interval", func(t *testing.T) {
		obs := &recordingObserver{}
		const interval = 150 * time.Millisecond
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(100), // interval 独占触发
			batcher.WithInterval(interval),
			batcher.WithObserver(obs),
		)
		go func() { _, _ = b.Do(context.Background(), 1) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			obs.mu.Lock()
			n := len(obs.flushed)
			obs.mu.Unlock()
			if n == 1 || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.flushed) != 1 {
			t.Fatalf("flushed events = %d, want 1", len(obs.flushed))
		}
		if w := obs.flushed[0].WaitSeconds; w < interval.Seconds()*0.8 {
			t.Fatalf("WaitSeconds = %v, want >= ~interval（interval 触发）", w)
		}
	})
}

// 约束（ADR-0008）：WaitSeconds 是组批耗时口径（首条 Item 入批 →
// Flush 触发，成批处结算）——槽位排队不计入。慢批 A 执行期间成批
// 排队的批 B，其 WaitSeconds 仍 ≈ interval；若改在执行入口结算，会
// 被等槽时间污染成「组批 + 排队」的端到端值（≈0.4s）。
func TestWaitSecondsExcludesSlotQueueing(t *testing.T) {
	obs := &recordingObserver{}
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		if calls.Add(1) == 1 {
			time.Sleep(300 * time.Millisecond) // 批 A 慢执行，批 B 成批后排队等槽
		}
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(100*time.Millisecond),
		batcher.WithObserver(obs),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	done := make(chan struct{}, 3)
	release := launchTogether(2, func(i int) {
		defer func() { done <- struct{}{} }()
		_, _ = b.Do(context.Background(), i+1) // 屏障确保同批：size=2 立即触发
	})
	release()
	go func() {
		defer func() { done <- struct{}{} }()
		_, _ = b.Do(context.Background(), 3) // 执行期到达，独自成批排队
	}()
	<-done
	<-done
	<-done // 三条全部交付（批 B 已执行完）

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.flushed) != 2 {
		t.Fatalf("flushed events = %d, want 2", len(obs.flushed))
	}
	if w := obs.flushed[1].WaitSeconds; w < 0.08 || w > 0.25 {
		t.Fatalf("batch B WaitSeconds = %v, want ≈interval 0.1s（成批处结算，不含槽位排队）", w)
	}
}

// seqObserver 按到达顺序记录事件种类。
type seqObserver struct {
	mu  sync.Mutex
	seq []string
}

func (o *seqObserver) OnCreated(batcher.CreatedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "created")
}

func (o *seqObserver) OnBatchFlushed(batcher.BatchFlushedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "flushed")
}

func (o *seqObserver) OnClosed(batcher.ClosedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "closed")
}

// 约束：事件全序为 Created → Flushed* → Closed，
// 且并发 Close 下 OnClosed 恰好一次。
func TestEventOrderingAndOnClosedExactlyOnce(t *testing.T) {
	obs := &seqObserver{}
	b := batcher.New[int, int](identityFn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(500*time.Millisecond), // 放宽组批窗口；两批均由 size=2 触发
		batcher.WithObserver(obs),
	)
	done := make(chan struct{}, 4)
	release := launchTogether(4, func(i int) { // 两批：任一到达序都配成 2+2
		defer func() { done <- struct{}{} }()
		if _, err := b.Do(context.Background(), i+1); err != nil {
			t.Errorf("Do(%d): %v", i+1, err)
		}
	})
	release()
	for i := 0; i < 4; i++ {
		<-done
	}

	var closeWg sync.WaitGroup
	for i := 0; i < 5; i++ {
		closeWg.Add(1)
		go func() {
			defer closeWg.Done()
			if err := b.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	closeWg.Wait()

	obs.mu.Lock()
	defer obs.mu.Unlock()
	want := "[created flushed flushed closed]"
	if got := fmt.Sprint(obs.seq); got != want {
		t.Fatalf("event sequence = %s, want %s", got, want)
	}
}
