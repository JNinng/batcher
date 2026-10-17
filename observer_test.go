package batcher_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/batcher"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// recordingObserver 记录全部事件（回调必须并发安全）。
type recordingObserver struct {
	mu         sync.Mutex
	created    []batcher.CreatedEvent
	flushStart []batcher.FlushStartEvent
	flushed    []batcher.BatchFlushedEvent
	discarded  []batcher.ItemDiscardedEvent
	closed     int
}

func (o *recordingObserver) OnCreated(e batcher.CreatedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.created = append(o.created, e)
}

func (o *recordingObserver) OnFlushStart(e batcher.FlushStartEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushStart = append(o.flushStart, e)
}

func (o *recordingObserver) OnBatchFlushed(e batcher.BatchFlushedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushed = append(o.flushed, e)
}

func (o *recordingObserver) OnItemDiscarded(e batcher.ItemDiscardedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.discarded = append(o.discarded, e)
}

func (o *recordingObserver) OnClosed(e batcher.ClosedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed++
}

// testMeter 用 sdk/metric 的 ManualReader 承接真实仪表值断言：otel
// 同步仪表接口密封（内嵌未导出标记方法，外部包不可实现），fake 至多
// 落在 Meter 一侧且工厂仍须返回真实仪表；断言经 Collect 读取
// metricdata——Counter 取累计 Sum、Gauge 取最新值、Histogram 取计数。
type testMeter struct {
	reader *sdkmetric.ManualReader
	meter  metric.Meter
}

func newTestMeter(t *testing.T) *testMeter {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return &testMeter{reader: reader, meter: provider.Meter("github.com/jninng/batcher")}
}

// find 返回名为 name 的指标快照；尚无任何记录（未打点）时 ok 为 false。
func (m *testMeter) find(t *testing.T, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := m.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name == name {
				return md, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// counterValue 返回计数器累计和；「未打点」在累计语义下与「值为 0」等价。
func (m *testMeter) counterValue(t *testing.T, name string) int64 {
	t.Helper()
	md, ok := m.find(t, name)
	if !ok {
		return 0
	}
	sum, ok := md.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s: 数据类型 %T, want Sum[int64]", name, md.Data)
	}
	var v int64
	for _, dp := range sum.DataPoints {
		v += dp.Value
	}
	return v
}

// gaugeValue 返回 gauge 最新采样值（采集时刻的即时读数）。与 counter
// 不同，gauge 的「未打点」不与任何值等价——此处要求仪表已被批边界
// 写入过（缺记录即失败），want 0 断言因此仍钉住「采样过且为 0」。
func (m *testMeter) gaugeValue(t *testing.T, name string) int64 {
	t.Helper()
	md, ok := m.find(t, name)
	if !ok {
		t.Fatalf("%s: 尚无记录, want 至少一次批边界采样", name)
	}
	g, ok := md.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("%s: 数据类型 %T, want Gauge[int64]", name, md.Data)
	}
	var v int64
	for _, dp := range g.DataPoints {
		v = dp.Value
	}
	return v
}

// histogram 返回直方图唯一数据点（无属性指标恒单点）。
func (m *testMeter) histogram(t *testing.T, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	md, ok := m.find(t, name)
	if !ok {
		t.Fatalf("%s: 尚无记录", name)
	}
	h, ok := md.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s: 数据类型 %T, want Histogram[float64]", name, md.Data)
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("%s: 数据点 %d 个, want 1", name, len(h.DataPoints))
	}
	return h.DataPoints[0]
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
	if len(obs.flushStart) != 1 || len(obs.flushed) != 1 {
		t.Fatalf("flushStart = %d, flushed = %d, want 各 1（以 Seq 恒成对）", len(obs.flushStart), len(obs.flushed))
	}
	if f := obs.flushed[0]; f.Seq != obs.flushStart[0].Seq {
		t.Fatalf("Seq 配对失败：flushStart.Seq = %d, flushed.Seq = %d", obs.flushStart[0].Seq, f.Seq)
	}
	f := obs.flushed[0]
	if f.Size != 2 || f.Err != nil || f.WaitSeconds < 0 || f.DurationSeconds < 0 {
		t.Fatalf("flushed event = %+v, want {Size:2 Err:nil 非负耗时}", f)
	}
	if f.UniqueSize != f.Size {
		t.Fatalf("UniqueSize = %d, want == Size %d（普通 New 不去重）", f.UniqueSize, f.Size)
	}
	if f.Trigger != batcher.TriggerSize {
		t.Fatalf("Trigger = %q, want %q（两条即满批）", f.Trigger, batcher.TriggerSize)
	}
	if f.Panic != nil || f.Stack != nil {
		t.Fatalf("Panic = %v, Stack 非空——正常批不得携带 panic 现场", f.Panic)
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

func (panicOnFlushObserver) OnCreated(batcher.CreatedEvent)             {}
func (panicOnFlushObserver) OnFlushStart(batcher.FlushStartEvent)       {}
func (panicOnFlushObserver) OnBatchFlushed(batcher.BatchFlushedEvent)   { panic("observer explodes") }
func (panicOnFlushObserver) OnItemDiscarded(batcher.ItemDiscardedEvent) {}
func (panicOnFlushObserver) OnClosed(batcher.ClosedEvent)               {}

func TestMetricsRecordedOnFlush(t *testing.T) {
	m := newTestMeter(t)
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Second),
		batcher.WithMeter(m.meter),
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

	if v := m.counterValue(t, "batcher_batch_total"); v != 1 {
		t.Fatalf("batcher_batch_total = %v, want 1", v)
	}
	if v := m.counterValue(t, "batcher_batch_failed_total"); v != 0 {
		t.Fatalf("batcher_batch_failed_total = %v, want 0", v)
	}
	dp := m.histogram(t, "batcher_batch_size")
	if dp.Count != 1 || dp.Sum != 2 {
		t.Fatalf("batcher_batch_size = (count %d, sum %v), want (1, 2)", dp.Count, dp.Sum)
	}
	dp = m.histogram(t, "batcher_batch_duration_seconds")
	if dp.Count != 1 || dp.Sum < 0 {
		t.Fatalf("batcher_batch_duration_seconds = (count %d, sum %v), want 一次非负耗时", dp.Count, dp.Sum)
	}
	dp = m.histogram(t, "batcher_wait_seconds")
	if dp.Count != 2 {
		t.Fatalf("batcher_wait_seconds count = %d（每条 Item 一次）, want 2", dp.Count)
	}
	if v := m.gaugeValue(t, "batcher_queue_depth"); v != 0 {
		t.Fatalf("batcher_queue_depth = %v, want 0（Close 后无积压）", v)
	}
}

func TestMetricsRecordFailure(t *testing.T) {
	m := newTestMeter(t)
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return nil, errors.New("boom")
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMeter(m.meter),
	)
	if _, err := b.Do(context.Background(), 1); err == nil {
		t.Fatal("Do err = nil, want boom")
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if v := m.counterValue(t, "batcher_batch_failed_total"); v != 1 {
		t.Fatalf("batcher_batch_failed_total = %v, want 1", v)
	}
}

// 约束：WithLogger 注入的 *slog.Logger 收到 created…closed 完整
// 生命周期（构造期一次固定，构造后替换注入不追溯已建 Batcher）。
// 未注入时的默认静默是构造性保证——构造期固定 discard logger、
// 不存在输出路径，结构上不可观测，无（也无法有）测试断言。
func TestWithLoggerReceivesLifecycleLogs(t *testing.T) {
	var buf bytes.Buffer
	captured := slog.New(slog.NewTextHandler(&buf, nil))

	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithLogger(captured),
	)
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "batcher: created") || !strings.Contains(out, "batcher: closed") {
		t.Fatalf("log output = %q, want created…closed 生命周期", out)
	}
}

type panicOnEverythingObserver struct{}

func (panicOnEverythingObserver) OnCreated(batcher.CreatedEvent) { panic("created explodes") }
func (panicOnEverythingObserver) OnFlushStart(batcher.FlushStartEvent) {
	panic("flush-start explodes")
}
func (panicOnEverythingObserver) OnBatchFlushed(batcher.BatchFlushedEvent) {
	panic("flushed explodes")
}
func (panicOnEverythingObserver) OnItemDiscarded(batcher.ItemDiscardedEvent) {
	panic("discarded explodes")
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

func (o *seqObserver) OnFlushStart(batcher.FlushStartEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "flush-start")
}

func (o *seqObserver) OnBatchFlushed(batcher.BatchFlushedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "flushed")
}

func (o *seqObserver) OnItemDiscarded(batcher.ItemDiscardedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "discarded")
}

func (o *seqObserver) OnClosed(batcher.ClosedEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, "closed")
}

// 约束：事件全序为 Created → (FlushStart → Flushed)* → Closed，
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
	want := "[created flush-start flushed flush-start flushed closed]"
	if got := fmt.Sprint(obs.seq); got != want {
		t.Fatalf("event sequence = %s, want %s", got, want)
	}
}

// startFlagObserver 在 OnFlushStart 置位，供批次函数侧断言先后。
type startFlagObserver struct {
	started atomic.Bool
}

func (o *startFlagObserver) OnCreated(batcher.CreatedEvent)             {}
func (o *startFlagObserver) OnFlushStart(batcher.FlushStartEvent)       { o.started.Store(true) }
func (o *startFlagObserver) OnBatchFlushed(batcher.BatchFlushedEvent)   {}
func (o *startFlagObserver) OnItemDiscarded(batcher.ItemDiscardedEvent) {}
func (o *startFlagObserver) OnClosed(batcher.ClosedEvent)               {}

// 约束：OnFlushStart 在批次函数调用前触发——执行 goroutine 拿到
// 在途槽位、进入 execute 即发事件，fn 执行时必然已观察到置位。
func TestFlushStartFiresBeforeBatchFunction(t *testing.T) {
	obs := &startFlagObserver{}
	b := batcher.New[int, int](func(ctx context.Context, reqs []int) ([]int, error) {
		if !obs.started.Load() {
			t.Error("batch function ran before OnFlushStart")
		}
		return append([]int(nil), reqs...), nil
	},
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(obs),
	)
	if _, err := b.Do(context.Background(), 1); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// 约束：Seq 以成批序在 Batcher 内自 1 单调递增，OnFlushStart 与
// OnBatchFlushed 逐批配对（含关闭 drain 批）。
func TestFlushEventSeqMonotonicAndPaired(t *testing.T) {
	obs := &recordingObserver{}
	b := batcher.New[int, int](identityFn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(obs),
	)
	for i := 1; i <= 3; i++ {
		if _, err := b.Do(context.Background(), i); err != nil {
			t.Fatalf("Do(%d): %v", i, err)
		}
	}
	if err := b.Enqueue(context.Background(), 4); err != nil { // 收尾 drain 批同样领号
		t.Fatalf("Enqueue: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.flushStart) != 4 || len(obs.flushed) != 4 {
		t.Fatalf("flushStart = %d, flushed = %d, want 各 4", len(obs.flushStart), len(obs.flushed))
	}
	for i := 0; i < 4; i++ {
		if obs.flushStart[i].Seq != int64(i+1) || obs.flushed[i].Seq != int64(i+1) {
			t.Fatalf("batch %d Seq = (start %d, flushed %d), want 两侧均为 %d",
				i+1, obs.flushStart[i].Seq, obs.flushed[i].Seq, i+1)
		}
	}
}

// 约束：Trigger 报告组批的实际结束条件——interval 到期、字节达限、
// Close 截断（正常路径成批）与 Close 收尾 drain 各就各位。
func TestFlushEventTriggerReportsFormationEnd(t *testing.T) {
	t.Run("interval", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(100), // interval 独占触发
			batcher.WithInterval(60*time.Millisecond),
			batcher.WithObserver(obs),
		)
		if _, err := b.Do(context.Background(), 1); err != nil {
			t.Fatalf("Do: %v", err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.flushed) != 1 || obs.flushed[0].Trigger != batcher.TriggerInterval {
			t.Fatalf("flushed = %+v, want Trigger=%q", obs.flushed, batcher.TriggerInterval)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(100),
			batcher.WithInterval(10*time.Second), // 字节先到：8B 达限，interval 永不触发
			batcher.WithMaxBytes(8, func(int) int { return 4 }),
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
		<-done
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.flushed) != 1 || obs.flushed[0].Trigger != batcher.TriggerBytes {
			t.Fatalf("flushed = %+v, want Trigger=%q（4B+4B 达限）", obs.flushed, batcher.TriggerBytes)
		}
	})

	t.Run("close-cut", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(3), // 两条不满，组批停在 interval/shutdown 二选一
			batcher.WithInterval(10*time.Second),
			batcher.WithObserver(obs),
		)
		done := make(chan struct{}, 2)
		release := launchTogether(2, func(i int) {
			defer func() { done <- struct{}{} }()
			_, _ = b.Do(context.Background(), i+1) // Close 截断后照常交付
		})
		release()
		time.Sleep(50 * time.Millisecond) // 等两条全部被接收、批成型
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		<-done
		<-done

		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.flushed) != 1 || obs.flushed[0].Trigger != batcher.TriggerClose {
			t.Fatalf("flushed = %+v, want Trigger=%q（组批被 Close 截断）", obs.flushed, batcher.TriggerClose)
		}
		if obs.flushed[0].Seq != 1 {
			t.Fatalf("Seq = %d, want 1（正常路径成批领号，收尾不重复领取）", obs.flushed[0].Seq)
		}
	})

	t.Run("close-drain", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(100),
			batcher.WithInterval(10*time.Second), // 仅 Close 能触发 flush
			batcher.WithObserver(obs),
		)
		if err := b.Enqueue(context.Background(), 1); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.flushed) != 1 || obs.flushed[0].Trigger != batcher.TriggerClose {
			t.Fatalf("flushed = %+v, want Trigger=%q（关闭 flush 无条件）", obs.flushed, batcher.TriggerClose)
		}
	})
}

// 约束：Panic / Stack 仅当终态错误由批次函数 panic 降级而来时非 nil，
// 携带 recover 值与调用栈；key / size 函数的 panic 不捕获（Err 信息可辨）。
func TestFlushEventCapturesBatchFunctionPanic(t *testing.T) {
	t.Run("batch-function-panic-captured", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](func(ctx context.Context, reqs []int) ([]int, error) {
			panic("kaboom-payload")
		},
			batcher.WithMaxSize(1),
			batcher.WithInterval(10*time.Millisecond),
			batcher.WithObserver(obs),
		)
		if _, err := b.Do(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("Do err = %v, want 含 panicked 的 Batch Error", err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		obs.mu.Lock()
		defer obs.mu.Unlock()
		f := obs.flushed[0]
		if f.Panic != "kaboom-payload" {
			t.Fatalf("Panic = %v, want kaboom-payload（recover 原值）", f.Panic)
		}
		if len(f.Stack) == 0 || !bytes.Contains(f.Stack, []byte("goroutine")) {
			t.Fatalf("Stack = %q, want 非空的 recover 点调用栈", f.Stack)
		}
	})

	t.Run("size-function-panic-not-captured", func(t *testing.T) {
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(2), // 首条不满批：collect 走到字节评估，sizeFn panic 预置失败
			batcher.WithInterval(10*time.Millisecond),
			batcher.WithMaxBytes(8, func(int) int { panic("size explodes") }),
			batcher.WithObserver(obs),
		)
		if _, err := b.Do(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "size function panicked") {
			t.Fatalf("Do err = %v, want size 函数 panic 的批级错误", err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		obs.mu.Lock()
		defer obs.mu.Unlock()
		f := obs.flushed[0]
		if f.Panic != nil || f.Stack != nil {
			t.Fatalf("Panic = %v, Stack 非空——size 函数 panic 不捕获现场", f.Panic)
		}
		if !strings.Contains(f.Err.Error(), "size function panicked") {
			t.Fatalf("Err = %v, want 保留 size panic 信息", f.Err)
		}
	})
}

// 约束（近似信号口径）：仅 Do 来源、交付时 caller ctx 已取消才发
// OnItemDiscarded 并计 discarded_items；fire-and-forget 来源结果本就
// 旁路消费，不触发。
func TestItemDiscardedOnCallerCancelOnly(t *testing.T) {
	t.Run("do-cancelled-at-delivery", func(t *testing.T) {
		m := newTestMeter(t)
		obs := &recordingObserver{}
		entered := make(chan struct{})
		release := make(chan struct{})
		b := batcher.New[int, int](func(ctx context.Context, reqs []int) ([]int, error) {
			close(entered)
			<-release
			return append([]int(nil), reqs...), nil
		},
			batcher.WithMaxSize(1),
			batcher.WithInterval(10*time.Second),
			batcher.WithObserver(obs),
			batcher.WithMeter(m.meter),
		)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := b.Do(ctx, 1)
			done <- err
		}()
		<-entered // 批次函数已执行：Item 早已入队
		cancel()  // 调用方放弃等待；Item 保留照常执行
		close(release)
		// 不断言 Do 的返回：cancel 后 resCh/ctx.Done 双就绪，waiter 侧
		// select 随机选择（契约明文的近似信号口径）；交付侧事件才是确定的。
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.discarded) != 1 || obs.discarded[0].BatchSize != 1 {
			t.Fatalf("discarded = %+v, want 一条 {BatchSize:1}", obs.discarded)
		}
		if v := m.counterValue(t, "batcher_discarded_items_total"); v != 1 {
			t.Fatalf("batcher_discarded_items_total = %v, want 1", v)
		}
	})

	t.Run("enqueue-never-discarded", func(t *testing.T) {
		m := newTestMeter(t)
		obs := &recordingObserver{}
		b := batcher.New[int, int](identityFn,
			batcher.WithMaxSize(2), // 与 Do 来源同批交付
			batcher.WithInterval(10*time.Second),
			batcher.WithObserver(obs),
			batcher.WithMeter(m.meter),
		)
		ctx, cancel := context.WithCancel(context.Background())
		if err := b.Enqueue(ctx, 1); err != nil { // 握手后 ctx 取消：结果本就无人读取
			t.Fatalf("Enqueue: %v", err)
		}
		cancel()
		if _, err := b.Do(context.Background(), 2); err != nil {
			t.Fatalf("Do: %v", err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}

		obs.mu.Lock()
		defer obs.mu.Unlock()
		if len(obs.discarded) != 0 {
			t.Fatalf("discarded = %+v, want 空（fire-and-forget 不参与）", obs.discarded)
		}
		if v := m.counterValue(t, "batcher_discarded_items_total"); v != 0 {
			t.Fatalf("batcher_discarded_items_total = %v, want 0", v)
		}
	})
}
