package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// sizeResult 收集每个 Waiter 的对位结果。
type sizeResult struct {
	req int
	res string
	err error
}

// launchTogether 并发启动 n 个 goroutine，全部停泊在起跑线后由
// release 一次性放行（真屏障），消除 spawn 调度抖动对组批时序的
// 干扰。放行后到进入 Do 之间仍可能有个别 goroutine 被调度延迟，
// 故组批断言仍需与 interval 之间保留充足余量。
func launchTogether(n int, fn func(i int)) (release func()) {
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			ready.Done()
			<-start
			fn(i)
		}(i)
	}
	return func() {
		ready.Wait()
		close(start)
	}
}

func TestSizeTriggerFlushesOneBatchWithPositionalResults(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		res := make([]string, len(reqs))
		for i, r := range reqs {
			res[i] = fmt.Sprintf("r%d", r)
		}
		return res, nil
	}

	b := batcher.New[int, string](fn,
		batcher.WithMaxSize(3),
		batcher.WithInterval(10*time.Second), // 测试期内绝不触发，隔离 size 条件
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 3
	ch := make(chan sizeResult, n)
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := b.Do(context.Background(), i)
			ch <- sizeResult{req: i, res: res, err: err}
		}(i)
	}
	wg.Wait()
	close(ch)

	for r := range ch {
		if r.err != nil {
			t.Fatalf("Do(%d) error: %v", r.req, r.err)
		}
		// 按位置对位：每个 Waiter 拿到自己那条 req 的结果。
		if want := fmt.Sprintf("r%d", r.req); r.res != want {
			t.Fatalf("Do(%d) = %q, want %q", r.req, r.res, want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	got := append([]int(nil), calls[0]...)
	sort.Ints(got)
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("batch = %v, want [1 2 3]", got)
	}
}

func TestIntervalTriggerFlushesPartialBatch(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		res := append([]int(nil), reqs...)
		return res, nil
	}

	const interval = 500 * time.Millisecond // 组批窗口放宽：屏障放行后仍需容纳调度余量
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(100), // 测试期内绝不触发，隔离 interval 条件
		batcher.WithInterval(interval),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	done := make(chan struct{}, 2)
	release := launchTogether(2, func(i int) {
		defer func() { done <- struct{}{} }()
		res, err := b.Do(context.Background(), i+1)
		if err != nil {
			t.Errorf("Do(%d) error: %v", i+1, err)
		}
		if res != i+1 {
			t.Errorf("Do(%d) = %d, want %d", i+1, res, i+1)
		}
	})
	started := time.Now()
	release()
	<-done
	<-done
	elapsed := time.Since(started)

	// interval 是唯一触发条件：必须等满，且未久等。
	if elapsed < interval {
		t.Fatalf("flushed early: elapsed %v < interval %v", elapsed, interval)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("flushed late: elapsed %v", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	got := append([]int(nil), calls[0]...)
	sort.Ints(got)
	if fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("batch = %v, want [1 2]", got)
	}
}

func TestIdleNeverCallsBatchFunction(t *testing.T) {
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(5*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	// 空转远超若干个 interval 周期。
	time.Sleep(100 * time.Millisecond)
	if n := calls.Load(); n != 0 {
		t.Fatalf("fn calls while idle = %d, want 0", n)
	}

	// 有数据才触发。
	if _, err := b.Do(context.Background(), 42); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn calls after one Do = %d, want 1", n)
	}
}

func TestBatchErrorBroadcastToAllWaiters(t *testing.T) {
	boom := errors.New("downstream down")
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		return nil, boom
	}

	b := batcher.New[int, string](fn,
		batcher.WithMaxSize(3),
		batcher.WithInterval(20*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			res, err := b.Do(context.Background(), i)
			if res != "" {
				t.Errorf("res = %q, want zero value", res)
			}
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; !errors.Is(err, boom) {
			t.Fatalf("waiter error = %v, want boom (errors.Is)", err)
		}
	}
}

func TestLengthMismatchIsBatchError(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		return []string{"only-one"}, nil // 无论入参多长都只回一条
	}

	b := batcher.New[int, string](fn,
		batcher.WithMaxSize(4),
		batcher.WithInterval(20*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := b.Do(context.Background(), i)
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		err := <-errs
		if err == nil || !strings.Contains(err.Error(), "returned 1 results for 2 requests") {
			t.Fatalf("waiter error = %v, want length mismatch error", err)
		}
	}
}

func TestBatchFunctionPanicDegradesAndBatcherStaysUsable(t *testing.T) {
	var call atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		if call.Add(1) == 1 {
			panic("first call explodes")
		}
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(20*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	_, err := b.Do(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("panic-degraded error = %v, want contains %q", err, "panicked")
	}

	// panic 不得拖垮库：后续 Do 正常。
	if res, err := b.Do(context.Background(), 7); err != nil || res != 7 {
		t.Fatalf("Do after panic = (%v, %v), want (7, nil)", res, err)
	}
}

func TestCloseFlushesPendingItems(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Second), // 两条 Do 只能靠 Close 唤醒
	)

	results := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		go func(i int) {
			res, err := b.Do(context.Background(), i)
			if err != nil {
				t.Errorf("Do(%d): %v", i, err)
			}
			results <- res
		}(i)
	}
	time.Sleep(50 * time.Millisecond) // 确保两条都已入队组批

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := []int{<-results, <-results}
	sort.Ints(got)
	if fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("results = %v, want [1 2]", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1 (one final flush)", len(calls))
	}
}

func TestDoAfterCloseReturnsErrClosed(t *testing.T) {
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
	if _, err := b.Do(context.Background(), 1); !errors.Is(err, batcher.ErrClosed) {
		t.Fatalf("Do after Close = %v, want ErrClosed", err)
	}
}

func TestCloseIdempotentAndConcurrent(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("sequential second Close: %v", err)
	}
}

func TestCloseWithoutPendingSkipsBatchFunction(t *testing.T) {
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return append([]int(nil), reqs...), nil
	}
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("fn calls = %d, want 0 (no data, no flush)", n)
	}
}

func TestCloseTimeoutGivesUpButFlushCompletesInBackground(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		<-release // 模拟慢批次，Close 必然超时
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)

	resCh := make(chan int, 1)
	go func() {
		res, err := b.Do(context.Background(), 5)
		if err != nil {
			t.Errorf("Do: %v", err)
		}
		resCh <- res
	}()
	<-entered // 批次已 in-flight

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := b.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close timeout = %v, want DeadlineExceeded", err)
	}

	close(release) // 放行慢批次
	select {
	case res := <-resCh:
		if res != 5 {
			t.Fatalf("res = %d, want 5", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never resolved after background flush")
	}
}

func TestCallerCancelReturnsEarlyItemRetainedAndFnCtxUnaffected(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	var fnCtxErr error
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		fnCtxErr = ctx.Err() // 调用方取消不得波及批次函数的 ctx
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(10),
		batcher.WithInterval(300*time.Millisecond), // 窗口放宽：容纳屏障放行后的调度余量
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel) // 先入队、等待中取消（远早于 300ms flush）
	cancelledRes := make(chan error, 1)
	okRes := make(chan int, 1)
	release := launchTogether(2, func(i int) {
		if i == 0 {
			_, err := b.Do(ctx, 1)
			cancelledRes <- err
			return
		}
		res, err := b.Do(context.Background(), 2)
		if err != nil {
			t.Errorf("Do(2): %v", err)
		}
		okRes <- res
	})
	release()

	select {
	case err := <-cancelledRes:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter did not return early")
	}

	if res := <-okRes; res != 2 {
		t.Fatalf("surviving waiter res = %d, want 2", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	got := append([]int(nil), calls[0]...)
	sort.Ints(got)
	if fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("batch = %v, want [1 2]（取消的 Item 保留执行）", got)
	}
	if fnCtxErr != nil {
		t.Fatalf("fn ctx err = %v, want nil（基础 ctx 不受调用方取消影响）", fnCtxErr)
	}
}

func TestSingleInFlightBatch(t *testing.T) {
	var cur, maxSeen atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		n := cur.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 4
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		go func(i int) {
			res, err := b.Do(context.Background(), i)
			if err == nil && res != i {
				t.Errorf("Do(%d) = %d", i, res)
			}
			errs <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Do: %v", err)
		}
	}
	if m := maxSeen.Load(); m != 1 {
		t.Fatalf("max concurrent batch functions = %d, want 1", m)
	}
}

// WithMaxConcurrency：下游并发上限的硬界——同一时刻批次函数在途的
// 批次 ≤ n，且放宽后多批确实重叠执行（慢批次不再令后续批次空等）。
func TestMaxConcurrencyCapsDownstreamConcurrency(t *testing.T) {
	var cur, maxSeen atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		n := cur.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1), // 每条自成一批，纯流水线
		batcher.WithInterval(5*time.Millisecond),
		batcher.WithMaxConcurrency(4),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 8
	errs := make(chan error, n)
	release := launchTogether(n, func(i int) {
		res, err := b.Do(context.Background(), i+1)
		if err == nil && res != i+1 {
			t.Errorf("Do(%d) = %d", i+1, res)
		}
		errs <- err
	})
	release()
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Do: %v", err)
		}
	}
	// 重叠执行（≥2）且恒不越上限（槽位硬界，>4 即实现错误）。
	if m := maxSeen.Load(); m < 2 || m > 4 {
		t.Fatalf("max concurrent batch functions = %d, want 2..4", m)
	}
}

// WithMaxConcurrency + Close：优雅关闭等待全部在途执行 goroutine——
// n 批并行在途时 Close 阻塞至全部交付，正常返回即批次函数不再运行。
func TestMaxConcurrencyCloseWaitsAllInflightWorkers(t *testing.T) {
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		<-release
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(5*time.Millisecond),
		batcher.WithMaxConcurrency(3),
	)

	resCh := make(chan int, 3)
	for i := 1; i <= 3; i++ {
		go func(i int) {
			res, err := b.Do(context.Background(), i)
			if err != nil {
				t.Errorf("Do(%d): %v", i, err)
			}
			resCh <- res
		}(i)
	}
	<-entered
	<-entered
	<-entered // 三批并行在途

	closed := make(chan error, 1)
	go func() { closed <- b.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v while workers in flight, want blocked", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release) // 放行三批 → 全部交付 → Close 返回
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := []int{<-resCh, <-resCh, <-resCh}
	sort.Ints(got)
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("results = %v, want [1 2 3]", got)
	}
}

// identityFn 是最简单的批次函数：原样返回请求。
func identityFn(ctx context.Context, reqs []int) ([]int, error) {
	return append([]int(nil), reqs...), nil
}

// ADR-0008：组批（Formation）与执行重叠，执行开始时刻 =
// max(成批时刻, 槽位空出)。计时器仍自各批 Formation 起算——既不按
// 到达时刻补偿（ADR-0002 的论证保留），也不被空闲槽位提前。两个
// 子测试各钉一半：成批早于槽位空出 → 槽位空出即执行；成批晚于槽位
// 空出 → 等满自身 interval。串行默认（下游并发 1）下钉死该时序。
func TestNextBatchStartsWhenFlushedAndSlotBothReady(t *testing.T) {
	const interval = 100 * time.Millisecond

	setup := func(t *testing.T, firstLatency time.Duration) (mu *sync.Mutex, callStart, callEnd *[]time.Time, entered chan struct{}) {
		t.Helper()
		mu = &sync.Mutex{}
		started := []time.Time{}
		ended := []time.Time{}
		callStart, callEnd = &started, &ended
		entered = make(chan struct{}, 2)
		fn := func(ctx context.Context, reqs []int) ([]int, error) {
			mu.Lock()
			*callStart = append(*callStart, time.Now())
			mu.Unlock()
			entered <- struct{}{}
			if len(*callStart) == 1 {
				time.Sleep(firstLatency) // 第一批执行期间，第二批组批计时照常走
			}
			mu.Lock()
			*callEnd = append(*callEnd, time.Now())
			mu.Unlock()
			return append([]int(nil), reqs...), nil
		}

		b := batcher.New[int, int](fn,
			batcher.WithMaxSize(2), // 前两条按 size 立即 flush，与 interval 无关
			batcher.WithInterval(interval),
		)
		t.Cleanup(func() {
			if err := b.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})

		release := launchTogether(2, func(i int) {
			_, _ = b.Do(context.Background(), i+1) // 屏障确保同批：size=2 立即触发
		})
		release()
		<-entered // 第一批已开始执行

		go func() { _, _ = b.Do(context.Background(), 3) }() // 执行期到达，独自成批等 interval
		return mu, callStart, callEnd, entered
	}

	t.Run("slot-limits-starts-at-release", func(t *testing.T) {
		// 第一批慢执行（300ms > interval）：第二批在 ~interval 处成批、
		// 排队等槽位，第一批结束即执行——gap≈0，不追加 interval 等待。
		// 停泊 Item 在流水线下已成批（计时器执行期间走完），按到达
		// 时刻补偿无从谈起（ADR-0008）。
		mu, callStart, callEnd, entered := setup(t, 300*time.Millisecond)

		<-entered                         // 第二批已触发执行
		time.Sleep(50 * time.Millisecond) // 等第二批 callEnd 记录
		mu.Lock()
		defer mu.Unlock()
		if len(*callStart) != 2 {
			t.Fatalf("fn calls = %d, want 2", len(*callStart))
		}
		if gap := (*callStart)[1].Sub((*callEnd)[0]); gap >= 80*time.Millisecond {
			t.Fatalf("second batch started %v after first finished, want ≈0（成批先于槽位空出，槽位空出即执行）", gap)
		}
	})

	t.Run("flush-limits-waits-own-interval", func(t *testing.T) {
		// 第一批快执行（30ms < interval）：槽位在 ~30ms 即空出，第二批
		// 仍要等满自身 interval 才执行——不被空闲槽位提前。
		mu, callStart, callEnd, entered := setup(t, 30*time.Millisecond)

		<-entered                         // 第二批已触发执行
		time.Sleep(50 * time.Millisecond) // 等第二批 callEnd 记录
		mu.Lock()
		defer mu.Unlock()
		if len(*callStart) != 2 {
			t.Fatalf("fn calls = %d, want 2", len(*callStart))
		}
		if gap := (*callStart)[1].Sub((*callEnd)[0]); gap < 40*time.Millisecond {
			t.Fatalf("second batch started %v after first finished, want >= ~interval-30ms（成批晚于槽位空出，等满自身 interval）", gap)
		}
	})
}

// WithMaxPending：额度满员时新 Do 立即 ErrOverloaded；交付释放额度后
// 恢复准入，被卸载的调用不占用批次、不影响在途批交付。
func TestMaxPendingShedsWhenFull(t *testing.T) {
	m := newTestMeter(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		<-release
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxPending(2),
		batcher.WithMeter(m.meter),
	)

	d1 := make(chan int, 1)
	go func() {
		res, err := b.Do(context.Background(), 1)
		if err != nil {
			t.Errorf("Do(1): %v", err)
		}
		d1 <- res
	}()
	<-entered // D1 在途：额度 1/2

	d2 := make(chan int, 1)
	go func() {
		res, err := b.Do(context.Background(), 2)
		if err != nil {
			t.Errorf("Do(2): %v", err)
		}
		d2 <- res
	}()
	time.Sleep(50 * time.Millisecond) // D2 已准入、入队成批在执行入口排队：额度 2/2

	for _, id := range []int{3, 4} {
		res, err := b.Do(context.Background(), id)
		if !errors.Is(err, batcher.ErrOverloaded) {
			t.Fatalf("Do(%d) = (%d, %v), want ErrOverloaded", id, res, err)
		}
		if res != 0 {
			t.Fatalf("Do(%d) res = %d, want zero value", id, res)
		}
	}

	close(release) // D1 交付释放额度，D2 随即入批执行
	if res := <-d1; res != 1 {
		t.Fatalf("Do(1) = %d, want 1", res)
	}
	if res := <-d2; res != 2 {
		t.Fatalf("Do(2) = %d, want 2", res)
	}

	// 额度已释放：新提交恢复准入。
	if res, err := b.Do(context.Background(), 5); err != nil || res != 5 {
		t.Fatalf("Do(5) = (%d, %v), want (5, nil)", res, err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v := m.counterValue(t, "batcher_overloaded_total"); v != 2 {
		t.Fatalf("batcher_overloaded_total = %v, want 2", v)
	}
	if v := m.gaugeValue(t, "batcher_queue_depth"); v != 0 {
		t.Fatalf("queue_depth = %v, want 0（额度全量释放）", v)
	}
}

// 并发风暴：准入上限下每个 Do 必以「正确结果 / ErrOverloaded」收场，
// 绝不悬挂；卸载计数与实际拒绝数一致；额度最终全量释放。
func TestMaxPendingConcurrentSheddingStorm(t *testing.T) {
	m := newTestMeter(t)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		<-release // 首批阻塞在批次函数里，制造卸载窗口
		return append([]int(nil), reqs...), nil
	}

	const maxPending = 5
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(maxPending), // 满 5 条按 size 立即成批，不等 interval
		batcher.WithInterval(10*time.Second),
		batcher.WithMaxPending(maxPending),
		batcher.WithMeter(m.meter),
	)

	const n = 50
	outcomes := make(chan error, n)
	releaseGo := launchTogether(n, func(i int) {
		res, err := b.Do(context.Background(), i+1)
		if err == nil && res != i+1 {
			t.Errorf("Do(%d) = %d", i+1, res)
		}
		outcomes <- err
	})
	releaseGo()
	time.Sleep(200 * time.Millisecond) // 首批满员进入批次函数（阻塞），其余全部被卸载
	close(release)

	okCount := 0
	for i := 0; i < n; i++ {
		err := <-outcomes
		switch {
		case err == nil:
			okCount++
		case errors.Is(err, batcher.ErrOverloaded):
		default:
			t.Fatalf("越界错误: %v", err)
		}
	}
	if okCount < maxPending {
		t.Fatalf("ok = %d, want >= %d（首批必须成批进入批次函数）", okCount, maxPending)
	}
	if v := m.counterValue(t, "batcher_overloaded_total"); v != int64(n-okCount) {
		t.Fatalf("batcher_overloaded_total = %v, want %d（与实际拒绝数一致）", v, n-okCount)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v := m.gaugeValue(t, "batcher_queue_depth"); v != 0 {
		t.Fatalf("queue_depth = %v, want 0（额度全量释放）", v)
	}
}
