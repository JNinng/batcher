package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// retryable 包装哨兵错误：模拟下游瞬时 503。
func retryable(msg string) error {
	return fmt.Errorf("downstream 503 (%s): %w", msg, batcher.ErrRetryable)
}

// 约束（哨兵重试）：errors.Is(err, ErrRetryable) 的批级错误整批按原
// reqs 重执行，成功后 Waiter 正常收结果；重试次数计入事件 Retries
// 与 batcher_batch_retries_total（不含首次执行）。
func TestRetryReplaysBatchOnSentinelError(t *testing.T) {
	m := newFakeMeter()
	obs := &recordingObserver{}
	var mu sync.Mutex
	var calls [][]int
	var attempts atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		calls = append(calls, append([]int(nil), reqs...))
		mu.Unlock()
		if attempts.Add(1) <= 2 {
			return nil, retryable(fmt.Sprintf("attempt-%d", attempts.Load()))
		}
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Second), // 两条占满即触发，隔离 interval
		batcher.WithRetry(3, func(attempt int) time.Duration { return time.Millisecond }),
		batcher.WithMeter(m),
		batcher.WithObserver(obs),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	done := make(chan error, 2)
	release := launchTogether(2, func(i int) {
		res, err := b.Do(context.Background(), i+1)
		if err != nil {
			t.Errorf("Do(%d): %v", i+1, err)
		} else if res != i+1 {
			t.Errorf("Do(%d) = %d", i+1, res)
		}
		done <- err
	})
	release()
	for i := 0; i < 2; i++ {
		<-done
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("fn calls = %d, want 3（两次失败整批重放 + 一次成功）", len(calls))
	}
	for i, reqs := range calls {
		if got := fmt.Sprint(reqs); len(reqs) != 2 {
			t.Fatalf("calls[%d] = %v, want 整批重放（原 reqs，不缩水）", i, got)
		}
	}
	if v := m.counters["batcher_batch_retries_total"].Value(); v != 2 {
		t.Fatalf("batcher_batch_retries_total = %v, want 2（不含首次执行）", v)
	}
	if v := m.counters["batcher_batch_total"].Value(); v != 1 {
		t.Fatalf("batcher_batch_total = %v, want 1（重试不另计批次数）", v)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.flushed) != 1 || obs.flushed[0].Retries != 2 || obs.flushed[0].Err != nil {
		t.Fatalf("flushed = %+v, want 恰一事件 {Retries:2 Err:nil}", obs.flushed)
	}
}

// 约束（耗尽广播）：重试 n 次仍失败即最后一次错误广播给全部 Waiter，
// batch_failed_total 计一次（终态一次，非每次尝试）。
func TestRetryExhaustionBroadcastsLastError(t *testing.T) {
	m := newFakeMeter()
	boom := retryable("always-broken")
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return nil, boom
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithRetry(2, func(attempt int) time.Duration { return time.Millisecond }),
		batcher.WithMeter(m),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	if _, err := b.Do(context.Background(), 1); !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want 耗尽后广播 boom", err)
	}
	if c := calls.Load(); c != 3 {
		t.Fatalf("fn calls = %d, want 3（首次 + 重试 2 次）", c)
	}
	if v := m.counters["batcher_batch_retries_total"].Value(); v != 2 {
		t.Fatalf("batcher_batch_retries_total = %v, want 2", v)
	}
	if v := m.counters["batcher_batch_failed_total"].Value(); v != 1 {
		t.Fatalf("batcher_batch_failed_total = %v, want 1（终态一次）", v)
	}
}

// 约束（非哨兵不重试）：不携带 ErrRetryable 的批级错误照常广播，
// 不重试——默认语义不变。
func TestNonRetryableErrorNotRetried(t *testing.T) {
	boom := errors.New("permanent failure")
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return nil, boom
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithRetry(3, func(attempt int) time.Duration { return time.Millisecond }),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	if _, err := b.Do(context.Background(), 1); !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want boom", err)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("fn calls = %d, want 1（非哨兵错误不重试）", c)
	}
}

// 约束（未配置不重试）：错误携带 ErrRetryable 但未配置 WithRetry 时
// 只执行一次——哨兵本身不启用重试。
func TestRetryableErrorWithoutOptionNotRetried(t *testing.T) {
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return nil, retryable("no option configured")
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

	if _, err := b.Do(context.Background(), 1); !errors.Is(err, batcher.ErrRetryable) {
		t.Fatalf("Do err = %v, want ErrRetryable 广播", err)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("fn calls = %d, want 1（未配置 WithRetry 不重试）", c)
	}
}

// 约束（退避语义）：backoff 以重试序号调用（首个为 1）并原地 sleep，
// 重试间隔不小于退避之和。
func TestRetryBackoffWaitsBetweenAttemptsWithSequenceNumbers(t *testing.T) {
	var calls atomic.Int64
	var mu sync.Mutex
	var seen []int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		if calls.Add(1) <= 2 {
			return nil, retryable("flaky")
		}
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithRetry(3, func(attempt int) time.Duration {
			mu.Lock()
			seen = append(seen, attempt)
			mu.Unlock()
			return 50 * time.Millisecond
		}),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	started := time.Now()
	if _, err := b.Do(context.Background(), 1); err != nil {
		t.Fatalf("Do: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(seen) != "[1 2]" {
		t.Fatalf("backoff attempts = %v, want [1 2]（首个退避序号为 1）", seen)
	}
	if elapsed := time.Since(started); elapsed < 90*time.Millisecond {
		t.Fatalf("elapsed = %v, want >= ~两次退避之和 100ms", elapsed)
	}
}

// 约束（退避 panic）：降级为批级错误、批在此终止（不再重试），库
// 继续可用——与 size 函数 panic 同款约束。
func TestRetryBackoffPanicDegradesToBatchError(t *testing.T) {
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return nil, retryable("flaky")
	}

	m := newFakeMeter()
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithRetry(3, func(attempt int) time.Duration {
			panic("backoff explodes")
		}),
		batcher.WithMeter(m),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	_, err := b.Do(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "backoff function panicked") {
		t.Fatalf("Do err = %v, want backoff panic 降级的批级错误", err)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("fn calls = %d, want 1（退避失败后批终止）", c)
	}
	// panic 不得拖垮库：后续批次照常执行并独立失败（错误按批交付，
	// 进程不崩）。
	if _, err := b.Do(context.Background(), 2); err == nil || !strings.Contains(err.Error(), "backoff function panicked") {
		t.Fatalf("Do after backoff panic = %v, want 同款批级错误（库继续可用）", err)
	}
	if c := calls.Load(); c != 2 {
		t.Fatalf("fn calls = %d, want 2（后续批独立走完整重试路径）", c)
	}
	// 计数口径：退避失败的尝试未执行批次函数，不计入重试数。
	if v := m.counters["batcher_batch_retries_total"].Value(); v != 0 {
		t.Fatalf("batcher_batch_retries_total = %v, want 0（退避失败不计重试）", v)
	}
}

// 约束（退避负数）：负退避是程序错误，显式批级失败（与 size 函数
// 负数同款），批不再重试。
func TestRetryBackoffNegativeDegradesToBatchError(t *testing.T) {
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		return nil, retryable("flaky")
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithRetry(3, func(attempt int) time.Duration { return -time.Second }),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	_, err := b.Do(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "negative duration") {
		t.Fatalf("Do err = %v, want 负退避批级错误", err)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("fn calls = %d, want 1（退避失败后批终止）", c)
	}
}

// 约束（持槽退避交互，TODO 警示钉死）：重试退避占用在途槽位——
// MaxConcurrency=1 时退避期间后续批次无法执行，与 WithMaxPending
// 叠加即在退避窗口集中卸载（ErrOverloaded）。
func TestRetryBackoffHoldsSlotAndShedsDuringWindow(t *testing.T) {
	m := newFakeMeter()
	var attempts atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		if attempts.Add(1) == 1 {
			return nil, retryable("first attempt fails") // 触发 150ms 退避窗口
		}
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxConcurrency(1),
		batcher.WithMaxPending(2),
		batcher.WithRetry(2, func(attempt int) time.Duration { return 150 * time.Millisecond }),
		batcher.WithMeter(m),
	)

	d1 := make(chan int, 1)
	go func() {
		res, err := b.Do(context.Background(), 1)
		if err != nil {
			t.Errorf("Do(1): %v", err)
		}
		d1 <- res
	}()
	time.Sleep(50 * time.Millisecond) // D1 首次尝试失败、进入退避窗口（持槽）

	d2 := make(chan int, 1)
	go func() {
		res, err := b.Do(context.Background(), 2)
		if err != nil {
			t.Errorf("Do(2): %v", err)
		}
		d2 <- res
	}()
	time.Sleep(50 * time.Millisecond) // D2 已准入、成批后在执行入口排队等槽

	// 退避窗口内 D3 被集中卸载（D1 在途 + D2 已准入 = 2/2）。
	if _, err := b.Do(context.Background(), 3); !errors.Is(err, batcher.ErrOverloaded) {
		t.Fatalf("Do(3) during backoff window = %v, want ErrOverloaded（退避窗口集中卸载）", err)
	}

	if res := <-d1; res != 1 {
		t.Fatalf("Do(1) = %d, want 1（退避后重试成功）", res)
	}
	if res := <-d2; res != 2 {
		t.Fatalf("Do(2) = %d, want 2（退避释放槽位后执行）", res)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v := m.counters["batcher_overloaded_total"].Value(); v != 1 {
		t.Fatalf("batcher_overloaded_total = %v, want 1", v)
	}
}

// 约束（构造校验）：WithRetry 负值、n > 0 而 backoff 缺失属程序员
// 工错，构造期 panic。
func TestWithRetryPanicsOnInvalidConfig(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	cases := []struct {
		name string
		opts []batcher.Option
	}{
		{"negative retries", []batcher.Option{
			batcher.WithRetry(-1, func(int) time.Duration { return 0 }), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
		{"retries without backoff", []batcher.Option{
			batcher.WithRetry(2, nil), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
		{"zero retries with backoff", []batcher.Option{
			batcher.WithRetry(0, func(int) time.Duration { return 0 }), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("New did not panic")
				}
			}()
			b := batcher.New[int, int](fn, tc.opts...)
			// 若未 panic 也必须能安全关闭，避免泄漏 goroutine。
			if err := b.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}
