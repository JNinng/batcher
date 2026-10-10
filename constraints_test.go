package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// 本文件钉住功能约束（ADR/CONTEXT 语义的边界与不变量），
// 与 batcher_test.go 的行为测试互补。

// 约束：Do 与 Close 并发对撞时，每个 Do 必须以
// 「正确结果 / ErrClosed / ctx.Err」之一收场，绝不悬挂、绝不错配。
func TestCloseRaceWithInflightDos(t *testing.T) {
	fn := identityFn
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(5*time.Millisecond),
	)

	const n = 50
	outcomes := make(chan int, n) // 0=正确结果 1=ErrClosed 2=其他错误
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := b.Do(context.Background(), i)
			switch {
			case err == nil && res == i:
				outcomes <- 0
			case errors.Is(err, batcher.ErrClosed):
				outcomes <- 1
			default:
				t.Errorf("Do(%d) = (%d, %v)：越界结果", i, res, err)
				outcomes <- 2
			}
		}(i)
	}
	// 与 Do 风暴并发触发关闭。
	time.Sleep(5 * time.Millisecond)
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	close(outcomes)

	counts := [3]int{}
	for o := range outcomes {
		counts[o]++
	}
	if counts[0]+counts[1] != n {
		t.Fatalf("outcomes = 结果:%d ErrClosed:%d 越界:%d", counts[0], counts[1], counts[2])
	}
	// Close 完成后新 Do 必得 ErrClosed（确定性路径）。
	if _, err := b.Do(context.Background(), 0); !errors.Is(err, batcher.ErrClosed) {
		t.Fatalf("Do after Close = %v, want ErrClosed", err)
	}
}

// 约束：持续扇入形成多批时，每条 Item 按位置拿到自己的结果。
func TestManyBatchesPositionalIntegrity(t *testing.T) {
	var calls atomic.Int64
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		calls.Add(1)
		res := make([]string, len(reqs))
		for i, r := range reqs {
			res[i] = fmt.Sprintf("u%d", r)
		}
		return res, nil
	}

	b := batcher.New[int, string](fn,
		batcher.WithMaxSize(10),
		batcher.WithInterval(5*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 100
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		go func(i int) {
			res, err := b.Do(context.Background(), i)
			if err == nil && res != fmt.Sprintf("u%d", i) {
				err = fmt.Errorf("Do(%d) = %q", i, res)
			}
			errs <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("waiter: %v", err)
		}
	}
	// maxSize=10、interval=5ms：100 条必然产生多个批次。
	if c := calls.Load(); c < 2 {
		t.Fatalf("fn calls = %d, want multiple batches", c)
	}
}

// 约束（ADR-0003）：取消风暴下 Item 全部保留执行、批次不变形，
// 且准入额度全量清算（queue_depth 归零——准入口径记账回归钉子）。
func TestCancelledWaitersStillExecuteAndPendingDrainsToZero(t *testing.T) {
	m := newFakeMeter()
	var mu sync.Mutex
	var batches [][]int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		batches = append(batches, append([]int(nil), reqs...))
		return append([]int(nil), reqs...), nil
	}

	const interval = 500 * time.Millisecond // 组批窗口放宽：屏障放行后仍需容纳调度余量
	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(100), // 只靠 interval 触发，制造充足取消窗口
		batcher.WithInterval(interval),
		batcher.WithMeter(m),
	)

	const n = 20
	results := make(chan int, n)
	cancelled := make(chan int, n)
	release := launchTogether(n, func(i int) {
		id := i + 1
		ctx, cancel := context.WithCancel(context.Background())
		if id%2 == 0 {
			time.AfterFunc(30*time.Millisecond, cancel) // 等待中取消
		}
		res, err := b.Do(ctx, id)
		cancel()
		switch {
		case err == nil:
			if res != id {
				t.Errorf("Do(%d) = %d", id, res)
			}
			results <- id
		case errors.Is(err, context.Canceled):
			cancelled <- id
		default:
			t.Errorf("Do(%d) err = %v", id, err)
		}
	})
	release()
	for resolved := 0; resolved < n; {
		select {
		case <-results:
			resolved++
		case <-cancelled:
			resolved++
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 1 {
		t.Fatalf("batches = %d, want 1（interval 内全部同批）", len(batches))
	}
	// 取消的 Item 保留执行：批内 20 条一条不少、无重复。
	got := append([]int(nil), batches[0]...)
	sort.Ints(got)
	if len(got) != n || got[0] != 1 || got[n-1] != n {
		t.Fatalf("batch = %d 条（首 %d 末 %d）, want %d 条完整批次", len(got), got[0], got[n-1], n)
	}
	if v := m.gauges["batcher_queue_depth"].Value(); v != 0 {
		t.Fatalf("queue_depth = %v, want 0（准入额度全量清算）", v)
	}
	if n := len(m.histogram["batcher_wait_seconds"].Values()); n != 20 {
		t.Fatalf("wait_seconds observations = %d, want 20（取消的 Item 也交付计费）", n)
	}
}

// 约束（准入回滚）：停泊在无缓冲握手上的提交被调用方取消时，准入
// 额度必须回滚——否则额度泄漏会让后续提交在未满载时被误卸载。
func TestCancelledWhileParkedReleasesAdmissionQuota(t *testing.T) {
	m := newFakeMeter()
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
	<-entered // D1 在途：额度 1/2，dispatcher 忙于批次函数，inbox 无人接收

	ctx, cancel := context.WithCancel(context.Background())
	parked := make(chan error, 1)
	go func() {
		_, err := b.Do(ctx, 2)
		parked <- err
	}()
	time.Sleep(50 * time.Millisecond) // D2 已准入、停泊在握手上：额度 2/2
	cancel()
	if err := <-parked; !errors.Is(err, context.Canceled) {
		t.Fatalf("parked Do err = %v, want context.Canceled", err)
	}

	// 额度须已回滚至 1/2：D3 可准入（回滚缺失则此处被误卸载）。
	d3 := make(chan int, 1)
	go func() {
		res, err := b.Do(context.Background(), 3)
		if err != nil {
			t.Errorf("Do(3): %v", err)
			d3 <- -1
			return
		}
		d3 <- res
	}()

	close(release) // D1、D3 依次成批执行并交付
	if res := <-d1; res != 1 {
		t.Fatalf("Do(1) = %d, want 1", res)
	}
	if res := <-d3; res != 3 {
		t.Fatalf("Do(3) = %d, want 3", res)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v := m.gauges["batcher_queue_depth"].Value(); v != 0 {
		t.Fatalf("queue_depth = %v, want 0（取消与交付路径额度均清算）", v)
	}
}

// 约束（准入口径 gauge）：queue_depth 含停泊在握手上的发送方——
// 首批阻塞在批次函数内、另两条停泊时，首批批边界必报 2（在途 1
// 已交付递减 + 停泊 2）。接收侧口径对这部分积压构造性失明，恒报 0。
func TestQueueDepthGaugeIncludesParkedSenders(t *testing.T) {
	m := newFakeMeter()
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		<-release
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxPending(3),
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
	<-entered // D1 在途且阻塞在批次函数内：首批批边界的 gauge 尚未刷新

	parked := make(chan int, 2)
	for _, id := range []int{2, 3} {
		go func(id int) {
			res, err := b.Do(context.Background(), id)
			if err != nil {
				t.Errorf("Do(%d): %v", id, err)
			}
			parked <- res
		}(id)
	}
	time.Sleep(50 * time.Millisecond) // D2、D3 已准入、停泊在握手上

	close(release) // D1 交付递减后批边界 Set(2)；停泊两条串行执行 → Set(1)、Set(0)
	if res := <-d1; res != 1 {
		t.Fatalf("Do(1) = %d, want 1", res)
	}
	// 先停泊者先交付，但两条 goroutine 的调度顺序不定，按集合断言。
	parkedGot := []int{<-parked, <-parked}
	sort.Ints(parkedGot)
	if parkedGot[0] != 2 || parkedGot[1] != 3 {
		t.Fatalf("parked results = %v, want [2 3]", parkedGot)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 串行单在途：三个批边界按序 2 → 1 → 0，停泊发送方计入 gauge。
	got := m.gauges["batcher_queue_depth"].History()
	if fmt.Sprint(got) != "[2 1 0]" {
		t.Fatalf("queue_depth history = %v, want [2 1 0]（停泊发送方计入 gauge）", got)
	}
}
