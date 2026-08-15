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
// 且 pending 全量清算（queue_depth 归零——接收侧记账回归钉子）。
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
		t.Fatalf("queue_depth = %v, want 0（pending 全量清算）", v)
	}
	if n := len(m.histogram["batcher_wait_seconds"].Values()); n != 20 {
		t.Fatalf("wait_seconds observations = %d, want 20（取消的 Item 也交付计费）", n)
	}
}
