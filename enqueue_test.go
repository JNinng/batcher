package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// 约束：Enqueue 不阻塞等待结果——提交即返回，Item 与 Do 来源一视同仁
// 地组批执行；准入额度计入并在交付后全量清算。
func TestEnqueueSubmitsWithoutWaiting(t *testing.T) {
	m := newFakeMeter()
	var mu sync.Mutex
	var calls [][]string
	fn := func(ctx context.Context, lines []string) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]string(nil), lines...))
		return make([]int, len(lines)), nil
	}

	b := batcher.New[string, int](fn,
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Second), // 只能靠 Close flush
		batcher.WithMeter(m),
	)

	started := time.Now()
	for _, line := range []string{"a", "b", "c"} {
		if err := b.Enqueue(context.Background(), line); err != nil {
			t.Fatalf("Enqueue(%q): %v", line, err)
		}
	}
	// 提交即返回：远小于 interval，写路径不被下游耗时阻塞。
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Enqueue blocked %v, want immediate return", elapsed)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1（Close flush 未满批）", len(calls))
	}
	got := append([]string(nil), calls[0]...)
	sort.Strings(got)
	if fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("batch = %v, want [a b c]", got)
	}
	if v := m.gauges["batcher_queue_depth"].Value(); v != 0 {
		t.Fatalf("queue_depth = %v, want 0（Enqueue 计入准入并全量清算）", v)
	}
	// wait_seconds 对 Enqueue 条目同样计费：每条一次（入队 → 下游写出）。
	if vs := m.histogram["batcher_wait_seconds"].Values(); len(vs) != 3 {
		t.Fatalf("wait_seconds = %d 次, want 3（Enqueue 条目同口径计费）", len(vs))
	}
	// Close 后新提交必得 ErrClosed（确定性路径）。
	if err := b.Enqueue(context.Background(), "x"); !errors.Is(err, batcher.ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
}

// observerFunc 以函数字段实现 Observer，测试里按需注入单个事件。
type observerFunc struct {
	onFlushed func(batcher.BatchFlushedEvent)
}

func (f observerFunc) OnCreated(batcher.CreatedEvent) {}

func (f observerFunc) OnFlushStart(batcher.FlushStartEvent) {}

func (f observerFunc) OnBatchFlushed(e batcher.BatchFlushedEvent) {
	if f.onFlushed != nil {
		f.onFlushed(e)
	}
}

func (f observerFunc) OnItemDiscarded(batcher.ItemDiscardedEvent) {}

func (f observerFunc) OnClosed(batcher.ClosedEvent) {}

// 约束（EnqueueFunc 回调）：交付点同步执行、先于该批 OnBatchFlushed；
// 成功收 (值, nil)、批级失败收 (零值, Batch Error)，与同批 Do 口径一致。
func TestEnqueueFuncCallbackReceivesOutcomeAtDelivery(t *testing.T) {
	boom := errors.New("downstream down")
	var mu sync.Mutex
	var seq []string // 事件序：cb 先于 flushed

	obs := observerFunc{
		onFlushed: func(batcher.BatchFlushedEvent) {
			mu.Lock()
			seq = append(seq, "flushed")
			mu.Unlock()
		},
	}
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		if reqs[0] == 3 {
			return nil, boom
		}
		res := make([]string, len(reqs))
		for i, r := range reqs {
			res[i] = fmt.Sprintf("u%d", r)
		}
		return res, nil
	}

	b := batcher.New[int, string](fn,
		batcher.WithMaxSize(1), // 每条一个批次，交付序确定
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(obs),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	type outcome struct {
		res string
		err error
	}
	outcomes := make(chan outcome, 2)
	record := func(res string, err error) {
		mu.Lock()
		seq = append(seq, "cb")
		mu.Unlock()
		outcomes <- outcome{res, err}
	}

	if err := b.EnqueueFunc(context.Background(), 2, record); err != nil {
		t.Fatalf("EnqueueFunc(2): %v", err)
	}
	if err := b.EnqueueFunc(context.Background(), 3, record); err != nil {
		t.Fatalf("EnqueueFunc(3): %v", err)
	}

	o := <-outcomes
	if o.res != "u2" || o.err != nil {
		t.Fatalf("callback(2) = (%q, %v), want (u2, nil)", o.res, o.err)
	}
	o = <-outcomes
	if o.res != "" || !errors.Is(o.err, boom) {
		t.Fatalf("callback(3) = (%q, %v), want (零值, Batch Error boom)", o.res, o.err)
	}
	// 同批口径对照：Do 收到的结果与回调一致。
	if res, err := b.Do(context.Background(), 4); err != nil || res != "u4" {
		t.Fatalf("Do(4) = (%q, %v), want (u4, nil)", res, err)
	}
	// Close 等 dispatcher 退出后，flushed 事件全部落定，事件序可全量断言。
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(seq) != "[cb flushed cb flushed flushed]" {
		t.Fatalf("事件序 = %v, want 回调先于所在批的 OnBatchFlushed", seq)
	}
}

// 约束：回调 panic 不得改变交付语义、不得拖垮库。
func TestEnqueueFuncCallbackPanicRecovered(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		res := make([]string, len(reqs))
		for i, r := range reqs {
			res[i] = fmt.Sprintf("u%d", r)
		}
		return res, nil
	}

	b := batcher.New[int, string](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	if err := b.EnqueueFunc(context.Background(), 1, func(res string, err error) {
		panic("callback explodes")
	}); err != nil {
		t.Fatalf("EnqueueFunc: %v", err)
	}

	// panic 不得拖垮库：后续 Do 正常。
	if res, err := b.Do(context.Background(), 2); err != nil || res != "u2" {
		t.Fatalf("Do after callback panic = (%q, %v), want (u2, nil)", res, err)
	}
}

// 约束：WithMaxPending 对 Enqueue 一视同仁——额度满员即卸载拒绝，
// 且与 Do 同口径计入 batcher_overloaded_total。
func TestEnqueueShedsWhenFull(t *testing.T) {
	m := newFakeMeter()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		<-release
		return append([]int(nil), reqs...), nil
	}

	b := batcher.New[int, int](fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxPending(1),
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
	<-entered // Do(1) 在途：额度 1/1

	if err := b.Enqueue(context.Background(), 2); !errors.Is(err, batcher.ErrOverloaded) {
		t.Fatalf("Enqueue(2) = %v, want ErrOverloaded", err)
	}
	if v := m.counters["batcher_overloaded_total"].Value(); v != 1 {
		t.Fatalf("batcher_overloaded_total = %v, want 1（Enqueue 卸载同口径计费）", v)
	}

	close(release)
	if res := <-d1; res != 1 {
		t.Fatalf("Do(1) = %d, want 1", res)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// 约束：EnqueueFunc 叠加批内去重——重复 Item 各得一回调、共享同一条
// 结果；NewKeyedMap 下缺 key 经回调以 Result.Err 表达，批级 err 仍 nil。
func TestEnqueueFuncCallbackPerDuplicateWithKeyed(t *testing.T) {
	t.Run("newkeyed-dedup", func(t *testing.T) {
		var mu sync.Mutex
		var calls [][]int
		fn := func(ctx context.Context, ids []int) ([]string, error) {
			mu.Lock()
			calls = append(calls, append([]int(nil), ids...))
			mu.Unlock()
			res := make([]string, len(ids))
			for i, id := range ids {
				res[i] = fmt.Sprintf("u%d", id)
			}
			return res, nil
		}

		b := batcher.NewKeyed(func(id int) int { return id }, fn,
			batcher.WithMaxSize(4),
			batcher.WithInterval(10*time.Second), // 4 条原始占满即触发
		)
		defer func() {
			if err := b.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}()

		ids := []int{7, 42, 7, 9} // 7 重复：4 条原始折叠为 3 条 unique
		results := make([]string, len(ids))
		var wg sync.WaitGroup
		wg.Add(len(ids))
		for i, id := range ids {
			if err := b.EnqueueFunc(context.Background(), id, func(res string, err error) {
				defer wg.Done()
				if err != nil {
					t.Errorf("callback(%d) err = %v, want nil", id, err)
				}
				results[i] = res // 每个提交恰一回调，i 唯一
			}); err != nil {
				t.Fatalf("EnqueueFunc(%d): %v", id, err)
			}
		}
		wg.Wait()

		// 重复 Item 各得一回调且共享同一条结果。
		if results[0] != "u7" || results[2] != "u7" {
			t.Errorf("重复 key 回调结果 = %q / %q, want 均 u7", results[0], results[2])
		}
		if results[1] != "u42" || results[3] != "u9" {
			t.Errorf("unique 回调结果 = %q / %q, want u42 / u9", results[1], results[3])
		}
		mu.Lock()
		defer mu.Unlock()
		if len(calls) != 1 {
			t.Fatalf("fn calls = %d, want 1", len(calls))
		}
		got := append([]int(nil), calls[0]...)
		sort.Ints(got)
		if fmt.Sprint(got) != "[7 9 42]" {
			t.Fatalf("unique reqs = %v, want [7 9 42]", got)
		}
	})

	t.Run("keyedmap-missing-via-result", func(t *testing.T) {
		mapFn := func(ctx context.Context, ids []int) (map[int]string, error) {
			return map[int]string{42: "u42"}, nil // id 7 缺失
		}

		b := batcher.NewKeyedMap(func(id int) int { return id }, mapFn,
			batcher.WithMaxSize(2),
			batcher.WithInterval(10*time.Second),
		)
		defer func() {
			if err := b.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}()

		type cbOut struct {
			r   batcher.Result[string]
			err error
		}
		out := make(chan cbOut, 2)
		for _, id := range []int{42, 7} {
			if err := b.EnqueueFunc(context.Background(), id, func(r batcher.Result[string], err error) {
				out <- cbOut{r, err}
			}); err != nil {
				t.Fatalf("EnqueueFunc(%d): %v", id, err)
			}
		}
		first, second := <-out, <-out

		missing := 0
		for _, o := range []cbOut{first, second} {
			if o.err != nil {
				t.Errorf("批级 err = %v, want nil（缺 key 不是批级失败）", o.err)
			}
			switch {
			case errors.Is(o.r.Err, batcher.ErrMissingResult):
				missing++
			case o.r.Err == nil && o.r.Value == "u42":
			default:
				t.Errorf("意外回调结果: %+v", o.r)
			}
		}
		if missing != 1 {
			t.Errorf("缺 key 回调 = %d 条, want 1", missing)
		}
	})
}
