package sharded_test

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
	"github.com/jninng/batcher/sharded"
)

// recordingObserver 记录生命周期事件计数（回调必须并发安全：n 个分片
// 并发回调同一 Observer）。
type recordingObserver struct {
	mu      sync.Mutex
	created int
	closed  int
}

func (o *recordingObserver) OnCreated(batcher.CreatedEvent) {
	o.mu.Lock()
	o.created++
	o.mu.Unlock()
}

func (o *recordingObserver) OnBatchFlushed(batcher.BatchFlushedEvent) {}

func (o *recordingObserver) OnClosed(batcher.ClosedEvent) {
	o.mu.Lock()
	o.closed++
	o.mu.Unlock()
}

func (o *recordingObserver) counts() (created, closed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.created, o.closed
}

// echoFn 原样回显请求。
func echoFn(ctx context.Context, reqs []int) ([]int, error) {
	return append([]int(nil), reqs...), nil
}

// mustClose 关闭分片 Batcher，失败即致命。
func mustClose[Req, Res any](t *testing.T, s *sharded.Sharded[Req, Res]) {
	t.Helper()
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// 约束（路由对位 + 工厂 shard 下标）：每条请求路由到 route 指定的
// 分片；自定义工厂以 shard 下标包装批次函数（cluster/分表选连接的
// 挂点），可观测每批落在哪个分片。
func TestRoutesEachRequestToItsShard(t *testing.T) {
	const n = 4
	var mu sync.Mutex
	seen := map[int][]int{} // shard → 该分片批次函数收到的 reqs

	factory := func(shard int) *batcher.Batcher[int, int] {
		// cluster 模式：包装 fn 给每批打分片标——下游按 shard 选连接。
		tagged := func(ctx context.Context, reqs []int) ([]int, error) {
			mu.Lock()
			seen[shard] = append(seen[shard], reqs...)
			mu.Unlock()
			return echoFn(ctx, reqs)
		}
		return batcher.New(tagged,
			batcher.WithMaxSize(1), // 每条自成一批，分片归属可精确断言
			batcher.WithInterval(5*time.Millisecond),
		)
	}

	s := sharded.New(func(id int) int { return id % n }, n, factory)
	defer mustClose(t, s)

	const total = 32
	var wg sync.WaitGroup
	for id := 0; id < total; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := s.Do(context.Background(), id)
			if err != nil || res != id {
				t.Errorf("Do(%d) = (%d, %v)", id, res, err)
			}
		}(id)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	covered := 0
	for shard, reqs := range seen {
		for _, r := range reqs {
			if r%n != shard {
				t.Fatalf("req %d 落在分片 %d, want %d（路由错位）", r, shard, r%n)
			}
		}
		covered += len(reqs)
	}
	if covered != total {
		t.Fatalf("covered = %d, want %d（有请求未被任何分片执行）", covered, total)
	}
}

// 约束（工厂契约）：New 对 [0, n) 每个下标恰调一次工厂，shard 下标
// 逐一分发。
func TestFactoryReceivesEachShardIndexOnce(t *testing.T) {
	const n = 3
	var mu sync.Mutex
	var got []int
	factory := func(shard int) *batcher.Batcher[int, int] {
		mu.Lock()
		got = append(got, shard)
		mu.Unlock()
		return batcher.New(echoFn, batcher.WithMaxSize(1), batcher.WithInterval(time.Millisecond))
	}

	s := sharded.New(func(id int) int { return 0 }, n, factory)
	mustClose(t, s)

	mu.Lock()
	defer mu.Unlock()
	sort.Ints(got)
	if fmt.Sprint(got) != "[0 1 2]" {
		t.Fatalf("shards = %v, want [0 1 2]", got)
	}
}

// 约束（NewKeyedFactory 组合）：route 与 key 对齐时批内去重跨请求
// 生效；不对齐时同 key 落入不同分片、跨片重复——去重失效属用户
// 责任（契约钉子）。
func TestKeyedFactoryComposesWithSharding(t *testing.T) {
	t.Run("aligned-route-and-key", func(t *testing.T) {
		var calls atomic.Int64
		fn := func(ctx context.Context, ids []int) ([]string, error) {
			calls.Add(1)
			res := make([]string, len(ids))
			for i, id := range ids {
				res[i] = fmt.Sprintf("u%d", id)
			}
			return res, nil
		}
		key := func(id int) int { return id % 4 }

		s := sharded.New(key, 4, sharded.NewKeyedFactory(key, fn,
			batcher.WithMaxSize(100),
			batcher.WithInterval(300*time.Millisecond), // 窗口放宽：风暴全部同批
		))
		defer mustClose(t, s)

		const n = 8
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := s.Do(context.Background(), 7) // 同 key 同分片
				if err != nil || res != "u7" {
					t.Errorf("Do(7) = (%q, %v)", res, err)
				}
			}()
		}
		wg.Wait()
		if c := calls.Load(); c != 1 {
			t.Fatalf("fn calls = %d, want 1（同 key 折叠为一条执行）", c)
		}
	})

	t.Run("misaligned-route-and-key", func(t *testing.T) {
		var mu sync.Mutex
		var calls [][]int
		fn := func(ctx context.Context, ids []int) ([]int, error) {
			mu.Lock()
			calls = append(calls, append([]int(nil), ids...))
			mu.Unlock()
			return append([]int(nil), ids...), nil
		}
		key := func(id int) int { return id % 3 } // key(3) == key(6) == 0

		s := sharded.New(func(id int) int { return id % 2 }, 2, sharded.NewKeyedFactory(key, fn,
			batcher.WithMaxSize(100),
			batcher.WithInterval(10*time.Second), // 只能靠 Close flush
		))

		var wg sync.WaitGroup
		for _, id := range []int{3, 6} { // 同 key 0，分片 1 与 0：跨片重复
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				if _, err := s.Do(context.Background(), id); err != nil {
					t.Errorf("Do(%d): %v", id, err)
				}
			}(id)
		}
		wg.Wait()
		mustClose(t, s)

		mu.Lock()
		defer mu.Unlock()
		if len(calls) != 2 {
			t.Fatalf("fn calls = %d, want 2（route 与 key 不一致 → 跨片重复、去重失效）", len(calls))
		}
	})
}

// 约束（NewKeyedMapFactory 组合）：分片 × 批内去重 × map 回查三层
// 叠加——缺 key 经 Result.Err 逐条表达，非整批失败。
func TestKeyedMapFactoryComposesWithSharding(t *testing.T) {
	key := func(id int) int { return id % 4 }
	mapFn := func(ctx context.Context, ids []int) (map[int]string, error) {
		m := make(map[int]string, len(ids))
		for _, id := range ids {
			k := key(id) // map 按 key 索引（同 key ⇒ 语义等价），非原始 id
			if k == 1 {
				continue // 模拟 mget 缺 key（13 → key 1）
			}
			m[k] = fmt.Sprintf("u%d", id)
		}
		return m, nil
	}

	s := sharded.New(key, 4, sharded.NewKeyedMapFactory(key, mapFn,
		batcher.WithMaxSize(100),
		batcher.WithInterval(300*time.Millisecond), // 窗口放宽：全部同批
	))
	defer mustClose(t, s)

	ids := []int{13, 7, 7, 42} // 13 缺 key，7 重复；同 key ⇒ 同分片
	var wg sync.WaitGroup
	resByID := make([]batcher.Result[string], len(ids))
	for i, id := range ids {
		wg.Add(1)
		go func(i, id int) {
			defer wg.Done()
			r, err := s.Do(context.Background(), id)
			if err != nil {
				t.Errorf("Do(%d) batch err = %v, want nil", id, err)
			}
			resByID[i] = r
		}(i, id)
	}
	wg.Wait()

	if !errors.Is(resByID[0].Err, batcher.ErrMissingResult) {
		t.Fatalf("Do(13) = %+v, want ErrMissingResult（缺 key 逐条表达）", resByID[0])
	}
	if resByID[1].Value != "u7" || resByID[2].Value != "u7" {
		t.Fatalf("重复 key 结果 = %q / %q, want 均 u7", resByID[1].Value, resByID[2].Value)
	}
	if resByID[3].Value != "u42" || resByID[3].Err != nil {
		t.Fatalf("Do(42) = %+v, want {u42 nil}", resByID[3])
	}
}

// 约束（分片隔离）：慢分片的在途执行不阻塞其他分片——各分片独立
// 组批与在途，天然缓解队头阻塞。
func TestSlowShardDoesNotBlockOthers(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		if reqs[0] == 0 {
			<-release // 分片 0 慢执行
		}
		return append([]int(nil), reqs...), nil
	}

	s := sharded.New(func(id int) int { return id % 2 }, 2, sharded.NewFactory(fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(5*time.Millisecond),
	))
	defer mustClose(t, s)

	d0 := make(chan int, 1)
	go func() {
		res, err := s.Do(context.Background(), 0)
		if err != nil {
			t.Errorf("Do(0): %v", err)
		}
		d0 <- res
	}()
	<-entered // 分片 0 已在途阻塞

	resCh := make(chan int, 1)
	go func() {
		res, err := s.Do(context.Background(), 1)
		if err != nil {
			t.Errorf("Do(1): %v", err)
		}
		resCh <- res
	}()
	select {
	case res := <-resCh:
		if res != 1 {
			t.Fatalf("Do(1) = %d, want 1", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("慢分片阻塞了其他分片（隔离失效）")
	}

	close(release)
	if res := <-d0; res != 0 {
		t.Fatalf("Do(0) = %d, want 0", res)
	}
}

// 约束（级联 Close）：Close 并行 drain 全部分片并等待——未满批在
// 各分片各自交付、在途批次执行完毕后才返回；Observer 每分片各发一次
// OnCreated/OnClosed；幂等、可并发调用；关闭后新提交 ErrClosed。
func TestCascadeCloseFlushesAllShards(t *testing.T) {
	obs := &recordingObserver{}
	const n = 3
	var mu sync.Mutex
	var shards []int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		shards = append(shards, reqs...)
		mu.Unlock()
		return append([]int(nil), reqs...), nil
	}

	s := sharded.New(func(id int) int { return id }, n, sharded.NewFactory(fn,
		batcher.WithMaxSize(100),
		batcher.WithInterval(10*time.Second), // 只能靠 Close flush
		batcher.WithObserver(obs),
	))

	results := make(chan int, n)
	for id := 0; id < n; id++ {
		go func(id int) {
			res, err := s.Do(context.Background(), id)
			if err != nil {
				t.Errorf("Do(%d): %v", id, err)
			}
			results <- res
		}(id)
	}
	time.Sleep(50 * time.Millisecond) // 三条各自入其分片组批

	mustClose(t, s)
	got := []int{<-results, <-results, <-results}
	sort.Ints(got)
	if fmt.Sprint(got) != "[0 1 2]" {
		t.Fatalf("results = %v, want [0 1 2]（每分片未满批各自 flush）", got)
	}
	mu.Lock()
	sort.Ints(shards)
	if fmt.Sprint(shards) != "[0 1 2]" {
		mu.Unlock()
		t.Fatalf("executed = %v, want 每分片恰一批（并行 drain，序不定）", shards)
	}
	mu.Unlock()

	if created, closed := obs.counts(); created != n || closed != n {
		t.Fatalf("events = created %d / closed %d, want %d / %d（每分片各发一次）", created, closed, n, n)
	}

	// 幂等 + 并发安全。
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
	mustClose(t, s)

	if _, err := s.Do(context.Background(), 0); !errors.Is(err, batcher.ErrClosed) {
		t.Fatalf("Do after Close = %v, want ErrClosed", err)
	}
	if err := s.Enqueue(context.Background(), 0); !errors.Is(err, batcher.ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
}

// 约束（级联 Close 超时）：ctx 超时仅放弃等待、返回 ctx.Err()，慢
// 分片的后台 flush 照常完成、Waiter 被唤醒——不放大为全库丢失。
func TestCascadeCloseTimeoutGivesUpFlushCompletes(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		entered <- struct{}{}
		if reqs[0] == 0 {
			<-release // 分片 0 慢执行，Close 必然超时
		}
		return append([]int(nil), reqs...), nil
	}

	s := sharded.New(func(id int) int { return id % 2 }, 2, sharded.NewFactory(fn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(5*time.Millisecond),
	))

	d0 := make(chan int, 1)
	go func() {
		res, err := s.Do(context.Background(), 0)
		if err != nil {
			t.Errorf("Do(0): %v", err)
		}
		d0 <- res
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close timeout = %v, want DeadlineExceeded", err)
	}

	close(release) // 放行慢分片，后台 flush 完成
	select {
	case res := <-d0:
		if res != 0 {
			t.Fatalf("Do(0) = %d, want 0", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never resolved after background flush")
	}
}

// 约束（路由健壮性）：route 越界（负值或 ≥ n）属程序错误，调用期
// panic；route 是逐请求执行的用户代码，无法在构造期校验。
func TestRouteOutOfRangePanics(t *testing.T) {
	for _, route := range []func(int) int{
		func(id int) int { return 2 },  // == n
		func(id int) int { return -1 }, // 负下标
	} {
		s := sharded.New(route, 2, sharded.NewFactory(echoFn,
			batcher.WithMaxSize(1),
			batcher.WithInterval(time.Millisecond),
		))
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("越界路由未 panic")
				}
			}()
			_, _ = s.Do(context.Background(), 1)
		}()
		mustClose(t, s)
	}
}

// 约束（构造校验）：route/factory 为 nil、n 非正属程序员工错，
// 构造期 panic。
func TestNewValidatesArguments(t *testing.T) {
	route := func(id int) int { return 0 }
	factory := sharded.NewFactory(echoFn,
		batcher.WithMaxSize(1), batcher.WithInterval(time.Millisecond),
	)
	cases := []struct {
		name    string
		route   func(int) int
		n       int
		factory sharded.Factory[int, int]
	}{
		{"nil route", nil, 1, factory},
		{"zero shards", route, 0, factory},
		{"negative shards", route, -1, factory},
		{"nil factory", route, 1, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("New did not panic")
				}
			}()
			s := sharded.New(tc.route, tc.n, tc.factory)
			// 若未 panic 也必须能安全关闭，避免泄漏 dispatcher。
			if err := s.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// 约束（构造中断回收）：工厂对某分片返回 nil 即 panic，且已建分片
// 被级联回收（各发一次 OnClosed）——构造失败的 New 不泄漏 dispatcher。
func TestFactoryReturningNilPanicsAndReclaimsCreatedShards(t *testing.T) {
	obs := &recordingObserver{}
	const n = 3
	var built atomic.Int64
	factory := func(shard int) *batcher.Batcher[int, int] {
		if shard == 2 {
			return nil
		}
		built.Add(1)
		return batcher.New(echoFn,
			batcher.WithMaxSize(1), batcher.WithInterval(time.Millisecond),
			batcher.WithObserver(obs),
		)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("工厂返回 nil 未 panic")
			}
		}()
		_ = sharded.New(func(id int) int { return 0 }, n, factory)
	}()

	// 分片 0、1 已建并随即回收；分片 2 未建。
	if c := built.Load(); c != 2 {
		t.Fatalf("built shards = %d, want 2", c)
	}
	if created, closed := obs.counts(); created != 2 || closed != 2 {
		t.Fatalf("events = created %d / closed %d, want 2 / 2（已建分片级联回收）", created, closed)
	}
}

// 约束（fire-and-forget 委托）：Enqueue/EnqueueFunc 路由到对应分片，
// 回调在交付点收到与 Do 相同口径的结果。
func TestEnqueueRoutesAndDeliversCallbacks(t *testing.T) {
	s := sharded.New(func(id int) int { return id % 2 }, 2, sharded.NewFactory(echoFn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Second), // 两条同分片占满即触发
	))
	defer mustClose(t, s)

	type outcome struct {
		res int
		err error
	}
	outcomes := make(chan outcome, 2)
	for _, id := range []int{1, 3} { // 同落分片 1
		if err := s.EnqueueFunc(context.Background(), id, func(res int, err error) {
			outcomes <- outcome{res, err}
		}); err != nil {
			t.Fatalf("EnqueueFunc(%d): %v", id, err)
		}
	}
	for i := 0; i < 2; i++ {
		o := <-outcomes
		if o.err != nil || (o.res != 1 && o.res != 3) {
			t.Fatalf("callback = (%d, %v), want 对位结果", o.res, o.err)
		}
	}
	// Enqueue 纯写路径：提交即返回，Close flush。
	if err := s.Enqueue(context.Background(), 5); err != nil {
		t.Fatalf("Enqueue(5): %v", err)
	}
}
