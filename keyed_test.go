package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// 约束（NewKeyed 契约）：同 key 的 N 个并发请求折叠为一条执行——
// 批次函数只收到去重切片，全部 Waiter 各自拿到结果。
func TestKeyedDedupesHotKeyWithinBatch(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		res := make([]string, len(reqs))
		for i, r := range reqs {
			res[i] = fmt.Sprintf("u%d", r)
		}
		return res, nil
	}

	b := batcher.NewKeyed(func(id int) int { return id }, fn,
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Second), // 由 size 触发，隔离 interval
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 10
	const hot = 7
	errs := make(chan error, n)
	release := launchTogether(n, func(i int) {
		res, err := b.Do(context.Background(), hot)
		if err == nil && res != "u7" {
			t.Errorf("Do = %q, want u7", res)
		}
		errs <- err
	})
	release()
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("waiter: %v", err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1（同 key 并发共享一条执行）", len(calls))
	}
	if fmt.Sprint(calls[0]) != "[7]" {
		t.Fatalf("unique reqs = %v, want [7]", calls[0])
	}
}

// 约束（计数口径）：重复 Item 照常占槽——maxSize 按原始条数计数，
// 重复占满即按 size 触发、不等 interval；事件双口径 Size（原始）/
// UniqueSize（去重后）；结果按原位置对位归还。
func TestKeyedDuplicatesOccupySlotsAndEventReportsBothSizes(t *testing.T) {
	obs := &recordingObserver{}
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		return append([]int(nil), reqs...), nil
	}

	// id 1..4 → key 奇偶 1,0,1,0：原始 4 条折叠为 2 条 unique。
	b := batcher.NewKeyed(func(id int) int { return id % 2 }, fn,
		batcher.WithMaxSize(4),
		batcher.WithInterval(10*time.Second), // 占满即触发，interval 永不该等到
		batcher.WithObserver(obs),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 4
	resByID := make([]int, n+1)
	done := make(chan struct{}, n)
	release := launchTogether(n, func(i int) {
		defer func() { done <- struct{}{} }()
		id := i + 1
		res, err := b.Do(context.Background(), id)
		if err != nil {
			t.Errorf("Do(%d): %v", id, err)
		}
		resByID[id] = res
	})
	started := time.Now()
	release()
	for i := 0; i < n; i++ {
		<-done
	}
	// 若重复不占槽，4 条只折叠出 2 条 unique、永远凑不满 maxSize=4，
	// 只能白等满 interval——耗时远超正常 size 触发即为回归。
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("flush 等待 %v, 重复应占槽按 size 触发", elapsed)
	}

	// 同 key 共享首达 Req 的执行：到达序不定，但奇偶两组各自一致，
	// 且结果都是本 key 的奇数/偶数 id。
	if resByID[1] != resByID[3] || resByID[2] != resByID[4] {
		t.Fatalf("同 key Waiter 结果不一致: %v", resByID)
	}
	if resByID[1]%2 != 1 || resByID[2]%2 != 0 {
		t.Fatalf("结果应为首达 Req: %v", resByID)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	// 去重切片 = 各 key 的首达 Req（集合恰为奇偶各一）。
	got := append([]int(nil), calls[0]...)
	sort.Ints(got)
	want := []int{resByID[1], resByID[2]}
	sort.Ints(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("unique reqs = %v, want 首达 Req %v", got, want)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.flushed) != 1 {
		t.Fatalf("flushed events = %d, want 1", len(obs.flushed))
	}
	f := obs.flushed[0]
	if f.Size != 4 || f.UniqueSize != 2 {
		t.Fatalf("event = {Size:%d UniqueSize:%d}, want {4 2}（原始条数/去重后）", f.Size, f.UniqueSize)
	}
}

// 约束（ADR-0004 首现序）：去重切片保持原始条目的首次出现投影——
// 到达序不定，但 unique 恒等于 keyFn 所见序列的首现投影；且 key 函数
// 对每条原始 Item 恰调一次（只在折叠处、批次函数之外）。
func TestKeyedUniquePreservesFirstOccurrenceOrder(t *testing.T) {
	var mu sync.Mutex
	var rawSeq []int    // keyFn 所见：折叠时的原始条目序
	var uniqueSeq []int // 批次函数收到的去重切片
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		uniqueSeq = append([]int(nil), reqs...)
		mu.Unlock()
		return append([]int(nil), reqs...), nil
	}

	b := batcher.NewKeyed(func(id int) int {
		mu.Lock()
		rawSeq = append(rawSeq, id)
		mu.Unlock()
		return id
	}, fn,
		batcher.WithMaxSize(6),
		batcher.WithInterval(10*time.Second), // 占满即按 size 触发
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	const n = 6
	done := make(chan struct{}, n)
	release := launchTogether(n, func(i int) {
		defer func() { done <- struct{}{} }()
		if _, err := b.Do(context.Background(), i%3+1); err != nil { // 1,2,3,1,2,3
			t.Errorf("Do: %v", err)
		}
	})
	release()
	for i := 0; i < n; i++ {
		<-done
	}

	mu.Lock()
	defer mu.Unlock()
	if len(rawSeq) != n {
		t.Fatalf("keyFn 调用 %d 次, want %d（每条原始 Item 恰调一次）", len(rawSeq), n)
	}
	seen := map[int]bool{}
	var want []int
	for _, id := range rawSeq {
		if !seen[id] {
			seen[id] = true
			want = append(want, id)
		}
	}
	if fmt.Sprint(uniqueSeq) != fmt.Sprint(want) {
		t.Fatalf("unique = %v, want 首现投影 %v", uniqueSeq, want)
	}
}

// 约束（与 WithMaxPending 组合）：准入按原始条数记账、折叠只发生在
// 交付边界——重复 Item 各占一个额度、交付时各自释放，额度不得泄漏。
func TestKeyedWithMaxPendingAdmissionDrainsToZero(t *testing.T) {
	m := newTestMeter(t)
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		return append([]int(nil), reqs...), nil
	}

	// 4 条原始、2 个 key：额度恰为 4，全部准入成批；若折叠误按 unique
	// 条数释放额度，交付后 queue_depth 停在 2 不归零。
	b := batcher.NewKeyed(func(id int) int { return id }, fn,
		batcher.WithMaxSize(4),
		batcher.WithInterval(10*time.Second), // 占满即按 size 触发
		batcher.WithMaxPending(4),
		batcher.WithMeter(m.meter),
	)

	const n = 4
	done := make(chan struct{}, n)
	release := launchTogether(n, func(i int) {
		defer func() { done <- struct{}{} }()
		id := i%2 + 1 // 1,2,1,2
		res, err := b.Do(context.Background(), id)
		if err != nil || res != id {
			t.Errorf("Do(%d) = (%d, %v)", id, res, err)
		}
	})
	release()
	for i := 0; i < n; i++ {
		<-done
	}

	mu.Lock()
	if len(calls) != 1 || len(calls[0]) != 2 {
		mu.Unlock()
		t.Fatalf("calls = %v, want 一次折叠为 2 条 unique 的批次", calls)
	}
	mu.Unlock()

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v := m.counterValue(t, "batcher_overloaded_total"); v != 0 {
		t.Fatalf("batcher_overloaded_total = %v, want 0（4 条原始恰占满 4 额度）", v)
	}
	if v := m.gaugeValue(t, "batcher_queue_depth"); v != 0 {
		t.Fatalf("queue_depth = %v, want 0（交付按原始条数释放额度）", v)
	}
}

// 约束（ADR-0003 兼容）：同 key 多 Waiter 共享一条执行，单个取消只
// 解除自己，其余照常收结果，Item 保留执行。
func TestKeyedSameKeyCancelOnlyReleasesSelf(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, append([]int(nil), reqs...))
		return append([]int(nil), reqs...), nil
	}

	b := batcher.NewKeyed(func(id int) int { return id }, fn,
		batcher.WithMaxSize(100),
		batcher.WithInterval(300*time.Millisecond), // 窗口放宽：取消落在组批等待中
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel) // 等待中取消（远早于 flush）

	const n = 3
	done := make(chan struct{}, n)
	release := launchTogether(n, func(i int) {
		defer func() { done <- struct{}{} }()
		if i == 0 {
			if _, err := b.Do(ctx, 9); !errors.Is(err, context.Canceled) {
				t.Errorf("cancelled waiter err = %v, want context.Canceled", err)
			}
			return
		}
		res, err := b.Do(context.Background(), 9)
		if err != nil || res != 9 {
			t.Errorf("Do(9) = (%d, %v)", res, err)
		}
	})
	release()
	for i := 0; i < n; i++ {
		<-done
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 || fmt.Sprint(calls[0]) != "[9]" {
		t.Fatalf("calls = %v, want [[9]]（取消不改批次形状，去重后单条执行）", calls)
	}
}

// 约束：跨批去重不做——执行期到达的重复 key 落入下一批重查。
func TestKeyedNoCrossBatchDedup(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		mu.Lock()
		calls = append(calls, append([]int(nil), reqs...))
		first := len(calls) == 1
		mu.Unlock()
		entered <- struct{}{}
		if first {
			<-release // 第一批慢执行，期间同 key 请求到达
		}
		return append([]int(nil), reqs...), nil
	}

	b := batcher.NewKeyed(func(id int) int { return id }, fn,
		batcher.WithMaxSize(2), // 同 key ×2 占满即触发，与 interval 无关
		batcher.WithInterval(10*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	releaseGo := launchTogether(2, func(i int) {
		_, _ = b.Do(context.Background(), 5) // 第一批：size=2 立即触发
	})
	releaseGo()
	<-entered // 第一批已开始慢执行

	go func() { _, _ = b.Do(context.Background(), 5) }() // 执行期到达
	go func() { _, _ = b.Do(context.Background(), 5) }()

	close(release) // 放行第一批 → 第二批随即执行
	<-entered      // 第二批已触发

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("fn calls = %d, want 2（同 key 跨批重查，不做跨批去重）", len(calls))
	}
	for i, reqs := range calls {
		if fmt.Sprint(reqs) != "[5]" {
			t.Fatalf("calls[%d] = %v, want [5]", i, reqs)
		}
	}
}

// 约束：key 函数 panic 在折叠处单独 recover、降级为批级错误（callFn
// 的 recover 只包批次函数，折叠发生在其外）；库继续可用；事件口径上
// 折叠失败的批 UniqueSize 报 0（ADR-0004）。
func TestKeyedKeyFnPanicDegradesToBatchError(t *testing.T) {
	obs := &recordingObserver{}
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	key := func(id int) int {
		if id == 13 {
			panic("key explodes")
		}
		return id
	}

	b := batcher.NewKeyed(key, fn,
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithObserver(obs),
	)

	errs := make(chan error, 2)
	release := launchTogether(2, func(i int) {
		ids := []int{13, 1}
		_, err := b.Do(context.Background(), ids[i])
		if err == nil || !strings.Contains(err.Error(), "key function panicked") {
			t.Errorf("Do(%d) err = %v, want keyFn panic 降级的批级错误", ids[i], err)
		}
		errs <- err
	})
	release()
	for i := 0; i < 2; i++ {
		<-errs
	}

	// panic 不得拖垮库：后续 keyed 批正常折叠与执行。
	if res, err := b.Do(context.Background(), 7); err != nil || res != 7 {
		t.Fatalf("Do after key panic = (%d, %v), want (7, nil)", res, err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.flushed) != 2 {
		t.Fatalf("flushed events = %d, want 2（panic 批 + 恢复后的正常批）", len(obs.flushed))
	}
	if f := obs.flushed[0]; f.Size != 2 || f.UniqueSize != 0 {
		t.Fatalf("event = {Size:%d UniqueSize:%d}, want {2 0}（折叠失败批 UniqueSize 报 0）", f.Size, f.UniqueSize)
	}
	if f := obs.flushed[1]; f.Size != 1 || f.UniqueSize != 1 {
		t.Fatalf("event = {Size:%d UniqueSize:%d}, want {1 1}（后续批正常折叠）", f.Size, f.UniqueSize)
	}
}

func TestNewKeyedPanicsOnNilKey(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewKeyed(nil key) did not panic")
		}
	}()
	_ = batcher.NewKeyed[int, int, int](nil, identityFn)
}

func TestNewKeyedPanicsOnNilFn(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewKeyed(nil fn) did not panic")
		}
	}()
	b := batcher.NewKeyed[int, int, int](func(id int) int { return id }, nil)
	// 若未 panic 也必须能安全关闭，避免泄漏 goroutine。
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
