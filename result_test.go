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

// 约束：以 Result 作 Res 时库行为不变——结果按位置交付、逐条失败互不
// 影响、批级错误仍走 Do 的 error（逐条失败不是批级失败）。
func TestResultCarriesPerItemFailure(t *testing.T) {
	notFound := errors.New("not found")
	fn := func(ctx context.Context, ids []int) ([]batcher.Result[string], error) {
		res := make([]batcher.Result[string], len(ids)) // 结果切片仍等长
		for i, id := range ids {
			if id == 2 {
				res[i] = batcher.Result[string]{Err: notFound}
				continue
			}
			res[i] = batcher.Result[string]{Value: fmt.Sprintf("u%d", id)}
		}
		return res, nil
	}

	b := batcher.New[int, batcher.Result[string]](fn,
		batcher.WithMaxSize(3),
		batcher.WithInterval(10*time.Second), // size 触发，隔离 interval
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	resByID := make([]batcher.Result[string], 4)
	done := make(chan struct{}, 3)
	release := launchTogether(3, func(i int) {
		defer func() { done <- struct{}{} }()
		id := i + 1
		r, err := b.Do(context.Background(), id)
		if err != nil {
			t.Errorf("Do(%d) batch err = %v, want nil（逐条失败不广播）", id, err)
		}
		resByID[id] = r
	})
	release()
	for i := 0; i < 3; i++ {
		<-done
	}

	if resByID[1].Value != "u1" || resByID[1].Err != nil {
		t.Errorf("Do(1) = %+v, want {u1 nil}", resByID[1])
	}
	if !errors.Is(resByID[2].Err, notFound) || resByID[2].Value != "" {
		t.Errorf("Do(2) = %+v, want {零值 notFound}", resByID[2])
	}
	if resByID[3].Value != "u3" || resByID[3].Err != nil {
		t.Errorf("Do(3) = %+v, want {u3 nil}", resByID[3])
	}
}

// 约束（NewKeyedMap）：去重折叠之上按 key 回查 map；缺 key 转
// ErrMissingResult（单条、非整批）；同 key 的多 Waiter（含缺失）共享
// 同一结果；map 多余 key 忽略。
func TestKeyedMapDeliversMissingPerItem(t *testing.T) {
	var mu sync.Mutex
	var calls [][]int
	mapFn := func(ctx context.Context, ids []int) (map[int]string, error) {
		mu.Lock()
		calls = append(calls, append([]int(nil), ids...))
		mu.Unlock()
		m := make(map[int]string, len(ids))
		for _, id := range ids {
			if id == 13 {
				continue // 模拟 mget 缺 key
			}
			m[id] = fmt.Sprintf("u%d", id)
		}
		m[99] = "多余 key 应被忽略"
		return m, nil
	}

	b := batcher.NewKeyedMap(func(id int) int { return id }, mapFn,
		batcher.WithMaxSize(6),
		batcher.WithInterval(10*time.Second), // 6 条原始占满即触发
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	// 原始 6 条：13×3（缺 key）、7×2（重复）、42×1。
	ids := []int{13, 13, 13, 7, 7, 42}
	resByID := make([]batcher.Result[string], len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i, id int) {
			defer wg.Done()
			r, err := b.Do(context.Background(), id)
			if err != nil {
				t.Errorf("Do(%d) batch err = %v, want nil", id, err)
			}
			resByID[i] = r
		}(i, id)
	}
	wg.Wait()

	for i, id := range ids {
		r := resByID[i]
		if id == 13 {
			if !errors.Is(r.Err, batcher.ErrMissingResult) {
				t.Errorf("Do(13) = %+v, want Result{Err: ErrMissingResult}", r)
			}
			continue
		}
		if r.Err != nil || r.Value != fmt.Sprintf("u%d", id) {
			t.Errorf("Do(%d) = %+v, want {u%d nil}", id, r, id)
		}
	}
	// 同 key 结果一致：缺失的三条同错、重复的两条同值。
	if !errors.Is(resByID[0].Err, batcher.ErrMissingResult) ||
		!errors.Is(resByID[1].Err, batcher.ErrMissingResult) ||
		!errors.Is(resByID[2].Err, batcher.ErrMissingResult) {
		t.Errorf("同 key 缺失结果不一致: %v", resByID[:3])
	}
	if resByID[3].Value != resByID[4].Value {
		t.Errorf("同 key 结果不一致: %q vs %q", resByID[3].Value, resByID[4].Value)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	got := append([]int(nil), calls[0]...)
	sort.Ints(got)
	if fmt.Sprint(got) != "[7 13 42]" {
		t.Fatalf("unique reqs = %v, want [7 13 42]", got)
	}
}

// 约束：NewKeyedMap 下批次函数自身的错误仍是批级错误（广播），不被
// 误转为逐条 Result.Err。
func TestKeyedMapFnErrorBroadcasts(t *testing.T) {
	boom := errors.New("downstream down")
	mapFn := func(ctx context.Context, ids []int) (map[int]string, error) {
		return nil, boom
	}

	b := batcher.NewKeyedMap(func(id int) int { return id }, mapFn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	r, err := b.Do(context.Background(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("Do = (%+v, %v), want 批级错误 boom", r, err)
	}
}

// 约束：回查相位的 keyFn panic 与折叠同口径降级为批级错误。maxSize=1
// 下每批 keyFn 恰调两次（折叠一次、回查一次），计数器可精确定位相位；
// panic 须报「key function panicked」而非「batch function panicked」
// （回查发生在 callFn recover 域内、经 callKey 降级），且库继续可用。
func TestKeyFnPanicDuringLookupDegradesToBatchError(t *testing.T) {
	var keyCalls int
	key := func(id int) int {
		keyCalls++
		if keyCalls == 2 { // 第一次在折叠相位（成功），第二次在回查相位
			panic("lookup explodes")
		}
		return id
	}
	mapFn := func(ctx context.Context, ids []int) (map[int]string, error) {
		m := make(map[int]string, len(ids))
		for _, id := range ids {
			m[id] = fmt.Sprintf("u%d", id)
		}
		return m, nil
	}

	b := batcher.NewKeyedMap(key, mapFn,
		batcher.WithMaxSize(1),
		batcher.WithInterval(10*time.Millisecond),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	_, err := b.Do(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "key function panicked") {
		t.Fatalf("Do err = %v, want 回查 panic 降级的 key function panicked", err)
	}

	// panic 不得拖垮库：后续批折叠与回查均正常。
	if r, err := b.Do(context.Background(), 2); err != nil || r.Value != "u2" {
		t.Fatalf("Do after lookup panic = (%+v, %v), want (u2, nil)", r, err)
	}
}

func TestNewKeyedMapPanicsOnNilFn(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewKeyedMap(nil fn) did not panic")
		}
	}()
	b := batcher.NewKeyedMap[int, int, string](func(id int) int { return id }, nil)
	// 若未 panic 也必须能安全关闭，避免泄漏 goroutine。
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewKeyedMapPanicsOnNilKey(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewKeyedMap(nil key) did not panic")
		}
	}()
	_ = batcher.NewKeyedMap[int, int, string](nil,
		func(ctx context.Context, ids []int) (map[int]string, error) { return nil, nil })
}
