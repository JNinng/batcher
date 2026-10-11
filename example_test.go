package batcher_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

func TestNewPanicsOnInvalidConfiguration(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	cases := []struct {
		name string
		opts []batcher.Option
	}{
		{"missing maxSize", []batcher.Option{batcher.WithInterval(time.Second)}},
		{"maxSize zero", []batcher.Option{batcher.WithMaxSize(0), batcher.WithInterval(time.Second)}},
		{"maxSize negative", []batcher.Option{batcher.WithMaxSize(-1), batcher.WithInterval(time.Second)}},
		{"missing interval", []batcher.Option{batcher.WithMaxSize(1)}},
		{"interval zero", []batcher.Option{batcher.WithMaxSize(1), batcher.WithInterval(0)}},
		{"interval negative", []batcher.Option{batcher.WithMaxSize(1), batcher.WithInterval(-time.Second)}},
		{"maxPending negative", []batcher.Option{batcher.WithMaxSize(1), batcher.WithInterval(time.Second), batcher.WithMaxPending(-1)}},
		{"maxConcurrency negative", []batcher.Option{batcher.WithMaxSize(1), batcher.WithInterval(time.Second), batcher.WithMaxConcurrency(-1)}},
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

	t.Run("nil fn", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("New(nil) did not panic")
			}
		}()
		_ = batcher.New[int, int](nil,
			batcher.WithMaxSize(1), batcher.WithInterval(time.Second))
	})
}

// ExampleBatcher 展示最基本的用法：多个单次查询扇入为一个批量查询。
func ExampleBatcher() {
	// 模拟下游批量查询接口。
	queryUsers := func(ctx context.Context, ids []int) ([]string, error) {
		users := make([]string, len(ids))
		for i, id := range ids {
			users[i] = fmt.Sprintf("user-%d", id)
		}
		return users, nil
	}

	b := batcher.New[int, string](queryUsers,
		batcher.WithMaxSize(10),
		batcher.WithInterval(50*time.Millisecond),
	)
	defer b.Close(context.Background())

	res, err := b.Do(context.Background(), 42)
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
	// Output: user-42
}

// ExampleNewKeyed 展示批内 key 去重：同 key 的并发请求共享一次下游执行。
func ExampleNewKeyed() {
	var fnCalls int
	queryUsers := func(ctx context.Context, ids []int) ([]string, error) {
		fnCalls++
		users := make([]string, len(ids))
		for i, id := range ids {
			users[i] = fmt.Sprintf("user-%d", id)
		}
		return users, nil
	}

	b := batcher.NewKeyed(func(id int) int { return id }, queryUsers,
		batcher.WithMaxSize(3),
		batcher.WithInterval(50*time.Millisecond),
	)
	defer b.Close(context.Background())

	// 3 个并发请求中 2 个是同一 id：折叠后批次函数只收到 2 条 unique。
	ids := []int{42, 42, 7}
	results := make([]string, len(ids))
	var wg sync.WaitGroup
	wg.Add(len(ids))
	release := launchTogether(len(ids), func(i int) {
		defer wg.Done()
		res, err := b.Do(context.Background(), ids[i])
		if err != nil {
			panic(err)
		}
		results[i] = res
	})
	release() // 屏障确保三条请求同批到达，fnCalls 断言不依赖调度时序
	wg.Wait()
	for _, res := range results {
		fmt.Println(res)
	}
	fmt.Println("batch fn calls:", fnCalls)
	// Output:
	// user-42
	// user-42
	// user-7
	// batch fn calls: 1
}

// ExampleNewKeyedMap 展示 keyed-map 变体：批次函数返回 map[K]Res，
// 缺 key 转为单条 ErrMissingResult、不构成整批失败。
func ExampleNewKeyedMap() {
	// 模拟下游 mget：id 7 缺失。
	mget := func(ctx context.Context, ids []int) (map[int]string, error) {
		m := make(map[int]string, len(ids))
		for _, id := range ids {
			if id == 7 {
				continue
			}
			m[id] = fmt.Sprintf("user-%d", id)
		}
		return m, nil
	}

	b := batcher.NewKeyedMap(func(id int) int { return id }, mget,
		batcher.WithMaxSize(3),
		batcher.WithInterval(50*time.Millisecond),
	)
	defer b.Close(context.Background())

	// 3 个并发请求中 2 个是同一 id（去重后 2 条 unique），id 7 缺失。
	ids := []int{42, 42, 7}
	results := make([]batcher.Result[string], len(ids))
	var wg sync.WaitGroup
	wg.Add(len(ids))
	release := launchTogether(len(ids), func(i int) {
		defer wg.Done()
		r, err := b.Do(context.Background(), ids[i])
		if err != nil {
			panic(err)
		}
		results[i] = r
	})
	release() // 屏障确保三条请求同批到达
	wg.Wait()
	for _, r := range results {
		if r.Err != nil {
			fmt.Println("missing:", r.Err)
			continue
		}
		fmt.Println(r.Value)
	}
	// Output:
	// user-42
	// user-42
	// missing: batcher: missing result
}

// ExampleBatcher_Enqueue 展示 fire-and-forget 入队：写路径提交即返回，
// 攒批写出，不等待下游。
func ExampleBatcher_Enqueue() {
	flushed := make(chan []string, 1)
	b := batcher.New[string, struct{}](
		func(ctx context.Context, lines []string) ([]struct{}, error) {
			flushed <- append([]string(nil), lines...) // 模拟批量写出
			return make([]struct{}, len(lines)), nil
		},
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Second),
	)

	// Enqueue 不等待结果：提交即返回，写路径不被下游耗时阻塞。
	if err := b.Enqueue(context.Background(), "log-line-1"); err != nil {
		panic(err)
	}
	if err := b.Enqueue(context.Background(), "log-line-2"); err != nil {
		panic(err)
	}
	// Close flush 未满批，保证写出完成后才返回。
	if err := b.Close(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(<-flushed)
	// Output: [log-line-1 log-line-2]
}

// ExampleBatcher_EnqueueFunc 展示带 per-item 回调的 fire-and-forget：
// 交付点同步回调结果，批级失败即回调的 err。
func ExampleBatcher_EnqueueFunc() {
	b := batcher.New[int, string](
		func(ctx context.Context, ids []int) ([]string, error) {
			users := make([]string, len(ids))
			for i, id := range ids {
				users[i] = fmt.Sprintf("user-%d", id)
			}
			return users, nil
		},
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Second),
	)
	defer b.Close(context.Background())

	done := make(chan string, 2)
	for _, id := range []int{42, 7} {
		if err := b.EnqueueFunc(context.Background(), id, func(res string, err error) {
			if err != nil {
				done <- "failed"
				return
			}
			done <- res
		}); err != nil {
			panic(err)
		}
	}
	fmt.Println(<-done, <-done)
	// Output: user-42 user-7
}
