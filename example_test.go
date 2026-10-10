package batcher_test

import (
	"context"
	"fmt"
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
