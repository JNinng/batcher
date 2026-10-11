package batcher_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// echoLines 批次函数：按请求等长回显，记录每次调用。
func echoLines(mu *sync.Mutex, calls *[][]string) batcher.BatchFunc[string, string] {
	return func(ctx context.Context, lines []string) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		*calls = append(*calls, append([]string(nil), lines...))
		return append([]string(nil), lines...), nil
	}
}

// 约束（字节触发）：Formation 内累计字节达到上限即 Flush，不等条数、
// 不等 interval。
func TestMaxBytesTriggersFlush(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	forty := strings.Repeat("x", 40) // 3 条 120 字节，第 3 条越过 100
	b := batcher.New(echoLines(&mu, &calls),
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Second), // 绝不该等到
		batcher.WithMaxBytes(100, func(line string) int { return len(line) }),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	done := make(chan struct{}, 3)
	release := launchTogether(3, func(i int) {
		defer func() { done <- struct{}{} }()
		if _, err := b.Do(context.Background(), forty); err != nil {
			t.Errorf("Do: %v", err)
		}
	})
	started := time.Now()
	release()
	for i := 0; i < 3; i++ {
		<-done
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("flush 等待 %v, 字节触发不该等 interval", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	if len(calls[0]) != 3 {
		t.Fatalf("batch = %d 条, want 3", len(calls[0]))
	}
}

// 约束（孤批）：单条即超限不成批大小 1 的孤批——不拆分、不丢弃。
func TestMaxBytesOversizedItemFlushesAlone(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fn := func(ctx context.Context, lines []string) ([]string, error) {
		mu.Lock()
		calls = append(calls, append([]string(nil), lines...))
		mu.Unlock()
		entered <- struct{}{}
		if len(calls) == 1 {
			<-release // 首批（孤批）在途，与后续组批隔离
		}
		return append([]string(nil), lines...), nil
	}

	b := batcher.New(fn,
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxBytes(10, func(line string) int { return len(line) }),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	dA := make(chan string, 1)
	go func() {
		res, err := b.Do(context.Background(), strings.Repeat("A", 50)) // 50 > 10
		if err != nil {
			t.Errorf("Do(A): %v", err)
		}
		dA <- res
	}()
	<-entered // 孤批已触发并在途

	db := make(chan string, 1)
	go func() {
		res, err := b.Do(context.Background(), "bbb") // 3 < 10：interval 触发
		if err != nil {
			t.Errorf("Do(b): %v", err)
		}
		db <- res
	}()

	close(release)
	if res := <-dA; res != strings.Repeat("A", 50) {
		t.Fatalf("Do(A) = %q", res)
	}
	if res := <-db; res != "bbb" {
		t.Fatalf("Do(b) = %q", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || fmt.Sprint(calls[0]) == fmt.Sprint(calls[1]) {
		t.Fatalf("calls = %q, want [[AAA…][bbb]]（孤批单独执行）", calls)
	}
	if len(calls[0]) != 1 || len(calls[1]) != 1 {
		t.Fatalf("batch sizes = %d/%d, want 1/1", len(calls[0]), len(calls[1]))
	}
}

// 约束（并列先到先触发）：配置 WithMaxBytes 不抢占条数触发——
// 条数先到仍按条数 Flush。
func TestMaxBytesDoesNotPreemptSizeTrigger(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	forty := strings.Repeat("x", 40) // 2 条 80 字节 < 1000：只能条数触发
	b := batcher.New(echoLines(&mu, &calls),
		batcher.WithMaxSize(2),
		batcher.WithInterval(10*time.Second),
		batcher.WithMaxBytes(1000, func(line string) int { return len(line) }),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	done := make(chan struct{}, 2)
	release := launchTogether(2, func(i int) {
		defer func() { done <- struct{}{} }()
		if _, err := b.Do(context.Background(), forty); err != nil {
			t.Errorf("Do: %v", err)
		}
	})
	started := time.Now()
	release()
	for i := 0; i < 2; i++ {
		<-done
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("flush 等待 %v, 条数触发不该被字节上限抢占", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 || len(calls[0]) != 2 {
		t.Fatalf("calls = %q, want 一批 2 条", calls)
	}
}

// 约束（keyed 叠加按原始条目计）：重复 Item 照常占槽计费——同 key
// 两条 10 字节即可越过 15 字节上限按字节触发，实际调用批次函数的是
// 去重后切片（载荷不大于计费值）。
func TestKeyedBytesCountRaw(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	hot := strings.Repeat("h", 10)
	b := batcher.NewKeyed(func(line string) string { return line }, echoLines(&mu, &calls),
		batcher.WithMaxSize(10),
		batcher.WithInterval(10*time.Second), // 绝不该等到
		batcher.WithMaxBytes(15, func(line string) int { return len(line) }),
	)
	defer func() {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	res := make(chan string, 2)
	release := launchTogether(2, func(i int) {
		r, err := b.Do(context.Background(), hot)
		if err != nil {
			t.Errorf("Do: %v", err)
		}
		res <- r
	})
	started := time.Now()
	release()
	if got := <-res; got != hot {
		t.Fatalf("Do = %q", got)
	}
	if got := <-res; got != hot {
		t.Fatalf("Do = %q", got)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("flush 等待 %v, 重复应占槽按字节触发", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("fn calls = %d, want 1", len(calls))
	}
	if fmt.Sprint(calls[0]) != fmt.Sprintf("[%s]", hot) {
		t.Fatalf("unique reqs = %q, want 去重后单条", calls[0])
	}
}

// 约束：size 函数 panic 降级为批级错误——整批预置失败、不调批次函数，
// 库继续可用。顺序提交（Enqueue 在无缓冲握手上返回即已被组批接收）
// 保证 panic 条目与先到条目同批。
func TestSizeFnPanicDegradesToBatchError(t *testing.T) {
	var fnCalls int
	var mu sync.Mutex
	fn := func(ctx context.Context, lines []string) ([]string, error) {
		mu.Lock()
		fnCalls++
		mu.Unlock()
		return append([]string(nil), lines...), nil
	}

	b := batcher.New(fn,
		batcher.WithMaxSize(5),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxBytes(1000, func(line string) int {
			if line == "boom" {
				panic("size explodes")
			}
			return len(line)
		}),
	)

	type outcome struct {
		res string
		err error
	}
	outcomes := make(chan outcome, 2)
	record := func(res string, err error) { outcomes <- outcome{res, err} }

	if err := b.Enqueue(context.Background(), "ok"); err != nil { // 返回即已被组批接收
		t.Fatalf("Enqueue(ok): %v", err)
	}
	if err := b.EnqueueFunc(context.Background(), "boom", record); err != nil {
		t.Fatalf("EnqueueFunc(boom): %v", err)
	}

	o := <-outcomes
	if o.res != "" || o.err == nil || !strings.Contains(o.err.Error(), "size function panicked") {
		t.Fatalf("callback(boom) = (%q, %v), want size panic 降级的批级错误", o.res, o.err)
	}

	// panic 不得拖垮库：后续批次正常。
	if err := b.EnqueueFunc(context.Background(), "fine", record); err != nil {
		t.Fatalf("EnqueueFunc(fine): %v", err)
	}
	o = <-outcomes
	if o.res != "fine" || o.err != nil {
		t.Fatalf("callback(fine) = (%q, %v), want (fine, nil)", o.res, o.err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if fnCalls != 1 { // 仅 "fine" 一批；panic 批不调批次函数
		t.Fatalf("fn calls = %d, want 1（panic 批不调批次函数）", fnCalls)
	}
}

// 约束：size 函数返回负数同样批级失败（负计费会让字节触发静默失效），
// 库继续可用。
func TestSizeFnNegativeDegradesToBatchError(t *testing.T) {
	var fnCalls int
	var mu sync.Mutex
	fn := func(ctx context.Context, lines []string) ([]string, error) {
		mu.Lock()
		fnCalls++
		mu.Unlock()
		return append([]string(nil), lines...), nil
	}

	b := batcher.New(fn,
		batcher.WithMaxSize(5),
		batcher.WithInterval(10*time.Millisecond),
		batcher.WithMaxBytes(1000, func(line string) int {
			if line == "bad" {
				return -1
			}
			return len(line)
		}),
	)

	type outcome struct {
		res string
		err error
	}
	outcomes := make(chan outcome, 2)
	record := func(res string, err error) { outcomes <- outcome{res, err} }

	if err := b.Enqueue(context.Background(), "ok"); err != nil { // 返回即已被组批接收
		t.Fatalf("Enqueue(ok): %v", err)
	}
	if err := b.EnqueueFunc(context.Background(), "bad", record); err != nil {
		t.Fatalf("EnqueueFunc(bad): %v", err)
	}

	o := <-outcomes
	if o.res != "" || o.err == nil || !strings.Contains(o.err.Error(), "negative value") {
		t.Fatalf("callback(bad) = (%q, %v), want 负值批级错误", o.res, o.err)
	}

	// 负值不得拖垮库：后续批次正常。
	if err := b.EnqueueFunc(context.Background(), "fine", record); err != nil {
		t.Fatalf("EnqueueFunc(fine): %v", err)
	}
	o = <-outcomes
	if o.res != "fine" || o.err != nil {
		t.Fatalf("callback(fine) = (%q, %v), want (fine, nil)", o.res, o.err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if fnCalls != 1 { // 仅 "fine" 一批；负值批不调批次函数
		t.Fatalf("fn calls = %d, want 1（负值批不调批次函数）", fnCalls)
	}
}

func TestWithMaxBytesPanicsOnInvalidConfig(t *testing.T) {
	fn := func(ctx context.Context, reqs []int) ([]int, error) {
		return append([]int(nil), reqs...), nil
	}
	cases := []struct {
		name string
		opts []batcher.Option
	}{
		{"maxBytes negative", []batcher.Option{
			batcher.WithMaxBytes(-1, func(int) int { return 1 }), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
		{"maxBytes zero with size", []batcher.Option{
			batcher.WithMaxBytes(0, func(int) int { return 1 }), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
		{"maxBytes without size", []batcher.Option{
			batcher.WithMaxBytes[int](10, nil), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
		{"size Req mismatch", []batcher.Option{
			batcher.WithMaxBytes(10, func(string) int { return 1 }), batcher.WithMaxSize(1), batcher.WithInterval(time.Second)}},
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
