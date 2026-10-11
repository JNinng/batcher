package batcher_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jninng/batcher"
)

// BenchmarkMaxConcurrency 量化串行单在途的队头阻塞代价（ADR-0008 前置
// 数据）：扇入宽度（闭环连续 Do 的 client goroutine 数，模拟持续到达的
// 单条查询）× 下游单批耗时（批次函数 sleep 模拟 I/O）× 并发上限三维
// 交叉。maxSize=1 为最恶劣扇入形态（每条 Item 自成一批，攒批不可用），
// maxSize=64 对照攒批吸收后的剩余瓶颈。串行（默认 conc=1）的执行容量
// 为 batchSize/latency 批每秒，扇入需求超过它即成瓶颈；WithMaxConcurrency
// 把执行容量抬到 n 倍——ns/op 的下降倍数即下游并发带来的吞吐增益，
// 而 Formation 受限（interval 触发、扇入不足）的组合不受益。
func BenchmarkMaxConcurrency(b *testing.B) {
	for _, latency := range []time.Duration{100 * time.Microsecond, time.Millisecond} {
		for _, maxSize := range []int{1, 64} {
			for _, clients := range []int{1, 16, 128} {
				for _, conc := range []int{1, 4} {
					name := fmt.Sprintf("latency=%s maxSize=%d clients=%d conc=%d", latency, maxSize, clients, conc)
					b.Run(name, func(b *testing.B) {
						fn := func(ctx context.Context, reqs []int) ([]int, error) {
							time.Sleep(latency)
							return append([]int(nil), reqs...), nil
						}
						bb := batcher.New(fn,
							batcher.WithMaxSize(maxSize),
							batcher.WithInterval(time.Millisecond),
							batcher.WithMaxConcurrency(conc),
						)
						defer bb.Close(context.Background())

						n := b.N
						base, extra := n/clients, n%clients
						var wg sync.WaitGroup
						b.ResetTimer()
						for c := 0; c < clients; c++ {
							wg.Add(1)
							go func(c, iters int) {
								defer wg.Done()
								for i := 0; i < iters; i++ {
									if _, err := bb.Do(context.Background(), i); err != nil {
										b.Error(err)
										return
									}
								}
							}(c, base+boolToInt(c < extra))
						}
						wg.Wait()
						b.ReportMetric(float64(n)/b.Elapsed().Seconds(), "items/s")
					})
				}
			}
		}
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
