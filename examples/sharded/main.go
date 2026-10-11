// Command sharded 模拟分库分表场景：8 个分库各自独立组批、独立执行，
// 订单 ID 按 id%8 路由；工厂以 shard 下标包装批次函数选择对应连接
// （cluster/分表模式），分片内按订单 ID 去重合并热点查询，每分片限
// 两个并发在途（WithMaxConcurrency 分片级生效）。流量向 3 号分库
// 偏斜，运行后打印各分片负载——负载不均一目了然，各分片独立退化。
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jninng/batcher"
	"github.com/jninng/batcher/sharded"
)

const shards = 8

func main() {
	var mu sync.Mutex
	batches := make([]int, shards) // 每分片批次数
	items := make([]int, shards)   // 每分片实际送达下游的条数（去重后）

	lookup := func(ctx context.Context, ids []int) ([]string, error) {
		res := make([]string, len(ids))
		for i, id := range ids {
			res[i] = fmt.Sprintf("order-%d", id)
		}
		time.Sleep(5 * time.Millisecond) // 模拟分库批量查询
		return res, nil
	}

	key := func(id int) int { return id }

	// 自定义工厂：以 shard 下标包装批次函数——真实场景在此按分片选
	// 连接（connect(shard).QueryOrders）；分片级配置全部走工厂捕获。
	factory := func(shard int) *batcher.Batcher[int, string] {
		tagged := func(ctx context.Context, ids []int) ([]string, error) {
			res, err := lookup(ctx, ids)
			mu.Lock()
			batches[shard]++
			if err == nil {
				items[shard] += len(ids)
			}
			mu.Unlock()
			return res, err
		}
		return batcher.NewKeyed(key, tagged,
			batcher.WithMaxSize(20),                   // 攒满 20 条立即查询
			batcher.WithInterval(20*time.Millisecond), // 否则 20ms 后查询
			batcher.WithMaxConcurrency(2),             // 分片级下游并发上限
		)
	}

	route := func(id int) int { return id % shards }
	s := sharded.New(route, shards, factory)
	defer func() {
		// 级联 Close：并行 drain 全部分片并等待，返回即全部交付。
		if err := s.Close(context.Background()); err != nil {
			log.Fatalf("Close: %v", err)
		}
		fmt.Println("closed: all shards drained")
	}()

	// 400 笔并发查询：每 5 笔有 2 笔挤进热点分库 3（id 落在 3+8k）。
	var wg sync.WaitGroup
	for i := 0; i < 400; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := i
			if i%5 < 2 {
				id = 3 + 8*(i%20) // 热点分库内的 20 个不同订单，循环复用
			}
			if _, err := s.Do(context.Background(), id); err != nil {
				log.Printf("Do(%d): %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	fmt.Println("分片负载（批次数 / 送达下游条数）:")
	for i := range batches {
		bar := strings.Repeat("#", batches[i])
		fmt.Printf("  分片 %d: %3d 批 / %3d 条 %s\n", i, batches[i], items[i], bar)
	}
}
