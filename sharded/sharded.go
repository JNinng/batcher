// Package sharded 为 batcher 提供路由层：把并发到达的单条请求按路由
// 函数分发给 n 个独立组批、独立执行的分片 Batcher。场景为分片下游
// （Redis cluster、分库分表）——每个分片各连一个下游节点，批次函数
// 经工厂的 shard 下标选择对应连接；也顺带解决下游并发问题（各分片
// 独立在途，总吞吐随分片数扩展）。
//
// 分层：组批层构造器（batcher.New 等）返回同一个
// *Batcher；路由层返回实现同一 API 面（Do / Enqueue / EnqueueFunc /
// Close）的包装类型 *Sharded。Option 类能力（WithMaxPending /
// WithMaxConcurrency / WithMaxBytes 等）对每个分片直接生效，分片级
// 配置一律经工厂构造器的捕获选项统一配置。
//
// 契约与注意：
//   - 路由函数直接产出分片下标 [0, n)，须对同一请求返回稳定下标；
//     不做 hashFn → K comparable 的中间形态（comparable 类型无法直接
//     映射到下标，二次哈希留给 route 内做）。越界下标属程序错误，
//     调用期 panic——route 是逐请求执行的用户代码，无法在构造期校验。
//   - 与 NewKeyedFactory 组合的前提是「同 key ⇒ 同分片」：route 与
//     key 不一致则同 key 落入不同分片、跨片重复，去重失效，属用户
//     责任。
//   - Observer 与指标按分片各发/各计：每分片各触发一次 OnCreated/
//     OnClosed；同一 Observer 被 n 个分片并发回调（实现必须并发安全，
//     跨分片事件顺序不保证）；同一 Meter 会被 n 个分片重复注册同名
//     无 label 指标——Meter 须幂等容忍重复注册，或经工厂按 shard
//     注入带 label 的 adapter（带 label 聚合由用户侧完成，同核心包
//     约定）。
//   - 准入与并发上限均为分片级：WithMaxPending 限单片已准入未交付
//     提交数，WithMaxConcurrency 限单片下游并发；全库总量为分片数
//     乘单片上限，负载不均时各分片独立退化。
package sharded

import (
	"context"
	"fmt"
	"sync"

	"github.com/jninng/batcher"
)

// Factory 构造单个分片的 Batcher：shard 为分片下标 [0, n)。批次函数
// 与分片级配置（含必填的 MaxSize/Interval）都由工厂捕获——内置工厂
// 为 NewFactory / NewKeyedFactory / NewKeyedMapFactory（分别包装
// batcher.New / NewKeyed / NewKeyedMap）；自定义工厂可用 shard 下标
// 按分片选择连接、注入带 label 的 Meter adapter 等分片级差异
// （cluster/分表硬需求）。
//
// 批次函数归工厂而非 New 接收：柯里化工厂构造器的类型参数须能从
// 实参完整推断，Res 只出现在返回类型——fn 若延迟到 New 传入，调用
// 点将被迫写显式类型参数（Go 推断限制，见各构造器）。
type Factory[Req, Res any] func(shard int) *batcher.Batcher[Req, Res]

// NewFactory 返回包装 batcher.New 的默认工厂：每个分片以同一批次
// 函数与选项构造普通 Batcher。opts 为分片级配置（必含 MaxSize 与
// Interval），由工厂捕获、New 构造每个分片时应用。fn 为 nil 属程序
// 工错（首个分片构造期 panic，New 级联回收）。
func NewFactory[Req, Res any](fn batcher.BatchFunc[Req, Res], opts ...batcher.Option) Factory[Req, Res] {
	return func(shard int) *batcher.Batcher[Req, Res] {
		return batcher.New(fn, opts...)
	}
}

// NewKeyedFactory 返回包装 batcher.NewKeyed 的工厂——batcher.NewKeyed
// 去掉 opts 的柯里化形式，使分片与批内去重可组合。组合前提「同 key ⇒
// 同分片」：route 与 key 不一致则同 key 落入不同分片、去重失效，属
// 用户责任。key 为 nil 属程序员工错，本构造期 panic；fn 为 nil 在
// 首个分片构造期 panic（NewKeyedMap 同款校验），New 级联回收。
func NewKeyedFactory[K comparable, Req, Res any](key func(Req) K, fn batcher.BatchFunc[Req, Res], opts ...batcher.Option) Factory[Req, Res] {
	if key == nil {
		panic("batcher/sharded: key function must not be nil")
	}
	return func(shard int) *batcher.Batcher[Req, Res] {
		return batcher.NewKeyed(key, fn, opts...)
	}
}

// NewKeyedMapFactory 返回包装 batcher.NewKeyedMap 的工厂：在分片与
// 批内去重之上以 map 回查逐条缺失（Result.Err 表达），批次函数返回
// map[K]Res。组合前提同 NewKeyedFactory（同 key ⇒ 同分片）。key 为
// nil 属程序员工错，本构造期 panic；fn 为 nil 在首个分片构造期
// panic（NewKeyedMap 同款校验），New 级联回收。
func NewKeyedMapFactory[K comparable, Req, Res any](key func(Req) K, fn batcher.MapBatchFunc[K, Req, Res], opts ...batcher.Option) Factory[Req, batcher.Result[Res]] {
	if key == nil {
		panic("batcher/sharded: key function must not be nil")
	}
	return func(shard int) *batcher.Batcher[Req, batcher.Result[Res]] {
		return batcher.NewKeyedMap(key, fn, opts...)
	}
}

// Sharded 是分片 Batcher：n 个独立组批、独立执行的 *Batcher 加路由
// 函数。构造后即可并发使用；用毕必须 Close（级联关闭全部分片）。
// 零可变状态——全部方法并发安全。
type Sharded[Req, Res any] struct {
	route  func(Req) int
	shards []*batcher.Batcher[Req, Res]
}

// New 构造分片 Batcher：对 [0, n) 每个下标以 shard 调 factory 各建
// 一个分片。route、factory 为 nil 或 n 非正属程序员工错，构造期
// panic；工厂对任一分片返回 nil 同样 panic，且构造中断（返回 nil
// 或 panic）时级联回收已建分片，不泄漏 dispatcher。
func New[Req, Res any](route func(Req) int, n int, factory Factory[Req, Res]) *Sharded[Req, Res] {
	switch {
	case route == nil:
		panic("batcher/sharded: route function must not be nil")
	case n <= 0:
		panic("batcher/sharded: shard count must be > 0")
	case factory == nil:
		panic("batcher/sharded: shard factory must not be nil")
	}
	s := &Sharded[Req, Res]{route: route, shards: make([]*batcher.Batcher[Req, Res], 0, n)}
	defer func() {
		if len(s.shards) < n { // 构造中断：级联回收已建分片
			for _, b := range s.shards {
				_ = b.Close(context.Background())
			}
		}
	}()
	for i := 0; i < n; i++ {
		b := factory(i)
		if b == nil {
			panic(fmt.Sprintf("batcher/sharded: shard factory returned nil batcher for shard %d", i))
		}
		s.shards = append(s.shards, b)
	}
	return s
}

// shard 按路由取分片。越界下标属程序错误（route 须返回 [0, n)），
// 调用期 panic。
func (s *Sharded[Req, Res]) shard(req Req) *batcher.Batcher[Req, Res] {
	i := s.route(req)
	if i < 0 || i >= len(s.shards) {
		panic(fmt.Sprintf("batcher/sharded: route returned out-of-range shard index %d, want [0, %d)", i, len(s.shards)))
	}
	return s.shards[i]
}

// Do 提交一条 Item 并阻塞等待自己那份结果，行为同 *Batcher.Do——
// 路由至 req 对应分片后同步等待，调用方 ctx 治理提交与等待两个阶段。
func (s *Sharded[Req, Res]) Do(ctx context.Context, req Req) (Res, error) {
	return s.shard(req).Do(ctx, req)
}

// Enqueue 提交一条 fire-and-forget Item，行为同 *Batcher.Enqueue：
// 返回 nil 即已入队对应分片，此后不再有取消语义。
func (s *Sharded[Req, Res]) Enqueue(ctx context.Context, req Req) error {
	return s.shard(req).Enqueue(ctx, req)
}

// EnqueueFunc 在 Enqueue 之上接受 per-item 回调，行为同
// *Batcher.EnqueueFunc。回调运行于所在分片的执行 goroutine，「不得
// 同步再入本 Batcher」按分片论——再入其他分片不会自死锁，再入本
// 分片同核心约束。
func (s *Sharded[Req, Res]) EnqueueFunc(ctx context.Context, req Req, cb func(res Res, err error)) error {
	return s.shard(req).EnqueueFunc(ctx, req, cb)
}

// Close 级联关闭全部分片并等待：并发调用各分片的 Close（并行
// drain），全部返回 nil 即所有分片的已入队 Waiter 均被唤醒、批次
// 函数不再运行。幂等，可并发调用；ctx 超时仅放弃等待（各分片后台
// flush 照常完成），返回 ctx.Err()。分片 closing 后路由到它的新
// 提交立即 ErrClosed。
func (s *Sharded[Req, Res]) Close(ctx context.Context) error {
	errCh := make(chan error, len(s.shards))
	var wg sync.WaitGroup
	for _, b := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- b.Close(ctx)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil { // 分片 Close 只可能返回 ctx.Err()
			return err
		}
	}
	return nil
}
