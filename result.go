package batcher

import (
	"context"
	"errors"
)

// Result 是位置对位 API 下逐条业务失败的通用表示：批级失败走 Do 返回
// 的 error，逐条失败由 Err 承载，Value 仅在 Err == nil 时有意义。以
// Result 作 Res 的 Batcher 行为与其余完全一致——库不解读 Result，
// 批次仍整批成败，结果切片与请求恒等长。
type Result[Res any] struct {
	Value Res
	Err   error
}

// ErrMissingResult 表示 keyed-map 批次中该 key 不在批次函数返回的
// map 里。它只表达单条缺失（经 Result.Err 交付），不构成整批失败。
var ErrMissingResult = errors.New("batcher: missing result")

// MapBatchFunc 是 keyed-map 变体的批次函数：接收去重后的 []Req
// （同 NewKeyed），返回按 key 索引的结果表；返回非 nil 错误即整批失败。
type MapBatchFunc[K comparable, Req any, Res any] func(ctx context.Context, reqs []Req) (map[K]Res, error)

// NewKeyedMap 构造 keyed-map 变体的 Batcher 并启动 dispatcher：在
// NewKeyed 的批内去重之上，批次函数返回 map[K]Res，按去重切片的
// key 回查交付；缺 key 转 Result{Err: ErrMissingResult}——单条缺失
// 是业务事实而非整批失败，map 中多余的 key 忽略。返回
// *Batcher[Req, Result[Res]]，与 New 下以 Result 作 Res 的用法同构：
// 批级失败看 Do 的 error，逐条失败看 Result.Err。key 函数在折叠与
// 回查各调一次，须为纯函数（同 Req 恒同 key）；回查阶段的 panic 与
// 折叠同口径降级为批级错误。key 或 fn 为 nil 属程序员工错；选项校验
// 同 New。均为构造期 panic。
func NewKeyedMap[K comparable, Req, Res any](key func(Req) K, fn MapBatchFunc[K, Req, Res], opts ...Option) *Batcher[Req, Result[Res]] {
	if fn == nil {
		panic("batcher: map batch function must not be nil")
	}
	positional := func(ctx context.Context, reqs []Req) ([]Result[Res], error) {
		m, err := fn(ctx, reqs)
		if err != nil {
			return nil, err
		}
		out := make([]Result[Res], len(reqs)) // 长度契约内建满足
		for i, req := range reqs {
			k, kerr := callKey(key, req)
			if kerr != nil {
				return nil, kerr
			}
			if v, ok := m[k]; ok {
				out[i] = Result[Res]{Value: v}
			} else {
				out[i] = Result[Res]{Err: ErrMissingResult}
			}
		}
		return out, nil
	}
	return NewKeyed(key, positional, opts...)
}
