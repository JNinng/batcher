# 逐条结果表示：Result 与 keyed-map 变体

ADR-0001 说「单条业务失败由用户在 Res 中自行表达」；本决定提供该表达的标准形态，两部分收敛于 P0 的 key 映射骨架。其一，`Result[Res]{Value, Err}` 辅助类型：位置对位 API 下逐条表达业务失败的通用表示——库不解读 Result，批次仍整批成败，批级失败走 `Do` 返回的 error、逐条失败由 `Result.Err` 承载，结果切片与请求恒等长。其二，keyed-map 变体 `NewKeyedMap`：在 NewKeyed 的折叠骨架之上，批次函数返回 `map[K]Res`，按去重切片的 key 回查交付，缺 key 转 `Result{Err: ErrMissingResult}`——单条缺失是业务事实而非整批失败，`Err` 缺失与否只影响该条。不做 `map[Req]Res`：与「不直接拿 Req 当 map key」的设计决策冲突（ADR-0004），且强加 Req comparable 约束。`NewKeyedMap` 返回 `*Batcher[Req, Result[Res]]`，与 `New` 下以 Result 作 Res 的用法同构，两条提交路径共享同一套调用方规则。key 函数在折叠与回查各调一次，须为纯函数（同 Req 恒同 key）；回查阶段的 panic 与折叠同口径降级为批级错误（callKey recover）。

## Consequences

- 调用方统一规则：批级失败看 `Do` 的 error（`errors.Is` 判 `ErrClosed` / `ErrOverloaded` 等），逐条失败看 `Result.Err`（`errors.Is` 判 `ErrMissingResult`），两层正交。
- `NewKeyedMap` 以 NewKeyed 适配器实现（fn 包装为返回 `[]Result[Res]` 的 BatchFunc），折叠、占槽、准入、观测口径零改动；map 中多余的 key 忽略，长度契约由转换内建满足。
- examples/httpserver 示范部分失败惯用法：下游单条不存在 → 该请求 404，其余照常 200，下游一挂仍整批 500。
- fire-and-forget（P1 `Enqueue`）落地时逐条失败表示与其正交：无 Waiter 读取结果，逐条口径由 per-item 回调届时定义。
