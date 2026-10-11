# 分片 Batcher：独立包 sharded，工厂按分片构造

分片下游（Redis cluster、分库分表）需要按 key 把并发请求路由到 n 个独立组批、独立执行的分片 Batcher——每个分片各连一个下游节点，批次函数按分片选连接；各分片独立在途也天然缓解下游并发压力。落地为独立子包 `batcher/sharded`，核心包不感知路由层。

## 决策

一、**独立子包**：组合面 = 组批层形态（New/NewKeyed/NewKeyedMap）× 路由层，泛型组合爆炸留在子包消化，核心包只保留组批层。构造器相应消除 stutter：`NewSharded` → `sharded.New`。返回类型为包装类型 `*Sharded[Req, Res]`，实现与 `*Batcher` 相同的 API 面（Do / Enqueue / EnqueueFunc / Close）——单个 Batcher 只有一个 dispatcher，路由层必然是包装（组批层构造器返回同一个 `*Batcher`）。

二、**路由函数直接产出分片下标 [0, n)**：不做 hashFn → K comparable 中间形态（comparable 无法直接映射到下标，二次哈希留给 route 内做）。route 须对同一请求返回稳定下标；越界属程序错误、调用期 panic——route 是逐请求执行的用户代码，无法在构造期校验。

三、**工厂按分片构造，fn 归工厂**（审计修正）：原定 `NewSharded(route, fn, n, factory)`——New 收 fn、经 Factory 形参逐分片传入。落地时发现该形状被 Go 类型推断否决——柯里化工厂构造器（如 `NewKeyedFactory(key, opts...)`）的类型参数中 Res 只出现在返回类型，fn 延迟到 New 传入则调用点必须写显式 `[K, Req, Res]` 三类型参数，既定的 `NewSharded(routeFn, myFn, 8, NewKeyedFactory(keyFn))` 调用形状无法编译。修正为 fn 由工厂构造器接收：`Factory[Req, Res] func(shard int) *Batcher[Req, Res]`，`New(route, n, factory)` 不收 fn 与 opts（配置本就一律走工厂，New 无物可传）。内置工厂 `NewFactory(fn, opts...)` / `NewKeyedFactory(key, fn, opts...)` / `NewKeyedMapFactory(key, fn, opts...)` 分别包装三个组批层构造器，类型参数全部可从实参推断；fn 无来源的自相矛盾不存在——来源即工厂构造器实参。审计修正的副产品：原「NewKeyedMap 的 fn 形状（map[K]Res）与 Factory 统一签名冲突」不再成立，`NewKeyedMapFactory` 得以补全组合矩阵。自定义工厂以 shard 下标按分片选连接、注入带 label 的 Meter adapter（cluster/分表硬需求）。

四、**「同 key ⇒ 同分片」为组合前提**：route 与 key 不一致则同 key 落入不同分片、跨片重复、去重失效，属用户责任，库不校验（route 与 key 是独立函数，无法静态判定一致性）。

五、**生命周期级联**：`Sharded.Close` 并发调用各分片 Close（并行 drain）并等待，全部返回 nil 即所有分片 Waiter 已唤醒、批次函数不再运行；幂等、可并发调用；ctx 超时仅放弃等待，慢分片后台 flush 照常完成——不放大为全库丢失。构造中断（工厂 panic / 返回 nil）时级联回收已建分片，不泄漏 dispatcher。分片 closing 后路由到它的新提交立即 ErrClosed。

六、**观测按分片各发/各计**：每分片各发一次 OnCreated/OnClosed；同一 Observer 被 n 个分片并发回调——实现必须并发安全（该契约已随 ADR-0008 的 MaxConcurrency 先行落地，跨分片事件顺序不保证）。同一 Meter 会被 n 个分片重复注册同名无 label 指标：Meter 须幂等容忍重复注册，或经工厂按 shard 注入带 label 的 adapter（带 label 聚合由用户侧完成，同核心包约定）。

## Consequences

- 准入与并发上限均为分片级：WithMaxPending 限单片已准入未交付提交数、WithMaxConcurrency 限单片下游并发，全库总量为分片数乘单片上限；负载不均时热点分片独立退化，其余分片不受拖累（分片隔离钉死）。
- goroutine 峰值 ≈ n ×（单片 dispatcher + 在途执行）；分片数即资源倍数，n 的取值应与下游分片数对齐而非任意调大。
- 钉子：路由对位与工厂 shard 下标（TestRoutesEachRequestToItsShard / TestFactoryReceivesEachShardIndexOnce）、keyed 对齐/错位组合（TestKeyedFactoryComposesWithSharding）、keyed-map 三层叠加（TestKeyedMapFactoryComposesWithSharding）、分片隔离、级联 Close（含超时与构造回收）、越界路由 panic、构造校验。
- 示例 examples/sharded 演示工厂按分片选连接、分片内去重、分片级 MaxConcurrency 与热点分库的负载不均可见性。
