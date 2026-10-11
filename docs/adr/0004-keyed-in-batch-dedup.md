# 批内 key 去重：独立构造器 NewKeyed，折叠在交付边界

ADR-0001 曾明确排除按 key 去重；本决定将其翻转为独立构造器 `NewKeyed[K comparable, Req, Res any](key func(Req) K, fn, opts...)`，返回同一个 `*Batcher`。契约变化足以改变批次形状——批次函数收到去重后的 `[]Req`、`len(res) == len(unique)`——按演化原则不做 `WithXxx` 选项，让契约差异在类型签名上显式可见，避免 ADR-0001 的位置对位语义被 option 静默稀释。key 函数由用户显式提供而非直接拿 Req 当 map key：去重即声明「key 相同 ⇒ 语义等价」，等价性判断属业务知识，库不代猜；重复 key 只首个 Req 送达下游。折叠发生在交付边界：maxSize 仍按原始条数计数，重复 Item 照常占槽触发 Flush——若重复不占槽，单一热点 key 的流永远凑不满 maxSize 个 unique，每批白等满 interval。`BatchFlushedEvent.Size` 报原始条数、`UniqueSize` 报去重后条数，去重率本身是观测指标。

同 key 的多 Waiter 共享同一条执行：单个取消只解除自己，其余照常收结果，与 ADR-0003 兼容。跨批去重不做：串行单在途（ADR-0002）下执行期到达的重复 key 落入下一批重查，批界去重已消掉绝大部分重复。keyFn 的 panic 在折叠处单独 recover、降级为批级错误——callFn 的 recover 只包批次函数，折叠发生在其外，无人 recover 会打穿执行 goroutine、进程崩溃；折叠失败的批内全部原始 Item 以该错误收场，`UniqueSize` 报 0。

## Consequences

- 普通 `New` 行为不变（不去重），`UniqueSize == Size`；keyed 批的位置对位与长度契约对去重切片成立。
- 去重切片保持首次出现序，下游可见的请求顺序稳定；结果回填发生在折叠处，key 函数每条原始 Item 恰调一次（`NewKeyedMap` 的回查路径例外：每条 unique 再调一次，见 ADR 0005）。
- 折叠使批内重复不放大下游负载，但 maxSize 语义（原始条数）与 `batcher_batch_size` 口径保持一致，调参直觉不变。
- `fn` 返回 `map[K]Res` 的变体已落地为 `NewKeyedMap`（缺 key 转单条错误 `ErrMissingResult`，非整批失败），与逐条结果表示共享同一套 key 映射骨架，见 ADR 0005。
- 与分片路由组合的前提「同 key ⇒ 同分片」已在契约中写明（`sharded` 包文档与 ADR 0009）：route 与 key 不一致则同 key 跨片重复、去重失效，属用户责任。
