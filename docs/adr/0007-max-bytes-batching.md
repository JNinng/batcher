# 按字节攒批：WithMaxBytes 三决策

按字节攒批（WithMaxBytes）落地裁定三项决策如下。其一，字节口径来源取**显式 size 函数** `WithMaxBytes[Req](n int, size func(Req) int)`，不做接口约束——与 key 函数同款哲学：字节量是业务知识，由用户显式声明，接口约束能隐式强加契约且无法事后补给第三方类型。Option 机制不携带类型参数，size 函数以 `any` 进 config、newBatcher 断言回 `func(Req) int`，与 Req 失配在构造期 panic（保持「程序员工错构造期 panic」惯例）。其二，与 maxSize 的关系取**并列先到先触发**：条数、字节、时间三条件竞速，任一先到即 Flush——延续本库的核心触发哲学与「Option 正交叠加」原则；字节独占触发会让 maxSize 失效、破坏正交性。其三，与 NewKeyed 叠加取**按原始条目字节计**：折叠发生在交付边界，重复 Item 照常占槽计费，与 maxSize 的「原始计数」完全同构——否则单一热点条目永远凑不满 n、每批白等满 interval；实际调用批次函数的是去重后切片，载荷不大于计费值。

组批语义：Formation 内累计字节达到 n 即 Flush；单条即超限自成批大小 1 的孤批——不拆分、不丢弃（拆分属批次函数的业务知识）。sizeFn 在组批路径运行于 dispatcher goroutine，panic 与负数返回同款降级为批级错误（与 key 函数同款约束）：整批预置失败、不调批次函数（UniqueSize 为 0，与折叠前失败口径一致）。drain（关闭 flush）不计字节——关闭 flush 无条件。未配置（不调 WithMaxBytes）时字节路径零开销：sizeFn 不被调用。

## Consequences

- 触发条件从两条变三条，`WaitSeconds` 口径不变（组批耗时）；`batcher_batch_size` 仍报条数，字节分布暂无指标，调参依据留待可观测性 hook 波次一并评估。
- 写路径（`Enqueue`）与 `WithMaxBytes` 组合覆盖高吞吐写出场景（ADR-0006 预留）。
- 未来 `WithInFlight`（P2）落地后字节累计仍发生在 Formation，与执行侧并发正交，无需改动。
- 组批期失败（sizeFn panic / 负数返回）以 ferr 预置整批失败、不调批次函数，UniqueSize 记 0；sizeFn 返回负数与 panic 同路径批级失败（负计费会让字节触发静默失效）。退化竞态：此时恰逢 Close，drain 捞入的条目从未被 size 评估、却随整批收到同一错误——与「整批失败」语义自洽，不单独治理。
- `WithMaxBytes(n>0, nil)`（含 typed-nil）在构造期显式拒绝：config 只在 size 非 nil 时存储，若只按 `cfg.sizeFn != nil` 校验，该组合会静默通过构造、以每批 nil 调用 panic 的批级错误在运行期暴露。
