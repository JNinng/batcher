# 调用方取消仅解除自身等待，Item 保留照常执行

批次函数运行于 batcher 级基础 ctx，不携带任何单个调用方的 deadline；该 ctx 的生命周期与 dispatcher 一致——优雅 flush 完成后随 dispatcher 退出而取消（Close 瞬时取消会杀死最终 flush，与优雅关闭自相矛盾）。调用方 ctx 只治理自己的等待——取消即返回 ctx.Err()，其 Item 留在批中照常执行、结果丢弃。摘除已取消调用方的 Item 会让按位置对位与计时器逻辑复杂化，为省一点下游开销不值得；换来的是批次形状（size、对位）绝不因取消而变化。同理，Close(ctx) 正常返回即保证所有已入队 Waiter 已被唤醒（超时返回 ctx.Err() 时可能仍有在途，后台 flush 以基础 ctx 照常完成）。
