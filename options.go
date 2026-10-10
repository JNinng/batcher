package batcher

import (
	"time"

	"github.com/jninng/observ"
)

// Option 定制 Batcher 构造参数；MaxSize 与 Interval 必填。
// config 不含泛型字段，故 Option 无需类型参数、可直接推断。
type Option func(*config)

type config struct {
	maxSize    int
	interval   time.Duration
	maxPending int
	observer   Observer
	meter      observ.Meter
	logger     observ.Logger
}

// WithMaxSize 设置批大小上限（Flush 触发条件之一），必须 > 0。
func WithMaxSize(n int) Option {
	return func(c *config) { c.maxSize = n }
}

// WithInterval 设置组批时间间隔（Flush 触发条件之一，自组批起算的
// 尽力而为软上限），必须 > 0。
func WithInterval(d time.Duration) Option {
	return func(c *config) { c.interval = d }
}

// WithMaxPending 设置已准入未交付提交数上限，实施负载卸载：达上限后
// 新 Do 立即返回 ErrOverloaded，不入队、不堆积 goroutine。准入采用
// Add-then-check（原子加一后比较自身返回值），已准入未交付提交数恒
// 不超过 n；上限的软性仅在于并发竞争下的计数瞬时虚高（其他提交的
// 拒绝尚未回滚），额度尚空的提交可能被假阳性提前拒绝。Formation 与
// 在途中的 Item 同样占用额度：批次大小恒不超过 n，n < WithMaxSize 时
// size 触发不可达，Flush 全部退化为 interval 触发。未设置或 0 即无界，
// 负值属程序员工错（New 构造期 panic）。
func WithMaxPending(n int) Option {
	return func(c *config) { c.maxPending = n }
}

// WithObserver 注入类型化旁路观测接口，默认 Noop。
func WithObserver(o Observer) Option {
	return func(c *config) { c.observer = o }
}

// WithMeter 注入指标 Meter，默认 Noop（NoopMeter 可比较，
// 命中时库整体跳过埋点）。
func WithMeter(m observ.Meter) Option {
	return func(c *config) { c.meter = m }
}

// WithLogger 注入日志 Logger；未注入时构造期快照
// observ.DefaultLogger() 并固定。
func WithLogger(l observ.Logger) Option {
	return func(c *config) { c.logger = l }
}
