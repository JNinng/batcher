package batcher

import (
	"time"

	"github.com/jninng/observ"
)

// Option 定制 Batcher 构造参数；MaxSize 与 Interval 必填。
// config 不含泛型字段，故 Option 无需类型参数、可直接推断。
type Option func(*config)

type config struct {
	maxSize  int
	interval time.Duration
	observer Observer
	meter    observ.Meter
	logger   observ.Logger
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
