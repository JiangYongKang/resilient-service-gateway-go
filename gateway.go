package gateway

import (
	"sync"
	"time"
)

// LogEvent 是一次网关决策的结构化日志记录。单测可通过 Gateway.Logger
// 捕获这些字段并打印：下游标识、熔断状态、剩余预算、尝试次数、失败分类等。
type LogEvent struct {
	RequestID string
	Route     string
	Phase     string // "reject" | "dispatch" | "backoff" | "terminal" | "success"
	// Breaker 是决策时刻的熔断状态文本；Remaining 是剩余总预算。
	Breaker   string
	Remaining time.Duration
	// Unlimited 为 true 时该调用不设总截止时间，Remaining 不代表真实预算，
	// 对外必须展示为“不设限（unlimited）”而不是某个具体时长。
	Unlimited  bool
	Attempt    int
	MaxAttempt int
	Reason     RejectReason
	Class      string
	Kind       TerminalKind
	InFlight   int
}

type routeEntry struct {
	cfg     RouteConfig
	breaker *Breaker
	bulk    *Bulkhead
}

// Gateway 是本地服务调用网关，按路由维护独立的熔断、隔离与重试治理。
type Gateway struct {
	mu     sync.RWMutex
	routes map[string]*routeEntry

	// Logger 用于输出决策日志；为 nil 时使用标准库 log.Printf。
	Logger func(ev LogEvent)
	// Sleep 用于重试退避，默认 time.Sleep；测试可注入以避免真实等待。
	Sleep func(d time.Duration)
}

// New 创建空网关。
func New() *Gateway {
	return &Gateway{routes: make(map[string]*routeEntry)}
}

// RegisterRoute 注册一条路由及其治理配置。
func (g *Gateway) RegisterRoute(name string, cfg RouteConfig) {
	if cfg.Retry.MaxAttempts < 1 {
		cfg.Retry.MaxAttempts = 1
	}
	if cfg.Classifier == nil {
		cfg.Classifier = func(error) FailureClass { return FailureNonRetryable }
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.routes[name] = &routeEntry{
		cfg:     cfg,
		breaker: NewBreaker(name, cfg.Threshold, cfg.OpenCooldown, cfg.ProbeMaxConcurrent, nil),
		bulk:    NewBulkhead(cfg.MaxConcurrent),
	}
}

func (g *Gateway) lookup(route string) *routeEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.routes[route]
}

func (g *Gateway) log(ev LogEvent) {
	if g.Logger != nil {
		g.Logger(ev)
		return
	}
	logEventDefault(ev)
}

func (g *Gateway) sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	if g.Sleep != nil {
		g.Sleep(d)
	} else {
		time.Sleep(d)
	}
}

// RouteStatus 是观测用的路由状态快照。
type RouteStatus struct {
	Route              string
	Breaker            breakerState
	InFlight           int
	MaxConcurrent      int
	ProbeMaxConcurrent int
	Threshold          int
	OpenCooldown       time.Duration
}
