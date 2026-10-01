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
	Breaker   string
	Remaining time.Duration
	// Limited 表示调用方是否给出了有限的总截止时间。
	// 为 false 时调用方不设限，Remaining 不具语义，展示层必须使用 BudgetText()
	// 输出 “unlimited”，而不是把 Remaining 的哨兵值当作真实预算。
	Limited bool
	// AttemptRemaining 仅在 dispatch 事件中有意义：本次尝试实际传播给下游的
	// 时长（单次尝试预算/默认超时与剩余总预算取小后的结果）；0 表示本次尝试
	// 也不设限（AttemptText 返回 “unlimited”）。
	AttemptRemaining time.Duration
	Attempt          int
	MaxAttempt       int
	Reason           RejectReason
	Class            string
	Kind             TerminalKind
	InFlight         int
}

// BudgetText 返回剩余总预算的对外表达：调用方不设限时返回 "unlimited"，
// 避免用某个具体时长误导排查；有限时返回剩余时长的标准表示。
func (ev LogEvent) BudgetText() string {
	if !ev.Limited {
		return "unlimited"
	}
	return ev.Remaining.String()
}

// AttemptText 返回本次尝试实际预算的对外表达：不设限时返回 "unlimited"。
func (ev LogEvent) AttemptText() string {
	if ev.AttemptRemaining <= 0 {
		return "unlimited"
	}
	return ev.AttemptRemaining.String()
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
