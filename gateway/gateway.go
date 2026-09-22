package gateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

// Request 是经网关派发的请求。
type Request struct {
	// Route 目标下游标识。
	Route string
	// Idempotent 声明调用是否幂等；仅幂等请求允许重试。
	Idempotent bool
	// Payload 不透明的请求负载。
	Payload any
}

// Response 是下游返回的不透明结果。
type Response struct {
	Payload any
}

// Downstream 是下游实现需要满足的接口。
type Downstream interface {
	// Invoke 发起一次调用；返回的 error 会由 Classifier 分类。
	Invoke(ctx context.Context, req *Request) (*Response, error)
}

// DownstreamFunc 便于用函数实现 Downstream。
type DownstreamFunc func(ctx context.Context, req *Request) (*Response, error)

func (f DownstreamFunc) Invoke(ctx context.Context, req *Request) (*Response, error) {
	return f(ctx, req)
}

// Classifier 将下游错误分类为可重试/不可重试；nil 错误必须返回 ClassNone。
type Classifier func(err error) FailureClass

// Logger 是网关使用的最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// DownstreamConfig 单个下游的隔离与策略配置。
type DownstreamConfig struct {
	// Handler 下游实现。
	Handler Downstream
	// Classifier 失败分类器；为 nil 时使用默认分类器。
	Classifier Classifier
	// MaxConcurrency 该下游最大并发调用数（含重试）；默认 8。
	MaxConcurrency int
	// FailureThreshold Closed 下连续失败多少次后熔断；默认 5。
	FailureThreshold int
	// OpenDuration 熔断打开持续时长；默认 5s。
	OpenDuration time.Duration
	// ProbeConcurrency HalfOpen 下允许的探测并发；默认 1。
	ProbeConcurrency int
	// SuccessThreshold HalfOpen 下连续探测成功多少次后关闭；默认 1。
	SuccessThreshold int
	// MaxAttempts 最大尝试次数（1 表示不重试）；默认 1。
	MaxAttempts int
	// PerAttemptTimeout 单次尝试超时；为 0 表示仅受调用方截止时间约束。
	PerAttemptTimeout time.Duration
	// Backoff 重试退避；为 0 表示立即重试。
	Backoff time.Duration
}

// Config 网关配置。
type Config struct {
	Routes map[string]DownstreamConfig
	Logger Logger
}

// routeRuntime 是单个下游的运行时资源集合（熔断与并发均按下游隔离）。
type routeRuntime struct {
	name    string
	cfg     DownstreamConfig
	breaker *Breaker
	pool    *Pool
}

// Gateway 本地服务调用网关。
type Gateway struct {
	routes map[string]*routeRuntime
	logger Logger
}

// New 构造网关。
func New(cfg Config) *Gateway {
	g := &Gateway{routes: map[string]*routeRuntime{}, logger: cfg.Logger}
	if g.logger == nil {
		g.logger = defaultLogger()
	}
	for name, dc := range cfg.Routes {
		d := dc
		if d.Handler == nil {
			continue
		}
		if d.MaxConcurrency < 1 {
			d.MaxConcurrency = 8
		}
		if d.FailureThreshold < 1 {
			d.FailureThreshold = 5
		}
		if d.OpenDuration <= 0 {
			d.OpenDuration = 5 * time.Second
		}
		if d.ProbeConcurrency < 1 {
			d.ProbeConcurrency = 1
		}
		if d.SuccessThreshold < 1 {
			d.SuccessThreshold = 1
		}
		if d.MaxAttempts < 1 {
			d.MaxAttempts = 1
		}
		if d.Classifier == nil {
			d.Classifier = defaultClassifier
		}
		g.routes[name] = &routeRuntime{
			name: name,
			cfg:  d,
			breaker: newBreaker(name, BreakerConfig{
				FailureThreshold: d.FailureThreshold,
				OpenDuration:     d.OpenDuration,
				ProbeConcurrency: d.ProbeConcurrency,
				SuccessThreshold: d.SuccessThreshold,
			}),
			pool: newPool(name, d.MaxConcurrency),
		}
	}
	return g
}

// Call 按路由派发请求，统一处理熔断、隔离、超时与重试。
//
// 拒绝/终局语义（返回 *RejectError，绝不直接透出下游原始错误）：
//   - RejectRouteNotFound    路由不存在
//   - RejectDeadlineExpired  首次派发前调用方截止时间已过期
//   - RejectInsufficientBudget 剩余预算不足以完成一次配置好的尝试
//   - RejectCircuitOpen      熔断打开/半开探测并发已满
//   - RejectNonRetryable     下游不可重试失败（或可重试失败但请求非幂等）
//   - RejectBudgetExhausted  可重试失败但尝试次数或预算已耗尽
func (g *Gateway) Call(ctx context.Context, req *Request) (*Response, error) {
	rt, ok := g.routes[req.Route]
	if !ok {
		err := newReject(RejectRouteNotFound, req.Route, "route is not registered")
		g.logger.Printf("downstream=%q reject reason=%s", req.Route, RejectRouteNotFound)
		return nil, err
	}

	budget := newBudget(ctx)
	now := time.Now()
	if r := preDispatchGate(rt, budget, now, 1); r != nil {
		g.logReject(rt, budget, 1, ClassNone, r)
		return nil, r
	}

	attempt := 0
	for {
		attempt++
		now := time.Now()

		// 每次尝试（含重试）派发前都重新做预算门控。
		if r := preDispatchGate(rt, budget, now, attempt); r != nil {
			g.logReject(rt, budget, attempt, ClassRetryable, r)
			return nil, r
		}

		// 熔断许可
		permit, reason := rt.breaker.Acquire(now)
		if reason == RejectCircuitOpen {
			r := newReject(RejectCircuitOpen, rt.name, "circuit breaker is open or probe concurrency is full")
			g.logReject(rt, budget, attempt, ClassNone, r)
			return nil, r
		}

		// 并发槽位：先非阻塞尝试，满了则在调用方预算内等待；
		// 重试也必须重新排队获取槽位，因此重试不会突破并发上限。
		release, slotOK := rt.pool.tryAcquire()
		if !slotOK {
			g.logf(rt, budget, attempt, ClassNone, "concurrency limit reached (max=%d), waiting for slot", rt.cfg.MaxConcurrency)
			rl, err := rt.pool.acquireCtx(ctx)
			if err != nil {
				permit.Release()
				r := waitInterruptedReject(rt, budget, attempt, ctx.Err())
				g.logReject(rt, budget, attempt, ClassNone, r)
				return nil, r
			}
			release = rl
		}

		// 构造尝试上下文：把调用方截止时间传播给下游，
		// 单次尝试超时不得超过剩余预算。
		attemptCtx, cancel := buildAttemptContext(ctx, budget, rt.cfg.PerAttemptTimeout, now)

		rem, _ := budget.Remaining(time.Now())
		g.logf(rt, budget, attempt, ClassNone,
			"dispatch attempt=%d/%d breaker=%s remaining_budget=%s",
			attempt, rt.cfg.MaxAttempts, rt.breaker.State(), fmtDur(rem))

		resp, callErr := rt.cfg.Handler.Invoke(attemptCtx, req)
		cancel()
		release()

		if callErr == nil {
			permit.ReportResult(true)
			g.logf(rt, budget, attempt, ClassNone, "attempt succeeded; breaker=%s", rt.breaker.State())
			return resp, nil
		}

		class := rt.cfg.Classifier(callErr)
		permit.ReportResult(false)
		// 仅在日志内部保留原因用于解释，不向调用方透出。
		g.logf(rt, budget, attempt, class,
			"attempt failed class=%s breaker=%s cause=%q", class, rt.breaker.State(), errorText(callErr))

		// 不可重试，或虽可重试但调用方未声明幂等：终局。
		if class != ClassRetryable || !req.Idempotent {
			detail := "downstream reported non-retryable failure"
			if class == ClassRetryable {
				detail = "retryable failure but request is not declared idempotent"
			}
			r := newReject(RejectNonRetryable, rt.name, detail)
			r.Class = class
			g.logReject(rt, budget, attempt, class, r)
			return nil, r
		}

		// 可重试且幂等：先看次数预算。
		if attempt >= rt.cfg.MaxAttempts {
			r := newReject(RejectBudgetExhausted, rt.name, "max attempts exhausted")
			r.Class = ClassRetryable
			g.logReject(rt, budget, attempt, ClassRetryable, r)
			return nil, r
		}

		// 退避必须在剩余预算内等待；等待期间预算到期则终局。
		if rt.cfg.Backoff > 0 {
			if r := sleepWithinBudget(ctx, rt, budget, attempt, rt.cfg.Backoff); r != nil {
				g.logReject(rt, budget, attempt, ClassRetryable, r)
				return nil, r
			}
		}
	}
}

// preDispatchGate 在派发前检查调用方截止时间；attempt==1 时给出“首次拒绝”语义。
func preDispatchGate(rt *routeRuntime, b Budget, now time.Time, attempt int) *RejectError {
	rem, has := b.Remaining(now)
	if b.Expired(now) {
		if attempt == 1 {
			return newReject(RejectDeadlineExpired, rt.name, "caller deadline already expired before dispatch")
		}
		r := newReject(RejectBudgetExhausted, rt.name, "caller deadline expired before next attempt")
		r.Class = ClassRetryable
		return r
	}
	// 配置了单次尝试超时时，剩余预算不足以支撑一次完整尝试，则在派发前拒绝。
	if has && rt.cfg.PerAttemptTimeout > 0 && rem < rt.cfg.PerAttemptTimeout {
		if attempt == 1 {
			return newReject(RejectInsufficientBudget, rt.name,
				"remaining budget is smaller than per-attempt timeout")
		}
		r := newReject(RejectBudgetExhausted, rt.name,
			"remaining budget cannot cover another attempt")
		r.Class = ClassRetryable
		return r
	}
	return nil
}

func buildAttemptContext(parent context.Context, b Budget, perAttempt time.Duration, now time.Time) (context.Context, context.CancelFunc) {
	if perAttempt <= 0 {
		return parent, func() {}
	}
	d := perAttempt
	if rem, ok := b.Remaining(now); ok && rem < d {
		d = rem
	}
	if d <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, d)
}

func waitInterruptedReject(rt *routeRuntime, b Budget, attempt int, ctxErr error) *RejectError {
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		if attempt == 1 {
			return newReject(RejectDeadlineExpired, rt.name, "deadline reached while waiting for concurrency slot")
		}
		r := newReject(RejectBudgetExhausted, rt.name, "deadline reached while waiting for concurrency slot")
		r.Class = ClassRetryable
		return r
	}
	return newReject(RejectBudgetExhausted, rt.name, "canceled while waiting for concurrency slot")
}

func sleepWithinBudget(ctx context.Context, rt *routeRuntime, b Budget, attempt int, backoff time.Duration) *RejectError {
	wait := backoff
	if rem, ok := b.Remaining(time.Now()); ok {
		if rem <= 0 {
			r := newReject(RejectBudgetExhausted, rt.name, "no budget left for retry backoff")
			r.Class = ClassRetryable
			return r
		}
		if rem < wait {
			wait = rem
		}
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		r := newReject(RejectBudgetExhausted, rt.name, "context ended during retry backoff")
		r.Class = ClassRetryable
		return r
	case <-t.C:
		return nil
	}
}

func defaultClassifier(err error) FailureClass {
	if c := ClassOf(err); c != ClassNone {
		return c
	}
	// 默认规则：超时/取消视为瞬时可重试，其余视为不可重试。
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ClassRetryable
	}
	return ClassNonRetryable
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.String()
}

func (g *Gateway) logf(rt *routeRuntime, b Budget, attempt int, class FailureClass, format string, args ...any) {
	rem, has := b.Remaining(time.Now())
	remText := "infinite"
	if has {
		remText = fmtDur(rem)
	}
	g.logger.Printf("downstream=%q breaker=%s remaining_budget=%s attempt=%d class=%s: %s",
		rt.name, rt.breaker.State(), remText, attempt, class, fmt.Sprintf(format, args...))
}

func (g *Gateway) logReject(rt *routeRuntime, b Budget, attempt int, class FailureClass, r *RejectError) {
	rem, has := b.Remaining(time.Now())
	remText := "infinite"
	if has {
		remText = fmtDur(rem)
	}
	g.logger.Printf("downstream=%q breaker=%s remaining_budget=%s attempt=%d class=%s: reject reason=%s detail=%s",
		rt.name, rt.breaker.State(), remText, attempt, class, r.Reason, r.Detail)
}

func defaultLogger() Logger {
	return log.New(os.Stderr, "[gateway] ", log.LstdFlags|log.Lmicroseconds)
}
