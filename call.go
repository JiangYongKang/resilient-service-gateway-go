package gateway

import (
	"time"
)

// remainingBudget 计算请求的剩余时间预算；未声明截止时间时返回一个大值。
func remainingBudget(deadline time.Time) time.Duration {
	if deadline.IsZero() {
		return time.Hour
	}
	return time.Until(deadline)
}

// reject 在派发前拒绝并记录日志。
func (g *Gateway) reject(req Request, reason RejectReason, e *routeEntry) error {
	rem := remainingBudget(req.Deadline)
	state := ""
	inFlight := 0
	if e != nil {
		state = e.breaker.State().String()
		inFlight = e.bulk.InFlight()
	}
	g.log(LogEvent{
		RequestID:  req.ID,
		Route:      req.Route,
		Phase:      "reject",
		Breaker:    state,
		Remaining:  rem,
		Reason:     reason,
		InFlight:   inFlight,
		MaxAttempt: 0,
	})
	return &RejectError{Route: req.Route, Reason: reason, Remaining: rem}
}

// Call 按请求中的路由执行调用，返回经过治理后的结果。
//
// 执行顺序（每次尝试）：
//  1. 截止时间预检：已过期 -> RejectError(deadline_expired)；
//  2. 并发隔离舱：槽位已满 -> RejectError(concurrency_limit)；
//  3. 熔断器：打开 -> RejectError(circuit_open)；探测名额满 -> RejectError(probe_concurrency_limit)；
//  4. 构造带传播截止时间的 AttemptContext 派发下游；
//  5. 按失败分类 + 幂等性 + 剩余预算决定是否重试；
//     退避后预算耗尽 -> TerminalError(budget_exhausted)；
//     不可重试（含非幂等） -> TerminalError(non_retryable_failure)。
//
// 下游原始错误不会出现在返回值中，仅用于失败分类。
func (g *Gateway) Call(req Request) (*Response, error) {
	e := g.lookup(req.Route)
	if e == nil {
		return nil, g.reject(req, ReasonUnknownRoute, nil)
	}
	cfg := e.cfg

	// 派发前总预算预检：已过期的请求直接拒绝，不占用任何下游资源。
	if rem := remainingBudget(req.Deadline); rem <= 0 {
		return nil, g.reject(req, ReasonDeadlineExpired, e)
	}

	maxAttempts := cfg.Retry.MaxAttempts
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 每次尝试前都重新检查预算（重试退避可能已耗尽预算）。
		rem := remainingBudget(req.Deadline)
		if rem <= 0 {
			// 已有尝试发生：属于预算耗尽的终局，而不是派发前拒绝。
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalBudgetExhausted, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonDeadlineExpired, e)
		}

		// 并发上限：快速拒绝，不排队，绝不突破 MaxConcurrent。
		if !e.bulk.TryAcquire() {
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalCircuitOpen, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonConcurrencyLimit, e)
		}

		// 熔断判定：打开快速拒绝；半开探测名额有限。
		reason, gen := e.breaker.Allow()
		if reason != "" {
			e.bulk.Release()
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalCircuitOpen, FailureNonRetryable)
			}
			return nil, g.reject(req, reason, e)
		}

		// 构造本次尝试上下文：把调用方剩余预算传播下去，
		// 无调用方截止时间时使用路由默认超时。
		deadline := req.Deadline
		if deadline.IsZero() && cfg.DefaultTimeout > 0 {
			deadline = time.Now().Add(cfg.DefaultTimeout)
		}
		ctx := AttemptContext{Attempt: attempt, Deadline: deadline}

		g.log(LogEvent{
			RequestID: req.ID, Route: req.Route, Phase: "dispatch",
			Breaker: e.breaker.State().String(), Remaining: remainingBudget(deadline),
			Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
		})

		payload, err := cfg.Downstream.Call(ctx, req)

		if err == nil {
			e.breaker.RecordSuccess(gen)
			e.bulk.Release()
			g.log(LogEvent{
				RequestID: req.ID, Route: req.Route, Phase: "success",
				Breaker: e.breaker.State().String(), Remaining: remainingBudget(req.Deadline),
				Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
			})
			return &Response{RequestID: req.ID, Payload: payload, Attempts: attempt}, nil
		}

		// 下游原始错误仅用于分类与内部日志，不透出给调用方。
		class := cfg.Classifier(err)
		e.breaker.RecordFailure(gen)
		e.bulk.Release()

		// 本次尝试已经用尽调用方预算：即使失败可重试也无预算再试，
		// 终局原因为预算耗尽（与不可重试失败相区分）。
		if remainingBudget(req.Deadline) <= 0 {
			return nil, g.terminal(req, e, attempt, TerminalBudgetExhausted, class)
		}

		canRetry := attempt < maxAttempts &&
			req.Idempotent &&
			class == FailureRetryable

		if !canRetry {
			// 预算仍有剩余但不允许重试（不可重试分类，或调用方未声明幂等）。
			return nil, g.terminal(req, e, attempt, TerminalNonRetryable, class)
		}

		// 退避时间被剩余预算截断；退避后不足以再尝试，则以预算耗尽终局结束。
		backoff := cfg.Retry.Backoff
		rem = remainingBudget(req.Deadline)
		if backoff > rem {
			backoff = rem
		}
		g.log(LogEvent{
			RequestID: req.ID, Route: req.Route, Phase: "backoff",
			Breaker: e.breaker.State().String(), Remaining: rem,
			Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
			Class: class.String(),
		})
		if backoff > 0 {
			g.sleep(backoff)
		}
		if remainingBudget(req.Deadline) <= 0 {
			return nil, g.terminal(req, e, attempt, TerminalBudgetExhausted, class)
		}
	}

	// 理论上循环内部必然返回；兜底为不可重试终局，保证不透出下游错误。
	return nil, g.terminal(req, e, maxAttempts, TerminalNonRetryable, FailureNonRetryable)
}

// terminal 构造不可重试终局失败并记录日志（包含失败分类与尝试次数）。
func (g *Gateway) terminal(req Request, e *routeEntry, attempts int, kind TerminalKind, class FailureClass) error {
	g.log(LogEvent{
		RequestID: req.ID, Route: req.Route, Phase: "terminal",
		Breaker: e.breaker.State().String(), Remaining: remainingBudget(req.Deadline),
		Attempt: attempts, MaxAttempt: e.cfg.Retry.MaxAttempts, InFlight: e.bulk.InFlight(),
		Class: class.String(), Kind: kind,
	})
	return &TerminalError{Route: req.Route, Kind: kind, Class: class, Attempts: attempts}
}

// Status 返回指定路由的状态快照；未知路由返回 false。
func (g *Gateway) Status(route string) (RouteStatus, bool) {
	e := g.lookup(route)
	if e == nil {
		return RouteStatus{}, false
	}
	return RouteStatus{
		Route:              route,
		Breaker:            e.breaker.State(),
		InFlight:           e.bulk.InFlight(),
		MaxConcurrent:      e.cfg.MaxConcurrent,
		ProbeMaxConcurrent: e.cfg.ProbeMaxConcurrent,
		Threshold:          e.cfg.Threshold,
		OpenCooldown:       e.cfg.OpenCooldown,
	}, true
}
