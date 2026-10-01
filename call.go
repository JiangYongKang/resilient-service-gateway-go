package gateway

import (
	"time"
)

// reject 在派发前拒绝并记录日志。
func (g *Gateway) reject(req Request, reason RejectReason, e *routeEntry) error {
	budget := totalBudget{deadline: req.Deadline}
	rem, limited := budget.remaining()
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
		Unlimited:  !limited,
		Reason:     reason,
		InFlight:   inFlight,
		MaxAttempt: 0,
	})
	return &RejectError{
		Route: req.Route, Reason: reason, Remaining: rem, Unlimited: !limited,
	}
}

// Call 按请求中的路由执行调用，返回经过治理后的结果。
//
// 执行顺序（首次及每次重试都重新执行）：
//  1. 预算预检（在占用任何并发名额之前）：
//     总截止时间已过 -> RejectError(deadline_expired)（重试链路则
//     TerminalError(budget_exhausted)）；剩余总预算为正但不够一次
//     AttemptBudget -> RejectError(budget_exhausted)（重试链路则
//     TerminalError(budget_insufficient)）；不设限 -> 永远通过；
//  2. 并发隔离舱：槽位已满 -> RejectError(concurrency_limit)；
//  3. 熔断器：打开 -> RejectError(circuit_open)；探测名额满
//     -> RejectError(probe_concurrency_limit)；
//  4. 按“AttemptBudget 与当时剩余总预算取小”构造本次尝试截止时间并派发；
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

	maxAttempts := cfg.Retry.MaxAttempts
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 第一步：预算预检。判定先于并发名额与熔断，预算不够时不占任何资源。
		// 首次拒绝是 RejectError；重试链路上已有尝试发生，改以终局表达。
		switch preDispatchCheck(cfg, req.Deadline) {
		case budgetDeadlinePassed:
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalBudgetExhausted, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonDeadlineExpired, e)
		case budgetTooShort:
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalBudgetInsufficient, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonBudgetExhausted, e)
		}

		// 第二步：并发上限，快速拒绝，不排队，绝不突破 MaxConcurrent。
		if !e.bulk.TryAcquire() {
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalCircuitOpen, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonConcurrencyLimit, e)
		}

		// 第三步：熔断判定，打开快速拒绝；半开探测名额有限。
		reason, gen := e.breaker.Allow()
		if reason != "" {
			e.bulk.Release()
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalCircuitOpen, FailureNonRetryable)
			}
			return nil, g.reject(req, reason, e)
		}

		// 第四步：每次尝试都按“单次尝试预算与当时剩余总预算取小”
		// 重新计算截止时间；不设限（无调用方截止时间、无 AttemptBudget、
		// 无 DefaultTimeout）时 Deadline 为零值并原样传给下游。
		deadline := attemptDeadline(cfg, time.Now(), req.Deadline)
		ctx := AttemptContext{Attempt: attempt, Deadline: deadline}

		rem, limited := (totalBudget{deadline: req.Deadline}).remaining()
		g.log(LogEvent{
			RequestID: req.ID, Route: req.Route, Phase: "dispatch",
			Breaker:   e.breaker.State().String(),
			Remaining: rem, Unlimited: !limited,
			Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
		})

		payload, err := cfg.Downstream.Call(ctx, req)

		if err == nil {
			e.breaker.RecordSuccess(gen)
			e.bulk.Release()
			rem, limited := (totalBudget{deadline: req.Deadline}).remaining()
			g.log(LogEvent{
				RequestID: req.ID, Route: req.Route, Phase: "success",
				Breaker:   e.breaker.State().String(),
				Remaining: rem, Unlimited: !limited,
				Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
			})
			return &Response{RequestID: req.ID, Payload: payload, Attempts: attempt}, nil
		}

		// 下游原始错误仅用于分类与内部日志，不透出给调用方。
		class := cfg.Classifier(err)
		e.breaker.RecordFailure(gen)
		e.bulk.Release()

		budget := totalBudget{deadline: req.Deadline}

		// 本次尝试之后调用方总预算已经耗尽：即使失败可重试也无预算再试，
		// 终局原因为预算耗尽（与不可重试失败、剩余为正但不够一次尝试相区分）。
		if rem, limited := budget.remaining(); limited && rem <= 0 {
			return nil, g.terminal(req, e, attempt, TerminalBudgetExhausted, class)
		}

		canRetry := attempt < maxAttempts &&
			req.Idempotent &&
			class == FailureRetryable

		if !canRetry {
			// 预算仍有剩余但不允许重试（不可重试分类，或调用方未声明幂等）。
			return nil, g.terminal(req, e, attempt, TerminalNonRetryable, class)
		}

		// 还能重试，但下一次尝试派发前剩余总预算已不足一次单次尝试：
		// 在退避之前直接以 budget_insufficient 终局结束（确定、不依赖退避
		// 是否把剩余的几十毫秒恰好睡到 0），不再派发后续尝试。
		if preDispatchCheck(cfg, req.Deadline) == budgetTooShort {
			return nil, g.terminal(req, e, attempt, TerminalBudgetInsufficient, class)
		}

		// 退避时间被剩余总预算截断；不设限时退避不被截断。
		backoff := cfg.Retry.Backoff
		rem, limited = budget.remaining()
		if limited && backoff > rem {
			backoff = rem
		}
		g.log(LogEvent{
			RequestID: req.ID, Route: req.Route, Phase: "backoff",
			Breaker:   e.breaker.State().String(),
			Remaining: rem, Unlimited: !limited,
			Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
			Class: class.String(),
		})
		if backoff > 0 {
			g.sleep(backoff)
		}
		// 下一轮循环开头的预算预交会区分：截止时间已过 -> budget_exhausted；
		// 剩余为正但不够一次 AttemptBudget -> budget_insufficient。
	}

	// 理论上循环内部必然返回；兜底为不可重试终局，保证不透出下游错误。
	return nil, g.terminal(req, e, maxAttempts, TerminalNonRetryable, FailureNonRetryable)
}

// terminal 构造不可重试终局失败并记录日志（包含失败分类与尝试次数）。
func (g *Gateway) terminal(req Request, e *routeEntry, attempts int, kind TerminalKind, class FailureClass) error {
	rem, limited := (totalBudget{deadline: req.Deadline}).remaining()
	g.log(LogEvent{
		RequestID: req.ID, Route: req.Route, Phase: "terminal",
		Breaker:   e.breaker.State().String(),
		Remaining: rem, Unlimited: !limited,
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
