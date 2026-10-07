package gateway

import (
	"time"
)

// reject 在派发前拒绝并记录日志。预算信息按调用方总截止时间如实表达，
// 不设限时日志与错误信息均输出 “unlimited”，绝不展示成某个具体时长。
func (g *Gateway) reject(req Request, reason RejectReason, e *routeEntry) error {
	var attemptTimeout time.Duration
	state := ""
	inFlight := 0
	if e != nil {
		attemptTimeout = e.cfg.AttemptTimeout
		state = e.breaker.State().String()
		inFlight = e.bulk.InFlight()
	}
	s := budgetSummary(req.Deadline, attemptTimeout)
	g.log(LogEvent{
		RequestID:  req.ID,
		Route:      req.Route,
		Phase:      "reject",
		Breaker:    state,
		Limited:    s.limited,
		Remaining:  s.remaining,
		Reason:     reason,
		InFlight:   inFlight,
		MaxAttempt: 0,
	})
	return &RejectError{Route: req.Route, Reason: reason, Limited: s.limited, Remaining: s.remaining}
}

// Call 按请求中的路由执行调用，返回经过治理后的结果。
//
// 执行顺序（每次尝试，重试也逐次重新执行）：
//  1. 总预算预检：调用方截止时间已过期 -> RejectError(deadline_expired)；
//  2. 单次尝试预算预检（仅路由配置了 AttemptTimeout 时）：剩余总预算为正但
//     不足以支撑一次单次尝试 -> 首次 RejectError(budget_exhausted)，重试途中
//     TerminalError(budget_insufficient)；预检先于并发舱占用，不占下游名额；
//  3. 并发隔离舱：槽位已满 -> RejectError(concurrency_limit)；
//  4. 熔断器：打开 -> RejectError(circuit_open)；探测名额满 -> RejectError(probe_concurrency_limit)；
//  5. 构造本次尝试截止时间：取“单次尝试预算（含无调用方截止时间时的默认超时）”
//     与“当时剩余总预算”的更小值，传播给下游；
//  6. 按失败分类 + 幂等性 + 剩余预算决定是否重试；
//     退避后总预算真正耗尽（剩余 ≤ 0）-> TerminalError(budget_exhausted)；
//     剩余仍为正但不足以再跑一次 -> TerminalError(budget_insufficient)；
//     不可重试（含非幂等） -> TerminalError(non_retryable_failure)。
//
// 四种预算相关的拒绝/终局只靠返回值即可区分：
// 还没派发就时限已过 RejectError(deadline_expired)、还没派发就不够跑一次
// RejectError(budget_exhausted)、已派发过重试时不够跑一次
// TerminalError(budget_insufficient)、已派发过重试时总预算真被耗光
// TerminalError(budget_exhausted)。
//
// 调用方未给总截止时间、路由也未配置单次尝试预算与默认超时的调用属于不设限：
// 不会被任何预算判定拒绝，成功、重试、熔断行为保持不变。
// 下游原始错误不会出现在返回值中，仅用于失败分类。
func (g *Gateway) Call(req Request) (*Response, error) {
	e := g.lookup(req.Route)
	if e == nil {
		return nil, g.reject(req, ReasonUnknownRoute, nil)
	}
	cfg := e.cfg

	// 派发前总预算预检：已过期的请求直接拒绝，不占用任何下游资源。
	if s := budgetSummary(req.Deadline, cfg.AttemptTimeout); s.expired() {
		return nil, g.reject(req, ReasonDeadlineExpired, e)
	}

	maxAttempts := cfg.Retry.MaxAttempts
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 每次尝试前都重新计算预算视图（重试退避可能已消耗预算）。
		s := budgetSummary(req.Deadline, cfg.AttemptTimeout)

		// 语义一：调用方截止时间已耗尽（或在退避后刚好到期）。
		if s.expired() {
			if attempt > 1 {
				// 已有尝试发生：属于预算耗尽的终局，而不是派发前拒绝。
				return nil, g.terminal(req, e, attempt-1, TerminalBudgetExhausted, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonDeadlineExpired, e)
		}

		// 语义二：剩余总预算仍为正，但已不足以支撑一次配置的单次尝试。
		// 仅当路由配置了 AttemptTimeout 时该预检才生效；未配置时 canAttempt
		// 只要求未过期，行为与历史版本一致。预检先于并发舱，绝不占用名额。
		// 注意：同一预算视图上 expired() 已先判过，此处剩余必为正，
		// 因此重试途中命中的是 budget_insufficient 而非 budget_exhausted。
		if !s.canAttempt() {
			if attempt > 1 {
				return nil, g.terminal(req, e, attempt-1, TerminalBudgetInsufficient, FailureNonRetryable)
			}
			return nil, g.reject(req, ReasonBudgetExhausted, e)
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

		// 构造本次尝试上下文：单次尝试预算与当时剩余总预算取更小值；
		// 无调用方截止时间时单次尝试预算/默认超时单独生效；均无则不设限。
		now := time.Now()
		attemptDeadline := s.attemptDeadline(now, cfg.DefaultTimeout)
		ctx := AttemptContext{Attempt: attempt, Deadline: attemptDeadline}

		attemptRemaining := time.Duration(0)
		if !attemptDeadline.IsZero() {
			attemptRemaining = time.Until(attemptDeadline)
		}
		g.log(LogEvent{
			RequestID: req.ID, Route: req.Route, Phase: "dispatch",
			Breaker: e.breaker.State().String(), Limited: s.limited,
			Remaining: s.remaining, AttemptRemaining: attemptRemaining,
			Attempt: attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
		})

		payload, err := cfg.Downstream.Call(ctx, req)

		if err == nil {
			e.breaker.RecordSuccess(gen)
			e.bulk.Release()
			s = budgetSummary(req.Deadline, cfg.AttemptTimeout)
			g.log(LogEvent{
				RequestID: req.ID, Route: req.Route, Phase: "success",
				Breaker: e.breaker.State().String(), Limited: s.limited,
				Remaining: s.remaining,
				Attempt:   attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
			})
			return &Response{RequestID: req.ID, Payload: payload, Attempts: attempt}, nil
		}

		// 下游原始错误仅用于分类与内部日志，不透出给调用方。
		class := cfg.Classifier(err)
		e.breaker.RecordFailure(gen)
		e.bulk.Release()

		// 本次尝试返回后重新评估总预算。
		s = budgetSummary(req.Deadline, cfg.AttemptTimeout)

		// 本次尝试已经用尽调用方预算：即使失败可重试也无预算再试，
		// 终局原因为预算耗尽（与不可重试失败相区分）。
		if s.expired() {
			return nil, g.terminal(req, e, attempt, TerminalBudgetExhausted, class)
		}

		canRetry := attempt < maxAttempts &&
			req.Idempotent &&
			class == FailureRetryable

		if !canRetry {
			// 预算仍有剩余但不允许重试（不可重试分类，或调用方未声明幂等）。
			return nil, g.terminal(req, e, attempt, TerminalNonRetryable, class)
		}

		// 即使还能重试，剩余总预算已不足以支撑一次配置的单次尝试：
		// 直接以“预算不足”终局结束（剩余仍为正，与 budget_exhausted 区分），
		// 不再派发，也不再做无意义的退避等待。
		if !s.canAttempt() {
			return nil, g.terminal(req, e, attempt, TerminalBudgetInsufficient, class)
		}

		// 退避时间被剩余预算截断；退避后不足以再尝试，则以预算耗尽终局结束。
		backoff := cfg.Retry.Backoff
		rem := s.remaining
		if backoff > rem {
			backoff = rem
		}
		g.log(LogEvent{
			RequestID: req.ID, Route: req.Route, Phase: "backoff",
			Breaker: e.breaker.State().String(), Limited: s.limited,
			Remaining: rem,
			Attempt:   attempt, MaxAttempt: maxAttempts, InFlight: e.bulk.InFlight(),
			Class: class.String(),
		})
		if backoff > 0 {
			g.sleep(backoff)
		}
		if budgetSummary(req.Deadline, cfg.AttemptTimeout).expired() {
			return nil, g.terminal(req, e, attempt, TerminalBudgetExhausted, class)
		}
	}

	// 理论上循环内部必然返回；兜底为不可重试终局，保证不透出下游错误。
	return nil, g.terminal(req, e, maxAttempts, TerminalNonRetryable, FailureNonRetryable)
}

// terminal 构造不可重试终局失败并记录日志（包含失败分类与尝试次数）。
func (g *Gateway) terminal(req Request, e *routeEntry, attempts int, kind TerminalKind, class FailureClass) error {
	s := budgetSummary(req.Deadline, e.cfg.AttemptTimeout)
	g.log(LogEvent{
		RequestID: req.ID, Route: req.Route, Phase: "terminal",
		Breaker: e.breaker.State().String(), Limited: s.limited,
		Remaining: s.remaining,
		Attempt:   attempts, MaxAttempt: e.cfg.Retry.MaxAttempts, InFlight: e.bulk.InFlight(),
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
