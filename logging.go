package gateway

import "log"

func logEventDefault(ev LogEvent) {
	log.Printf("gateway request=%s route=%s phase=%s breaker=%s remaining=%s attempt_budget=%s attempt=%d/%d inflight=%d reason=%q class=%q kind=%q",
		ev.RequestID, ev.Route, ev.Phase, ev.Breaker, ev.BudgetText(), ev.AttemptText(),
		ev.Attempt, ev.MaxAttempt,
		ev.InFlight, ev.Reason, ev.Class, ev.Kind)
}
