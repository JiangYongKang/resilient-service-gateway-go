package gateway

import (
	"fmt"
	"time"
)

// RejectError 表示请求在派发给下游之前被网关拒绝。
// 不同 RejectReason 对应不同的拒绝语义，调用方可据此区分处理。
type RejectError struct {
	Route  string
	Reason RejectReason
	// Limited 表示拒绝时调用方是否给出了有限的总截止时间。
	Limited bool
	// Remaining 拒绝时刻的剩余预算（仅当 Limited 为 true 时有意义，
	// 用于日志与可解释性）；不设限时为 0，Error() 会输出 “unlimited”。
	Remaining time.Duration
}

func (e *RejectError) Error() string {
	if !e.Limited {
		return fmt.Sprintf("gateway: request to route %q rejected: %s (remaining budget: unlimited)",
			e.Route, e.Reason)
	}
	return fmt.Sprintf("gateway: request to route %q rejected: %s (remaining budget: %s)",
		e.Route, e.Reason, e.Remaining)
}

// TerminalError 表示一次调用的不可重试终局失败。
// 下游原始错误不会被透出，仅保留失败分类语义。
type TerminalError struct {
	Route string
	Kind  TerminalKind
	// Class 为下游返回的失败分类（预算类终局与成功后的治理层拒绝为 FailureNonRetryable）。
	Class FailureClass
	// Attempts 已发生的尝试次数。
	Attempts int
	// Limited 表示终局时调用方是否给出了有限的总截止时间。
	// 预算类终局（budget_exhausted / budget_too_small）恒为 true。
	Limited bool
	// Remaining 终局时刻的剩余总预算（仅当 Limited 为 true 时有意义）：
	// budget_too_small 时严格为正但小于单次尝试预算；budget_exhausted 时 ≤ 0。
	Remaining time.Duration
	// AttemptBudget 为路由配置的单次尝试预算（未配置时为 0）；
	// budget_too_small 时满足 0 < Remaining < AttemptBudget。
	AttemptBudget time.Duration
}

func (e *TerminalError) Error() string {
	switch e.Kind {
	case TerminalBudgetExhausted, TerminalBudgetTooSmall:
		return fmt.Sprintf(
			"gateway: terminal failure on route %q: %s (remaining budget: %s, attempt budget: %s, attempts: %d)",
			e.Route, e.Kind, budgetText(e.Limited, e.Remaining), attemptBudgetText(e.AttemptBudget), e.Attempts)
	default:
		return fmt.Sprintf("gateway: terminal failure on route %q: %s", e.Route, e.Kind)
	}
}

// budgetText 统一预算对外表达：不设限输出 "unlimited"，绝不展示成某个具体时长。
func budgetText(limited bool, remaining time.Duration) string {
	if !limited {
		return "unlimited"
	}
	return remaining.String()
}

// attemptBudgetText 统一单次尝试预算对外表达：未配置输出 "unconfigured"。
func attemptBudgetText(budget time.Duration) string {
	if budget <= 0 {
		return "unconfigured"
	}
	return budget.String()
}
