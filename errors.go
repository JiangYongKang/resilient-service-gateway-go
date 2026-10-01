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
	// Class 为下游返回的失败分类（预算耗尽时为 FailureNonRetryable）。
	Class FailureClass
	// Attempts 已发生的尝试次数。
	Attempts int
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("gateway: terminal failure on route %q: %s", e.Route, e.Kind)
}
