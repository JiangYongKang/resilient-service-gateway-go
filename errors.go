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
	// Remaining 拒绝时刻的剩余预算（仅用于日志与可解释性）。
	Remaining time.Duration
	// Unlimited 为 true 时调用方未声明总截止时间（不设限），
	// Remaining 不代表真实预算；不设限的调用不会因预算被拒绝。
	Unlimited bool
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("gateway: request to route %q rejected: %s", e.Route, e.Reason)
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
