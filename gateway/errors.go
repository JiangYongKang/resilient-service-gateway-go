package gateway

import (
	"errors"
	"fmt"
)

// FailureClass 描述一次下游调用失败的分类。
type FailureClass int

const (
	// ClassNone 未发生失败。
	ClassNone FailureClass = iota
	// ClassRetryable 可重试失败（瞬时故障，例如超时、连接重置、5xx）。
	ClassRetryable
	// ClassNonRetryable 不可重试失败（确定性业务错误，例如 4xx）。
	ClassNonRetryable
)

func (c FailureClass) String() string {
	switch c {
	case ClassRetryable:
		return "retryable"
	case ClassNonRetryable:
		return "non-retryable"
	default:
		return "none"
	}
}

// RejectReason 描述请求在派发前或执行后被网关拒绝/终止的语义原因。
type RejectReason int

const (
	// RejectNone 未拒绝。
	RejectNone RejectReason = iota
	// RejectRouteNotFound 路由不存在。
	RejectRouteNotFound
	// RejectDeadlineExpired 调用方截止时间已过期。
	RejectDeadlineExpired
	// RejectInsufficientBudget 剩余预算不足以完成哪怕一次尝试。
	RejectInsufficientBudget
	// RejectCircuitOpen 熔断打开，请求被快速拒绝。
	RejectCircuitOpen
	// RejectNonRetryable 下游返回不可重试失败（终局）。
	RejectNonRetryable
	// RejectBudgetExhausted 可重试失败但重试预算/次数已耗尽（终局）。
	RejectBudgetExhausted
)

func (r RejectReason) String() string {
	switch r {
	case RejectRouteNotFound:
		return "route-not-found"
	case RejectDeadlineExpired:
		return "deadline-expired"
	case RejectInsufficientBudget:
		return "insufficient-budget"
	case RejectCircuitOpen:
		return "circuit-open"
	case RejectNonRetryable:
		return "non-retryable-failure"
	case RejectBudgetExhausted:
		return "budget-exhausted"
	default:
		return "none"
	}
}

// UpstreamError 包装下游返回的错误，只暴露分类，不直接透出原始错误。
type UpstreamError struct {
	downstream string
	class      FailureClass
	cause      error
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("gateway: downstream %q returned %s failure", e.downstream, e.class)
}

// Class 返回失败分类。
func (e *UpstreamError) Class() FailureClass { return e.class }

// Unwrap 仅供内部日志/诊断使用，调用方不应据此改变控制流。
func (e *UpstreamError) Unwrap() error { return e.cause }

// RejectError 是网关给出的可解释拒绝/终局错误。
type RejectError struct {
	Reason     RejectReason
	Downstream string
	// Class 终局失败对应的失败分类（派发前拒绝为 ClassNone）。
	Class  FailureClass
	Detail string
}

func (e *RejectError) Error() string {
	if e.Downstream == "" {
		return fmt.Sprintf("gateway: rejected: %s: %s", e.Reason, e.Detail)
	}
	return fmt.Sprintf("gateway: downstream %q rejected: %s: %s", e.Downstream, e.Reason, e.Detail)
}

func newReject(reason RejectReason, downstream, detail string) *RejectError {
	return &RejectError{Reason: reason, Downstream: downstream, Detail: detail}
}

// classifiedError 供下游显式标记错误的可重试性。
type classifiedError struct {
	class FailureClass
	cause error
}

func (e *classifiedError) Error() string {
	if e.cause == nil {
		return e.class.String() + " failure"
	}
	return e.cause.Error()
}

func (e *classifiedError) Unwrap() error { return e.cause }

// Retryable 将错误包装为可重试失败。
func Retryable(err error) error {
	return &classifiedError{class: ClassRetryable, cause: err}
}

// NonRetryable 将错误包装为不可重试失败。
func NonRetryable(err error) error {
	return &classifiedError{class: ClassNonRetryable, cause: err}
}

// ClassOf 提取被标记的失败分类；未标记返回 ClassNone。
func ClassOf(err error) FailureClass {
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class
	}
	return ClassNone
}
