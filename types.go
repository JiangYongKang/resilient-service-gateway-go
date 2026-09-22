package gateway

import "time"

// RejectReason 描述请求在被派发给下游之前被拒绝的原因。
type RejectReason string

const (
	// ReasonDeadlineExpired 调用方声明的截止时间在派发前已经过期。
	ReasonDeadlineExpired RejectReason = "deadline_expired"
	// ReasonBudgetExhausted 剩余时间预算不足以再发起一次尝试（派发前预检保留值；
	// 已有尝试发生后的预算耗尽统一以 TerminalError(budget_exhausted) 表达）。
	ReasonBudgetExhausted RejectReason = "budget_exhausted"
	// ReasonCircuitOpen 下游熔断器处于打开状态，请求被快速拒绝。
	ReasonCircuitOpen RejectReason = "circuit_open"
	// ReasonProbeLimit 熔断恢复探测中，允许通过的探测并发已被占满。
	ReasonProbeLimit RejectReason = "probe_concurrency_limit"
	// ReasonConcurrencyLimit 下游并发隔离舱已满，请求被快速拒绝。
	ReasonConcurrencyLimit RejectReason = "concurrency_limit"
	// ReasonUnknownRoute 请求的路由未在网关注册。
	ReasonUnknownRoute RejectReason = "unknown_route"
)

// TerminalKind 描述不可重试终局错误的类别。
type TerminalKind string

const (
	// TerminalNonRetryable 下游返回了被分类为不可重试的失败。
	TerminalNonRetryable TerminalKind = "non_retryable_failure"
	// TerminalBudgetExhausted 重试用尽了调用方声明的时间预算（或在等待重试退避后预算耗尽）。
	TerminalBudgetExhausted TerminalKind = "budget_exhausted"
	// TerminalCircuitOpen 调用已发生过尝试，但在重试途中熔断器打开（或隔离舱打满），
	// 后续尝试在派发前被治理层拒绝，调用以终局结束。
	TerminalCircuitOpen TerminalKind = "circuit_open"
)

// FailureClass 是下游失败的分类结果。
type FailureClass int

const (
	// FailureNonRetryable 不可重试失败（默认分类）。
	FailureNonRetryable FailureClass = iota
	// FailureRetryable 可重试失败。
	FailureRetryable
)

// String 返回失败分类的可读名称。
func (c FailureClass) String() string {
	switch c {
	case FailureRetryable:
		return "retryable"
	default:
		return "non_retryable"
	}
}

// Request 是调用方发起的一次网关调用。
type Request struct {
	ID         string
	Route      string
	Idempotent bool
	// Deadline 为调用方声明的绝对截止时间；零值表示不设截止时间。
	// 网关负责把剩余预算传播给下游。
	Deadline time.Time
	Payload  any
}

// AttemptContext 是网关为单次下游尝试构造的上下文，
// 下游实现必须尊重其中的截止时间。
type AttemptContext struct {
	Attempt  int
	Deadline time.Time
}

// Remaining 返回该次尝试的剩余时间预算；无截止时间时返回一个很大的值。
func (c AttemptContext) Remaining() time.Duration {
	if c.Deadline.IsZero() {
		return time.Hour
	}
	d := time.Until(c.Deadline)
	if d < 0 {
		return 0
	}
	return d
}

// Response 是网关返回给调用方的结果。原始下游错误永远不会出现在这里。
type Response struct {
	RequestID string
	Payload   any
	Attempts  int
}

// Downstream 是下游服务实现需要满足的接口。
type Downstream interface {
	Name() string
	Call(ctx AttemptContext, req Request) (any, error)
}

// Classifier 根据下游错误决定失败分类。
type Classifier func(err error) FailureClass

// RetryConfig 描述重试规则。
type RetryConfig struct {
	// MaxAttempts 最大尝试次数（含首次），<=1 表示不重试。
	MaxAttempts int
	// Backoff 固定重试退避；实际退避会被剩余预算截断。
	Backoff time.Duration
}

// RouteConfig 描述一条路由（对应一个独立下游）的全部治理配置。
type RouteConfig struct {
	Downstream Downstream
	// Threshold 连续失败达到该阈值后熔断打开。
	Threshold int
	// OpenCooldown 熔断打开后多久进入半开探测。
	OpenCooldown time.Duration
	// ProbeMaxConcurrent 半开状态下允许同时通过的探测请求数。
	ProbeMaxConcurrent int
	// MaxConcurrent 关闭（及半开探测外）状态下该下游的最大并发。
	MaxConcurrent int
	// DefaultTimeout 调用方未声明截止时间时，单次尝试使用的默认超时；0 表示不限制。
	DefaultTimeout time.Duration
	// Retry 重试配置。
	Retry RetryConfig
	// Classifier 失败分类器；为 nil 时一律判定为不可重试。
	Classifier Classifier
}
