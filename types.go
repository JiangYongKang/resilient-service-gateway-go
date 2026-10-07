package gateway

import "time"

// RejectReason 描述请求在被派发给下游之前被拒绝的原因。
type RejectReason string

const (
	// ReasonDeadlineExpired 调用方声明的截止时间在派发前已经过期。
	ReasonDeadlineExpired RejectReason = "deadline_expired"
	// ReasonBudgetExhausted 首次派发前，调用方总预算仍为正但已不足以支撑一次
	// 单次尝试（小于路由配置的 AttemptTimeout）；下游调用次数为 0，不占用并发名额。
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
	// TerminalBudgetExhausted 已有至少一次尝试发生后，调用方总预算真正耗尽
	// （剩余 ≤ 0，调用方截止时间已经到期）：可能是本次尝试跑满了剩余预算，
	// 也可能是退避后到期，还可能是重试链路在派发前预检时发现截止时间已过。
	// 调用方应按“总时限见底、超时上报”处理。
	TerminalBudgetExhausted TerminalKind = "budget_exhausted"
	// TerminalBudgetTooSmall 已有至少一次尝试发生后，进入重试时调用方总预算
	// 仍为正数、但已小于路由配置的一次单次尝试预算（AttemptTimeout），剩余
	// 预算不足以再完整跑一次尝试：后续尝试不再派发，也不做无意义的退避等待。
	// 它与 TerminalBudgetExhausted 互斥且只凭 Kind 即可区分：前者只是预算被
	// 切得太碎（可加大预算或换更快的下游再试），后者才是总时限真正见底。
	// 仅配置了 AttemptTimeout 的路由才可能产生该终局。
	TerminalBudgetTooSmall TerminalKind = "budget_too_small"
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
	// AttemptTimeout 单次尝试预算：一次向下游派发最多允许占用的时长。
	// 调用方给了总截止时间时，每次尝试实际取本预算与当时剩余总预算的更小值；
	// 调用方未给总截止时间时，本预算单独生效（与 DefaultTimeout 取更小值）。
	// 配置后，每次派发前都会检查剩余总预算是否足以支撑一次尝试：
	// 首次不足直接拒绝（RejectError(budget_exhausted)，下游调用 0 次、不占并发名额），
	// 重试途中剩余为正但不足一次尝试则以 TerminalError(budget_too_small) 结束，
	// 重试途中总预算真正耗尽（剩余 ≤ 0）则以 TerminalError(budget_exhausted) 结束，
	// 两种终局只凭 TerminalError.Kind 即可区分；均不再派发后续尝试。
	// 0 表示不配置该预算，行为与历史版本完全一致。
	AttemptTimeout time.Duration
	// Retry 重试配置。
	Retry RetryConfig
	// Classifier 失败分类器；为 nil 时一律判定为不可重试。
	Classifier Classifier
}
