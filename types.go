package gateway

import "time"

// RejectReason 描述请求在被派发给下游之前被拒绝的原因。
type RejectReason string

const (
	// ReasonDeadlineExpired 调用方声明的截止时间在派发前已经过期
	// （剩余总预算 <= 0），与“仍有正剩余但不够一次尝试”相区分。
	ReasonDeadlineExpired RejectReason = "deadline_expired"
	// ReasonBudgetExhausted 首次派发前，调用方剩余总预算仍为正数，
	// 但已不足以支撑一次配置的单次尝试预算（AttemptBudget）。
	// 此时下游调用次数为 0，也不占用并发名额。
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
	// TerminalBudgetExhausted 已经派发过至少一次尝试，随后调用方总预算
	// 被尝试本身或重试退避耗尽（剩余 <= 0）。
	TerminalBudgetExhausted TerminalKind = "budget_exhausted"
	// TerminalBudgetInsufficient 重试链路上下一次尝试派发前，剩余总预算
	// 仍为正数但不足以支撑一次单次尝试预算——此前已派发过尝试，
	// 故以终局结束而非派发前拒绝。与 TerminalBudgetExhausted 相区分。
	TerminalBudgetInsufficient TerminalKind = "budget_insufficient"
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
	Attempt int
	// Deadline 为本次尝试允许占用的最晚时刻，由“单次尝试预算”
	// 与“当时剩余的调用方总预算”取小得到；零值表示本次尝试不设限，
	// 下游不得把零值误解为一个很短的超时。
	Deadline time.Time
}

// Remaining 返回该次尝试的剩余时间预算；无截止时间（不设限）时返回一个
// 很大的值并伴随 ok=false，调用方不应把该数值当作真实预算展示。
// 已过期时返回 0。
func (c AttemptContext) Remaining() (d time.Duration, ok bool) {
	if c.Deadline.IsZero() {
		return time.Hour, false
	}
	d = time.Until(c.Deadline)
	if d < 0 {
		return 0, true
	}
	return d, true
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
	// AttemptBudget 是每次向下游派发的单次尝试预算：一次尝试最多允许
	// 占用下游多久。调用方给了总截止时间时，每次尝试的实际可用时间取
	// AttemptBudget 与当时剩余总预算中的更小值；调用方未给总截止时间时，
	// AttemptBudget 单独生效（优先于 DefaultTimeout）。0 表示不配置该预算，
	// 此时行为与历史一致：仅有调用方截止时间或 DefaultTimeout 生效。
	AttemptBudget time.Duration
	// DefaultTimeout 调用方未声明截止时间、且未配置 AttemptBudget 时，
	// 单次尝试使用的默认超时；0 表示不限制。
	DefaultTimeout time.Duration
	// Retry 重试配置。
	Retry RetryConfig
	// Classifier 失败分类器；为 nil 时一律判定为不可重试。
	Classifier Classifier
}
