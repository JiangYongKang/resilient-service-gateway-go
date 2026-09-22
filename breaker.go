package gateway

import (
	"sync"
	"time"
)

// breakerState 熔断器状态。
type breakerState int

const (
	// stateClosed：请求正常通过，累计连续失败。
	stateClosed breakerState = iota
	// stateOpen：快速拒绝所有请求，等待冷却时间结束。
	stateOpen
	// stateHalfOpen：仅允许有限数量的探测请求通过；
	stateHalfOpen
	// 任一探测成功立即回到 closed，任一探测失败立即重新回到 open。
)

func (s breakerState) String() string {
	switch s {
	case stateOpen:
		return "OPEN"
	case stateHalfOpen:
		return "HALF_OPEN"
	default:
		return "CLOSED"
	}
}

// Breaker 维护单个下游的熔断状态。所有方法均为并发安全。
//
// 状态迁移规则（确定性，无随机半开探测）：
//   - CLOSED    --连续失败数达到 threshold--> OPEN（记录打开时刻）
//   - OPEN      --距打开时刻超过 OpenCooldown--> HALF_OPEN（首个请求触发迁移并占用一个探测名额）
//   - HALF_OPEN --任一探测成功--> CLOSED
//   - HALF_OPEN --任一探测失败--> OPEN（重新计时冷却）
//   - CLOSED 状态下任一成功会把连续失败计数清零。
//
// 为避免并发下状态错乱，熔断器维护单调递增的代次（generation）：
// 每次状态迁移代次加一，Allow 返回准入时的代次，请求结束上报时必须
// 原样带回；与当前代次不符的迟到报告会被忽略，上一代半开探测的结果
// 不会污染新一代状态。
type Breaker struct {
	mu sync.Mutex

	route     string
	threshold int
	cooldown  time.Duration
	probeMax  int
	now       func() time.Time

	state          breakerState
	consecFailures int
	openedAt       time.Time
	probeInFlight  int
	gen            int64
}

// NewBreaker 创建熔断器。now 用于注入时钟，传 nil 时使用 time.Now。
func NewBreaker(route string, threshold int, cooldown time.Duration, probeMax int, now func() time.Time) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if probeMax < 1 {
		probeMax = 1
	}
	if now == nil {
		now = time.Now
	}
	return &Breaker{
		route:     route,
		threshold: threshold,
		cooldown:  cooldown,
		probeMax:  probeMax,
		now:       now,
		state:     stateClosed,
	}
}

// maybeExpireOpen 必须在持锁状态下调用：冷却到期则把 OPEN 迁移为 HALF_OPEN。
func (b *Breaker) maybeExpireOpen() {
	if b.state == stateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state = stateHalfOpen
		b.probeInFlight = 0
		b.gen++
	}
}

// State 返回当前状态（仅供观测/日志使用）；该调用同样会推进冷却到期迁移。
func (b *Breaker) State() breakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeExpireOpen()
	return b.state
}

// Allow 判定一个新请求是否允许通过。允许时返回空原因与准入代次；
// 拒绝时返回非空原因（熔断打开 / 探测并发已满），调用方不得再上报。
func (b *Breaker) Allow() (RejectReason, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case stateClosed:
		return "", b.gen
	case stateOpen:
		b.maybeExpireOpen()
		if b.state == stateOpen {
			return ReasonCircuitOpen, b.gen
		}
		// 冷却到期，首个请求作为探测进入 HALF_OPEN。
		b.probeInFlight = 1
		return "", b.gen
	default: // stateHalfOpen
		if b.probeInFlight < b.probeMax {
			b.probeInFlight++
			return "", b.gen
		}
		return ReasonProbeLimit, b.gen
	}
}

// RecordSuccess 报告一次下游调用成功；gen 为 Allow 返回的准入代次。
func (b *Breaker) RecordSuccess(gen int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen != b.gen {
		return // 上一代状态下准入的请求，迟到结果忽略
	}
	switch b.state {
	case stateClosed:
		b.consecFailures = 0
	case stateHalfOpen:
		if b.probeInFlight > 0 {
			b.probeInFlight--
		}
		b.state = stateClosed
		b.consecFailures = 0
		b.gen++
	}
}

// RecordFailure 报告一次下游调用失败；gen 为 Allow 返回的准入代次。
func (b *Breaker) RecordFailure(gen int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen != b.gen {
		return // 上一代状态下准入的请求，迟到结果忽略
	}
	switch b.state {
	case stateClosed:
		b.consecFailures++
		if b.consecFailures >= b.threshold {
			b.state = stateOpen
			b.openedAt = b.now()
			b.gen++
		}
	case stateHalfOpen:
		if b.probeInFlight > 0 {
			b.probeInFlight--
		}
		b.state = stateOpen
		b.openedAt = b.now()
		b.gen++
	}
}
