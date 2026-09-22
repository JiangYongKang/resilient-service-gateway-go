package gateway

import (
	"sync"
	"time"
)

// BreakerState 熔断器状态。
type BreakerState int

const (
	// StateClosed 关闭：正常放行。
	StateClosed BreakerState = iota
	// StateOpen 打开：快速拒绝全部请求。
	StateOpen
	// StateHalfOpen 半开：仅允许有限并发的探测请求。
	StateHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// BreakerConfig 为单个下游维护的熔断配置。
type BreakerConfig struct {
	// FailureThreshold 连续失败达到该阈值后熔断打开。
	FailureThreshold int
	// OpenDuration 熔断打开持续时间，之后进入半开探测。
	OpenDuration time.Duration
	// ProbeConcurrency 半开状态下允许同时通过的探测请求数。
	ProbeConcurrency int
	// SuccessThreshold 半开状态下连续探测成功达到该阈值后关闭熔断。
	SuccessThreshold int
}

// Permit 是一次通过熔断器的调用许可，必须以 ReportResult 终结。
type Permit interface {
	// ReportResult 上报本次调用成功与否。
	ReportResult(success bool)
	// Release 放弃该许可（未实际发起调用时使用），不影响熔断计数。
	Release()
}

// Breaker 是按下游独立维护的熔断器。
//
// 迁移规则：
//   - Closed：连续失败计数达到 FailureThreshold -> Open（openUntil = now + OpenDuration）。
//   - Open：Acquire 时若 now 已过 openUntil -> HalfOpen，否则快速拒绝。
//   - HalfOpen：仅 ProbeConcurrency 个许可可通过；任一探测失败立即回到 Open；
//     连续探测成功达到 SuccessThreshold -> Closed。
//
// 所有计数与状态变更均在同一互斥锁内完成，保证并发请求下状态不错乱。
type Breaker struct {
	name string
	cfg  BreakerConfig

	mu        sync.Mutex
	state     BreakerState
	gen       uint64 // 每次状态迁移 +1，用于让迟到许可失效
	failures  int    // Closed 下连续失败数
	probeSucc int    // HalfOpen 下连续探测成功数
	openUntil time.Time
	probes    chan struct{} // 仅 HalfOpen 非 nil
}

func newBreaker(name string, cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 1
	}
	if cfg.ProbeConcurrency < 1 {
		cfg.ProbeConcurrency = 1
	}
	if cfg.SuccessThreshold < 1 {
		cfg.SuccessThreshold = 1
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = time.Second
	}
	return &Breaker{name: name, cfg: cfg, state: StateClosed}
}

// State 返回当前熔断状态（惰性推进 Open -> HalfOpen）。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	b.advanceLocked(time.Now())
	s := b.state
	b.mu.Unlock()
	return s
}

// Acquire 尝试获取一个调用许可；熔断打开时返回 RejectCircuitOpen 与 nil 许可。
func (b *Breaker) Acquire(now time.Time) (Permit, RejectReason) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.advanceLocked(now)

	switch b.state {
	case StateClosed:
		return &breakerPermit{b: b, probe: false, gen: b.gen}, RejectNone
	case StateOpen:
		return nil, RejectCircuitOpen
	default: // HalfOpen
		select {
		case b.probes <- struct{}{}:
			return &breakerPermit{b: b, probe: true, gen: b.gen}, RejectNone
		default:
			// 探测并发已满，其余请求仍被快速拒绝。
			return nil, RejectCircuitOpen
		}
	}
}

// advanceLocked 必须在持锁状态下调用；若 Open 冷却已过则进入 HalfOpen。
func (b *Breaker) advanceLocked(now time.Time) {
	if b.state == StateOpen && !now.Before(b.openUntil) {
		b.toHalfOpenLocked()
	}
}

func (b *Breaker) toHalfOpenLocked() {
	b.gen++
	b.state = StateHalfOpen
	b.probeSucc = 0
	b.probes = make(chan struct{}, b.cfg.ProbeConcurrency)
}

func (b *Breaker) toOpenLocked(now time.Time) {
	b.gen++
	b.state = StateOpen
	b.failures = 0
	b.probeSucc = 0
	b.openUntil = now.Add(b.cfg.OpenDuration)
	b.probes = nil
}

func (b *Breaker) toClosedLocked() {
	b.gen++
	b.state = StateClosed
	b.failures = 0
	b.probeSucc = 0
	b.probes = nil
}

// report 由许可在实际调用结束后调用。
func (b *Breaker) report(p *breakerPermit, success bool, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 迟到许可（许可发出后状态又发生过迁移）一律失效：
	// 既不归还槽位，也不参与计数，避免并发下状态错乱。
	if p.gen != b.gen {
		return
	}

	if p.probe {
		if b.probes != nil {
			<-b.probes
		}
		if success {
			b.probeSucc++
			if b.probeSucc >= b.cfg.SuccessThreshold {
				b.toClosedLocked()
			}
		} else {
			b.toOpenLocked(now)
		}
		return
	}

	// Closed 许可：p.gen == b.gen 保证当前状态仍是 Closed。
	if success {
		b.failures = 0
		return
	}
	b.failures++
	if b.failures >= b.cfg.FailureThreshold {
		b.toOpenLocked(now)
	}
}

type breakerPermit struct {
	b     *Breaker
	probe bool
	gen   uint64
	once  sync.Once
}

func (p *breakerPermit) ReportResult(success bool) {
	p.once.Do(func() {
		p.b.report(p, success, time.Now())
	})
}

func (p *breakerPermit) Release() {
	p.once.Do(func() {
		if !p.probe {
			return
		}
		p.b.mu.Lock()
		// 仅当许可仍属于当前 HalfOpen 代时归还探测槽位。
		if p.gen == p.b.gen && p.b.probes != nil {
			<-p.b.probes
		}
		p.b.mu.Unlock()
	})
}
