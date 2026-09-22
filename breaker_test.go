package gateway

import (
	"testing"
	"time"
)

// TestBreakerClosedToOpen 连续失败达到阈值后熔断打开，且此前请求正常放行。
func TestBreakerClosedToOpen(t *testing.T) {
	now := time.Now()
	clock := &now
	adv := func(d time.Duration) { *clock = clock.Add(d) }
	b := NewBreaker("svc-a", 3, 50*time.Millisecond, 1, func() time.Time { return *clock })

	if got := b.State(); got != stateClosed {
		t.Fatalf("initial state = %s, want CLOSED", got)
	}
	for i := 1; i <= 2; i++ {
		r, gen := b.Allow()
		if r != "" {
			t.Fatalf("failure %d: rejected before threshold: %s", i, r)
		}
		b.RecordFailure(gen)
		if b.State() != stateClosed {
			t.Fatalf("failure %d: opened too early", i)
		}
	}
	// 第三次连续失败 -> OPEN
	r, gen := b.Allow()
	if r != "" {
		t.Fatalf("want 3rd request allowed, got %s", r)
	}
	b.RecordFailure(gen)
	if got := b.State(); got != stateOpen {
		t.Fatalf("state after threshold failures = %s, want OPEN", got)
	}

	// 打开期间全部快速拒绝。
	if r, _ := b.Allow(); r != ReasonCircuitOpen {
		t.Fatalf("open state rejection = %q, want circuit_open", r)
	}
	// 冷却未到期，即使成功/失败上报也不应改变状态（这里没有合法准入，不应被调用）。
	adv(49 * time.Millisecond)
	if r, _ := b.Allow(); r != ReasonCircuitOpen {
		t.Fatalf("still within cooldown, rejection = %q, want circuit_open", r)
	}
}

// TestBreakerHalfOpenProbeSuccess 冷却到期后首个请求作为探测，探测成功则恢复。
func TestBreakerHalfOpenProbeSuccess(t *testing.T) {
	now := time.Now()
	b := NewBreaker("svc-b", 1, 50*time.Millisecond, 1, func() time.Time { return now })

	r, gen := b.Allow()
	if r != "" {
		t.Fatalf("initial allow: %s", r)
	}
	b.RecordFailure(gen) // threshold=1 -> OPEN

	now = now.Add(50 * time.Millisecond) // 冷却到期
	r, genP := b.Allow()
	if r != "" {
		t.Fatalf("first post-cooldown request should be a probe, got %s", r)
	}
	if got := b.State(); got != stateHalfOpen {
		t.Fatalf("state = %s, want HALF_OPEN", got)
	}
	// 探测在途时，第二个请求受探测并发上限拒绝。
	if r, _ := b.Allow(); r != ReasonProbeLimit {
		t.Fatalf("probe slot occupied, rejection = %q, want probe_concurrency_limit", r)
	}
	b.RecordSuccess(genP)
	if got := b.State(); got != stateClosed {
		t.Fatalf("after probe success state = %s, want CLOSED", got)
	}
	// 恢复后计数清零：单次失败不应立刻重新打开（阈值 1 除外，这里重新验证计数）。
	r, gen = b.Allow()
	if r != "" {
		t.Fatalf("after recovery allow: %s", r)
	}
	b.RecordSuccess(gen)
}

// TestBreakerProbeFailureReopens 探测失败立即重新打开并重新计时。
func TestBreakerProbeFailureReopens(t *testing.T) {
	now := time.Now()
	b := NewBreaker("svc-c", 1, 50*time.Millisecond, 1, func() time.Time { return now })
	_, gen := b.Allow()
	b.RecordFailure(gen)
	if b.State() != stateOpen {
		t.Fatalf("want OPEN")
	}

	now = now.Add(50 * time.Millisecond)
	r, genP := b.Allow()
	if r != "" {
		t.Fatalf("want probe allowed: %s", r)
	}
	b.RecordFailure(genP) // 探测失败
	if got := b.State(); got != stateOpen {
		t.Fatalf("probe failure state = %s, want OPEN (re-opened)", got)
	}
	// 重新计时：刚重开的冷却尚未到期。
	now = now.Add(49 * time.Millisecond)
	if r, _ := b.Allow(); r != ReasonCircuitOpen {
		t.Fatalf("re-opened cooldown not elapsed, got %q", r)
	}
}

// TestBreakerStaleGenerationIgnored 上一代半开探测的迟到结果不得污染新一代状态。
func TestBreakerStaleGenerationIgnored(t *testing.T) {
	now := time.Now()
	b := NewBreaker("svc-d", 1, 20*time.Millisecond, 1, func() time.Time { return now })

	// 第一代：打开 -> 冷却 -> 半开探测，探测请求在途。
	_, g1 := b.Allow()
	b.RecordFailure(g1)
	now = now.Add(20 * time.Millisecond)
	r, genProbe1 := b.Allow()
	if r != "" || b.State() != stateHalfOpen {
		t.Fatalf("want half-open probe, r=%q state=%s", r, b.State())
	}

	// 假设探测迟到：先有一条更快的路径（这里模拟代次已前进）。
	// 让探测1失败 -> 重新 OPEN -> 再冷却 -> 再 HALF_OPEN（第二代）。
	b.RecordFailure(genProbe1)
	if b.State() != stateOpen {
		t.Fatalf("want OPEN after probe failure")
	}
	now = now.Add(20 * time.Millisecond)
	r, genProbe2 := b.Allow()
	if r != "" || b.State() != stateHalfOpen {
		t.Fatalf("want second-gen probe, r=%q state=%s", r, b.State())
	}

	// 第一代探测如果此时才"迟到"上报成功，必须被忽略：状态仍为 HALF_OPEN。
	b.RecordSuccess(genProbe1)
	if got := b.State(); got != stateHalfOpen {
		t.Fatalf("stale success changed state to %s, want HALF_OPEN unchanged", got)
	}
	// 第二代探测成功才真正恢复。
	b.RecordSuccess(genProbe2)
	if got := b.State(); got != stateClosed {
		t.Fatalf("current-gen success state = %s, want CLOSED", got)
	}
}

// TestBreakerProbeConcurrentLimit 半开探测并发不得超过 ProbeMaxConcurrent。
func TestBreakerProbeConcurrentLimit(t *testing.T) {
	now := time.Now()
	b := NewBreaker("svc-e", 1, time.Hour, 3, func() time.Time { return now })
	_, g := b.Allow()
	b.RecordFailure(g) // OPEN
	now = now.Add(time.Hour)
	for i := 0; i < 3; i++ {
		if r, _ := b.Allow(); r != "" {
			t.Fatalf("probe slot %d should be available: %s", i+1, r)
		}
	}
	if r, _ := b.Allow(); r != ReasonProbeLimit {
		t.Fatalf("4th probe = %q, want probe_concurrency_limit", r)
	}
}
