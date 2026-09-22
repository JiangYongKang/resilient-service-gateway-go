package gateway

import (
	"sync"
	"testing"
	"time"
)

func testBreakerCfg() BreakerConfig {
	return BreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     20 * time.Millisecond,
		ProbeConcurrency: 1,
		SuccessThreshold: 2,
	}
}

func TestBreaker_ClosedToOpenOnConsecutiveFailures(t *testing.T) {
	b := newBreaker("svc-a", testBreakerCfg())
	base := time.Now()

	for i := 1; i <= 2; i++ {
		p, r := b.Acquire(base)
		if r != RejectNone {
			t.Fatalf("attempt %d: expected permit, got reject %s", i, r)
		}
		p.ReportResult(false)
		if got := b.State(); got != StateClosed {
			t.Fatalf("attempt %d: expected closed, got %s", i, got)
		}
		t.Logf("downstream=svc-a failure#%d breaker=%s", i, b.State())
	}

	p, _ := b.Acquire(base)
	p.ReportResult(false)
	if got := b.State(); got != StateOpen {
		t.Fatalf("expected open after threshold, got %s", got)
	}
	t.Logf("downstream=svc-a failure#3 reached threshold, breaker=%s", b.State())

	// 熔断期间快速拒绝
	if _, r := b.Acquire(base.Add(5 * time.Millisecond)); r != RejectCircuitOpen {
		t.Fatalf("expected circuit-open reject, got %s", r)
	}
	t.Logf("downstream=svc-a request during open rejected reason=%s", RejectCircuitOpen)
}

func TestBreaker_ProbeSuccessClosesAndResetsStreak(t *testing.T) {
	b := newBreaker("svc-b", testBreakerCfg())
	openAt := time.Now()
	trip(b, openAt)
	if b.State() != StateOpen {
		t.Fatalf("setup: expected open")
	}

	after := openAt.Add(25 * time.Millisecond)

	// 第一次探测成功：仍 HalfOpen
	p1, r := b.Acquire(after)
	if r != RejectNone {
		t.Fatalf("probe 1: expected permit, got %s", r)
	}
	p1.ReportResult(true)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("probe 1: expected half-open, got %s", got)
	}
	t.Logf("downstream=svc-b probe success#1 breaker=%s", b.State())

	// 第二次探测成功：关闭
	p2, _ := b.Acquire(after.Add(time.Millisecond))
	p2.ReportResult(true)
	if got := b.State(); got != StateClosed {
		t.Fatalf("probe 2: expected closed, got %s", got)
	}
	t.Logf("downstream=svc-b probe success#2 breaker=%s", b.State())

	// 关闭后连续失败计数应已重置：2 次失败不应再次熔断
	for i := 0; i < 2; i++ {
		p, _ := b.Acquire(after.Add(2 * time.Millisecond))
		p.ReportResult(false)
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("expected reset streak, got %s", got)
	}
}

func TestBreaker_ProbeFailureReopens(t *testing.T) {
	b := newBreaker("svc-c", testBreakerCfg())
	openAt := time.Now()
	trip(b, openAt)

	p, _ := b.Acquire(openAt.Add(25 * time.Millisecond))
	if b.State() != StateHalfOpen {
		t.Fatalf("expected half-open, got %s", b.State())
	}
	p.ReportResult(false)
	if got := b.State(); got != StateOpen {
		t.Fatalf("probe failure: expected open, got %s", got)
	}
	t.Logf("downstream=svc-c probe failed, breaker=%s", b.State())
}

func TestBreaker_ProbeConcurrencyIsBounded(t *testing.T) {
	cfg := testBreakerCfg()
	cfg.ProbeConcurrency = 2
	b := newBreaker("svc-d", cfg)
	openAt := time.Now()
	trip(b, openAt)
	after := openAt.Add(25 * time.Millisecond)

	p1, _ := b.Acquire(after)
	p2, _ := b.Acquire(after)
	if _, r := b.Acquire(after); r != RejectCircuitOpen {
		t.Fatalf("third probe must be rejected, got %s", r)
	}
	t.Logf("downstream=svc-d probes in flight=2, third reject reason=%s", RejectCircuitOpen)

	// 一个探测成功释放槽位后，新探测可进入（SuccessThreshold=2 故仍 half-open）
	p1.ReportResult(true)
	if _, r := b.Acquire(after.Add(time.Millisecond)); r != RejectNone {
		t.Fatalf("expected slot freed for new probe, got %s", r)
	}
	p2.Release()
}

func TestBreaker_ConcurrentReportsNoStateCorruption(t *testing.T) {
	cfg := testBreakerCfg()
	cfg.FailureThreshold = 1
	cfg.OpenDuration = 50 * time.Millisecond
	cfg.SuccessThreshold = 1
	b := newBreaker("svc-e", cfg)

	var wg sync.WaitGroup
	// 先触发熔断
	p0, _ := b.Acquire(time.Now())
	p0.ReportResult(false)

	// 并发：等待冷却后多个 goroutine 同时尝试探测（ProbeConcurrency=1），
	// 且探测结果有成功有失败，状态最终必须合法。
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(55 * time.Millisecond)
			now := time.Now()
			p, r := b.Acquire(now)
			if r != RejectNone {
				return // 未抢到探测槽：合法
			}
			p.ReportResult(i%2 == 0)
		}(i)
	}
	wg.Wait()

	switch s := b.State(); s {
	case StateClosed, StateOpen, StateHalfOpen:
		t.Logf("downstream=svc-e concurrent probes finished, final breaker=%s (valid)", s)
	default:
		t.Fatalf("invalid final state: %s", s)
	}
}

// trip 将熔断器确定性地驱动到 Open。
func trip(b *Breaker, now time.Time) {
	for i := 0; i < b.cfg.FailureThreshold; i++ {
		p, _ := b.Acquire(now)
		p.ReportResult(false)
	}
}
