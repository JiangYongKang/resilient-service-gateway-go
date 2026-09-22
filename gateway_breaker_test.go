package gateway

import (
	"errors"
	"testing"
	"time"
)

// TestGatewayCircuitOpensAndFastRejects 阈值内连续失败后熔断打开，
// 后续请求在派发前被 circuit_open 快速拒绝，不调用下游。
func TestGatewayCircuitOpensAndFastRejects(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-gcb", fakeCall{fail: true})
	cfg := testConfig(ds, retryableClassifier)
	cfg.Threshold = 2
	cfg.Retry.MaxAttempts = 1 // 关闭重试，专注观察熔断对不同请求的作用
	g.RegisterRoute("svc-gcb", cfg)
	log := attachLogger(t, g)

	// 两个独立请求连续失败 -> 熔断打开。
	for i := 0; i < 2; i++ {
		_, err := g.Call(Request{ID: "failing", Route: "svc-gcb", Idempotent: true})
		var term *TerminalError
		if !errors.As(err, &term) {
			t.Fatalf("request %d: err = %v, want terminal", i+1, err)
		}
	}
	st, _ := g.Status("svc-gcb")
	if st.Breaker != stateOpen {
		t.Fatalf("breaker = %s, want OPEN", st.Breaker)
	}

	before := ds.gotCalls()
	_, err := g.Call(Request{ID: "blocked", Route: "svc-gcb", Idempotent: true})
	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonCircuitOpen {
		t.Fatalf("err = %v, want RejectError(circuit_open)", err)
	}
	if ds.gotCalls() != before {
		t.Fatalf("downstream was called while breaker OPEN: %d -> %d", before, ds.gotCalls())
	}
	if log.last().Reason != ReasonCircuitOpen || log.last().Breaker != "OPEN" {
		t.Fatalf("unexpected reject log: %+v", log.last())
	}
}

// TestGatewayHalfOpenRecovery 冷却到期后半开探测成功，网关恢复正常服务。
func TestGatewayHalfOpenRecovery(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-rec",
		fakeCall{fail: true}, fakeCall{fail: true}, // 触发熔断
		fakeCall{}, // 半开探测：成功
		fakeCall{}, // 恢复后正常请求
	)
	cfg := testConfig(ds, nonRetryableClassifier)
	cfg.Threshold = 2
	cfg.OpenCooldown = 30 * time.Millisecond
	cfg.Retry.MaxAttempts = 1
	g.RegisterRoute("svc-rec", cfg)
	log := attachLogger(t, g)

	for i := 0; i < 2; i++ {
		_, _ = g.Call(Request{ID: "trip", Route: "svc-rec", Idempotent: true})
	}
	if st, _ := g.Status("svc-rec"); st.Breaker != stateOpen {
		t.Fatalf("want OPEN, got %s", st.Breaker)
	}

	// 冷却中：仍被快速拒绝。
	if _, err := g.Call(Request{ID: "during-cooldown", Route: "svc-rec"}); true {
		var rej *RejectError
		if !errors.As(err, &rej) || rej.Reason != ReasonCircuitOpen {
			t.Fatalf("during cooldown err = %v, want circuit_open", err)
		}
	}

	time.Sleep(35 * time.Millisecond)

	// 冷却到期：首个请求成为探测，成功后恢复。
	resp, err := g.Call(Request{ID: "probe", Route: "svc-rec", Idempotent: true})
	if err != nil {
		t.Fatalf("probe request failed: %v", err)
	}
	if resp.Attempts != 1 {
		t.Fatalf("probe attempts = %d, want 1", resp.Attempts)
	}
	if st, _ := g.Status("svc-rec"); st.Breaker != stateClosed {
		t.Fatalf("after successful probe breaker = %s, want CLOSED", st.Breaker)
	}
	if log.last().Phase != "success" || log.last().Breaker != "CLOSED" {
		t.Fatalf("unexpected last log: %+v", log.last())
	}

	// 恢复后新请求正常通过。
	resp2, err := g.Call(Request{ID: "after-recovery", Route: "svc-rec"})
	if err != nil {
		t.Fatalf("post-recovery request failed: %v", err)
	}
	if resp2.Attempts != 1 {
		t.Fatalf("post-recovery attempts = %d, want 1", resp2.Attempts)
	}
}

// TestUnknownRouteRejected 未注册路由被明确拒绝。
func TestUnknownRouteRejected(t *testing.T) {
	g := New()
	log := attachLogger(t, g)
	_, err := g.Call(Request{ID: "x", Route: "nope"})
	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonUnknownRoute {
		t.Fatalf("err = %v, want RejectError(unknown_route)", err)
	}
	if log.last().Reason != ReasonUnknownRoute {
		t.Fatalf("last reason = %q", log.last().Reason)
	}
	if _, ok := g.Status("nope"); ok {
		t.Fatalf("Status should report unknown route")
	}
}
