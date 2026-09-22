package gateway

import (
	"errors"
	"testing"
	"time"
)

// TestOpenCircuitPreemptsRetry 熔断打开优先于重试：打开期间任何新请求
// （即使幂等且失败可重试、重试预算充足）都在派发前以 circuit_open 被拒绝，
// 不会产生一次下游调用，也不会得到 terminal 语义。
func TestOpenCircuitPreemptsRetry(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-int", fakeCall{fail: true})
	cfg := testConfig(ds, retryableClassifier)
	cfg.Threshold = 2
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.Backoff = time.Millisecond
	g.RegisterRoute("svc-int", cfg)
	log := attachLogger(t, g)

	// 单次调用内的重试即会连续累计失败：第 2 次尝试达到阈值，
	// 第 3 次尝试在派发前被熔断拒绝，以终局 circuit_open 结束。
	_, err := g.Call(Request{ID: "tripping", Route: "svc-int", Idempotent: true})
	var term *TerminalError
	if !errors.As(err, &term) || term.Kind != TerminalCircuitOpen {
		t.Fatalf("tripping call err = %v, want TerminalError(circuit_open)", err)
	}
	st, _ := g.Status("svc-int")
	if st.Breaker != stateOpen {
		t.Fatalf("want OPEN, got %s", st.Breaker)
	}
	callsWhenOpen := ds.gotCalls()

	// 打开期间的全新请求（再怎么具备重试条件）只能得到派发前拒绝。
	_, err = g.Call(Request{ID: "while-open", Route: "svc-int", Idempotent: true})
	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonCircuitOpen {
		t.Fatalf("err = %v, want RejectError(circuit_open)", err)
	}
	if ds.gotCalls() != callsWhenOpen {
		t.Fatalf("downstream invoked while OPEN: %d -> %d", callsWhenOpen, ds.gotCalls())
	}
	last := log.last()
	if last.Phase != "reject" || last.Breaker != "OPEN" || last.Reason != ReasonCircuitOpen {
		t.Fatalf("unexpected last event: %+v", last)
	}
}

// TestRejectVsTerminalErrorTypesAreExhaustive 调用方总能拿到两类网关错误之一，
// 且可以无歧义地用 errors.As 区分派发前拒绝与终局失败。
func TestRejectVsTerminalErrorTypesAreExhaustive(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-types", fakeCall{fail: true})
	cfg := testConfig(ds, nonRetryableClassifier)
	cfg.Retry.MaxAttempts = 1
	g.RegisterRoute("svc-types", cfg)
	attachLogger(t, g)

	checkKind := func(t *testing.T, err error, reject bool) {
		t.Helper()
		var r *RejectError
		var te *TerminalError
		asR, asT := errors.As(err, &r), errors.As(err, &te)
		if reject {
			if !asR || asT {
				t.Fatalf("err=%v(%T): want reject only", err, err)
			}
		} else {
			if asR || !asT {
				t.Fatalf("err=%v(%T): want terminal only", err, err)
			}
		}
	}

	// 已过期 -> reject(deadline_expired)
	_, err := g.Call(Request{ID: "1", Route: "svc-types",
		Deadline: time.Now().Add(-time.Second)})
	checkKind(t, err, true)

	// 下游不可重试失败 -> terminal(non_retryable)
	_, err = g.Call(Request{ID: "2", Route: "svc-types", Idempotent: true})
	checkKind(t, err, false)

	// 用独立路由触发熔断后再验证 -> reject(circuit_open)
	ds2 := newFakeDownstream("svc-types2", fakeCall{fail: true})
	cfg2 := cfg
	cfg2.Downstream = ds2
	g.RegisterRoute("svc-types2", cfg2)
	_, _ = g.Call(Request{ID: "t1", Route: "svc-types2"})
	_, _ = g.Call(Request{ID: "t2", Route: "svc-types2"})
	if st, _ := g.Status("svc-types2"); st.Breaker != stateOpen {
		t.Fatalf("want OPEN, got %s", st.Breaker)
	}
	_, err = g.Call(Request{ID: "3", Route: "svc-types2"})
	checkKind(t, err, true)
	if r, _ := err.(*RejectError); r == nil || r.Reason != ReasonCircuitOpen {
		t.Fatalf("want circuit_open reject, got %v", err)
	}

	// 未知路由 -> reject(unknown_route)
	_, err = g.Call(Request{ID: "4", Route: "missing"})
	checkKind(t, err, true)
}
