package gateway

import (
	"errors"
	"testing"
	"time"
)

// TestDeadlineExpiredRejectedBeforeDispatch 已过期的请求必须在派发前拒绝，
// 下游一次都不应被调用，原因为 deadline_expired。
func TestDeadlineExpiredRejectedBeforeDispatch(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-deadline", fakeCall{fail: true})
	g.RegisterRoute("svc-deadline", testConfig(ds, retryableClassifier))
	log := attachLogger(t, g)

	req := Request{ID: "r1", Route: "svc-deadline", Idempotent: true,
		Deadline: time.Now().Add(-time.Millisecond)}
	_, err := g.Call(req)

	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonDeadlineExpired {
		t.Fatalf("err = %v, want RejectError(deadline_expired)", err)
	}
	if ds.gotCalls() != 0 {
		t.Fatalf("downstream invoked %d times, want 0 (must reject before dispatch)", ds.gotCalls())
	}
	if log.last().Reason != ReasonDeadlineExpired || log.last().Breaker != "CLOSED" {
		t.Fatalf("unexpected last log event: %+v", log.last())
	}
}

// TestDeadlinePropagatedToDownstream 剩余预算必须传播到下游，
// 下游观察到的截止时间应约等于调用方声明的截止时间。
func TestDeadlinePropagatedToDownstream(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-prop", fakeCall{delay: time.Millisecond})
	g.RegisterRoute("svc-prop", testConfig(ds, retryableClassifier))
	attachLogger(t, g)

	deadline := time.Now().Add(250 * time.Millisecond)
	req := Request{ID: "r2", Route: "svc-prop", Idempotent: true, Deadline: deadline}
	resp, err := g.Call(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", resp.Attempts)
	}
	ds.mu.Lock()
	got := ds.lastCtx.Deadline
	ds.mu.Unlock()
	if d := got.Sub(deadline); d > 5*time.Millisecond || d < -5*time.Millisecond {
		t.Fatalf("propagated deadline %v != caller deadline %v", got, deadline)
	}
}

// TestAttemptTimeoutAbortsAndIsClassified 下游耗时超过传播的剩余预算时，
// 尝试被截止时间中止；在无预算重试的情况下以 budget_exhausted 终局结束，
// 且下游原始错误不会透出。
func TestAttemptTimeoutLeadsToBudgetExhausted(t *testing.T) {
	g := New()
	// 下游每次都慢到超过剩余预算。
	ds := newFakeDownstream("svc-slow",
		fakeCall{delay: 100 * time.Millisecond},
		fakeCall{delay: 100 * time.Millisecond},
	)
	cfg := testConfig(ds, retryableClassifier)
	cfg.Retry.Backoff = time.Millisecond
	g.RegisterRoute("svc-slow", cfg)
	log := attachLogger(t, g)
	// 保留真实 sleep，使退避与时间流逝真实发生。
	g.Sleep = time.Sleep

	req := Request{ID: "r3", Route: "svc-slow", Idempotent: true,
		Deadline: time.Now().Add(30 * time.Millisecond)}
	_, err := g.Call(req)

	var term *TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("err = %v, want *TerminalError", err)
	}
	if term.Kind != TerminalBudgetExhausted {
		t.Fatalf("terminal kind = %s, want budget_exhausted", term.Kind)
	}
	if errors.Is(err, errFakeDownstream) {
		t.Fatalf("downstream raw error leaked: %v", err)
	}
	if log.last().Kind != TerminalBudgetExhausted {
		t.Fatalf("last event kind = %q, want budget_exhausted", log.last().Kind)
	}
	t.Logf("budget exhausted after %d attempt(s), last remaining=%s", term.Attempts, log.last().Remaining)
}

// TestBackoffThatConsumesBudget 退避时间受剩余预算截断；退避后预算耗尽则不再派发。
func TestBackoffThatConsumesBudget(t *testing.T) {
	g := New()
	// 第一次快速失败（可重试），之后若再派发会被记录；我们要求只派发一次。
	ds := newFakeDownstream("svc-budget",
		fakeCall{delay: 2 * time.Millisecond, fail: true},
		fakeCall{fail: true},
	)
	cfg := testConfig(ds, retryableClassifier)
	cfg.Retry.Backoff = 5 * time.Millisecond
	g.RegisterRoute("svc-budget", cfg)
	log := attachLogger(t, g)
	g.Sleep = time.Sleep // 真实退避以耗尽预算

	req := Request{ID: "r4", Route: "svc-budget", Idempotent: true,
		Deadline: time.Now().Add(3 * time.Millisecond)}
	_, err := g.Call(req)

	var term *TerminalError
	if !errors.As(err, &term) || term.Kind != TerminalBudgetExhausted {
		t.Fatalf("err = %v, want TerminalError(budget_exhausted)", err)
	}
	if n := ds.gotCalls(); n != 1 {
		t.Fatalf("downstream calls = %d, want exactly 1 (no dispatch after budget exhausted)", n)
	}
	if log.last().Phase != "terminal" {
		t.Fatalf("last phase = %q, want terminal", log.last().Phase)
	}
}
