package gateway

import (
	"errors"
	"strings"
	"testing"
)

// TestRetrySucceedsOnSecondAttempt 幂等 + 可重试：首次失败后重试，第二次成功。
func TestRetrySucceedsOnSecondAttempt(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-retry",
		fakeCall{fail: true},
		fakeCall{fail: false},
	)
	g.RegisterRoute("svc-retry", testConfig(ds, retryableClassifier))
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "r1", Route: "svc-retry", Idempotent: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", resp.Attempts)
	}
	if ds.gotCalls() != 2 {
		t.Fatalf("downstream calls = %d, want 2", ds.gotCalls())
	}
	if log.last().Phase != "success" {
		t.Fatalf("last phase = %q, want success", log.last().Phase)
	}
}

// TestNonRetryableFailureIsTerminal 不可重试失败只尝试一次，给出 non_retryable_failure。
func TestNonRetryableFailureIsTerminal(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-nr", fakeCall{fail: true}, fakeCall{}, fakeCall{})
	g.RegisterRoute("svc-nr", testConfig(ds, nonRetryableClassifier))
	log := attachLogger(t, g)

	_, err := g.Call(Request{ID: "r2", Route: "svc-nr", Idempotent: true})
	var term *TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("err = %v(%T), want *TerminalError", err, err)
	}
	if term.Kind != TerminalNonRetryable {
		t.Fatalf("kind = %s, want non_retryable_failure", term.Kind)
	}
	if term.Class != FailureNonRetryable {
		t.Fatalf("class = %s, want non_retryable", term.Class)
	}
	if term.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry for non-retryable)", term.Attempts)
	}
	if ds.gotCalls() != 1 {
		t.Fatalf("downstream calls = %d, want 1", ds.gotCalls())
	}
	if log.last().Class != "non_retryable" {
		t.Fatalf("last log class = %q, want non_retryable", log.last().Class)
	}
}

// TestNonIdempotentNotRetried 即便失败被分类为可重试，调用方未声明幂等也不得重试。
func TestNonIdempotentNotRetried(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-idem", fakeCall{fail: true}, fakeCall{}, fakeCall{})
	g.RegisterRoute("svc-idem", testConfig(ds, retryableClassifier))
	log := attachLogger(t, g)

	_, err := g.Call(Request{ID: "r3", Route: "svc-idem", Idempotent: false})
	var term *TerminalError
	if !errors.As(err, &term) || term.Kind != TerminalNonRetryable {
		t.Fatalf("err = %v, want TerminalError(non_retryable_failure)", err)
	}
	if ds.gotCalls() != 1 {
		t.Fatalf("downstream calls = %d, want 1 (non-idempotent must not retry)", ds.gotCalls())
	}
	if len(log.phases()) != 2 { // dispatch + terminal，无 backoff
		t.Fatalf("phases = %v, want [dispatch terminal]", log.phases())
	}
}

// TestRetryExhaustsMaxAttempts 持续可重试失败时，尝试次数不超过 MaxAttempts，
// 最终以 non_retryable_failure（无剩余尝试）终局结束，且下游原始错误不透出。
func TestRetryExhaustsMaxAttempts(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-exhaust", fakeCall{fail: true})
	cfg := testConfig(ds, retryableClassifier)
	cfg.Retry.MaxAttempts = 3
	cfg.Threshold = 10 // 熔断阈值高于尝试次数，专注验证重试次数上限
	g.RegisterRoute("svc-exhaust", cfg)
	log := attachLogger(t, g)

	_, err := g.Call(Request{ID: "r4", Route: "svc-exhaust", Idempotent: true})
	var term *TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("err = %v, want *TerminalError", err)
	}
	if term.Kind != TerminalNonRetryable {
		t.Fatalf("kind = %s, want non_retryable_failure after attempts exhausted", term.Kind)
	}
	if term.Attempts != 3 {
		t.Fatalf("attempts = %d, want exactly MaxAttempts=3", term.Attempts)
	}
	if ds.gotCalls() != 3 {
		t.Fatalf("downstream calls = %d, want 3", ds.gotCalls())
	}
	if errors.Is(err, errFakeDownstream) || strings.Contains(err.Error(), "fake downstream") {
		t.Fatalf("raw downstream error leaked to caller: %v", err)
	}
	if log.last().Attempt != 3 {
		t.Fatalf("last logged attempt = %d, want 3", log.last().Attempt)
	}
}

// TestRetryRespectsAttemptNumberContext 重试时向下游传递递增的尝试序号。
func TestRetryRespectsAttemptNumberContext(t *testing.T) {
	g := New()
	ds := newFakeDownstream("svc-seq",
		fakeCall{fail: true}, fakeCall{fail: true}, fakeCall{})
	cfgSeq := testConfig(ds, retryableClassifier)
	cfgSeq.Threshold = 10
	g.RegisterRoute("svc-seq", cfgSeq)
	attachLogger(t, g)

	resp, err := g.Call(Request{ID: "r5", Route: "svc-seq", Idempotent: true})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if resp.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", resp.Attempts)
	}
	ds.mu.Lock()
	last := ds.lastCtx.Attempt
	ds.mu.Unlock()
	if last != 3 {
		t.Fatalf("last downstream attempt number = %d, want 3", last)
	}
}
