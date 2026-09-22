package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGateway_RetrySucceedsWithinAttempts(t *testing.T) {
	d := &fakeDownstream{behavior: func(n int32) (*Response, error) {
		if n < 3 {
			return nil, Retryable(errors.New("transient"))
		}
		return &Response{Payload: "ok"}, nil
	}}
	g, l := newTestGateway(t, d, DownstreamConfig{MaxAttempts: 3})

	resp, err := g.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Payload != "ok" {
		t.Fatalf("unexpected payload: %v", resp.Payload)
	}
	if d.attemptsCount() != 3 {
		t.Fatalf("expected 3 attempts, got %d", d.attemptsCount())
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_NonIdempotentNotRetried(t *testing.T) {
	d := &fakeDownstream{behavior: func(int32) (*Response, error) {
		return nil, Retryable(errors.New("transient"))
	}}
	g, l := newTestGateway(t, d, DownstreamConfig{MaxAttempts: 5})

	_, err := g.Call(context.Background(), &Request{Route: "svc", Idempotent: false})
	re := mustReject(t, err, RejectNonRetryable)
	if re.Class != ClassRetryable {
		t.Fatalf("terminal class should retain retryable class, got %s", re.Class)
	}
	if d.attemptsCount() != 1 {
		t.Fatalf("non-idempotent request must be attempted once, got %d", d.attemptsCount())
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_AttemptsExhaustedGivesBudgetExhausted(t *testing.T) {
	d := &fakeDownstream{behavior: func(int32) (*Response, error) {
		return nil, Retryable(errors.New("transient"))
	}}
	g, l := newTestGateway(t, d, DownstreamConfig{MaxAttempts: 3})

	_, err := g.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
	re := mustReject(t, err, RejectBudgetExhausted)
	if re.Class != ClassRetryable {
		t.Fatalf("expected retryable class on exhaustion, got %s", re.Class)
	}
	if d.attemptsCount() != 3 {
		t.Fatalf("expected 3 attempts, got %d", d.attemptsCount())
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_NonRetryableIsTerminalAndDistinct(t *testing.T) {
	d := &fakeDownstream{behavior: func(n int32) (*Response, error) {
		if n == 1 {
			return nil, NonRetryable(errors.New("bad request"))
		}
		t.Fatalf("non-retryable must not be retried, attempt=%d", n)
		return nil, nil
	}}
	g, l := newTestGateway(t, d, DownstreamConfig{MaxAttempts: 5})

	_, err := g.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
	mustReject(t, err, RejectNonRetryable)
	if d.attemptsCount() != 1 {
		t.Fatalf("expected single attempt, got %d", d.attemptsCount())
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_RetryBudgetExpiresBetweenAttempts(t *testing.T) {
	// 第 1 次尝试很快失败可重试（50ms 单次超时、200ms 总预算可覆盖）；
	// 重试退避 150ms，退避结束时预算已耗尽，第 2 次尝试在派发前被拒，
	// 终局原因为 budget-exhausted，与 non-retryable 可区分。
	// 重试退避 160ms，退避结束时剩余约 40ms < 50ms 单次超时，
	// 第 2 次尝试在派发前因预算不足被拒。
	d := &fakeDownstream{behavior: func(int32) (*Response, error) {
		return nil, Retryable(errors.New("transient"))
	}}
	g, l := newTestGateway(t, d, DownstreamConfig{
		MaxAttempts:       3,
		Backoff:           160 * time.Millisecond,
		PerAttemptTimeout: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := g.Call(ctx, &Request{Route: "svc", Idempotent: true})
	mustReject(t, err, RejectBudgetExhausted)
	// 只派发了 1 次：第二次尝试在派发前被预算门控拒绝。
	if d.attemptsCount() != 1 {
		t.Fatalf("expected 1 dispatch, got %d", d.attemptsCount())
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("call took too long: %s", elapsed)
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_BackoffCappedByBudget(t *testing.T) {
	// 退避长于预算：第一次尝试后立刻在退避中预算耗尽，不应尝试第 2 次。
	d := &fakeDownstream{behavior: func(int32) (*Response, error) {
		return nil, Retryable(errors.New("transient"))
	}}
	g, l := newTestGateway(t, d, DownstreamConfig{
		MaxAttempts: 3,
		Backoff:     300 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := g.Call(ctx, &Request{Route: "svc", Idempotent: true})
	mustReject(t, err, RejectBudgetExhausted)
	if d.attemptsCount() != 1 {
		t.Fatalf("expected 1 dispatch, got %d", d.attemptsCount())
	}
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("backoff was not capped by remaining budget")
	}
	t.Logf("logs:\n%s", l.String())
}
