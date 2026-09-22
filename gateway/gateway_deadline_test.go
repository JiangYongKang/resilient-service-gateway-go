package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestGateway(t *testing.T, d *fakeDownstream, cfg DownstreamConfig) (*Gateway, *memLogger) {
	l := newMemLogger(t)
	if cfg.Handler == nil {
		cfg.Handler = d
	}
	g := New(Config{
		Logger: l,
		Routes: map[string]DownstreamConfig{"svc": cfg},
	})
	return g, l
}

func TestGateway_RouteNotFound(t *testing.T) {
	g, l := newTestGateway(t, &fakeDownstream{}, DownstreamConfig{})
	_, err := g.Call(context.Background(), &Request{Route: "missing"})
	mustReject(t, err, RejectRouteNotFound)
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_DeadlinePropagatedToDownstream(t *testing.T) {
	d := &fakeDownstream{}
	g, l := newTestGateway(t, d, DownstreamConfig{
		PerAttemptTimeout: 80 * time.Millisecond,
	})
	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	resp, err := g.Call(ctx, &Request{Route: "svc"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	if len(d.deadlineObserved) != 1 {
		t.Fatalf("expected 1 deadline observed, got %d", len(d.deadlineObserved))
	}
	// 传播给下游的截止时间不得晚于调用方截止时间。
	if d.deadlineObserved[0].After(deadline.Add(5 * time.Millisecond)) {
		t.Fatalf("propagated deadline %v later than caller %v", d.deadlineObserved[0], deadline)
	}
	t.Logf("downstream=svc caller_deadline=%v downstream_deadline=%v",
		deadline, d.deadlineObserved[0])
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_AlreadyExpiredRejectedBeforeDispatch(t *testing.T) {
	d := &fakeDownstream{}
	g, l := newTestGateway(t, d, DownstreamConfig{})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := g.Call(ctx, &Request{Route: "svc"})
	mustReject(t, err, RejectDeadlineExpired)
	if d.attemptsCount() != 0 {
		t.Fatalf("expired request must not be dispatched, attempts=%d", d.attemptsCount())
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_InsufficientBudgetRejectedBeforeDispatch(t *testing.T) {
	d := &fakeDownstream{delay: 50 * time.Millisecond}
	g, l := newTestGateway(t, d, DownstreamConfig{
		PerAttemptTimeout: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := g.Call(ctx, &Request{Route: "svc"})
	mustReject(t, err, RejectInsufficientBudget)
	if d.attemptsCount() != 0 {
		t.Fatalf("request must be rejected pre-dispatch, attempts=%d", d.attemptsCount())
	}
	t.Logf("logs:\n%s", l.String())
}

func TestGateway_DownstreamRawErrorNotLeaked(t *testing.T) {
	sentinel := errors.New("super-secret internal failure code=13")
	d := &fakeDownstream{behavior: func(int32) (*Response, error) {
		return nil, NonRetryable(sentinel)
	}}
	g, _ := newTestGateway(t, d, DownstreamConfig{})

	_, err := g.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
	re := mustReject(t, err, RejectNonRetryable)
	if errors.Is(err, sentinel) {
		t.Fatal("raw downstream error must not be reachable via errors.Is")
	}
	if re.Class != ClassNonRetryable {
		t.Fatalf("expected non-retryable class, got %s", re.Class)
	}
}

func TestGateway_AttemptTimeoutCanceledByGateway(t *testing.T) {
	started := make(chan struct{})
	released := make(chan struct{})
	d := DownstreamFunc(func(ctx context.Context, req *Request) (*Response, error) {
		close(started)
		<-ctx.Done()
		close(released)
		return nil, ctx.Err()
	})
	l := newMemLogger(t)
	g := New(Config{Logger: l, Routes: map[string]DownstreamConfig{
		"slow": {Handler: d, PerAttemptTimeout: 30 * time.Millisecond},
	}})

	// 无显式截止时间：仅受 per-attempt 超时控制，且不可重试 -> 终局 NonRetryable?
	// 默认分类器把 context.DeadlineExceeded 判为可重试，但请求非幂等，终局仍为 NonRetryable。
	_, err := g.Call(context.Background(), &Request{Route: "slow"})
	mustReject(t, err, RejectNonRetryable)
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("downstream ctx was not canceled at attempt timeout")
	}
	t.Logf("logs:\n%s", l.String())
}
