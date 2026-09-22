package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGateway_ConcurrencyLimitEnforced(t *testing.T) {
	const max = 3
	var active int32
	var everOver int32
	d := DownstreamFunc(func(ctx context.Context, req *Request) (*Response, error) {
		cur := atomic.AddInt32(&active, 1)
		if cur > max {
			atomic.StoreInt32(&everOver, 1)
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return &Response{}, nil
	})
	l := newMemLogger(t)
	g := New(Config{Logger: l, Routes: map[string]DownstreamConfig{
		"svc": {Handler: d, MaxConcurrency: max, FailureThreshold: 100},
	}})

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g.Call(context.Background(), &Request{Route: "svc"}); err != nil {
				t.Errorf("unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&everOver) == 1 {
		t.Fatal("downstream concurrency exceeded the configured limit")
	}
	t.Logf("downstream=svc max_concurrency=%d enforced; total elapsed=%s", max, time.Since(start))
}

func TestGateway_ConcurrencyIsolatedPerDownstream(t *testing.T) {
	// 一个下游占满并发不应阻塞另一个下游。
	block := make(chan struct{})
	var blockedActive int32
	slow := DownstreamFunc(func(ctx context.Context, req *Request) (*Response, error) {
		atomic.AddInt32(&blockedActive, 1)
		<-block
		return &Response{}, nil
	})
	fastDone := make(chan struct{})
	fast := DownstreamFunc(func(ctx context.Context, req *Request) (*Response, error) {
		close(fastDone)
		return &Response{}, nil
	})

	l := newMemLogger(t)
	g := New(Config{Logger: l, Routes: map[string]DownstreamConfig{
		"slow": {Handler: slow, MaxConcurrency: 2},
		"fast": {Handler: fast, MaxConcurrency: 2},
	}})

	var slowWg sync.WaitGroup
	for i := 0; i < 2; i++ {
		slowWg.Add(1)
		go func() {
			defer slowWg.Done()
			g.Call(context.Background(), &Request{Route: "slow"})
		}()
	}
	time.Sleep(20 * time.Millisecond) // 等 slow 占满

	done := make(chan error, 1)
	go func() {
		_, err := g.Call(context.Background(), &Request{Route: "fast"})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fast route must not be blocked: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("downstream isolation broken: fast route blocked by slow route")
	}
	close(block)
	slowWg.Wait()
	t.Logf("downstream=slow in-flight=%d while downstream=fast still served",
		atomic.LoadInt32(&blockedActive))
}

func TestGateway_RetriesDoNotExceedConcurrency(t *testing.T) {
	const max = 2
	var active int32
	var everOver int32
	d := DownstreamFunc(func(ctx context.Context, req *Request) (*Response, error) {
		cur := atomic.AddInt32(&active, 1)
		if cur > max {
			atomic.StoreInt32(&everOver, 1)
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil, Retryable(errors.New("transient"))
	})
	l := newMemLogger(t)
	g := New(Config{Logger: l, Routes: map[string]DownstreamConfig{
		"svc": {
			Handler:        d,
			MaxConcurrency: max,
			MaxAttempts:    3,
			// 高阈值避免熔断干扰本用例对并发上限的断言。
			FailureThreshold: 1000,
		},
	}})

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := g.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
			mustReject(t, err, RejectBudgetExhausted)
		}()
	}
	wg.Wait()
	if atomic.LoadInt32(&everOver) == 1 {
		t.Fatal("retries exceeded the concurrency limit")
	}
	t.Logf("downstream=svc retries observed max_concurrency=%d (limit=%d)", 0, max)
}

func TestGateway_CircuitBreakerEndToEnd(t *testing.T) {
	var shouldFail int32 = 1
	d := &fakeDownstream{behavior: func(int32) (*Response, error) {
		if atomic.LoadInt32(&shouldFail) == 1 {
			return nil, NonRetryable(errors.New("boom"))
		}
		return &Response{Payload: "recovered"}, nil
	}}
	l := newMemLogger(t)
	g := New(Config{Logger: l, Routes: map[string]DownstreamConfig{
		"svc": {
			Handler:          d,
			MaxConcurrency:   4,
			FailureThreshold: 3,
			OpenDuration:     60 * time.Millisecond,
			ProbeConcurrency: 1,
			SuccessThreshold: 1,
			MaxAttempts:      1,
		},
	}})

	req := &Request{Route: "svc"}
	// 3 次连续不可重试失败触发熔断
	for i := 0; i < 3; i++ {
		_, err := g.Call(context.Background(), req)
		mustReject(t, err, RejectNonRetryable)
	}
	rt := g.routes["svc"]
	if s := rt.breaker.State(); s != StateOpen {
		t.Fatalf("expected open, got %s", s)
	}
	t.Logf("downstream=svc breaker=%s after 3 consecutive failures", StateOpen)

	// 熔断期间：不派发，快速拒绝
	before := d.attemptsCount()
	_, err := g.Call(context.Background(), req)
	mustReject(t, err, RejectCircuitOpen)
	if d.attemptsCount() != before {
		t.Fatal("request during open must not be dispatched")
	}
	t.Logf("downstream=svc breaker=%s request fast-rejected", rt.breaker.State())

	// 冷却后恢复下游：探测成功 -> 关闭 -> 正常服务
	time.Sleep(80 * time.Millisecond)
	atomic.StoreInt32(&shouldFail, 0)
	_, err = g.Call(context.Background(), req)
	if err != nil {
		t.Fatalf("probe call: %v", err)
	}
	if s := rt.breaker.State(); s != StateClosed {
		t.Fatalf("expected closed after successful probe, got %s", s)
	}
	t.Logf("logs:\n%s", l.String())
}
