package gateway

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestGatewayConcurrencyLimitFastReject MaxConcurrent 已满时，
// 额外请求必须以 concurrency_limit 在派发前被拒绝。
func TestGatewayConcurrencyLimitFastReject(t *testing.T) {
	g := New()
	block := make(chan struct{})
	ds := newFakeDownstream("svc-conc", fakeCall{release: block})
	cfg := testConfig(ds, nonRetryableClassifier)
	cfg.MaxConcurrent = 2
	cfg.Threshold = 100
	cfg.Retry.MaxAttempts = 1
	g.RegisterRoute("svc-conc", cfg)
	attachLogger(t, g)

	var wg sync.WaitGroup
	errs := make([]error, 4)
	start := make(chan struct{})
	// 先占满 2 个槽位（阻塞在下游内）。
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = g.Call(Request{ID: "inflight", Route: "svc-conc"})
		}(i)
	}
	close(start)
	// 等待 2 个在途请求进入下游。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := g.Status("svc-conc"); st.InFlight == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if st, _ := g.Status("svc-conc"); st.InFlight != 2 {
		close(block)
		t.Fatalf("inflight = %d, want 2", st.InFlight)
	}

	// 额外 2 个请求必须被并发上限快速拒绝。
	for i := 2; i < 4; i++ {
		_, errs[i] = g.Call(Request{ID: "overflow", Route: "svc-conc"})
		var rej *RejectError
		if !errors.As(errs[i], &rej) || rej.Reason != ReasonConcurrencyLimit {
			t.Fatalf("overflow request %d err = %v, want concurrency_limit", i, errs[i])
		}
	}

	close(block)
	wg.Wait()
	for i := 0; i < 2; i++ {
		if errs[i] != nil {
			t.Fatalf("inflight request %d failed: %v", i, errs[i])
		}
	}
	// 槽位释放后可以再次获取。
	if _, err := g.Call(Request{ID: "after", Route: "svc-conc"}); err != nil {
		t.Fatalf("post-release call failed: %v", err)
	}
	if st, _ := g.Status("svc-conc"); st.InFlight != 0 {
		t.Fatalf("inflight after completion = %d, want 0", st.InFlight)
	}
}

// TestGatewayHalfOpenProbeRespectsBulkhead 半开探测同样受并发隔离舱约束：
// 探测名额（这里为 2）不得超过隔离舱容量（这里也为 2），普通流量不得超额。
func TestGatewayHalfOpenProbeRespectsBulkhead(t *testing.T) {
	g := New()
	block := make(chan struct{})
	// 前两次调用失败触发熔断（threshold=2），之后调用阻塞在 release 上。
	ds := newFakeDownstream("svc-halfconc",
		fakeCall{fail: true}, fakeCall{fail: true},
		fakeCall{release: block},
	)
	cfg := testConfig(ds, nonRetryableClassifier)
	cfg.Threshold = 2
	cfg.ProbeMaxConcurrent = 2
	cfg.MaxConcurrent = 2
	cfg.OpenCooldown = 25 * time.Millisecond
	cfg.Retry.MaxAttempts = 1
	g.RegisterRoute("svc-halfconc", cfg)
	attachLogger(t, g)

	_, _ = g.Call(Request{ID: "t1", Route: "svc-halfconc"})
	_, _ = g.Call(Request{ID: "t2", Route: "svc-halfconc"})
	if st, _ := g.Status("svc-halfconc"); st.Breaker != stateOpen {
		t.Fatalf("want OPEN, got %s", st.Breaker)
	}
	time.Sleep(30 * time.Millisecond)

	// 并发发起 4 个请求：半开最多 2 个成为探测进入下游，其余必须被
	// 派发前拒绝（探测名额满 probe_concurrency_limit，或隔离舱已满
	// concurrency_limit——竞争决定具体原因，二者语义都正确）。
	var wg sync.WaitGroup
	var mu sync.Mutex
	var allowed int
	reasons := map[RejectReason]int{}
	startGate := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			_, err := g.Call(Request{ID: "probe-burst", Route: "svc-halfconc"})
			if err == nil {
				mu.Lock()
				allowed++
				mu.Unlock()
				return
			}
			var rej *RejectError
			if errors.As(err, &rej) {
				mu.Lock()
				reasons[rej.Reason]++
				mu.Unlock()
			}
		}()
	}
	close(startGate)

	// 等待最多 2 个探测进入下游。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := g.Status("svc-halfconc"); st.InFlight == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if st, _ := g.Status("svc-halfconc"); st.InFlight > 2 {
		t.Fatalf("inflight = %d, probe breached concurrency limit", st.InFlight)
	}

	close(block)
	wg.Wait()
	mu.Lock()
	totalRejected := 0
	for r, n := range reasons {
		totalRejected += n
		switch r {
		case ReasonProbeLimit, ReasonConcurrencyLimit:
		default:
			t.Errorf("unexpected rejection reason %q", r)
		}
	}
	gotAllowed := allowed
	mu.Unlock()
	if gotAllowed+totalRejected != 4 {
		t.Fatalf("accounting: allowed=%d reasons=%v", gotAllowed, reasons)
	}
	if gotAllowed != 2 {
		t.Fatalf("allowed probes = %d, want exactly 2", gotAllowed)
	}
	if totalRejected != 2 {
		t.Fatalf("rejected = %d (%v), want 2 surplus requests rejected pre-dispatch", totalRejected, reasons)
	}
}
