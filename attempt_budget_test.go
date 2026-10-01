package gateway

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordedCall 记录一次下游尝试实际观察到的上下文与派发时刻。
type recordedCall struct {
	at       time.Time
	attempt  int
	deadline time.Time
}

// recordingDownstream 记录每次派发的 AttemptContext，并按脚本返回结果。
type recordingDownstream struct {
	mu    sync.Mutex
	name  string
	calls []recordedCall
	// fn 决定每次调用的结果；nil 表示成功。按调用序号取脚本，越界取最后一个。
	results []func(ctx AttemptContext) error
}

func newRecordingDownstream(name string, fns ...func(ctx AttemptContext) error) *recordingDownstream {
	return &recordingDownstream{name: name, results: fns}
}

func (d *recordingDownstream) Name() string { return d.name }

func (d *recordingDownstream) Call(ctx AttemptContext, req Request) (any, error) {
	d.mu.Lock()
	d.calls = append(d.calls, recordedCall{at: time.Now(), attempt: ctx.Attempt, deadline: ctx.Deadline})
	fn := func(AttemptContext) error { return nil }
	if len(d.results) > 0 {
		idx := len(d.calls) - 1
		if idx >= len(d.results) {
			idx = len(d.results) - 1
		}
		if d.results[idx] != nil {
			fn = d.results[idx]
		}
	}
	d.mu.Unlock()
	if err := fn(ctx); err != nil {
		return nil, err
	}
	return "ok", nil
}

func (d *recordingDownstream) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

func (d *recordingDownstream) snapshot() []recordedCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]recordedCall, len(d.calls))
	copy(out, d.calls)
	return out
}

// failRetryable 返回可重试的下游内部错误（脚本中统一包装 errFakeDownstream）。
func failRetryable(_ AttemptContext) error { return errFakeDownstream }

func attemptBudgetConfig(ds Downstream) RouteConfig {
	cfg := testConfig(ds, retryableClassifier)
	cfg.AttemptTimeout = 50 * time.Millisecond
	return cfg
}

// TestAttemptBudgetFirstRejectWhenRemainingTooSmall 首次派发前剩余总预算仍为正、
// 但不足一次单次尝试预算：必须直接拒绝，下游调用 0 次、不占用并发名额。
func TestAttemptBudgetFirstRejectWhenRemainingTooSmall(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("svc-short") // 拒绝发生在派发前，下游结果无关
	cfg := attemptBudgetConfig(ds)
	cfg.MaxConcurrent = 1
	g.RegisterRoute("svc-short", cfg)
	log := attachLogger(t, g)

	req := Request{ID: "a1", Route: "svc-short", Idempotent: true,
		Deadline: time.Now().Add(30 * time.Millisecond)} // 30ms < 50ms 单次预算
	_, err := g.Call(req)

	var rej *RejectError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want *RejectError", err)
	}
	if rej.Reason != ReasonBudgetExhausted {
		t.Fatalf("reason = %q, want budget_exhausted", rej.Reason)
	}
	if rej.Remaining <= 0 || rej.Remaining >= cfg.AttemptTimeout {
		t.Fatalf("remaining = %s, want in (0, %s)", rej.Remaining, cfg.AttemptTimeout)
	}
	if n := ds.count(); n != 0 {
		t.Fatalf("downstream calls = %d, want 0 (must not dispatch)", n)
	}
	ev := log.last()
	if ev.Phase != "reject" || ev.Reason != ReasonBudgetExhausted {
		t.Fatalf("last event = %+v, want reject/budget_exhausted", ev)
	}
	if ev.BudgetText() == "unlimited" || ev.Remaining <= 0 {
		t.Fatalf("budget text = %q remaining=%s, want positive finite", ev.BudgetText(), ev.Remaining)
	}
	// 不占用并发名额：后续一个充足预算的请求必须能正常派发成功。
	okReq := Request{ID: "a1-ok", Route: "svc-short", Idempotent: true,
		Deadline: time.Now().Add(time.Second)}
	resp, err2 := g.Call(okReq)
	if err2 != nil || resp == nil {
		t.Fatalf("follow-up call failed, concurrency slot leaked: err=%v", err2)
	}
	if got := ds.count(); got != 1 {
		t.Fatalf("downstream calls after follow-up = %d, want 1", got)
	}
	if inFlight := g.mustInFlight(t, "svc-short"); inFlight != 0 {
		t.Fatalf("inflight = %d, want 0", inFlight)
	}
}

func (g *Gateway) mustInFlight(t *testing.T, route string) int {
	t.Helper()
	st, ok := g.Status(route)
	if !ok {
		t.Fatalf("route %s missing", route)
	}
	return st.InFlight
}

// TestAttemptBudgetDeadlineExpiredIsDistinct 截止时间早已过去与“剩余为正但不足”
// 必须是两种不同的拒绝语义。
func TestAttemptBudgetDeadlineExpiredIsDistinct(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("svc-exp", failRetryable)
	g.RegisterRoute("svc-exp", attemptBudgetConfig(ds))
	attachLogger(t, g)

	req := Request{ID: "a2", Route: "svc-exp", Idempotent: true,
		Deadline: time.Now().Add(-10 * time.Millisecond)}
	_, err := g.Call(req)

	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonDeadlineExpired {
		t.Fatalf("err = %v, want RejectError(deadline_expired)", err)
	}
	if ds.count() != 0 {
		t.Fatalf("downstream calls = %d, want 0", ds.count())
	}
}

// TestAttemptBudgetRetryStopsWhenRemainingTooSmall 已经派发过一次、可重试失败后，
// 重试链路剩余总预算不足一次单次尝试：停在已发生的尝试数上，终局为
// budget_exhausted，与首次派发前拒绝区分，也不做退避等待。
func TestAttemptBudgetRetryStopsWhenRemainingTooSmall(t *testing.T) {
	g := New()
	// 第一次尝试尊重 ctx、耗时略小于单次预算（50ms）后可重试失败，
	// 把总预算消耗到“为正但不足一次尝试”；后续尝试绝不应被派发。
	slowFail := func(_ AttemptContext) error {
		time.Sleep(30 * time.Millisecond)
		return errFakeDownstream
	}
	ds := newRecordingDownstream("svc-retry-stop", slowFail, failRetryable, failRetryable)
	cfg := attemptBudgetConfig(ds)
	cfg.Threshold = 10            // 熔断阈值高于尝试次数，专注验证预算预检
	cfg.Retry.Backoff = time.Hour // 若错误地进入退避会卡死；预检应直接终局
	g.RegisterRoute("svc-retry-stop", cfg)
	log := attachLogger(t, g) // g.Sleep 被置为空实现，真实退避也不会等待

	deadline := time.Now().Add(70 * time.Millisecond)
	start := time.Now()
	req := Request{ID: "a3", Route: "svc-retry-stop", Idempotent: true, Deadline: deadline}
	_, err := g.Call(req)
	elapsed := time.Since(start)

	var term *TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("err = %v, want *TerminalError", err)
	}
	if term.Kind != TerminalBudgetExhausted {
		t.Fatalf("kind = %q, want budget_exhausted", term.Kind)
	}
	if term.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (stop after the dispatched attempt)", term.Attempts)
	}
	if n := ds.count(); n != 1 {
		t.Fatalf("downstream calls = %d, want exactly 1", n)
	}
	if elapsed < 25*time.Millisecond {
		t.Fatalf("call took %s, expected the slow first attempt (~30ms) to run", elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("call took %s, should stop without long backoff", elapsed)
	}
	// 终局前不应出现 backoff 事件。
	for _, ev := range log.ev {
		if ev.Phase == "backoff" {
			t.Fatalf("unexpected backoff event before budget-exhausted terminal: %+v", ev)
		}
	}
	if ev := log.last(); ev.Phase != "terminal" || ev.Kind != TerminalBudgetExhausted {
		t.Fatalf("last event = %+v, want terminal/budget_exhausted", ev)
	}
}

// TestAttemptBudgetDeadlinePropagatedAndCapped 单次尝试预算必须实际传给下游；
// 每次尝试的截止时间取“单次预算”与“当时剩余总预算”的更小值。
func TestAttemptBudgetDeadlinePropagatedAndCapped(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("svc-cap", failRetryable, failRetryable, failRetryable)
	cfg := attemptBudgetConfig(ds) // 单次预算 50ms
	cfg.Threshold = 10             // 避免熔断先于预算介入
	cfg.Retry.Backoff = 5 * time.Millisecond
	g.RegisterRoute("svc-cap", cfg)
	attachLogger(t, g)

	const total = 200 * time.Millisecond
	reqStart := time.Now()
	req := Request{ID: "a4", Route: "svc-cap", Idempotent: true,
		Deadline: reqStart.Add(total)}
	_, _ = g.Call(req) // 持续可重试失败；重点看每次派发的截止时间

	calls := ds.snapshot()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 dispatched attempts, got %d", len(calls))
	}
	for i, c := range calls {
		if c.deadline.IsZero() {
			t.Fatalf("attempt %d deadline zero, want capped finite deadline", i+1)
		}
		got := c.deadline.Sub(c.at)
		// 派发时刻的剩余总预算（总 200ms，减去真实流逝与退避）。
		remainingAtDispatch := reqStart.Add(total).Sub(c.at)
		want := remainingAtDispatch
		if cfg.AttemptTimeout < want {
			want = cfg.AttemptTimeout // 单次尝试预算封顶
		}
		d := got - want
		if d > 10*time.Millisecond || d < -10*time.Millisecond {
			t.Fatalf("attempt %d effective deadline = %s, want ~%s (min(50ms, remaining %s))",
				i+1, got, want, remainingAtDispatch)
		}
		if got > cfg.AttemptTimeout+10*time.Millisecond {
			t.Fatalf("attempt %d got %s, must not exceed attempt budget %s",
				i+1, got, cfg.AttemptTimeout)
		}
	}
	// 第一次派发时剩余总预算（~200ms）大于单次预算，必须被单次预算封顶在 ~50ms。
	first := calls[0].deadline.Sub(calls[0].at)
	if first < 35*time.Millisecond || first > 65*time.Millisecond {
		t.Fatalf("first attempt deadline = %s, want ~50ms (capped by attempt budget)", first)
	}
}

// TestAttemptBudgetEnforcedByDownstream 下游按传播的截止时间执行：单次尝试预算
// 把一次慢调用限制在单次预算内（这里用慢失败并以预算耗尽终局收尾，断言派发次数
// 与尝试序号递增，且没有任何一次尝试能拿到超过单次预算的时间）。
func TestAttemptBudgetEnforcedByDownstream(t *testing.T) {
	g := New()
	slow := func(_ AttemptContext) error {
		// 模拟尊重 ctx 的下游由 fakeDownstream 承担；这里用 recording 验证派发间隔。
		time.Sleep(45 * time.Millisecond) // 略小于 50ms 单次预算，保证被预算封顶约束
		return errFakeDownstream
	}
	ds := newRecordingDownstream("svc-force", slow, slow, slow)
	cfg := attemptBudgetConfig(ds)
	cfg.Retry.Backoff = time.Millisecond
	g.RegisterRoute("svc-force", cfg)
	attachLogger(t, g)
	g.Sleep = time.Sleep

	req := Request{ID: "a5", Route: "svc-force", Idempotent: true,
		Deadline: time.Now().Add(140 * time.Millisecond)}
	_, err := g.Call(req)

	var term *TerminalError
	if !errors.As(err, &term) || term.Kind != TerminalBudgetExhausted {
		t.Fatalf("err = %v, want TerminalError(budget_exhausted)", err)
	}
	calls := ds.snapshot()
	if len(calls) < 2 {
		t.Fatalf("expected retries before budget exhausted, got %d calls", len(calls))
	}
	for i, c := range calls {
		if d := c.deadline.Sub(c.at); d > cfg.AttemptTimeout+10*time.Millisecond {
			t.Fatalf("attempt %d exceeded attempt budget: %s > %s", i+1, d, cfg.AttemptTimeout)
		}
		if c.attempt != i+1 {
			t.Fatalf("attempt number = %d, want %d", c.attempt, i+1)
		}
	}
}

// TestAttemptBudgetWithoutCallerDeadlineStandsAlone 调用方没给总截止时间时，
// 单次尝试预算单独生效；调用可以成功，重试也正常工作，永远不会被判预算耗尽。
func TestAttemptBudgetWithoutCallerDeadlineStandsAlone(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("svc-standalone", failRetryable, nil)
	cfg := attemptBudgetConfig(ds)
	cfg.Threshold = 10
	g.RegisterRoute("svc-standalone", cfg)
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "a6", Route: "svc-standalone", Idempotent: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (retry works without caller deadline)", resp.Attempts)
	}
	calls := ds.snapshot()
	for i, c := range calls {
		if d := c.deadline.Sub(c.at); d < 35*time.Millisecond || d > 65*time.Millisecond {
			t.Fatalf("attempt %d deadline = %s, want ~50ms attempt budget", i+1, d)
		}
	}
	for _, ev := range log.ev {
		if ev.Limited {
			t.Fatalf("event %s should report unlimited caller budget", ev.Phase)
		}
	}
	if txt := log.ev[0].BudgetText(); txt != "unlimited" {
		t.Fatalf("BudgetText = %q, want unlimited", txt)
	}
	if txt := log.ev[0].AttemptText(); txt == "unlimited" {
		t.Fatalf("AttemptText = %q, want finite attempt budget", txt)
	}
}

// TestUnlimitedCallIsExpressedAsUnlimited 不设限（无调用方截止时间、无单次尝试
// 预算、无默认超时）：对外日志表达为 unlimited，下游拿到零截止时间，且不被任何
// 预算判定拒绝；成功、重试、熔断行为均保持。
func TestUnlimitedCallIsExpressedAsUnlimited(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("svc-unlimited", failRetryable, nil)
	cfg := testConfig(ds, retryableClassifier)
	cfg.DefaultTimeout = 0
	cfg.AttemptTimeout = 0
	g.RegisterRoute("svc-unlimited", cfg)
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "a7", Route: "svc-unlimited", Idempotent: true})
	if err != nil {
		t.Fatalf("unlimited call must not be rejected by budget checks: %v", err)
	}
	if resp.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", resp.Attempts)
	}
	for _, c := range ds.snapshot() {
		if !c.deadline.IsZero() {
			t.Fatalf("downstream deadline = %v, want zero (unlimited)", c.deadline)
		}
	}
	for _, ev := range log.ev {
		if ev.Limited || ev.BudgetText() != "unlimited" {
			t.Fatalf("phase=%s Limited=%v text=%q, want unlimited", ev.Phase, ev.Limited, ev.BudgetText())
		}
	}
	if txt := log.ev[0].AttemptText(); txt != "unlimited" {
		t.Fatalf("AttemptText = %q, want unlimited", txt)
	}
}

// TestUnlimitedRejectErrorText 不设限调用若因其他治理原因（如未知路由）被拒绝，
// RejectError 文本必须如实表达 unlimited，而不是某个具体时长。
func TestUnlimitedRejectErrorText(t *testing.T) {
	g := New()
	log := attachLogger(t, g)
	_, err := g.Call(Request{ID: "a8", Route: "no-such-route"})

	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonUnknownRoute {
		t.Fatalf("err = %v, want RejectError(unknown_route)", err)
	}
	if rej.Limited {
		t.Fatalf("Limited = true, want false for no caller deadline")
	}
	if !strings.Contains(err.Error(), "unlimited") {
		t.Fatalf("error text = %q, want it to state unlimited", err.Error())
	}
	if strings.Contains(err.Error(), "h0m0s") || strings.Contains(err.Error(), "1h0m0s") {
		t.Fatalf("error text leaks a concrete duration: %q", err.Error())
	}
	if txt := log.last().BudgetText(); txt != "unlimited" {
		t.Fatalf("BudgetText = %q, want unlimited", txt)
	}
}

// TestNoAttemptTimeoutKeepsLegacyBehavior 未配置单次尝试预算时，即使剩余总预算
// 很小，也按历史行为照常派发（把剩余预算传播给下游），不做额外预检。
func TestNoAttemptTimeoutKeepsLegacyBehavior(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("svc-legacy")
	cfg := testConfig(ds, retryableClassifier)
	cfg.AttemptTimeout = 0
	g.RegisterRoute("svc-legacy", cfg)
	attachLogger(t, g)

	req := Request{ID: "a9", Route: "svc-legacy", Idempotent: true,
		Deadline: time.Now().Add(20 * time.Millisecond)}
	resp, err := g.Call(req)
	if err != nil {
		t.Fatalf("legacy route with small remaining budget should dispatch: %v", err)
	}
	if resp.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", resp.Attempts)
	}
	calls := ds.snapshot()
	if len(calls) != 1 {
		t.Fatalf("downstream calls = %d, want 1", len(calls))
	}
	if d := calls[0].deadline.Sub(calls[0].at); d > 25*time.Millisecond || d < 0 {
		t.Fatalf("propagated deadline = %s, want ~20ms remaining caller budget", d)
	}
}
