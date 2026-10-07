package gateway

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// runawayDownstream 模拟无视 AttemptContext.Deadline、固定运行 run 时长后
// 可重试失败的失控慢下游，用于复现“单次尝试跑过调用方总时限”的真耗尽场景。
type runawayDownstream struct {
	name  string
	run   time.Duration
	calls *int
	mu    *sync.Mutex
}

func (d *runawayDownstream) Name() string { return d.name }

func (d *runawayDownstream) Call(AttemptContext, Request) (any, error) {
	d.mu.Lock()
	*d.calls++
	d.mu.Unlock()
	time.Sleep(d.run)
	return nil, errFakeDownstream
}

// 本文件覆盖四种与预算/时限有关的互斥结局，断言只凭返回值即可区分：
//
//	1. 还没派发、时限已过          -> *RejectError(deadline_expired)
//	2. 还没派发、不够跑一次        -> *RejectError(budget_exhausted)
//	3. 已派发过、重试时不够跑一次  -> *TerminalError(budget_too_small)
//	4. 已派发过、总预算真正耗光    -> *TerminalError(budget_exhausted)
//
// 同时回归不设限调用与未配置单次尝试预算路由的兼容行为。

// mustBudgetTerminal 断言 err 是指定 Kind、指定尝试次数的 *TerminalError，
// 并验证错误文本里带的 Kind 字符串（即调用方“只看返回值”的区分依据）。
func mustBudgetTerminal(t *testing.T, err error, kind TerminalKind, attempts int) *TerminalError {
	t.Helper()
	var term *TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("err = %v, want *TerminalError(%s)", err, kind)
	}
	if term.Kind != kind {
		t.Fatalf("kind = %q, want %q (return value must distinguish them)", term.Kind, kind)
	}
	if term.Attempts != attempts {
		t.Fatalf("attempts = %d, want %d", term.Attempts, attempts)
	}
	if !strings.Contains(term.Error(), string(kind)) {
		t.Fatalf("error text = %q, want it to contain %q", term.Error(), kind)
	}
	return term
}

// TestBudgetOutcomesAreDistinguishableByReturnValue 是四种结局的总入口，
// 用子测试逐个断言并打印区分依据。
func TestBudgetOutcomesAreDistinguishableByReturnValue(t *testing.T) {
	t.Run("not_dispatched_deadline_expired", testOutcomeNotDispatchedDeadlineExpired)
	t.Run("not_dispatched_budget_too_small", testOutcomeNotDispatchedBudgetTooSmall)
	t.Run("dispatched_retry_budget_too_small", testOutcomeDispatchedRetryBudgetTooSmall)
	t.Run("dispatched_retry_budget_exhausted", testOutcomeDispatchedRetryBudgetExhausted)
	t.Run("dispatched_retry_deadline_expired_after_backoff", testOutcomeDispatchedRetryDeadlineExpiredAfterBackoff)
}

// testOutcomeDispatchedRetryDeadlineExpiredAfterBackoff 已派发过一次，失败可重试，
// 且尝试返回后预算仍够再试一次（总预算 12ms、单次预算 5ms、首次耗时 2ms，
// 尝试后还剩 ~10ms ≥ 5ms）；但退避被截断为全部剩余预算（配 20ms），睡到期后
// 调用方总时限见底（剩余 ≤ 0）。终局必须是 budget_exhausted（而不是
// budget_too_small），下游只调用 1 次。这锁住“重试途中调用方截止时间过去”
// 与“重试时只是不够一次”的区分。
func testOutcomeDispatchedRetryDeadlineExpiredAfterBackoff(t *testing.T) {
	g := New()
	ds := newFakeDownstream("out-expired-after-backoff",
		fakeCall{delay: 2 * time.Millisecond, fail: true},
		fakeCall{fail: true},
	)
	cfg := testConfig(ds, retryableClassifier)
	cfg.AttemptTimeout = 5 * time.Millisecond // 首次剩余 12ms >= 5ms，可派发；尝试后 ~10ms 仍够再试
	cfg.Threshold = 10
	cfg.Retry.Backoff = 20 * time.Millisecond // 远大于剩余，会被截断为全部剩余预算并睡到期
	g.RegisterRoute("out-expired-after-backoff", cfg)
	log := attachLogger(t, g)
	g.Sleep = time.Sleep // 真实退避，让总时限在退避中到期

	req := Request{ID: "o7", Route: "out-expired-after-backoff", Idempotent: true,
		Deadline: time.Now().Add(12 * time.Millisecond)}
	_, err := g.Call(req)

	term := mustBudgetTerminal(t, err, TerminalBudgetExhausted, 1)
	if term.Remaining > 0 {
		t.Fatalf("remaining = %s, want <= 0 after backoff consumed the deadline", term.Remaining)
	}
	if n := ds.gotCalls(); n != 1 {
		t.Fatalf("downstream calls = %d, want 1 (no dispatch after deadline)", n)
	}
	phases := log.phases()
	hasBackoff := false
	for _, p := range phases {
		if p == "backoff" {
			hasBackoff = true
		}
	}
	if !hasBackoff {
		t.Fatalf("phases = %v, want a backoff event that consumes the deadline", phases)
	}
	if ev := log.last(); ev.Phase != "terminal" || ev.Kind != TerminalBudgetExhausted {
		t.Fatalf("last event = %+v, want terminal/budget_exhausted", ev)
	}
	t.Logf("outcome=dispatched_retry_deadline_expired_after_backoff kind=%s remaining=%s attempts=1",
		term.Kind, term.Remaining)
}

// 结局一：还没派发，调用方截止时间早就过去了。
// 必须是 *RejectError(deadline_expired)，下游 0 次，与“剩余为正但不够一次”分开。
func testOutcomeNotDispatchedDeadlineExpired(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("out-expired-before-dispatch", failRetryable)
	cfg := attemptBudgetConfig(ds) // AttemptTimeout=50ms
	cfg.Threshold = 10
	g.RegisterRoute("out-expired-before-dispatch", cfg)
	log := attachLogger(t, g)

	req := Request{ID: "o1", Route: "out-expired-before-dispatch", Idempotent: true,
		Deadline: time.Now().Add(-5 * time.Millisecond)}
	_, err := g.Call(req)

	var rej *RejectError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want *RejectError", err)
	}
	if rej.Reason != ReasonDeadlineExpired {
		t.Fatalf("reason = %q, want deadline_expired", rej.Reason)
	}
	if !rej.Limited || rej.Remaining > 0 {
		t.Fatalf("Limited=%v remaining=%s, want finite and <= 0", rej.Limited, rej.Remaining)
	}
	if n := ds.count(); n != 0 {
		t.Fatalf("downstream calls = %d, want 0", n)
	}
	if ev := log.last(); ev.Phase != "reject" || ev.Reason != ReasonDeadlineExpired {
		t.Fatalf("last event = %+v, want reject/deadline_expired", ev)
	}
	t.Logf("outcome=not_dispatched_deadline_expired reason=%s remaining=%s calls=0",
		rej.Reason, rej.Remaining)
}

// 结局二：还没派发，剩余总预算为正但不够再跑一次配置好的单次尝试。
// 必须是 *RejectError(budget_exhausted)，与结局一的 deadline_expired 分开。
func testOutcomeNotDispatchedBudgetTooSmall(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("out-toosmall-before-dispatch") // 默认成功；拒绝发生在派发前
	cfg := attemptBudgetConfig(ds)                               // AttemptTimeout=50ms
	cfg.Threshold = 10
	cfg.MaxConcurrent = 1
	g.RegisterRoute("out-toosmall-before-dispatch", cfg)
	log := attachLogger(t, g)

	req := Request{ID: "o2", Route: "out-toosmall-before-dispatch", Idempotent: true,
		Deadline: time.Now().Add(30 * time.Millisecond)} // 30ms > 0 但 < 50ms
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
		t.Fatalf("downstream calls = %d, want 0 (pre-dispatch reject)", n)
	}
	if ev := log.last(); ev.Phase != "reject" || ev.Reason != ReasonBudgetExhausted {
		t.Fatalf("last event = %+v, want reject/budget_exhausted", ev)
	}
	// 不占并发名额：紧随其后的充足预算请求必须成功。
	resp, err2 := g.Call(Request{ID: "o2-ok", Route: "out-toosmall-before-dispatch",
		Idempotent: true, Deadline: time.Now().Add(time.Second)})
	if err2 != nil || resp == nil {
		t.Fatalf("follow-up call failed, slot leaked: %v", err2)
	}
	if n := ds.count(); n != 1 {
		t.Fatalf("downstream calls = %d, want 1 after follow-up", n)
	}
	t.Logf("outcome=not_dispatched_budget_too_small reason=%s remaining=%s calls=0",
		rej.Reason, rej.Remaining)
}

// 结局三：已经派发过一次（可重试失败），进入重试时总预算仍为正、
// 但已不足再完整跑一次单次尝试。必须是 *TerminalError(budget_too_small)，
// Attempts=1，下游恰好被调用 1 次，且不发生退避等待。
func testOutcomeDispatchedRetryBudgetTooSmall(t *testing.T) {
	g := New()
	// 第一次尝试耗时 ~30ms（< 单次预算 50ms）后可重试失败；总预算 70ms，
	// 尝试返回后剩余 ~40ms：为正但小于 50ms，绝不能再派发第二次。
	slowFail := func(AttemptContext) error {
		time.Sleep(30 * time.Millisecond)
		return errFakeDownstream
	}
	ds := newRecordingDownstream("out-toosmall-on-retry", slowFail, failRetryable, failRetryable)
	cfg := attemptBudgetConfig(ds) // AttemptTimeout=50ms，分类器判 errFakeDownstream 可重试
	cfg.Threshold = 10
	cfg.Retry.Backoff = time.Hour // 正确行为应根本不产生 backoff 事件（Sleep 已注入为空实现）
	g.RegisterRoute("out-toosmall-on-retry", cfg)
	log := attachLogger(t, g)

	start := time.Now()
	req := Request{ID: "o3", Route: "out-toosmall-on-retry", Idempotent: true,
		Deadline: start.Add(70 * time.Millisecond)}
	_, err := g.Call(req)
	elapsed := time.Since(start)

	term := mustBudgetTerminal(t, err, TerminalBudgetTooSmall, 1)
	if term.Remaining <= 0 {
		t.Fatalf("remaining = %s, want strictly positive (this is budget_too_small)", term.Remaining)
	}
	if term.Remaining >= cfg.AttemptTimeout {
		t.Fatalf("remaining = %s, want < attempt budget %s", term.Remaining, cfg.AttemptTimeout)
	}
	if !term.Limited || term.AttemptBudget != cfg.AttemptTimeout {
		t.Fatalf("Limited=%v attemptBudget=%s, want finite/%s", term.Limited, term.AttemptBudget, cfg.AttemptTimeout)
	}
	if n := ds.count(); n != 1 {
		t.Fatalf("downstream calls = %d, want exactly 1", n)
	}
	if elapsed < 25*time.Millisecond {
		t.Fatalf("elapsed = %s, first attempt (~30ms) must have run", elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("elapsed = %s, must not wait for backoff", elapsed)
	}
	for _, ev := range log.ev {
		if ev.Phase == "backoff" {
			t.Fatalf("unexpected backoff event: %+v", ev)
		}
	}
	if ev := log.last(); ev.Phase != "terminal" || ev.Kind != TerminalBudgetTooSmall {
		t.Fatalf("last event = %+v, want terminal/budget_too_small", ev)
	}
	t.Logf("outcome=dispatched_retry_budget_too_small kind=%s remaining=%s attemptBudget=%s attempts=1 elapsed=%s",
		term.Kind, term.Remaining, term.AttemptBudget, elapsed)
}

// 结局四：已经派发过一次，下游尊重传播的截止时间并一直跑到调用方总时限见底
// （剩余 ≤ 0）。必须是 *TerminalError(budget_exhausted)，Attempts=1；
// 与结局三的 budget_too_small 只凭 Kind 即可区分，且 Remaining ≤ 0。
func testOutcomeDispatchedRetryBudgetExhausted(t *testing.T) {
	g := New()
	// 模拟一个“无视传播截止时间、实际跑过调用方总时限”的失控慢下游：
	// 首次派发时剩余 30ms >= 单次预算 20ms，可以派发；但下游固定睡 40ms
	// （超过调用方 30ms 总时限）才返回。返回时总预算已 ≤ 0，网关必须以
	// budget_exhausted 终局，不再派发，也不退避。这与“尊重截止、在尝试预算
	// 内返回、剩余仍为正”的 budget_too_small 形成直接对照。
	var dsCalls int
	var dsMu sync.Mutex
	ds := runawayDownstream{name: "out-exhausted-on-retry", run: 40 * time.Millisecond,
		calls: &dsCalls, mu: &dsMu}
	alwaysRetryable := func(error) FailureClass { return FailureRetryable }
	cfg := testConfig(&ds, alwaysRetryable)
	cfg.AttemptTimeout = 20 * time.Millisecond // 首次剩余 30ms >= 20ms，可以派发
	cfg.Threshold = 10
	cfg.Retry.Backoff = time.Millisecond
	g.RegisterRoute("out-exhausted-on-retry", cfg)
	log := attachLogger(t, g)
	g.Sleep = time.Sleep // 保留真实退避（正确行为不应走到退避）

	req := Request{ID: "o4", Route: "out-exhausted-on-retry", Idempotent: true,
		Deadline: time.Now().Add(30 * time.Millisecond)}
	_, err := g.Call(req)

	term := mustBudgetTerminal(t, err, TerminalBudgetExhausted, 1)
	if term.Remaining > 0 {
		t.Fatalf("remaining = %s, want <= 0 (caller deadline truly reached)", term.Remaining)
	}
	if !term.Limited {
		t.Fatalf("Limited = false, want true")
	}
	dsMu.Lock()
	got := dsCalls
	dsMu.Unlock()
	if got != 1 {
		t.Fatalf("downstream calls = %d, want exactly 1 (no retry after exhaustion)", got)
	}
	for _, ev := range log.ev {
		if ev.Phase == "backoff" {
			t.Fatalf("unexpected backoff event when total budget is gone: %+v", ev)
		}
	}
	if ev := log.last(); ev.Phase != "terminal" || ev.Kind != TerminalBudgetExhausted {
		t.Fatalf("last event = %+v, want terminal/budget_exhausted", ev)
	}
	t.Logf("outcome=dispatched_retry_budget_exhausted kind=%s remaining=%s attempts=1 calls=1",
		term.Kind, term.Remaining)
}

// TestBudgetOutcomeCompatibility 回归兼容范围：不设限调用与未配置单次尝试
// 预算的路由既不能被误拒，也不能被误判成预算耗尽。
func TestBudgetOutcomeCompatibility(t *testing.T) {
	t.Run("unlimited_caller_never_budget_terminal", testCompatUnlimitedNeverBudget)
	t.Run("no_attempt_timeout_keeps_legacy_dispatch", testCompatNoAttemptTimeout)
}

// testCompatUnlimitedNeverBudget 调用方没给总截止时间：即使配置了单次尝试预算，
// 也绝不能产生 budget_exhausted / budget_too_small 类终局或拒绝，成功与重试
// 行为与历史版本一致。
func testCompatUnlimitedNeverBudget(t *testing.T) {
	g := New()
	// 第一次可重试失败、第二次成功：无调用方截止时间时必须正常重试成功。
	ds := newRecordingDownstream("out-compat-unlimited", failRetryable, nil)
	cfg := attemptBudgetConfig(ds)
	cfg.Threshold = 10
	g.RegisterRoute("out-compat-unlimited", cfg)
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "o5", Route: "out-compat-unlimited", Idempotent: true})
	if err != nil {
		t.Fatalf("unlimited caller must never get a budget terminal: %v", err)
	}
	if resp.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", resp.Attempts)
	}
	for _, ev := range log.ev {
		if ev.Limited {
			t.Fatalf("phase=%s reported Limited=true, want unlimited", ev.Phase)
		}
		if ev.Kind == TerminalBudgetExhausted || ev.Kind == TerminalBudgetTooSmall {
			t.Fatalf("unlimited call got budget terminal kind=%q", ev.Kind)
		}
		if ev.Reason == ReasonBudgetExhausted || ev.Reason == ReasonDeadlineExpired {
			t.Fatalf("unlimited call got budget reject reason=%q", ev.Reason)
		}
	}
	t.Logf("outcome=compat_unlimited attempts=2 calls=%d budget=unlimited (no budget reject/terminal)",
		ds.count())
}

// testCompatNoAttemptTimeout 路由未配置 AttemptTimeout 时，即使剩余总预算很小，
// 也按历史行为照常派发（把剩余预算传播给下游），既不预检拒绝也不产生
// budget_too_small 终局。
func testCompatNoAttemptTimeout(t *testing.T) {
	g := New()
	ds := newRecordingDownstream("out-compat-no-budget")
	cfg := testConfig(ds, retryableClassifier)
	cfg.AttemptTimeout = 0
	g.RegisterRoute("out-compat-no-budget", cfg)
	attachLogger(t, g)

	req := Request{ID: "o6", Route: "out-compat-no-budget", Idempotent: true,
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
	t.Logf("outcome=compat_no_attempt_timeout attempts=1 calls=1 dispatched_with_remaining_budget")
}
