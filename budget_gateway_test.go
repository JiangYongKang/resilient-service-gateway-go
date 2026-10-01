package gateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recordingDownstream 记录每次尝试收到的 AttemptContext，并可脚本化
// 前 failFirst 次失败（可重试），之后成功。
type recordingDownstream struct {
	mu        sync.Mutex
	name      string
	failFirst int
	calls     int
	ctxs      []AttemptContext
}

func (r *recordingDownstream) Name() string { return r.name }

func (r *recordingDownstream) Call(ctx AttemptContext, req Request) (any, error) {
	r.mu.Lock()
	idx := r.calls
	r.calls++
	r.ctxs = append(r.ctxs, ctx)
	fail := idx < r.failFirst
	r.mu.Unlock()
	if fail {
		return nil, fmt.Errorf("%w: attempt=%d", errFakeDownstream, ctx.Attempt)
	}
	return "ok", nil
}

func (r *recordingDownstream) gotCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingDownstream) contexts() []AttemptContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AttemptContext, len(r.ctxs))
	copy(out, r.ctxs)
	return out
}

func budgetRouteCfg(ds Downstream) RouteConfig {
	return RouteConfig{
		Downstream:         ds,
		Threshold:          10,
		OpenCooldown:       40 * time.Millisecond,
		ProbeMaxConcurrent: 1,
		MaxConcurrent:      4,
		AttemptBudget:      200 * time.Millisecond,
		Retry: RetryConfig{
			MaxAttempts: 3,
			Backoff:     time.Millisecond,
		},
		Classifier: retryableClassifier,
	}
}

// TestFirstAttemptRejectedWhenRemainingShorterThanBudget 首次派发前剩余总预算
// 仍为正数但不够一次 AttemptBudget：直接以 RejectError(budget_exhausted)
// 拒绝，下游调用次数必须为 0，也不占用并发名额。
func TestFirstAttemptRejectedWhenRemainingShorterThanBudget(t *testing.T) {
	g := New()
	ds := new(recordingDownstream)
	g.RegisterRoute("svc-ab", budgetRouteCfg(ds))
	log := attachLogger(t, g)

	req := Request{ID: "b1", Route: "svc-ab", Idempotent: true,
		Deadline: time.Now().Add(50 * time.Millisecond)} // < AttemptBudget 200ms
	_, err := g.Call(req)

	var rej *RejectError
	if !errors.As(err, &rej) || rej.Reason != ReasonBudgetExhausted {
		t.Fatalf("err = %v, want RejectError(budget_exhausted)", err)
	}
	if ds.gotCalls() != 0 {
		t.Fatalf("downstream calls = %d, want 0", ds.gotCalls())
	}
	if st, _ := g.Status("svc-ab"); st.InFlight != 0 {
		t.Fatalf("inflight = %d, want 0 (must not occupy a slot)", st.InFlight)
	}
	if log.last().Phase != "reject" || log.last().Reason != ReasonBudgetExhausted {
		t.Fatalf("unexpected last event: %+v", log.last())
	}
}

// TestBudgetExhaustedRejectDiffersFromDeadlineExpired 同样是派发前拒绝，
// “剩余为正但不够一次尝试”与“截止时间早已过去”必须能被调用方区分。
func TestBudgetExhaustedRejectDiffersFromDeadlineExpired(t *testing.T) {
	g := New()
	ds := new(recordingDownstream)
	cfg := budgetRouteCfg(ds)
	g.RegisterRoute("svc-dist", cfg)
	attachLogger(t, g)

	_, errTooShort := g.Call(Request{ID: "short", Route: "svc-dist", Idempotent: true,
		Deadline: time.Now().Add(50 * time.Millisecond)})
	var r1 *RejectError
	if !errors.As(errTooShort, &r1) || r1.Reason != ReasonBudgetExhausted {
		t.Fatalf("err = %v, want budget_exhausted", errTooShort)
	}

	_, errExpired := g.Call(Request{ID: "past", Route: "svc-dist", Idempotent: true,
		Deadline: time.Now().Add(-time.Second)})
	var r2 *RejectError
	if !errors.As(errExpired, &r2) || r2.Reason != ReasonDeadlineExpired {
		t.Fatalf("err = %v, want deadline_expired", errExpired)
	}

	if errors.Is(errTooShort, errExpired) || errTooShort.Error() == errExpired.Error() {
		t.Fatalf("the two rejections must be distinguishable: %v vs %v", errTooShort, errExpired)
	}
}

// TestRetryStopsWithBudgetInsufficient 重试链路上，一次尝试结束后剩余总预算
// 仍为正数但已不够下一次 AttemptBudget：停在这里不再派发，调用方拿到
// TerminalError(budget_insufficient)，且与“预算彻底耗尽”相区分。
func TestRetryStopsWithBudgetInsufficient(t *testing.T) {
	g := New()
	// 每次尝试耗时 100ms 后以可重试错误失败。
	ds := newFakeDownstream("svc-ins", fakeCall{delay: 100 * time.Millisecond, fail: true})
	cfg := budgetRouteCfg(ds)
	cfg.Retry.Backoff = 0
	g.RegisterRoute("svc-ins", cfg)
	log := attachLogger(t, g)
	g.Sleep = time.Sleep // 让下游耗时真实流逝

	// 总预算 350ms、单次预算 200ms：
	// 第 1 次预检 350>=200 派发，结束剩约 250ms；
	// 第 2 次预检 250>=200 派发，结束剩约 150ms（为正）；
	// 第 3 次预检 150<200 -> budget_insufficient，不再派发。
	req := Request{ID: "b2", Route: "svc-ins", Idempotent: true,
		Deadline: time.Now().Add(350 * time.Millisecond)}
	_, err := g.Call(req)

	var term *TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("err = %v, want *TerminalError", err)
	}
	if term.Kind != TerminalBudgetInsufficient {
		t.Fatalf("kind = %s, want budget_insufficient", term.Kind)
	}
	if n := ds.gotCalls(); n != 2 {
		t.Fatalf("downstream calls = %d, want exactly 2 (stop before 3rd)", n)
	}
	if term.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", term.Attempts)
	}
	last := log.last()
	if last.Phase != "terminal" || last.Kind != TerminalBudgetInsufficient {
		t.Fatalf("unexpected last event: %+v", last)
	}
}

// TestRetryBudgetInsufficientDiffersFromBudgetExhausted 重试途中：
// “剩余为正但不足一次尝试”与“尝试把总预算吃到 <=0”两种终局必须可区分。
func TestRetryBudgetInsufficientDiffersFromBudgetExhausted(t *testing.T) {
	// 场景 A：剩余为正但不足单次预算（每次尝试真实耗时 100ms）。
	gA := New()
	dsA := newFakeDownstream("A", fakeCall{delay: 100 * time.Millisecond, fail: true})
	cfgA := budgetRouteCfg(dsA)
	cfgA.Retry.Backoff = 0
	gA.RegisterRoute("A", cfgA)
	attachLogger(t, gA)
	gA.Sleep = time.Sleep
	_, errA := gA.Call(Request{ID: "a", Route: "A", Idempotent: true,
		Deadline: time.Now().Add(350 * time.Millisecond)})
	var tA *TerminalError
	if !errors.As(errA, &tA) || tA.Kind != TerminalBudgetInsufficient {
		t.Fatalf("A err = %v, want budget_insufficient", errA)
	}
	if n := dsA.gotCalls(); n != 2 {
		t.Fatalf("A downstream calls = %d, want 2", n)
	}

	// 场景 B：首次快速失败（可重试），退避被剩余预算截断并一直睡到总截止
	// 时间；第 2 次预检时剩余 <= 0 -> budget_exhausted（与 A 的“为正但不足”区分）。
	gB := New()
	dsB := newFakeDownstream("B", fakeCall{fail: true})
	cfgB := budgetRouteCfg(dsB)
	cfgB.Retry.Backoff = 300 * time.Millisecond // 超过总预算，会被截断为剩余值
	gB.RegisterRoute("B", cfgB)
	attachLogger(t, gB)
	gB.Sleep = time.Sleep
	_, errB := gB.Call(Request{ID: "b", Route: "B", Idempotent: true,
		Deadline: time.Now().Add(250 * time.Millisecond)})
	var tB *TerminalError
	if !errors.As(errB, &tB) || tB.Kind != TerminalBudgetExhausted {
		t.Fatalf("B err = %v, want budget_exhausted", errB)
	}
	if n := dsB.gotCalls(); n != 1 {
		t.Fatalf("B downstream calls = %d, want 1", n)
	}
	if tA.Kind == tB.Kind {
		t.Fatalf("two terminal paths must differ, both %s", tA.Kind)
	}
}

// TestAttemptBudgetPassedToDownstreamWithoutCallerDeadline 调用方没给总截止
// 时间时，单次尝试预算单独生效：下游看到的截止时间约为 now+AttemptBudget。
func TestAttemptBudgetPassedToDownstreamWithoutCallerDeadline(t *testing.T) {
	g := New()
	ds := &recordingDownstream{name: "svc-cap", failFirst: 0}
	cfg := budgetRouteCfg(ds) // AttemptBudget = 200ms
	g.RegisterRoute("svc-cap", cfg)
	attachLogger(t, g)

	start := time.Now()
	resp, err := g.Call(Request{ID: "c1", Route: "svc-cap", Idempotent: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", resp.Attempts)
	}
	ctxs := ds.contexts()
	if len(ctxs) != 1 {
		t.Fatalf("recorded contexts = %d, want 1", len(ctxs))
	}
	want := start.Add(200 * time.Millisecond)
	if d := ctxs[0].Deadline.Sub(want); d > 20*time.Millisecond || d < -20*time.Millisecond {
		t.Fatalf("downstream deadline = %v, want ~%v (now+200ms)", ctxs[0].Deadline, want)
	}
}

// TestEachAttemptRecomputedMinBudget 有调用方总截止时间时，每次尝试的截止
// 时间都是 min(now+AttemptBudget, 调用方总截止时间)，且逐次重算、逐次逼近
// 总截止时间，永远不超过它。
func TestEachAttemptRecomputedMinBudget(t *testing.T) {
	g := New()
	// 前两次快速失败，第三次成功：捕获三次 AttemptContext。
	ds := &recordingDownstream{name: "svc-min", failFirst: 2}
	cfg := budgetRouteCfg(ds) // AttemptBudget = 200ms
	cfg.Retry.Backoff = 30 * time.Millisecond
	g.RegisterRoute("svc-min", cfg)
	log := attachLogger(t, g)

	deadline := time.Now().Add(3 * time.Second) // 总预算远大于单次预算
	resp, err := g.Call(Request{ID: "c2", Route: "svc-min", Idempotent: true, Deadline: deadline})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", resp.Attempts)
	}
	ctxs := ds.contexts()
	if len(ctxs) != 3 {
		t.Fatalf("recorded contexts = %d, want 3", len(ctxs))
	}
	for i, c := range ctxs {
		if c.Attempt != i+1 {
			t.Fatalf("ctx[%d].Attempt = %d, want %d", i, c.Attempt, i+1)
		}
		if c.Deadline.After(deadline) {
			t.Fatalf("attempt %d deadline %v exceeds caller deadline %v", i+1, c.Deadline, deadline)
		}
		// 总预算 3s 远大于单次预算 200ms，取小结果必须是 now+200ms，
		// 而不是调用方总截止时间。
		d := c.Deadline.Sub(time.Now())
		if d > 220*time.Millisecond || d < 120*time.Millisecond {
			t.Fatalf("attempt %d effective budget = %v, want ~200ms (min)", i+1, d)
		}
	}
	// 逐次重算：后一次的截止时间应晚于前一次（时间在流逝），而不是固定值。
	if !ctxs[1].Deadline.After(ctxs[0].Deadline) || !ctxs[2].Deadline.After(ctxs[1].Deadline) {
		t.Fatalf("deadlines not recomputed per attempt: %v %v %v",
			ctxs[0].Deadline, ctxs[1].Deadline, ctxs[2].Deadline)
	}
	if log.last().Phase != "success" {
		t.Fatalf("last phase = %q, want success", log.last().Phase)
	}
}

// TestAttemptMinCapsToCallerDeadline 当剩余总预算小于单次预算但路由未配置……
// （配置了单次预算时不会派发，见 budget_exhausted 用例；）这里验证一个边界：
// 未配置单次预算、仅有调用方截止时间时，下游截止时间原样等于调用方截止时间。
func TestAttemptMinCapsToCallerDeadline(t *testing.T) {
	g := New()
	ds := &recordingDownstream{name: "svc-cap2"}
	cfg := budgetRouteCfg(ds)
	cfg.AttemptBudget = 0 // 未配置单次预算
	g.RegisterRoute("svc-cap2", cfg)
	attachLogger(t, g)

	deadline := time.Now().Add(400 * time.Millisecond)
	if _, err := g.Call(Request{ID: "c3", Route: "svc-cap2", Idempotent: true, Deadline: deadline}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctxs := ds.contexts()
	if len(ctxs) != 1 || !ctxs[0].Deadline.Equal(deadline) {
		t.Fatalf("downstream deadline = %v, want caller deadline %v", ctxs, deadline)
	}
}

// TestUnlimitedCallExpressedAndNeverBudgetRejected 调用方没给总截止时间、
// 路由也没配 AttemptBudget/DefaultTimeout 时属于不设限：对外日志必须如实
// 表达为 unlimited（不得显示成某个具体时长），且不被预算判定误拒或误判耗尽。
func TestUnlimitedCallExpressedAndNeverBudgetRejected(t *testing.T) {
	g := New()
	ds := &recordingDownstream{name: "svc-unl"} // 直接成功
	cfg := budgetRouteCfg(ds)
	cfg.AttemptBudget = 0
	cfg.DefaultTimeout = 0
	g.RegisterRoute("svc-unl", cfg)
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "u1", Route: "svc-unl", Idempotent: true})
	if err != nil {
		t.Fatalf("unlimited call must not be rejected: %v", err)
	}
	if resp.Attempts != 1 || ds.gotCalls() != 1 {
		t.Fatalf("attempts=%d calls=%d, want 1/1", resp.Attempts, ds.gotCalls())
	}
	// 下游收到的必须是零截止时间（不设限），而不是某个伪造的短预算。
	ctxs := ds.contexts()
	if len(ctxs) != 1 || !ctxs[0].Deadline.IsZero() {
		t.Fatalf("downstream deadline = %v, want zero (unlimited)", ctxs)
	}
	for _, ev := range log.ev {
		if !ev.Unlimited {
			t.Fatalf("event phase=%s Unlimited=false, want true", ev.Phase)
		}
	}
}

// TestUnlimitedLogRendersAsText 通过默认日志渲染确认“不设限”输出为
// "unlimited"，而不是具体时长。
func TestUnlimitedLogRendersAsText(t *testing.T) {
	if got := formatBudget(0, false); got != "unlimited" {
		t.Fatalf("render = %q, want unlimited", got)
	}
	ev := LogEvent{Phase: "dispatch", Unlimited: true}
	// 模拟默认日志格式中的预算字段。
	rendered := formatBudget(ev.Remaining, !ev.Unlimited)
	if rendered != "unlimited" {
		t.Fatalf("unlimited event rendered as %q", rendered)
	}
}

// TestUnlimitedRetriesSucceedAndRemainUnlimited 不设限调用上的重试链路同样
// 不受预算判定影响：连续可重试失败后成功，三次都应派发，且每次下游截止时间
// 都是零值，终局日志仍是不设限。
func TestUnlimitedRetriesSucceedAndRemainUnlimited(t *testing.T) {
	g := New()
	ds := &recordingDownstream{name: "svc-unl-r", failFirst: 2}
	cfg := budgetRouteCfg(ds)
	cfg.AttemptBudget = 0
	cfg.DefaultTimeout = 0
	cfg.Retry.Backoff = 0
	g.RegisterRoute("svc-unl-r", cfg)
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "u2", Route: "svc-unl-r", Idempotent: true})
	if err != nil {
		t.Fatalf("unlimited retries must not be budget-rejected: %v", err)
	}
	if resp.Attempts != 3 || ds.gotCalls() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3/3", resp.Attempts, ds.gotCalls())
	}
	for i, c := range ds.contexts() {
		if !c.Deadline.IsZero() {
			t.Fatalf("attempt %d deadline = %v, want zero (unlimited)", i+1, c.Deadline)
		}
	}
	for _, ev := range log.ev {
		if !ev.Unlimited {
			t.Fatalf("event phase=%s Unlimited=false during unlimited retry", ev.Phase)
		}
	}
}

// TestNoAttemptBudgetKeepsLegacyBehavior 未配置 AttemptBudget 时行为保持与
// 历史一致：即使调用方剩余预算很短（为正），也照常派发并把总截止时间传播
// 下去，不报 budget_exhausted。
func TestNoAttemptBudgetKeepsLegacyBehavior(t *testing.T) {
	g := New()
	ds := &recordingDownstream{name: "svc-legacy"}
	cfg := budgetRouteCfg(ds)
	cfg.AttemptBudget = 0 // 关键：未配置单次尝试预算
	g.RegisterRoute("svc-legacy", cfg)
	log := attachLogger(t, g)

	resp, err := g.Call(Request{ID: "l1", Route: "svc-legacy", Idempotent: true,
		Deadline: time.Now().Add(time.Millisecond)}) // 为正但极短
	if err != nil {
		t.Fatalf("legacy behavior must dispatch regardless of short positive budget: %v", err)
	}
	if resp.Attempts != 1 || ds.gotCalls() != 1 {
		t.Fatalf("attempts=%d calls=%d, want 1/1", resp.Attempts, ds.gotCalls())
	}
	if log.phases()[0] != "dispatch" {
		t.Fatalf("first phase = %v, want dispatch (no budget reject)", log.phases())
	}
}

// TestBudgetRejectDoesNotConsumeConcurrency 预算不足拒绝时不得占用并发名额：
// 连续多次被预算拒绝后，在途数始终为 0，随后一个预算充足的调用仍可正常进入。
func TestBudgetRejectDoesNotConsumeConcurrency(t *testing.T) {
	g := New()
	ds := &recordingDownstream{name: "svc-slot"}
	cfg := budgetRouteCfg(ds) // MaxConcurrent = 4
	g.RegisterRoute("svc-slot", cfg)
	attachLogger(t, g)

	for i := 0; i < 10; i++ {
		_, err := g.Call(Request{ID: "short", Route: "svc-slot", Idempotent: true,
			Deadline: time.Now().Add(20 * time.Millisecond)})
		var rej *RejectError
		if !errors.As(err, &rej) || rej.Reason != ReasonBudgetExhausted {
			t.Fatalf("call %d err = %v, want budget_exhausted", i, err)
		}
	}
	if st, _ := g.Status("svc-slot"); st.InFlight != 0 {
		t.Fatalf("inflight after rejects = %d, want 0", st.InFlight)
	}
	// 预算充足时仍能成功，证明名额没有被预算拒绝泄漏占用。
	if _, err := g.Call(Request{ID: "ok", Route: "svc-slot", Idempotent: true,
		Deadline: time.Now().Add(time.Second)}); err != nil {
		t.Fatalf("budget-healthy call failed: %v", err)
	}
}
