package gateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var errFakeDownstream = errors.New("fake downstream internal failure")

// fakeCall 描述第 N 次下游调用的脚本行为。
type fakeCall struct {
	delay   time.Duration // 下游处理耗时（会尊重 ctx 截止时间，超时则返回 ctx.Err()）
	fail    bool          // 是否失败（返回内部错误，绝不允许透出给调用方）
	release chan struct{} // 若非 nil，下游在此通道关闭前阻塞（模拟在途请求）
}

// fakeDownstream 是脚本化下游：按调用序号返回预设行为，并记录观测信息。
type fakeDownstream struct {
	mu      sync.Mutex
	name    string
	calls   []fakeCall
	got     int
	lastCtx AttemptContext
}

func newFakeDownstream(name string, calls ...fakeCall) *fakeDownstream {
	return &fakeDownstream{name: name, calls: calls}
}

func (f *fakeDownstream) Name() string { return f.name }

func (f *fakeDownstream) Call(ctx AttemptContext, req Request) (any, error) {
	f.mu.Lock()
	idx := f.got
	f.got++
	f.lastCtx = ctx
	var c fakeCall
	if idx < len(f.calls) {
		c = f.calls[idx]
	} else if len(f.calls) > 0 {
		c = f.calls[len(f.calls)-1]
	}
	f.mu.Unlock()

	if c.release != nil {
		<-c.release
	}
	if c.delay > 0 {
		if !ctx.Deadline.IsZero() {
			rem := time.Until(ctx.Deadline)
			if rem <= 0 {
				return nil, contextDeadlineExceeded()
			}
			if c.delay >= rem {
				time.Sleep(rem)
				return nil, contextDeadlineExceeded()
			}
		}
		time.Sleep(c.delay)
	}
	if c.fail {
		return nil, fmt.Errorf("%w: attempt=%d", errFakeDownstream, ctx.Attempt)
	}
	return fmt.Sprintf("ok-%s-%d", f.name, ctx.Attempt), nil
}

func (f *fakeDownstream) gotCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got
}

// eventLog 捕获网关日志事件，并通过 t.Logf 打印关键字段：
// 下游标识、熔断状态、剩余预算、尝试次数、失败分类。
type eventLog struct {
	t  *testing.T
	mu sync.Mutex
	ev []LogEvent
}

func newEventLog(t *testing.T) *eventLog {
	return &eventLog{t: t}
}

func (l *eventLog) logger(ev LogEvent) {
	l.mu.Lock()
	l.ev = append(l.ev, ev)
	l.mu.Unlock()
	l.t.Logf("[gateway-log] route=%s phase=%s breaker=%s remaining=%s attempt_budget=%s attempt=%d/%d inflight=%d reason=%q class=%q kind=%q",
		ev.Route, ev.Phase, ev.Breaker, ev.BudgetText(), ev.AttemptText(), ev.Attempt, ev.MaxAttempt,
		ev.InFlight, ev.Reason, ev.Class, ev.Kind)
}

func (l *eventLog) phases() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.ev))
	for i, ev := range l.ev {
		out[i] = ev.Phase
	}
	return out
}

func (l *eventLog) last() LogEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ev[len(l.ev)-1]
}

// attachLogger 给网关挂接测试日志与零耗时退避，返回事件收集器。
func attachLogger(t *testing.T, g *Gateway) *eventLog {
	l := newEventLog(t)
	g.Logger = l.logger
	g.Sleep = func(time.Duration) {} // 测试中退避不真实等待
	return l
}

func retryableClassifier(err error) FailureClass {
	if errors.Is(err, errFakeDownstream) {
		return FailureRetryable
	}
	return FailureNonRetryable
}

func nonRetryableClassifier(error) FailureClass { return FailureNonRetryable }

func testConfig(ds Downstream, classifier Classifier) RouteConfig {
	return RouteConfig{
		Downstream:         ds,
		Threshold:          2,
		OpenCooldown:       40 * time.Millisecond,
		ProbeMaxConcurrent: 1,
		MaxConcurrent:      2,
		DefaultTimeout:     time.Second,
		Retry: RetryConfig{
			MaxAttempts: 3,
			Backoff:     5 * time.Millisecond,
		},
		Classifier: classifier,
	}
}
