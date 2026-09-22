package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memLogger 同时输出到测试日志与内存缓冲，便于断言。
type memLogger struct {
	t  *testing.T
	mu sync.Mutex
	sb strings.Builder
}

func newMemLogger(t *testing.T) *memLogger {
	return &memLogger{t: t}
}

func (l *memLogger) Printf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.t.Log(line)
	l.mu.Lock()
	l.sb.WriteString(line)
	l.sb.WriteByte('\n')
	l.mu.Unlock()
}

func (l *memLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sb.String()
}

// fakeDownstream 是可编程的下游。
type fakeDownstream struct {
	mu          sync.Mutex
	attempts    int32
	inFlight    int32
	maxInFlight int32
	// behavior 根据从 1 开始的尝试序号返回结果；nil 表示成功。
	behavior func(attempt int32) (resp *Response, err error)
	// delay 模拟调用耗时。
	delay time.Duration
	// deadlineSeen 记录下游观察到的 ctx 截止时间。
	deadlineObserved []time.Time
}

func (f *fakeDownstream) Invoke(ctx context.Context, req *Request) (*Response, error) {
	n := atomic.AddInt32(&f.attempts, 1)
	in := atomic.AddInt32(&f.inFlight, 1)
	defer atomic.AddInt32(&f.inFlight, -1)
	for {
		cur := atomic.LoadInt32(&f.maxInFlight)
		if in <= cur || atomic.CompareAndSwapInt32(&f.maxInFlight, cur, in) {
			break
		}
	}

	if dl, ok := ctx.Deadline(); ok {
		f.mu.Lock()
		f.deadlineObserved = append(f.deadlineObserved, dl)
		f.mu.Unlock()
	}

	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.behavior != nil {
		return f.behavior(n)
	}
	return &Response{Payload: n}, nil
}

func (f *fakeDownstream) attemptsCount() int32 { return atomic.LoadInt32(&f.attempts) }
func (f *fakeDownstream) maxConcurrent() int32 { return atomic.LoadInt32(&f.maxInFlight) }

func mustReject(t *testing.T, err error, reason RejectReason) *RejectError {
	t.Helper()
	var re *RejectError
	if !asError(err, &re) {
		t.Fatalf("expected *RejectError, got %T: %v", err, err)
	}
	if re.Reason != reason {
		t.Fatalf("expected reject reason %s, got %s", reason, re.Reason)
	}
	return re
}

func asError(err error, target **RejectError) bool {
	for err != nil {
		if re, ok := err.(*RejectError); ok {
			*target = re
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
