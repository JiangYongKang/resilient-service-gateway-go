package gateway

import (
	"context"
	"time"
)

// Budget 描述一次请求（可能含多次尝试）的剩余时间预算。
// 预算唯一来源于调用方 context 的截止时间，网关不自行创造预算。
type Budget struct {
	deadline time.Time
	has      bool
}

func newBudget(ctx context.Context) Budget {
	dl, ok := ctx.Deadline()
	return Budget{deadline: dl, has: ok}
}

// Remaining 返回剩余预算；无截止时间时第二返回值为 false，表示预算无限。
func (b Budget) Remaining(now time.Time) (time.Duration, bool) {
	if !b.has {
		return 0, false
	}
	return b.deadline.Sub(now), true
}

// Expired 报告截止时间是否已过期。
func (b Budget) Expired(now time.Time) bool {
	if !b.has {
		return false
	}
	return !now.Before(b.deadline)
}
