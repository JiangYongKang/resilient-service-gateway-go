package gateway

import (
	"context"
	"errors"
)

// ErrPoolAcquire 仅用于内部表示获取并发槽位失败（例如 ctx 取消或截止时间到达）。
var ErrPoolAcquire = errors.New("gateway: acquire concurrency slot failed")

// Pool 是单个下游独立的并发上限信号量。
// 不同下游使用不同 Pool，实现并发隔离：一个下游占满不影响其他下游。
type Pool struct {
	name  string
	slots chan struct{}
}

func newPool(name string, max int) *Pool {
	if max < 1 {
		max = 1
	}
	return &Pool{name: name, slots: make(chan struct{}, max)}
}

// tryAcquire 非阻塞获取一个槽位，成功返回 release 函数。
// 重试也必须先重新获取槽位，因此重试不会突破并发上限。
func (p *Pool) tryAcquire() (release func(), ok bool) {
	select {
	case p.slots <- struct{}{}:
		return p.release, true
	default:
		return nil, false
	}
}

// acquireCtx 在 ctx 截止前阻塞获取槽位；ctx 结束时返回 ErrPoolAcquire。
func (p *Pool) acquireCtx(ctx context.Context) (release func(), err error) {
	select {
	case p.slots <- struct{}{}:
		return p.release, nil
	case <-ctx.Done():
		return nil, ErrPoolAcquire
	}
}

func (p *Pool) release() {
	<-p.slots
}
