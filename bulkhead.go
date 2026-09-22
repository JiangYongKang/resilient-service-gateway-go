package gateway

import "sync/atomic"

// Bulkhead 是单个下游的并发隔离舱。
// 采用快速拒绝策略：占满时新请求立即被拒绝（不排队、不阻塞），
// 从而把超载压力挡在本地下游之前，并保证并发上限不会被突破。
type Bulkhead struct {
	tokens   chan struct{}
	inFlight atomic.Int64
}

// NewBulkhead 创建容量为 max 的隔离舱；max 小于 1 时按 1 处理。
func NewBulkhead(max int) *Bulkhead {
	if max < 1 {
		max = 1
	}
	return &Bulkhead{tokens: make(chan struct{}, max)}
}

// TryAcquire 尝试占用一个并发槽位；成功返回 true，已满返回 false。
func (b *Bulkhead) TryAcquire() bool {
	select {
	case b.tokens <- struct{}{}:
		b.inFlight.Add(1)
		return true
	default:
		return false
	}
}

// Release 归还一个并发槽位。
func (b *Bulkhead) Release() {
	select {
	case <-b.tokens:
		b.inFlight.Add(-1)
	default:
		// 未配对的 Release 视为编程错误：忽略而不是 panic，保持进程可用。
	}
}

// InFlight 返回当前占用中的槽位数（供观测/日志使用）。
func (b *Bulkhead) InFlight() int { return int(b.inFlight.Load()) }
