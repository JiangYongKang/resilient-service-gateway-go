package gateway

import (
	"sync"
	"testing"
)

// TestBulkheadRejectsWhenFull 占满后新请求被快速拒绝，释放后可再获取。
func TestBulkheadRejectsWhenFull(t *testing.T) {
	b := NewBulkhead(2)
	if !b.TryAcquire() || !b.TryAcquire() {
		t.Fatalf("first two acquires should succeed")
	}
	if b.TryAcquire() {
		t.Fatalf("third acquire must be rejected: concurrency limit breached")
	}
	if b.InFlight() != 2 {
		t.Fatalf("inflight = %d, want 2", b.InFlight())
	}
	b.Release()
	if b.InFlight() != 1 {
		t.Fatalf("inflight after release = %d, want 1", b.InFlight())
	}
	if !b.TryAcquire() {
		t.Fatalf("acquire after release should succeed")
	}
}

// TestBulkheadConcurrentNeverExceedsLimit 高并发抢占下在途数永不超过容量，
// 成功次数 + 拒绝次数 = 总请求数。
func TestBulkheadConcurrentNeverExceedsLimit(t *testing.T) {
	const max = 8
	const goroutines = 200
	b := NewBulkhead(max)

	// 先占满全部槽位且不释放，再让 goroutines 同时竞争，保证必然产生拒绝。
	holders := make([]chan struct{}, max)
	for i := range holders {
		if !b.TryAcquire() {
			t.Fatalf("warmup acquire %d failed", i)
		}
		holders[i] = make(chan struct{})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	acquired, rejected := 0, 0
	peak := b.InFlight()
	gate := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate // 同时释放，制造竞争
			if b.TryAcquire() {
				mu.Lock()
				acquired++
				if n := b.InFlight(); n > peak {
					peak = n
				}
				mu.Unlock()
				b.Release()
			} else {
				mu.Lock()
				rejected++
				mu.Unlock()
			}
		}()
	}
	close(gate)
	wg.Wait()

	if acquired != 0 || rejected != goroutines {
		t.Fatalf("while full: acquired=%d rejected=%d, want 0/%d", acquired, rejected, goroutines)
	}
	if peak > max {
		t.Fatalf("peak inflight = %d, exceeds limit %d", peak, max)
	}

	// 释放一半槽位后，竞争结果严格守恒。
	for i := 0; i < max/2; i++ {
		b.Release()
	}
	acquired, rejected = 0, 0
	close2 := make(chan struct{})
	const contenders = 100
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-close2
			if b.TryAcquire() {
				mu.Lock()
				acquired++
				mu.Unlock()
			} else {
				mu.Lock()
				rejected++
				mu.Unlock()
			}
		}()
	}
	close(close2)
	wg.Wait()
	if acquired+rejected != contenders {
		t.Fatalf("acquired(%d)+rejected(%d) != %d", acquired, rejected, contenders)
	}
	if acquired != max/2 {
		t.Fatalf("acquired after freeing %d slots = %d, want exactly %d", max/2, acquired, max/2)
	}
	if b.InFlight() != max {
		t.Fatalf("inflight after contention = %d, want %d", b.InFlight(), max)
	}
}
