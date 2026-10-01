package gateway

import (
	"testing"
	"time"
)

// TestPreDispatchCheckTable 派发前预算判定的纯函数语义：
// 不设限永远放行；过期与“为正但不够一次尝试”必须区分。
func TestPreDispatchCheckTable(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name   string
		budget time.Duration // RouteConfig.AttemptBudget，0 表示未配置
		caller time.Time     // 调用方截止时间
		want   budgetCheck
	}{
		{"unlimited_with_attempt_budget", 200 * time.Millisecond, time.Time{}, budgetOK},
		{"unlimited_without_attempt_budget", 0, time.Time{}, budgetOK},
		{"expired_with_attempt_budget", 200 * time.Millisecond, now.Add(-time.Second), budgetDeadlinePassed},
		{"expired_without_attempt_budget", 0, now.Add(-time.Second), budgetDeadlinePassed},
		{"positive_but_shorter_than_attempt_budget", 200 * time.Millisecond, now.Add(50 * time.Millisecond), budgetTooShort},
		{"positive_without_attempt_budget_always_ok", 0, now.Add(time.Millisecond), budgetOK},
		{"positive_covers_attempt_budget", 200 * time.Millisecond, now.Add(time.Second), budgetOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RouteConfig{AttemptBudget: tc.budget}
			if got := preDispatchCheck(cfg, tc.caller); got != tc.want {
				t.Fatalf("preDispatchCheck = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestAttemptDeadlineTable 单次尝试截止时间：
// 有调用方截止时间时取小传播，无时由 AttemptBudget / DefaultTimeout 生效，
// 三者皆无返回零值（不设限）。
func TestAttemptDeadlineTable(t *testing.T) {
	now := time.Now()
	const tol = 5 * time.Millisecond
	cases := []struct {
		name           string
		attemptBudget  time.Duration
		defaultTimeout time.Duration
		caller         time.Time
		wantZero       bool
		wantApprox     time.Time // 期望约等于的绝对时刻
		wantCaller     bool      // 期望原样等于调用方截止时间
	}{
		{"caller_and_budget_min_is_budget", 100 * time.Millisecond, 0,
			now.Add(time.Second), false, now.Add(100 * time.Millisecond), false},
		{"caller_without_budget_propagates_caller", 0, 0,
			now.Add(250 * time.Millisecond), false, time.Time{}, true},
		{"no_caller_attempt_budget_alone", 100 * time.Millisecond, 0,
			time.Time{}, false, now.Add(100 * time.Millisecond), false},
		{"no_caller_attempt_budget_beats_default_timeout", 100 * time.Millisecond, time.Second,
			time.Time{}, false, now.Add(100 * time.Millisecond), false},
		{"no_caller_falls_back_to_default_timeout", 0, 300 * time.Millisecond,
			time.Time{}, false, now.Add(300 * time.Millisecond), false},
		{"nothing_configured_means_unlimited", 0, 0,
			time.Time{}, true, time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RouteConfig{AttemptBudget: tc.attemptBudget, DefaultTimeout: tc.defaultTimeout}
			got := attemptDeadline(cfg, now, tc.caller)
			switch {
			case tc.wantZero:
				if !got.IsZero() {
					t.Fatalf("deadline = %v, want zero (unlimited)", got)
				}
			case tc.wantCaller:
				if !got.Equal(tc.caller) {
					t.Fatalf("deadline = %v, want caller deadline %v", got, tc.caller)
				}
			default:
				if d := got.Sub(tc.wantApprox); d > tol || d < -tol {
					t.Fatalf("deadline = %v, want ~%v", got, tc.wantApprox)
				}
			}
		})
	}
}

// TestAttemptDeadlineNeverExceedsCaller 即使单次预算比剩余总预算长，
// 取小结果也不得超过调用方截止时间（边界附近的硬性保证）。
func TestAttemptDeadlineNeverExceedsCaller(t *testing.T) {
	now := time.Now()
	caller := now.Add(50 * time.Millisecond)
	cfg := RouteConfig{AttemptBudget: 500 * time.Millisecond}
	got := attemptDeadline(cfg, now, caller)
	if got.After(caller) {
		t.Fatalf("attempt deadline %v exceeds caller deadline %v", got, caller)
	}
	if !got.Equal(caller) {
		t.Fatalf("attempt deadline = %v, want caller deadline %v as the min", got, caller)
	}
}

// TestFormatBudgetUnlimited 对外展示：不设限必须是 "unlimited"，
// 不允许拿具体时长冒充。
func TestFormatBudgetUnlimited(t *testing.T) {
	if got := formatBudget(0, false); got != "unlimited" {
		t.Fatalf("formatBudget(unlimited) = %q, want %q", got, "unlimited")
	}
	if got := formatBudget(123*time.Millisecond, true); got != "123ms" {
		t.Fatalf("formatBudget(limited) = %q, want %q", got, "123ms")
	}
}

// TestAttemptContextRemainingUnlimited 零截止时间（不设限）时 ok=false，
// 调用方不得把返回的大值当真实预算展示；过期时为 0 且 ok=true。
func TestAttemptContextRemainingUnlimited(t *testing.T) {
	d, ok := AttemptContext{}.Remaining()
	if ok {
		t.Fatalf("zero deadline: ok = true, want false (unlimited)")
	}
	if d <= 0 {
		t.Fatalf("zero deadline placeholder = %v, want large positive", d)
	}
	d, ok = AttemptContext{Deadline: time.Now().Add(-time.Second)}.Remaining()
	if !ok || d != 0 {
		t.Fatalf("past deadline: (%v,%v), want (0,true)", d, ok)
	}
}
