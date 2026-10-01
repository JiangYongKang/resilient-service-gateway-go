package gateway

import "time"

// budgetView 汇总一次调用的预算语义，避免在各处重复解释“不设限”。
//
// “有限”仅由调用方是否给出总截止时间决定：
//   - 调用方给了总截止时间：剩余总预算可能耗尽，每次尝试取“单次尝试预算”
//     与“当时剩余总预算”的更小值约束下游；
//   - 调用方没给总截止时间：不设限。单次尝试预算/默认超时仍可限制单次调用
//     的时长，但永远不会产生“总预算耗尽”类的拒绝或终局。
type budgetView struct {
	// limited 表示调用方是否给出了总截止时间（总预算是否有限）。
	limited bool
	// remaining 为有限总预算时的剩余值（可能为负，表示已过期）；
	// 不设限时无意义，必须以 limited=false 而不是某个具体时长表达。
	remaining time.Duration
	// attemptBudget 为路由配置的单次尝试预算；0 表示未配置。
	attemptBudget time.Duration
}

// budgetSummary 根据请求总截止时间与路由配置构造预算视图。
func budgetSummary(deadline time.Time, attemptTimeout time.Duration) budgetView {
	b := budgetView{attemptBudget: attemptTimeout}
	if deadline.IsZero() {
		return b // limited=false：不设限
	}
	b.limited = true
	b.remaining = time.Until(deadline)
	return b
}

// expired 表示有限总预算已经耗尽（含刚好为 0）；不设限恒为 false。
func (b budgetView) expired() bool {
	return b.limited && b.remaining <= 0
}

// canAttempt 判断当前剩余总预算是否足以支撑一次单次尝试。
//
//   - 未配置单次尝试预算（attemptBudget<=0）：只要求总预算未过期，
//     保持历史行为（剩余多少就尝试多少，不额外预检）；
//   - 配置了单次尝试预算：有限总预算必须严格大于单次尝试预算才派发，
//     否则即使剩余为正也不在此次派发；
//   - 不设限：恒为 true，绝不因预算判定拒绝调用。
func (b budgetView) canAttempt() bool {
	if !b.limited {
		return true
	}
	if b.remaining <= 0 {
		return false
	}
	if b.attemptBudget <= 0 {
		return true
	}
	return b.remaining >= b.attemptBudget
}

// attemptDeadline 返回本次尝试应使用的绝对截止时间；零值表示不设限
// （对应 AttemptContext.Deadline 零值，下游自行解释为无截止时间）。
//
// now 为当前时刻，defaultTimeout 为路由的默认超时。
func (b budgetView) attemptDeadline(now time.Time, defaultTimeout time.Duration) time.Time {
	effective := b.attemptBudget
	if defaultTimeout > 0 && (effective <= 0 || defaultTimeout < effective) {
		effective = defaultTimeout
	}
	if b.limited {
		// 调用方给了总截止时间：单次限制与剩余总预算取更小者。
		if effective <= 0 || b.remaining < effective {
			effective = b.remaining
		}
		if effective <= 0 {
			// 理论上 canAttempt 已拦截；给一个已过期的截止时间兜底。
			return now
		}
		return now.Add(effective)
	}
	// 不设限：仅单次尝试预算/默认超时单独生效；都没有则零值（不设限）。
	if effective <= 0 {
		return time.Time{}
	}
	return now.Add(effective)
}
