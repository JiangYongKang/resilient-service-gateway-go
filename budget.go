package gateway

import "time"

// totalBudget 是一次调用的调用方总预算视图。零截止时间表示不设限。
type totalBudget struct {
	deadline time.Time
}

// unlimited 报告调用方是否没有声明总截止时间（不设限）。
func (b totalBudget) unlimited() bool { return b.deadline.IsZero() }

// remaining 返回当前剩余总预算。limited 为 false 时表示不设限，
// 此时返回的 d 不代表任何真实预算，调用方必须按“不设限”展示。
func (b totalBudget) remaining() (d time.Duration, limited bool) {
	if b.deadline.IsZero() {
		return 0, false
	}
	d = time.Until(b.deadline)
	if d < 0 {
		d = 0
	}
	return d, true
}

// budgetCheck 是每次尝试派发前的预算判定类别。
type budgetCheck int

const (
	// budgetOK 预算允许派发本次尝试。
	budgetOK budgetCheck = iota
	// budgetDeadlinePassed 调用方总截止时间已经到达或过去（剩余 <= 0）。
	budgetDeadlinePassed
	// budgetTooShort 剩余总预算仍为正数，但不够一次配置的单次尝试预算。
	budgetTooShort
)

// preDispatchCheck 在每次尝试真正派发前判定预算。
//
//   - 调用方未给总截止时间：永远 OK（不设限，也不允许误判为耗尽）；
//   - 有限总预算且剩余 <= 0：budgetDeadlinePassed；
//   - 路由配置了 AttemptBudget 且正剩余不足一次尝试：budgetTooShort；
//   - 其余情况（含未配置 AttemptBudget）：budgetOK。
func preDispatchCheck(cfg RouteConfig, caller time.Time) budgetCheck {
	if caller.IsZero() {
		return budgetOK
	}
	rem := time.Until(caller)
	if rem <= 0 {
		return budgetDeadlinePassed
	}
	if cfg.AttemptBudget > 0 && rem < cfg.AttemptBudget {
		return budgetTooShort
	}
	return budgetOK
}

// attemptDeadline 计算一次尝试要传给下游的截止时间。
//
//   - 调用方给了总截止时间：配置了 AttemptBudget 时取
//     now+AttemptBudget 与总截止时间中更早（更小）的一个，
//     保证下游拿不到超过调用方总预算的时间；未配置时原样传播总截止时间。
//   - 调用方未给总截止时间：AttemptBudget 单独生效；未配置时回退
//     DefaultTimeout；两者皆为 0 返回零值，表示本次尝试不设限。
func attemptDeadline(cfg RouteConfig, now time.Time, caller time.Time) time.Time {
	if !caller.IsZero() {
		if cfg.AttemptBudget > 0 {
			d := now.Add(cfg.AttemptBudget)
			if d.After(caller) {
				return caller
			}
			return d
		}
		return caller
	}
	if cfg.AttemptBudget > 0 {
		return now.Add(cfg.AttemptBudget)
	}
	if cfg.DefaultTimeout > 0 {
		return now.Add(cfg.DefaultTimeout)
	}
	return time.Time{}
}

// formatBudget 渲染对外展示的剩余预算。不设限一律表达为 "unlimited"，
// 绝不用某个具体时长冒充不限预算。
func formatBudget(rem time.Duration, limited bool) string {
	if !limited {
		return "unlimited"
	}
	return rem.String()
}
