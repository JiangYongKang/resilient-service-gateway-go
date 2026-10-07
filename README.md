# Resilient Service Gateway（本地服务调用网关）

在调用方与下游实现之间提供一致且可解释的四种治理能力：**熔断、并发隔离、
超时（截止时间传播）与重试**。每个下游（路由）拥有独立的熔断状态与并发舱，
互不影响；所有失败都被归一为网关自身的错误类型，下游原始错误不会透出。

## 快速开始

```go
g := gateway.New()
g.RegisterRoute("payment", gateway.RouteConfig{
    Downstream:         paymentSvc,                 // 实现 gateway.Downstream
    Threshold:          5,                          // 连续失败 5 次触发熔断
    OpenCooldown:       2 * time.Second,            // 熔断打开后的冷却时间
    ProbeMaxConcurrent: 1,                          // 半开探测最大并发
    MaxConcurrent:      32,                         // 该下游最大并发
    DefaultTimeout:     500 * time.Millisecond,     // 调用方未给截止时间时的默认单次超时
    AttemptTimeout:     200 * time.Millisecond,     // 单次尝试预算：每次派发最多占用 200ms
    Retry: gateway.RetryConfig{
        MaxAttempts: 3,                             // 含首次的最大尝试次数
        Backoff:     50 * time.Millisecond,         // 固定退避（被剩余预算截断）
    },
    Classifier: func(err error) gateway.FailureClass {
        // 仅把幂等安全的、临时性的错误判为可重试
        if errors.Is(err, ErrTemporary) {
            return gateway.FailureRetryable
        }
        return gateway.FailureNonRetryable
    },
})

resp, err := g.Call(gateway.Request{
    ID: "req-1", Route: "payment",
    Idempotent: true,                          // 仅幂等请求允许重试
    Deadline:   time.Now().Add(time.Second),   // 绝对截止时间；零值表示不限
})
```

## 错误模型（可解释、互斥的两类失败）

| 类型 | 时机 | 区分字段 |
| --- | --- | --- |
| `*gateway.RejectError` | 请求**派发前**被拒绝，下游完全未被调用 | `RejectReason` |
| `*gateway.TerminalError` | 请求**已派发**，以终局失败结束 | `TerminalKind` |

调用方使用标准库 `errors.As` 区分，永不接触下游原始错误（原始错误仅用于
分类与内部日志）。

### RejectReason（派发前拒绝原因）

| 原因 | 含义 |
| --- | --- |
| `deadline_expired` | 首次派发前（或某次尝试前），调用方截止时间**已经过期**（剩余 ≤ 0） |
| `budget_exhausted` | 配置了单次尝试预算后，剩余总预算**仍为正但已不足一次单次尝试**；首次遇到时下游调用 0 次、不占用并发名额 |
| `circuit_open` | 熔断器处于 OPEN，快速拒绝 |
| `probe_concurrency_limit` | HALF_OPEN 探测名额已被在途探测占满 |
| `concurrency_limit` | 该下游并发隔离舱已满（不排队，立即拒绝） |
| `unknown_route` | 路由未注册 |

> `deadline_expired` 与 `budget_exhausted` 是两种不同语义：前者表示调用方截止
> 时间已经过去（剩余 ≤ 0），后者表示还有剩余时间但不够再做一次配置好的单次
> 尝试。预算预检在占用并发名额与熔断判定**之前**执行。

### TerminalKind（终局原因）

| 原因 | 含义 |
| --- | --- |
| `non_retryable_failure` | 失败分类为不可重试，或调用方未声明幂等，或已达 `MaxAttempts` |
| `budget_too_small` | **已有至少一次尝试发生后**，进入重试时调用方总预算**仍为正数**、但已小于路由配置的一次单次尝试预算（`0 < remaining < AttemptTimeout`）：预算只是被切得太碎，后续尝试不再派发、也不退避。可加大预算或换更快的下游再试 |
| `budget_exhausted` | **已有至少一次尝试发生后**，调用方总预算**真正耗尽**（`remaining ≤ 0`，截止时间已经到期）：可能是本次尝试跑过了总时限，也可能是退避把时间睡完，或重试循环顶部发现截止时间已过。总时限见底，只能按超时上报 |
| `circuit_open` | 同一调用的重试途中熔断器打开（或隔离舱打满），后续尝试被治理层拒绝 |

> `budget_too_small` 与 `budget_exhausted` 只凭 `TerminalError.Kind` 即可区分，
> 不需要解析错误文本：前者终局时 `Remaining > 0`（小于 `AttemptTimeout`），
> 后者 `Remaining ≤ 0`。`TerminalError` 同时把 `Limited`、`Remaining`、
> `AttemptBudget` 带回，便于排查；`Error()` 文本也会打印这三个值。

## 熔断阈值语义与状态迁移

熔断器是确定性的三态机（无随机探测），按**路由**独立维护，互斥锁 +
单调递增的**代次（generation）令牌**保证并发下状态不错乱：

```
                 连续失败数 >= Threshold
   CLOSED  ───────────────────────────────►  OPEN
     ▲                                         │
     │ 任一探测成功                            │ 经过 OpenCooldown
     │                                         ▼
     └──────────────────────────────────  HALF_OPEN
                 任一探测失败 ──► OPEN（重新计时冷却）
```

- **Threshold**：CLOSED 状态下**连续失败**计数阈值；达到即 OPEN。
  CLOSED 下任一成功立即把计数清零。
- **OpenCooldown**：OPEN 持续时长。冷却到期后状态迁移为 HALF_OPEN，
  冷却到期后的**第一个请求**自动成为探测请求。
- **ProbeMaxConcurrent**：HALF_OPEN 下允许同时在途的探测请求数；
  名额满后的请求收到 `probe_concurrency_limit`。
- **HALF_OPEN 任一探测成功 → CLOSED**（计数清零）；**任一探测失败 →
  OPEN**（冷却重新计时）。
- **代次隔离**：每次状态迁移代次加一。`Allow` 返回准入代次，请求结束时
  原样带回；与当代不符的迟到报告一律忽略——上一代在途探测的迟到结果
  不会污染新一代状态（见 `TestBreakerStaleGenerationIgnored`）。

熔断判定对所有尝试都生效：即使是重试链路上的后续尝试，熔断器 OPEN 时也
不会被派发（见 `TestOpenCircuitPreemptsRetry`）。

## 预算规则：总截止时间与单次尝试预算

### 两层预算

- **调用方总截止时间**：`Request.Deadline` 给出的**绝对截止时间**，是整笔调用
  （含所有重试）的总预算；零值表示调用方不设限。
- **单次尝试预算**：路由配置 `RouteConfig.AttemptTimeout`，表示**一次**向下游
  派发最多允许占用多久；`0` 表示不配置（行为与旧版本完全一致）。

每次尝试实际传播给下游的截止时间（`AttemptContext.Deadline`）按下表确定：

| 调用方总截止时间 | 路由 `AttemptTimeout` | 每次尝试的实际时长 |
| --- | --- | --- |
| 有 | 有 | `min(单次尝试预算, 当时剩余总预算)` |
| 有 | 无 | 当时剩余总预算（历史行为；`DefaultTimeout` 不参与） |
| 无 | 有 | 单次尝试预算（若还配了 `DefaultTimeout`，取两者更小值） |
| 无 | 无 | 采用 `DefaultTimeout`；也为 0 则**不设限**（传播零截止时间） |

重试的每一次都在**派发当时**重新计算“取小”，因此第一次尝试消耗掉的时间会
自动缩小后续尝试能拿到的预算，下游永远拿不到超过这条线的时间。

### 派发前预检（仅配置了 AttemptTimeout 时）

每次真正调下游之前（首次和每次重试都一样）先判断预算够不够，且预检发生在
占用并发名额、熔断判定**之前**：

1. 剩余总预算 `≤ 0`（调用方截止时间已过期）→ `deadline_expired`；
2. 剩余总预算 `> 0` 但 `< AttemptTimeout`（不够一次尝试）→ 首次
   `budget_exhausted`（`*RejectError`），重试途中 `budget_too_small`
   （`*TerminalError`）；
3. 否则才进入并发舱、熔断器并派发。

命中规则 2 时：

- **首次**就遇到：直接返回 `*RejectError(budget_exhausted)`，下游调用次数
  **必须为 0**，也**不占用并发名额**；
- **重试链路**上遇到（已经派发过一次或多次，且当时剩余总预算仍为正）：
  停止派发后续尝试，返回 `*TerminalError(budget_too_small)`，`Attempts` 等于
  已实际派发的次数；网关也不会再做一段注定无意义的退避等待。
- **重试链路**上若发现调用方截止时间已经真正到期（剩余 `≤ 0`，例如本次尝试
  跑过了总时限，或退避把时间睡完）：返回 `*TerminalError(budget_exhausted)`。

### 四种与预算/时限有关的结局（只看返回值即可区分）

| # | 场景 | 调用方看到的结果 | 关键判据 | 下游调用次数 |
| --- | --- | --- | --- | --- |
| 1 | 还没派发，时限已过（剩余 ≤ 0） | `*RejectError` / `deadline_expired` | `errors.As` + `Reason` | 0 |
| 2 | 还没派发，剩余为正但不够跑一次 | `*RejectError` / `budget_exhausted` | `0 < Remaining < AttemptTimeout` | 0 |
| 3 | 已派发过，重试时剩余为正但不够跑一次 | `*TerminalError` / `budget_too_small` | `Kind`；`Remaining > 0` | ≥ 1 |
| 4 | 已派发过，总预算真正耗尽（剩余 ≤ 0） | `*TerminalError` / `budget_exhausted` | `Kind`；`Remaining ≤ 0` | ≥ 1 |

调用方典型处理方式：

```go
var rej *gateway.RejectError
var term *gateway.TerminalError
switch {
case errors.As(err, &rej) && rej.Reason == gateway.ReasonDeadlineExpired:
    // 情形 1：还没派发，调用方时限早就过了（本调用根本没开始）
case errors.As(err, &rej) && rej.Reason == gateway.ReasonBudgetExhausted:
    // 情形 2：还没派发，预算切得太碎；可加大预算 / 换更快下游后整笔重试
case errors.As(err, &term) && term.Kind == gateway.TerminalBudgetTooSmall:
    // 情形 3：已派发过，重试时预算不够再跑一次；加大预算 / 换更快下游后整笔重试
case errors.As(err, &term) && term.Kind == gateway.TerminalBudgetExhausted:
    // 情形 4：已派发过，总时限真正见底；按超时上报，不应原样重发
}
```

> 情形 3 与情形 4 是本轮重点拆分：过去两者都是 `budget_exhausted`，无法只看
> 返回值区分；现在分别是 `budget_too_small` 与 `budget_exhausted`。
> 情形 1 与情形 4 同为“截止时间已过”，但一个发生在派发前（`*RejectError`）、
> 一个发生在已派发后（`*TerminalError`），错误类型本身就不同。

未配置 `AttemptTimeout` 的路由完全保持旧行为：剩余多少就尝试多少，不做
“不足一次尝试”的额外预检（见 `TestNoAttemptTimeoutKeepsLegacyBehavior`）；
情形 3 因此不可能在这类路由上出现。

### 不设限的表达

调用方未给总截止时间、路由也没配 `AttemptTimeout` 与 `DefaultTimeout` 的调用
属于**不设限**：

- 网关不会把它误拒或误判为预算耗尽，成功、重试、熔断行为一律不变；
- 决策日志中的总预算字段输出 `remaining=unlimited`（通过
  `LogEvent.BudgetText()` 获取），单次尝试预算字段为 `attempt_budget=unlimited`
  （`LogEvent.AttemptText()`），**不会**用 `1h0m0s` 之类的哨兵时长冒充真实预算；
- 此类调用若因其他治理原因（如 `unknown_route`）被拒绝，`*RejectError.Error()`
  同样写明 `(remaining budget: unlimited)`，`RejectError.Limited == false`；
- 传给下游的 `AttemptContext.Deadline` 为零值，由下游自行解释为无截止时间。

### 其他既有规则

- 每次尝试返回后、每次退避后都重新检查总预算；退避时间会被剩余总预算截断；
  退避（或本次尝试）后剩余 `≤ 0` 时终局为 `budget_exhausted`，剩余仍为正但
  不足一次尝试时终局为 `budget_too_small`。
- 重试不绕过熔断器、并发舱与单次尝试预算。

## 重试规则

只有**同时满足**以下条件才会重试：

1. 失败被路由的 `Classifier` 判定为 `FailureRetryable`；
2. `Request.Idempotent == true`（调用方显式声明幂等）；
3. 已用尝试次数 `< Retry.MaxAttempts`；
4. 仍在调用方预算内（当前剩余总预算未过期，且配置了 `AttemptTimeout` 时
   仍足以支撑一次单次尝试）。

`MaxAttempts` 含首次尝试（`3` 表示最多派发 3 次、退避 2 次）。不满足任一
条件即终局：不可重试/非幂等/次数用尽 → `non_retryable_failure`；
总预算真正耗尽（剩余 ≤ 0）→ `budget_exhausted`；总预算仍为正但不足再跑
一次 → `budget_too_small`。重试不绕过熔断器与并发舱。

## 并发限制配置

| 配置项 | 作用 |
| --- | --- |
| `MaxConcurrent` | 路由级并发隔离舱容量。采用**快速拒绝**策略：舱满立即返回 `concurrency_limit`，不排队、不阻塞，在途数绝不超过容量 |
| `ProbeMaxConcurrent` | HALF_OPEN 探测并发上限，且探测同样受 `MaxConcurrent` 约束 |
| `Threshold` / `OpenCooldown` | 熔断阈值 / 打开冷却 |
| `Retry.MaxAttempts` / `Retry.Backoff` | 最大尝试次数 / 固定退避 |
| `DefaultTimeout` | 无调用方截止时间时的默认单次尝试超时（与 `AttemptTimeout` 取更小值） |
| `AttemptTimeout` | **单次尝试预算**：每次派发的时长上限。有调用方截止时间时与剩余总预算取更小值；配置后启用“不足一次尝试即不派发”的预检；`0` 表示不配置，行为与旧版本一致 |
| `Classifier` | 下游错误 → 失败分类（nil 时一律不可重试） |

## 决策日志

`Gateway.Logger` 可注入自定义日志函数，每次决策产生一条结构化 `LogEvent`，
字段包含：**下游标识（route）、熔断状态（breaker）、剩余总预算（remaining）、
本次尝试预算（attempt_budget）、尝试次数（attempt/MaxAttempt）、在途并发
（inflight）、失败分类（class）、拒绝原因（reason）、终局原因（kind）**。
其中预算展示请使用 `LogEvent.BudgetText()` 与 `LogEvent.AttemptText()`：
不设限时返回字符串 `unlimited`，避免把内部哨兵时长当成真实预算。
未注入 Logger 时使用标准库 `log` 输出。`Gateway.Sleep` 可注入以在测试中跳过
真实退避。

## 验证方法

```bash
go vet ./...
go test ./... -race -count=10 -v
```

只验证“四种预算/时限结局”的区分（每个子测试都会用 `t.Logf` 打印自己命中的
业务结局：`outcome=...`，含 kind/reason、剩余预算、下游调用次数）：

```bash
go test ./... -race -count=50 -v -run 'TestBudgetOutcome'
```

只验证单次尝试预算相关行为可运行：

```bash
go test ./... -race -count=50 -v -run 'TestAttemptBudget|TestUnlimited|TestNoAttemptTimeout'
```

也可在接入代码中快速自查：

- 给路由配 `AttemptTimeout: 50 * time.Millisecond`，发起一个
  `Deadline: time.Now().Add(30 * time.Millisecond)` 的请求，应得到
  `*gateway.RejectError` 且 `Reason == gateway.ReasonBudgetExhausted`，
  下游完全不被调用；
- 把截止时间改为 `time.Now().Add(-time.Millisecond)`，应得到
  `ReasonDeadlineExpired`，与上一种原因明确不同；
- 让第一次尝试消耗约 30ms 后可重试失败、总预算 70ms、单次预算 50ms，应得到
  `*gateway.TerminalError` 且 `Kind == gateway.TerminalBudgetTooSmall`
  （`Remaining` 仍为正、小于 50ms，下游只调用 1 次、不退避）；
- 让下游实际跑过调用方总时限（无视传播的尝试截止）后可重试失败，应得到
  `Kind == gateway.TerminalBudgetExhausted`（`Remaining ≤ 0`），与上一种
  只凭 `Kind` 即可区分；
- 不传 `Deadline`、也不配 `AttemptTimeout`/`DefaultTimeout`，注入自定义
  `Logger` 后应看到 `remaining=unlimited`，下游收到的
  `AttemptContext.Deadline` 为零值。

测试覆盖（结论稳定可复现，单测日志通过 `t.Logf` 打印上述字段）：

- **四种预算/时限结局的返回值区分**：派发前时限已过、派发前不够跑一次、
  已派发后重试时不够跑一次（`budget_too_small`，剩余为正）、已派发后总预算
  真正耗光（`budget_exhausted`，剩余 ≤ 0，含尝试跑过总时限与退避睡到期两条
  路径）；以及不设限调用、未配单次尝试预算路由的兼容回归
  （`budget_outcome_test.go`，`TestBudgetOutcome*`）；
- **熔断状态迁移**：阈值触发 OPEN、冷却→HALF_OPEN、探测成功/失败迁移、
  探测并发上限、代次迟到报告忽略（`breaker_test.go`，
  `gateway_breaker_test.go`）；
- **并发上限**：满载快速拒绝、高并发抢占在途数永不越界、半开探测受隔离舱
  约束（`bulkhead_test.go`，`gateway_concurrency_test.go`）；
- **截止时间过期与传播**：过期派发前拒绝、截止时间原样传播、尝试跑过总时限/
  退避睡到期的 `budget_exhausted` 终局（`deadline_test.go`）；
- **单次尝试预算**：首次剩余为正但不足一次尝试直接拒绝（下游 0 次、不占
  并发名额）、与 `deadline_expired` 语义区分、重试途中剩余为正但不足一次
  尝试即 `budget_too_small` 终局且不再退避、每次尝试截止时间取“单次预算与
  剩余总预算更小值”、无调用方截止时间时单次预算单独生效、未配置时保持历史
  行为（`attempt_budget_test.go`）；
- **不设限表达**：日志 `remaining/attempt_budget=unlimited`、下游收到零截止
  时间、`RejectError` 文本写明 unlimited 且不泄露哨兵时长、不设限调用不被
  预算判定影响（`attempt_budget_test.go`）；
- **重试边界**：成功即止、不可重试不重试、非幂等不重试、达到次数上限、
  原始错误不透出（`retry_test.go`）；
- **混合失败链路一致性**：熔断优先于重试、两类错误互斥且可用
  `errors.As` 无歧义区分（`integration_test.go`）。

## 本轮变更的兼容范围

- **新增**终局原因 `TerminalBudgetTooSmall`（`"budget_too_small"`），仅用于
  “已派发过、重试时剩余为正但不足一次单次尝试”这一过去被并入
  `budget_exhausted` 的场景；`TerminalError` 新增 `Limited`、`Remaining`、
  `AttemptBudget` 三个只读字段，`Error()` 对预算类终局附带这些数值。
- **不变**：每次尝试仍取“单次尝试预算”与“当时剩余总预算”的更小值约束下游；
  不足跑一次就不再派发、不做无意义退避；首次派发前两类拒绝
  （`deadline_expired` / `budget_exhausted`）保持原样；
  `budget_exhausted` 仍只表示“总预算真正耗尽（剩余 ≤ 0）”；
  未配置 `AttemptTimeout` 的路由、调用方未给 `Deadline` 的调用，行为与此前
  完全一致，既不会被误拒，也不会被误判成预算耗尽；
  `circuit_open` / `non_retryable_failure` 终局与所有成功路径均无变化。
