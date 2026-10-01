# Resilient Service Gateway（本地服务调用网关）

在调用方与下游实现之间提供一致且可解释的治理能力：**熔断、并发隔离、
超时（调用方总截止时间传播 + 按下游的单次尝试预算）与重试**。每个下游
（路由）拥有独立的熔断状态与并发舱，互不影响；所有失败都被归一为网关
自身的错误类型，下游原始错误不会透出。

## 快速开始

```go
g := gateway.New()
g.RegisterRoute("payment", gateway.RouteConfig{
    Downstream:         paymentSvc,                 // 实现 gateway.Downstream
    Threshold:          5,                          // 连续失败 5 次触发熔断
    OpenCooldown:       2 * time.Second,            // 熔断打开后的冷却时间
    ProbeMaxConcurrent: 1,                          // 半开探测最大并发
    MaxConcurrent:      32,                         // 该下游最大并发
    AttemptBudget:      200 * time.Millisecond,     // 每次尝试最多占用下游 200ms
    DefaultTimeout:     500 * time.Millisecond,     // 无调用方截止时间且无 AttemptBudget 时的单次超时
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
| `deadline_expired` | 首次派发前，调用方截止时间已过期（剩余总预算 `<= 0`），下游 0 调用 |
| `budget_exhausted` | 首次派发前，剩余总预算**仍为正数**但不足以支撑一次 `AttemptBudget`，下游 0 调用、不占并发名额 |
| `circuit_open` | 熔断器处于 OPEN，快速拒绝 |
| `probe_concurrency_limit` | HALF_OPEN 探测名额已被在途探测占满 |
| `concurrency_limit` | 该下游并发隔离舱已满（不排队，立即拒绝） |
| `unknown_route` | 路由未注册 |

### TerminalKind（终局原因）

| 原因 | 含义 |
| --- | --- |
| `non_retryable_failure` | 失败分类为不可重试，或调用方未声明幂等，或已达 `MaxAttempts` |
| `budget_exhausted` | 已派发过尝试；尝试本身或重试退避把调用方总预算耗尽（剩余 `<= 0`） |
| `budget_insufficient` | 重试链路上，下一次尝试派发前剩余总预算**仍为正数**但不够一次 `AttemptBudget`，停止派发 |
| `circuit_open` | 同一调用的重试途中熔断器打开（或隔离舱打满），后续尝试被治理层拒绝 |

> **三种“预算/时间”终局的区分**：派发前用 `RejectError.Reason` 区分
> `deadline_expired`（已经过期）与 `budget_exhausted`（为正但不够一次尝试）；
> 已派发后用 `TerminalError.Kind` 区分 `budget_exhausted`（剩余真的到 0）与
> `budget_insufficient`（剩余为正但不够下一次尝试）。调用方只看返回错误即可判断。

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

## 预算规则：总截止时间 + 单次尝试预算

每条路由有两层时间预算，共同约束每一次下游尝试：

- **调用方总截止时间**（`Request.Deadline`，绝对时刻）：整笔调用的总预算，
  跨所有重试共享。
- **单次尝试预算**（`RouteConfig.AttemptBudget`）：一次向下游派发最多允许
  占用下游多久，**每次尝试（含首次和每一次重试）都按当时情况重新计算**。

### 单次尝试的实际截止时间

对第 N 次尝试，网关在派发前按下面的规则计算 `AttemptContext.Deadline`：

| 调用方总截止时间 | 路由 `AttemptBudget` | 该次尝试实际截止时间 |
| --- | --- | --- |
| 有 | 有 | `min(当时时刻 + AttemptBudget, 调用方总截止时间)` |
| 有 | 无（0） | 调用方总截止时间（原样传播，行为同旧版） |
| 无 | 有 | `当时时刻 + AttemptBudget`（单次预算单独生效，优先于 `DefaultTimeout`） |
| 无 | 无 | `DefaultTimeout`（配置时）；否则**不设限**（零值） |

下游永远拿不到超过“调用方总截止时间”这条线的时间；每次重试都重新取小，
而不是复用首次计算结果。

### 派发前预算预检（首次与每次重试都执行）

真正调用下游之前，先按下表判定（**判定先于并发名额与熔断**，预算不够时
不占用并发名额、不产生下游调用）：

| 情形 | 首次（此前 0 次尝试） | 重试链路（此前已派发过） |
| --- | --- | --- |
| 总截止时间已过（剩余 `<= 0`） | `RejectError(deadline_expired)`，下游 0 调用 | `TerminalError(budget_exhausted)` |
| 剩余为正，但 `< AttemptBudget` | `RejectError(budget_exhausted)`，下游 0 调用、不占并发名额 | `TerminalError(budget_insufficient)`，在此停止，不再派发后续尝试 |
| 未配 `AttemptBudget` 或剩余充足 | 正常派发 | 正常派发下一次尝试 |
| 调用方未给总截止时间（不设限） | 永远放行，**不会被预算判定误拒或误判耗尽** | 同左 |

> “剩余为正但不够一次尝试”与“截止时间早就过去了”是两种不同语义；
> “首次就不够”（`RejectError`、下游 0 调用）与“派发过一次、重试途中预算
> 才耗光”（`TerminalError`、已有尝试）也通过错误类型/原因明确区分。

### 不设限（unlimited）的表达

调用方没给总截止时间、路由也没配 `AttemptBudget` 和 `DefaultTimeout` 的
调用属于**不设限**：

- 决策日志的 `remaining` 字段一律输出 `unlimited`，**绝不会**拿某个具体
  时长（例如内部占位的 1h）冒充预算，避免排查时被误读为“只给了那么点预算”；
- 传播给下游的 `AttemptContext.Deadline` 为零值。下游可用
  `AttemptContext.Remaining()` 的第二个返回值 `ok` 判断：零截止时间时
  `ok == false`，不应把第一个返回值（内部大占位值）当作真实预算展示；
- `RejectError.Unlimited` / `LogEvent.Unlimited` 为 `true` 标记不设限；
- 不设限调用的成功、重试、熔断、并发行为完全不受预算判定影响。

### 重试与预算

- 每次尝试前、每次退避后都重新执行上表的预检；
- 重试退避时间会被剩余**总**预算截断（不设限时不截断）；退避（或本次尝试）
  使剩余归零，终局为 `budget_exhausted`；退避/尝试后剩余为正但不足一次
  `AttemptBudget`，终局为 `budget_insufficient`。

### 兼容范围

- `AttemptBudget == 0`（未配置）时行为与旧版完全一致：只有调用方截止时间
  或 `DefaultTimeout` 生效，剩余为正但很短的预算不会被提前拒绝；
- 新增字段均为可选；既有 `RejectReason` / `TerminalKind` 常量取值不变，
  仅新增 `budget_insufficient` 一个终局类别。

## 重试规则

只有**同时满足**以下条件才会重试：

1. 失败被路由的 `Classifier` 判定为 `FailureRetryable`；
2. `Request.Idempotent == true`（调用方显式声明幂等）；
3. 已用尝试次数 `< Retry.MaxAttempts`；
4. 仍在调用方预算内（退避后仍有剩余时间）。

`MaxAttempts` 含首次尝试（`3` 表示最多派发 3 次、退避 2 次）。不满足任一
条件即终局：不可重试/非幂等/次数用尽 → `non_retryable_failure`；
总预算耗尽 → `budget_exhausted`；剩余为正但不足一次 `AttemptBudget` →
`budget_insufficient`。重试不绕过熔断器、并发舱与预算预检。

## 并发限制配置

| 配置项 | 作用 |
| --- | --- |
| `MaxConcurrent` | 路由级并发隔离舱容量。采用**快速拒绝**策略：舱满立即返回 `concurrency_limit`，不排队、不阻塞，在途数绝不超过容量。预算预检在占用名额之前，预算不足被拒不占用名额 |
| `ProbeMaxConcurrent` | HALF_OPEN 探测并发上限，且探测同样受 `MaxConcurrent` 约束 |
| `Threshold` / `OpenCooldown` | 熔断阈值 / 打开冷却 |
| `Retry.MaxAttempts` / `Retry.Backoff` | 最大尝试次数 / 固定退避（被剩余总预算截断） |
| `AttemptBudget` | 每次尝试的单次预算；与调用方剩余总预算取小约束下游。`0` 表示不配置（兼容旧行为） |
| `DefaultTimeout` | 无调用方截止时间、且未配置 `AttemptBudget` 时的单次尝试超时；`0` 表示不限制 |
| `Classifier` | 下游错误 → 失败分类（nil 时一律不可重试） |

## 决策日志

`Gateway.Logger` 可注入自定义日志函数，每次决策产生一条结构化 `LogEvent`，
字段包含：**下游标识（route）、熔断状态（breaker）、剩余预算（remaining，
不设限时为 `unlimited`）、不设限标记（unlimited）、尝试次数
（attempt/MaxAttempt）、在途并发（inflight）、失败分类（class）、
拒绝原因（reason）、终局原因（kind）**。未注入时使用标准库 `log` 输出。
`Gateway.Sleep` 可注入以在测试中跳过真实退避。

## 验证方法

```bash
go vet ./...
go test ./... -race -count=10 -v
```

测试覆盖（结论稳定可复现，单测日志通过 `t.Logf` 打印上述字段）：

- **熔断状态迁移**：阈值触发 OPEN、冷却→HALF_OPEN、探测成功/失败迁移、
  探测并发上限、代次迟到报告忽略（`breaker_test.go`，
  `gateway_breaker_test.go`）；
- **并发上限**：满载快速拒绝、高并发抢占在途数永不越界、半开探测受隔离舱
  约束（`bulkhead_test.go`，`gateway_concurrency_test.go`）；
- **截止时间过期与传播**：过期派发前拒绝、截止时间原样传播、尝试超时/退避
  耗尽预算的终局区分（`deadline_test.go`）；
- **单次尝试预算**（`budget_test.go`、`budget_gateway_test.go`）：
  - 剩余为正但不够一次尝试时，**首次拒绝**（下游 0 调用、不占并发名额，
    `budget_exhausted`）与**重试中途停止**（`budget_insufficient`），
    且与 `deadline_expired` / 派发后的 `budget_exhausted` 可区分；
  - 单次预算确实传给下游：无调用方截止时间时按 `AttemptBudget` 生效，
    有调用方截止时间时每次尝试按“单次预算与剩余总预算取小”逐次重算、
    永不超过总截止时间；
  - 不设限场景日志/错误如实表达为 `unlimited`、下游收到零截止时间、
    重试不受预算判定影响；
  - 未配 `AttemptBudget` 时旧行为不变（为正的短预算照常派发）；
- **重试边界**：成功即止、不可重试不重试、非幂等不重试、达到次数上限、
  原始错误不透出（`retry_test.go`）；
- **混合失败链路一致性**：熔断优先于重试、两类错误互斥且可用
  `errors.As` 无歧义区分（`integration_test.go`）。
