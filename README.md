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
    DefaultTimeout:     500 * time.Millisecond,     // 调用方未给截止时间时的单次超时
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
| `deadline_expired` | 首次派发前，调用方截止时间已过期 |
| `budget_exhausted` | （保留）预算不足以再派发 |
| `circuit_open` | 熔断器处于 OPEN，快速拒绝 |
| `probe_concurrency_limit` | HALF_OPEN 探测名额已被在途探测占满 |
| `concurrency_limit` | 该下游并发隔离舱已满（不排队，立即拒绝） |
| `unknown_route` | 路由未注册 |

### TerminalKind（终局原因）

| 原因 | 含义 |
| --- | --- |
| `non_retryable_failure` | 失败分类为不可重试，或调用方未声明幂等，或已达 `MaxAttempts` |
| `budget_exhausted` | 尝试本身或重试退避耗尽了调用方预算 |
| `circuit_open` | 同一调用的重试途中熔断器打开（或隔离舱打满），后续尝试被治理层拒绝 |

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

## 预算（截止时间传播）规则

- 调用方在 `Request.Deadline` 给出**绝对截止时间**；网关把同一截止时间
  放入 `AttemptContext.Deadline` 传播给下游，下游必须尊重该截止时间。
- 首次派发前若已过期：立即 `RejectError(deadline_expired)`，不占用任何
  下游资源。
- 调用方未声明截止时间时，使用路由的 `DefaultTimeout` 作为单次尝试截止；
  两者均为零值则不设限。
- 每次尝试前、每次退避后都重新检查剩余预算；预算不足则不再派发。
- 重试退避时间会被剩余预算截断；退避（或本次尝试）耗尽预算时，终局原因为
  `budget_exhausted`，与 `non_retryable_failure` 明确区分。

## 重试规则

只有**同时满足**以下条件才会重试：

1. 失败被路由的 `Classifier` 判定为 `FailureRetryable`；
2. `Request.Idempotent == true`（调用方显式声明幂等）；
3. 已用尝试次数 `< Retry.MaxAttempts`；
4. 仍在调用方预算内（退避后仍有剩余时间）。

`MaxAttempts` 含首次尝试（`3` 表示最多派发 3 次、退避 2 次）。不满足任一
条件即终局：不可重试/非幂等/次数用尽 → `non_retryable_failure`；
预算耗尽 → `budget_exhausted`。重试不绕过熔断器与并发舱。

## 并发限制配置

| 配置项 | 作用 |
| --- | --- |
| `MaxConcurrent` | 路由级并发隔离舱容量。采用**快速拒绝**策略：舱满立即返回 `concurrency_limit`，不排队、不阻塞，在途数绝不超过容量 |
| `ProbeMaxConcurrent` | HALF_OPEN 探测并发上限，且探测同样受 `MaxConcurrent` 约束 |
| `Threshold` / `OpenCooldown` | 熔断阈值 / 打开冷却 |
| `Retry.MaxAttempts` / `Retry.Backoff` | 最大尝试次数 / 固定退避 |
| `DefaultTimeout` | 无调用方截止时间时的单次尝试超时 |
| `Classifier` | 下游错误 → 失败分类（nil 时一律不可重试） |

## 决策日志

`Gateway.Logger` 可注入自定义日志函数，每次决策产生一条结构化 `LogEvent`，
字段包含：**下游标识（route）、熔断状态（breaker）、剩余预算（remaining）、
尝试次数（attempt/MaxAttempt）、在途并发（inflight）、失败分类（class）、
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
- **重试边界**：成功即止、不可重试不重试、非幂等不重试、达到次数上限、
  原始错误不透出（`retry_test.go`）；
- **混合失败链路一致性**：熔断优先于重试、两类错误互斥且可用
  `errors.As` 无歧义区分（`integration_test.go`）。
