# resilient-service-gateway-go

本地服务调用网关：在调用方与下游实现之间统一处理**熔断、并发隔离、超时（截止时间传播）与重试**，
并对各类失败链路给出一致、可解释的拒绝语义。所有策略按下游（route）独立维护，互不影响。

## 快速开始

```go
g := gateway.New(gateway.Config{
    Logger: logger, // 可空，默认输出到 stderr
    Routes: map[string]gateway.DownstreamConfig{
        "orders": {
            Handler:           ordersDownstream,                 // 实现 gateway.Downstream

            MaxConcurrency:    16,
            FailureThreshold:  5,
            OpenDuration:      5 * time.Second,
            ProbeConcurrency:  2,
            SuccessThreshold:  3,
            MaxAttempts:       3,
            PerAttemptTimeout: 200 * time.Millisecond,
            Backoff:           25 * time.Millisecond,
        },
    },
})

resp, err := g.Call(ctx, &gateway.Request{Route: "orders", Idempotent: true, Payload: ...})
```

下游用 `gateway.Retryable(err)` / `gateway.NonRetryable(err)` 显式标记错误分类。

## 熔断阈值语义（按下游独立）

状态机：`Closed → Open → HalfOpen → Closed/Open`

| 状态 | 行为 |
|------|------|
| `Closed` | 正常放行；维护**连续**失败计数，任何成功清零；计数达到 `FailureThreshold` 立即进入 `Open`。 |
| `Open` | 所有请求**快速拒绝**（`circuit-open`），不派发、不占并发槽；持续 `OpenDuration`。 |
| `HalfOpen` | 冷却结束后进入；仅允许 `ProbeConcurrency` 个探测请求同时在飞，其余仍快速拒绝。 |

HalfOpen 迁移：

- 任一探测**失败**：立即回到 `Open`，重新计时 `OpenDuration`。
- **连续**探测成功达到 `SuccessThreshold`：回到 `Closed`（失败计数与成功计数均清零）。
- 探测许可带“代际（generation）”标记：许可发出后若状态又被其它请求迁移，
  迟到许可的上报会被忽略，不归还旧槽位也不参与计数，保证并发下状态不错乱。

注意：默认配置下 `FailureThreshold=5` 统计的是**连续失败**（成功会清零），不是滑动窗口错误率。

## 预算与重试规则

### 截止时间传播与预算门控

- 预算**唯一来源于调用方 `context.Context` 的 deadline**，网关不会自行创造或延长预算。
- 派发前检查：
  - deadline 已过期 → 首次尝试拒绝为 `deadline-expired`；发生在后续尝试前则终局为 `budget-exhausted`。
  - 配置了 `PerAttemptTimeout` 且剩余预算小于它 → 首次尝试拒绝为 `insufficient-budget`；
    后续尝试前则终局为 `budget-exhausted`。这两种是**不同的拒绝原因**，用于解释“没发出去”的语义。
- 每次尝试派生 `context.WithTimeout`，时长为 `min(PerAttemptTimeout, 剩余预算)`，
  因此传播给下游的截止时间**永远不晚于**调用方截止时间；尝试结束即取消上下文，避免泄漏。

### 重试

只有同时满足才会重试：

1. 下游错误经 `Classifier` 判定为**可重试**（`retryable`）；
2. 调用方在 `Request` 中**声明幂等**（`Idempotent: true`）；
3. 尝试次数 `< MaxAttempts`（`MaxAttempts=1` 表示不重试，**默认不重试**）；
4. 仍有剩余预算：退避 `Backoff` 会被剩余预算截断，退避期间 ctx 结束 → `budget-exhausted`。

两类终局原因刻意区分：

- `non-retryable-failure`：不可重试失败；**或**可重试失败但请求未声明幂等（只调用一次）。
  返回错误上带 `Class` 字段标明原始分类。
- `budget-exhausted`：可重试、幂等，但尝试次数耗尽或调用方预算耗尽。

下游原始错误**不会透出**给调用方：`Call` 只返回 `*gateway.RejectError`
（含 `Reason / Downstream / Class / Detail`）。原始错误仅出现在网关日志的 `cause=` 字段中供诊断。

### 默认失败分类器

- 经 `gateway.Retryable/NonRetryable` 标记的错误按标记分类；
- `context.DeadlineExceeded`、`context.Canceled` 视为 `retryable`（瞬时故障）；
- 其余错误视为 `non-retryable`。可通过 `DownstreamConfig.Classifier` 覆盖。

## 并发隔离配置

| 配置项 | 含义 | 默认值 |
|--------|------|--------|
| `MaxConcurrency` | 单个下游同时在飞调用上限，基于独立信号量 | `8` |
| `FailureThreshold` | Closed 下连续失败多少次熔断 | `5` |
| `OpenDuration` | Open 状态持续时长 | `5s` |
| `ProbeConcurrency` | HalfOpen 探测并发上限 | `1` |
| `SuccessThreshold` | HalfOpen 连续探测成功多少次关闭 | `1` |
| `MaxAttempts` | 最大尝试次数（含首次） | `1` |
| `PerAttemptTimeout` | 单次尝试超时；`0` 表示仅受调用方 deadline 约束 | `0` |
| `Backoff` | 重试前固定退避，受剩余预算截断 | `0` |

要点：

- 每个下游拥有独立的熔断器与独立信号量；一个下游占满并发不会阻塞其它下游。
- 槽位获取先非阻塞尝试；已满时在**调用方 ctx** 上等待，ctx 到期则拒绝
  （首次为 `deadline-expired`，重试中为 `budget-exhausted`）。
- 重试必须在每次尝试前**重新获取槽位**，因此重试永远不会突破 `MaxConcurrency`。

## 拒绝原因一览（`gateway.RejectReason`）

| Reason | 何时发生 | 是否终局失败 |
|--------|----------|--------------|
| `route-not-found` | 路由未注册 | 前置拒绝 |
| `deadline-expired` | 首次派发前 deadline 已过期（含等待槽位期间到期） | 前置拒绝 |
| `insufficient-budget` | 剩余预算小于一次配置好的尝试 | 前置拒绝 |
| `circuit-open` | 熔断 Open 或 HalfOpen 探测并发已满 | 前置拒绝 |
| `non-retryable-failure` | 不可重试失败 / 非幂等请求遇到可重试失败 | 终局 |
| `budget-exhausted` | 次数或预算耗尽（含退避期间到期） | 终局 |

## 可观测性

每次尝试与拒绝都会打印日志，字段固定包含：**下游标识、熔断状态、剩余预算、尝试次数、失败分类**，例如：

```
downstream="orders" breaker=closed remaining_budget=180ms attempt=2 class=retryable: attempt failed ... cause="..."
downstream="orders" breaker=open remaining_budget=120ms attempt=1 class=none: reject reason=circuit-open detail=...
```

无调用方 deadline 时 `remaining_budget=infinite`。可通过 `Config.Logger` 注入自定义实现。

## 验证方法

```bash
go test ./... -race -count=20
go vet ./...
```

测试覆盖：

- 熔断状态迁移：连续失败触发、Open 快速拒绝、HalfOpen 探测并发上限、探测成功关闭/失败重开、
  并发上报下无状态错乱（race + 多轮重复）。
- 并发上限：单下游不超限、下游间相互隔离、重试不突破上限。
- 截止时间：deadline 传播不晚于调用方、已过期/预算不足在派发前拒绝并区分原因、
  单次超时到期取消下游 ctx。
- 重试边界：幂等约束、次数耗尽（`budget-exhausted`）、不可重试（`non-retryable-failure`）、
  退避被预算截断、下游原始错误不外泄。

所有用例使用 `-race` 与 `-count=N` 重复执行，结论稳定可复现；
`t.Log` 中可看到下游标识、熔断状态、剩余预算、尝试次数与失败分类。
