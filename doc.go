// Package gateway 实现本地服务调用网关，在调用方与下游实现之间提供
// 一致且可解释的熔断、并发隔离、超时（截止时间传播与单次尝试预算）与重试行为。
//
// 失败语义分为两类：
//   - *RejectError：请求在派发给下游之前被拒绝（调用方截止时间已过期、剩余总
//     预算为正但不足一次单次尝试、熔断打开、探测并发受限、下游并发已满），
//     可通过 RejectReason 区分原因。
//   - *TerminalError：请求已派发但得到不可重试终局（不可重试失败、预算耗尽，
//     或重试途中治理层拒绝），可通过 TerminalKind 区分。下游原始错误永远不会
//     被透出。
//
// 预算语义：
//   - 调用方在 Request.Deadline 给出总截止时间；路由可用 RouteConfig.AttemptTimeout
//     配置“单次尝试预算”。每次尝试实际给下游的时间取两者（调用方有截止时间时）
//     的更小值；调用方未给截止时间时单次尝试预算（及 DefaultTimeout）单独生效。
//   - 配置了单次尝试预算后，每次派发前都做预检：剩余总预算为正但不足一次尝试时，
//     首次直接 RejectError(budget_exhausted)（下游 0 次、不占并发名额），重试途中
//     则 TerminalError(budget_exhausted)。它与 deadline_expired（截止时间已过期）
//     以及“已派发后预算才耗尽”都是不同语义。
//   - 调用方未给截止时间、路由也未配置单次尝试预算与默认超时的调用属于不设限：
//     日志与 RejectError 文本一律表达为 "unlimited"，且不会被任何预算判定拒绝。
package gateway
