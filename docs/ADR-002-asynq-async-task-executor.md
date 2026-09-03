# ADR-002: 引入 Asynq 作为"异步任务执行器"，不替代 L1 事件源

> 快照注记：本文为 zhuzhao 仓库 ADR-002 的快照副本，**zhuzhao 侧为 SSOT**；文中相对路径（`adr/`、`phase3/`、`modules/`）均指向 zhuzhao 仓库，非本仓库。2026-09-03 形态定稿的「独立 Redis」口径见 taskrunner.md §8。

## 日期
2026-08-25

## 状态
已采纳

## 背景
业务场景明确后，需要回答"Asynq 怎么用"：
- **场景 A（事件触发）**：对接数据自动/手动创建工单 → 审批通过 → 自动触发某个事件（如数据同步、外部系统回调、启动另一工作流）
- **场景 B（定时触发）**：预置任务周期性触发（如每天扫描过期工单、每周报表）

讨论点：Asynq 处理"审批通过触发事件 + 预置定时任务"是否有问题？结论：本质都是"执行异步任务"，正是 Asynq 主场；但必须厘清 Asynq 与 L1 的边界，否则会出现"事件源落到 Redis、崩溃丢事实"的坑。

## 决策
- **引入 Asynq 作为"异步任务执行器"**，复用现有 Redis（辅助角色，不新增基础设施）。
- **Asynq 负责"执行异步任务"**：场景 A 的"触发事件"动作（数据同步/外部回调/启动任务）、场景 B 的"预置定时任务"（asynq.PeriodicTask / Scheduler）。
- **L1 `ticket_events` 仍负责"记录事件事实"**：审批 `submit` 成功的**同一事务内**先落 L1 事件（持久化、可重放、崩溃不丢），再由消费者把"要触发的事件"转成 Asynq task 入队。**不要让 Asynq 承担事件持久化/重放职责（那是 L1 的活）。**
- **职责切分铁律**：L1 是"事件事实源"，Asynq 是"任务执行器"。审批通过先落 L1，消费者再 Enqueue Asynq task；消费者必须在 L1 事务提交后再 Enqueue，避免"事件未落库就触发任务"。
- **工作流实例启动本身归 `BranchedStateEngine`**：若"触发事件"指启动另一工作流实例，Asynq 只负责"入队一个启动任务"，真正状态推进仍由引擎内核完成，不让 Asynq 变成隐式流程引擎。
- 场景 B 定时任务（SLA 扫描、报表刷新）直接由 Asynq Scheduler 接管，替代原"进程内定时器"（2.3 的 60s ticker），多实例下 Asynq 天然单点调度无需自写分布式锁。

## 理由
- 场景 A/B 的本质都是"执行异步任务 / 周期触发"，Asynq 自带重试、定时、优先级，比自写 goroutine 定时器 + 重试更可靠、更省代码。
- Redis 已是项目辅助组件，用作任务执行器后端不新增基础设施；任务执行失败可重试、不丢"事实"（事实在 L1）。
- L1 与 Asynq 职责互补：L1 保证"审批通过"事实不丢可审计，Asynq 保证"触发数据同步"等耗时任务异步执行可重试——两者不冲突反而增强可靠性。
- 不把 Asynq 当事件总线：避免 Redis 升为事件关键路径、避免事件丢失无法重放（这是 ADR-001 已固化的红线）。

## 后果
- 正面：零新增基础设施；异步任务有重试/定时/可观测；定时任务免去自写定时器与分布式锁；L1 事件可重放，Redis 挂了事实仍在。
- 负面/风险：Redis 作为任务执行器后端需保证可用（当前已作辅助组件，影响可控）；L1 事件 → Asynq 入队需在事务提交后，有极小窗口需保证幂等（任务 handler 按 ticket_id + event_id 幂等）。
- 后续跟进：多消费者/异步邮件需求出现时启动 L2（Outbox + Asynq worker 消费，L1 直接换调度器）；拆微服务时评估 Kafka。

## 典型用例

| 用例 | 类型 | 触发方式 | 机制 | 说明 |
|------|------|---------|------|------|
| **场景 A：审批通过触发事件** | 事件触发 | L1 消费者收到 `ticket.approved` → `asynq.Enqueue` | 异步任务 | 数据同步 / 外部回调 / 启动另一工作流实例；不在审批事务内同步执行耗时动作 |
| **场景 B：预置定时任务** | 定时触发 | `asynq.PeriodicTask` / Scheduler | 周期任务 | 每日过期扫描、每周报表等；替代进程内 ticker，多实例天然单点调度 |
| **场景 C：SLA 违约告警+升级** | **A + B 组合态** | Scheduler 周期入队 `sla:scan` → 命中违约 → `Enqueue("sla:breach")` | 定时扫描 + 事件触发 | `sla:scan` 只探测标记 `breached_*`，worker 落 L1 事件 `ticket.sla_breached` 后驱动通知/升级（改派/升优先级/加签）。详见 zhuzhao 仓库 `phase3/10-ticket-business.md` §2.5（违约处理链：SLA 扫描 → asynq `sla:breach` → 通知升级） |
| **场景 D：消息通知异步发送** | 异步触发（A 延伸） | L1 事件消费 / `sla:breach` worker → `Enqueue("notify:send")` | 异步任务 | 站内通知写 `notifications` 表 + 邮件 SMTP 发送丢 Asynq worker 异步发，失败自动重试不阻塞主链路。通知既可作为**独立能力**（订阅 `ticket_events` 各类事件：分派/状态变更/SLA 违约），也是场景 A/C 的下游副作用。详见 zhuzhao 仓库 `phase3/10-ticket-business.md` §3.4（L2 升级暂缓、按需；Phase 3 L2 升级时迁 Asynq worker） |

> **场景 C / D 的本质**：C 同时用 B+A；**D 是 A 的延伸——把"发通知"这个耗时/易失败（SMTP 网络 IO）动作异步化**，正是 Asynq 的主场能力，用户明确后续会集成该能力。二者都不引入新机制，统一守"铁律"：L1 先落事实，消费者事务提交后再 Enqueue，handler 按业务键（`ticket_id + event_id` 或 `notification_id`）幂等。

## 关联文档
- zhuzhao 仓库 `adr/ADR-001-event-mechanism-l1-steady-state.md`（L1 事件源作为长期稳态，Asynq 不替代）
- zhuzhao 仓库 `phase3/10-ticket-business.md` §4.5(3)（审批完成与 L1 事件联动）、§4.6（Asynq 职责切分与场景落地）、§2.5（SLA 违约链路：场景 C 组合态）、§3（通知服务，场景 D 消息通知异步化）
- zhuzhao 仓库 `modules/ticket.md` §6（三档事件机制 L0/L1/L2 概述）
