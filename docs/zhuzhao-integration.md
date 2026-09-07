# zhuzhao 侧配套需求（接入 taskrunner）

> 本文档 = [taskrunner 设计](./taskrunner.md)引出的 **zhuzhao 侧工作项清单**，供 zhuzhao 仓库排期与实现参考。
> taskrunner 侧设计 SSOT = `taskrunner.md`；zhuzhao 侧排期 SSOT = zhuzhao `docs/phase3/13-implementation-plan.md`。
> 2026-09-03 从 taskrunner.md §11 拆出成档。

## 0. 交互总览

```
用户 → zhuzhao（三层校验 + 业务审计）
     │ 发起：提交任务 / 建·改定义 / 手动执行 / 查结果
     ▼
   taskrunner API（AK/SK HMAC 签名；写接口显式带 actor 工号 + source_ip）
     │ cron 到点 / 出队
     ▼
   taskrunner 回调 zhuzhao：POST /internal/jobs/callback（body：task_id + request_id + action_id + params——C10 约定化，action_id 不再走 URL 路径，2026-09-07）
     │ zhuzhao 查注册表执行 handler，HTTP 响应即单次结果
     ▼
   taskrunner 记 job_runs（详细过程只在 taskrunner，不回传）

zhuzhao 查执行结果：GET /v1/tasks/{id} / GET /v1/runs?request_id=…（拉取，taskrunner 不推送）
```

职责一句话：zhuzhao 负责「发起、下发、按需查」；调度、重试、死信、记录、看板全在 taskrunner。

## 1. zhuzhao 会用到的 taskrunner API（v1）

| 端点 | 用途 | 备注 |
|---|---|---|
| `POST /v1/tasks` | 提交一次性任务 | 受理语义：入队即返回 task_id，受理 ≠ 执行成功；自带幂等（调用方生成 task_id） |
| `POST /v1/jobs` / `GET /v1/jobs?dept=…` | 建 / 列任务定义（cron 或手动 + params + 归属标签） | 部门差异化展示 = 按标签过滤 |
| `POST /v1/jobs/update`（原 `PATCH /v1/jobs/{id}`，C10） | 改 cron / params / 启停（body 带 job_id） | API 约定见下 |
| `POST /v1/jobs/trigger`（原 `POST /v1/jobs/{id}/trigger`，C10） | 手动执行一次（前端「立即执行」；body 带 job_id） | |
| `GET /v1/tasks/{id}` / `GET /v1/runs?request_id=…&dept=…` | 查当前状态 / 查执行历史 | **执行结果的唯一出口**；**C11：task 响应补 `dept` 字段、runs 加 `dept` 多值过滤**（按 `runs.dept` 快照列，一次性任务提交时由 zhuzhao 携带 dept；zhuzhao E-⑤ 可见性前提，2026-09-04 登记 / 09-07 快照语义） |
| `POST /v1/tasks/cancel` `/retry`（原 `/v1/tasks/{id}/cancel` `/retry`，C10）、`GET /v1/dead-letters` | 干预与死信管理（body 带 task_id） | 运维向 |

**API 设计约定（2026-09-04 所有者拍板，SSOT = zhuzhao 16 号 §9）**：方法仅 GET/POST；POST URL 不携带业务信息（标识/参数全在 body；GET path/query 不受限）。上表 C10 改造随 M3 联调前实施，C11 随 zhuzhao E-⑤ 实施。

所有请求带 **AK/SK HMAC 签名**（zhuzhao client 以自身 SK 签名，taskrunner 验签——2026-09-03 基线修订，覆盖当日早前「零认证/静态 Bearer 过渡」口径；utils `aksk`，基线 SSOT = zhuzhao 16 号 §9）；**写接口**（提交 / 建改定义 / 触发 / 取消 / 重试）显式携带 `actor`（调用人工号）与 `source_ip`——taskrunner 原样存档仅作审计归因（actor 入签名覆盖，不可伪造）。

## 2. 需求清单

### 2.1 动作 handler 注册表 + 内网端点【必做，M3 前完成】

zhuzhao 用「接口 + 唯一 `action_id` 注册表」实现动作分发（`action_id` 即跨仓库契约）：

```go
// 示意
type JobHandler interface{ Handle(ctx context.Context, params json.RawMessage) error }

var registry = map[string]JobHandler{
    "audit_archive": auditArchiveHandler{},
}
// POST /internal/jobs/callback → 按 body.action_id 查表分发（C10 约定化，2026-09-07）
```

**回调契约**（taskrunner.md §4 摘要，zhuzhao handler 必须遵守）：

- 幂等：重试会重复回调，按 `task_id + request_id` 先查重再执行（「导出 + 删除」类副作用动作尤其必要）；
- 语义：5xx / 超时 → taskrunner 自动重试；4xx → 不重试直接判失败；2xx = **执行完全成功**（✅ 拍板 2026-09-03：无响应体状态字段——zhuzhao handler 业务失败直接映射状态码：不可重试 4xx / 可重试 5xx，走常规 errcode）；
- 全链路关联（2026-09-03 登记，taskrunner 侧小改随 M3）：回调请求带 **`X-Request-ID: <payload.request_id>` 头**（有则带，cron 触发为空则不带）——zhuzhao 入站 RequestID 中间件「接受入站」逻辑直接复用同一 rid，回调链路与 `job_runs` 不再断链；**回调请求带 AK/SK 签名**（C9，覆盖 X-Request-ID / X-Operator / body；zhuzhao `/internal` 验签——同日基线修订）；
- L1 边界：回调执行产生的业务事件由 handler 照常落 L1 `ticket_events`；taskrunner 不写业务事实。

### 2.2 动作清单端点【已定案 2026-09-07：可选增强，不实现】

~~待选型，M2 前定~~ ✅ **taskrunner §10 定案（M2）**：**不做前置校验**——不存在 / 未注册的 action 经回调 4xx 快速失败（non-retryable，failed 可见）。`GET /internal/jobs` 保留为可选增强（如动作清单展示页再启用）。

### 2.3 任务管理功能【必做，对齐 M2/M3】

提交任务、建/改任务定义、手动执行、查执行记录的**接口与前端页面**（代理 taskrunner API）。全部过三层校验后才能调 taskrunner；`request_id` 在此生成并透传（跨查关联键）。

任务完成情况查询的口径（2026-09-04 补充）：

- 前端「排队中 / 执行中 / 已完成 / 失败 / 死信」视图 = 代理 `GET /v1/runs?status=…`（各状态过滤，含 request_id/action/时间组合），单任务详情 = `GET /v1/tasks/{id}`——**数据源是 job_runs（业务语义、终身历史），不是 asynqmon**（那是 M4 运维看板，看队列内部态，无业务过滤、无历史，不能面向用户查询）；
- 若前端需要各状态计数总览卡片：现有端点需按 status 各查一次；可选增强 = taskrunner 加轻量聚合端点（如 `GET /v1/runs/stats` 返回各状态计数），做页面时按需启用，暂不预先实现。

### 2.4 部门可见性策略【必做，对齐 M2/M3】

- 「哪个部门 / 角色能看、能管哪些归属标签」的**策略模型 + 存储（zhuzhao DB 自有表，与用户/部门/角色模型放一起）+ 管理功能**；
- 由三层校验消费——决定传给 taskrunner 的 `dept` 过滤参数与写权限放行；
- 创建 job 定义时写入归属标签；策略只存在于 zhuzhao，taskrunner 不感知；
- **范围补全（2026-09-04 / 09-07 快照语义）**：可见性覆盖 job 定义**与执行记录**两层——`GET /v1/runs` 需支持 `dept` 多值过滤（**按 `runs.dept` 快照列**，一次性任务提交时由 zhuzhao 携带 dept）、task 查询响应需携带 `dept`（C11），否则 job 定义隔离被执行记录查询旁路。org code 不可变（zhuzhao Update 无 code 列）为「org code 即标签值」的稳定性前提。

### 2.5 任务提交日志【必做，对齐 §7 日志边界】

`{ action, task_id, request_id }` 落 zhuzhao（提交凭证，薄）；执行细节不回传，靠 request_id 跨查。

### 2.6 终败通知端点【后置】

`POST /internal/notifications/task-dead` 接收死信通知——仅当 taskrunner 侧启用终败通知（taskrunner.md §4，触发条件：无人盯守的周期任务需业务方知情）时需要，当前不做。

## 3. 里程碑对齐建议

| taskrunner 里程碑 | zhuzhao 侧需要就绪 |
|---|---|
| M2 HTTP API | 2.2 选型定案；2.3 / 2.4 开发中（联调依赖） |
| M3 首个预置动作 | 2.1（`audit_archive` handler + 端点）；2.5 提交日志；对齐 zhuzhao `docs/phase3/03-audit-l2.md`（B11②） |

## 4. 典型用例：审批通过 → activelist 加值 + 业务平台操作（2026-09-04 记录）

**场景**：用户发起工单 → 审批通过 → 自动 ① 向 activelist 指定类别添加值，② 把添加的值经 taskrunner 发起任务，调用预置好的能力去对应业务平台操作。

**链路映射（现有机制，零架构改动）**：

| 步骤 | 落点 | 机制 |
|---|---|---|
| 用户发起工单 | zhuzhao 工单领域 | 现成 |
| 审批通过 | zhuzhao **单库事务**：更新工单状态 + 落 L1 `ticket_events`（`ticket.approved`） | ADR-001/ADR-002 |
| 向 activelist 加值 | **zhuzhao 经网关调 activelist API**（activelist = 数据层 CRUD，独立库、网络仅 zhuzhao 可达、无业务 handler，不作为 taskrunner 回调对端） | activelist ADR-003 |
| 下发任务 | zhuzhao 调 taskrunner `POST /v1/tasks`（`action` + `params` 携带加值结果 / `request_id` 跨查） | taskrunner.md §5 |
| 业务平台操作 | taskrunner 可靠执行 → 回调预置动作端点 → **组合 handler** 串行「调 activelist 加值 → 拿值调业务平台」 | 三层模型 + 回调契约 |

**事务性约束（2026-09-04 确认）**：「activelist 加值」与「下发 taskrunner 任务」须保持一致性——跨 activelist / taskrunner 两个独立系统，无法单库强事务（2PC 跨 HTTP 异构系统不可用、补偿 Saga 过度），采用 **L2 事务性 Outbox**（ADR-001 预留演进路径；design-decisions §16）：

- zhuzhao 审批事务内**原子写入**：业务状态 + L1 事件 + **Outbox 两条命令**（① `activelist:add_value` ② `taskrunner:submit`），崩溃不丢、同生共死；
- **Outbox Relay**（zhuzhao 异步）至少一次投递：activelist 加值（**幂等键去重**）+ taskrunner 提交（**`task_id` 终身幂等**，Submit 先查 job_runs，重放安全）；任一投递失败 Outbox 行保留、Relay 重放，**最终一致、不丢**；
- 取取舍：最终一致（非强原子）——该场景无「同时读两状态」诉求，足够；
- **唯一新设计点**：activelist 加值需业务唯一键幂等（activelist 是「每类型一张表 + data JSONB」，需业务唯一键约束或 zhuzhao 先查重再写）；
- 本场景即 **L2 Outbox 落地触发信号**（「多消费者」= activelist + taskrunner 两个下游），落地全在 zhuzhao 侧（Outbox 表 + Relay），taskrunner / activelist 零改动。

