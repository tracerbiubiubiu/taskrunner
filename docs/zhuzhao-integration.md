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
   taskrunner 回调 zhuzhao：POST /internal/jobs/<action_id>（task_id + request_id + params）
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
| `PATCH /v1/jobs/{id}` | 改 cron / params / 启停 | |
| `POST /v1/jobs/{id}/trigger` | 手动执行一次（前端「立即执行」） | |
| `GET /v1/tasks/{id}` / `GET /v1/runs?request_id=…` | 查当前状态 / 查执行历史 | **执行结果的唯一出口** |
| `POST /v1/tasks/{id}/cancel` `/retry`、`GET /v1/dead-letters` | 干预与死信管理 | 运维向 |

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
// POST /internal/jobs/:action_id → 查表分发
```

**回调契约**（taskrunner.md §4 摘要，zhuzhao handler 必须遵守）：

- 幂等：重试会重复回调，按 `task_id + request_id` 先查重再执行（「导出 + 删除」类副作用动作尤其必要）；
- 语义：5xx / 超时 → taskrunner 自动重试；4xx → 不重试直接判失败；2xx = **执行完全成功**（✅ 拍板 2026-09-03：无响应体状态字段——zhuzhao handler 业务失败直接映射状态码：不可重试 4xx / 可重试 5xx，走常规 errcode）；
- 全链路关联（2026-09-03 登记，taskrunner 侧小改随 M3）：回调请求带 **`X-Request-ID: <payload.request_id>` 头**（有则带，cron 触发为空则不带）——zhuzhao 入站 RequestID 中间件「接受入站」逻辑直接复用同一 rid，回调链路与 `job_runs` 不再断链；**回调请求带 AK/SK 签名**（C9，覆盖 X-Request-ID / X-Operator / body；zhuzhao `/internal` 验签——同日基线修订）；
- L1 边界：回调执行产生的业务事件由 handler 照常落 L1 `ticket_events`；taskrunner 不写业务事实。

### 2.2 动作清单端点【待选型，M2 前定】

`GET /internal/jobs` 列出已注册 action，供 taskrunner 创建任务定义时校验 `action_id` 存在性。与「提交时探活」二选一。

### 2.3 任务管理功能【必做，对齐 M2/M3】

提交任务、建/改任务定义、手动执行、查执行记录的**接口与前端页面**（代理 taskrunner API）。全部过三层校验后才能调 taskrunner；`request_id` 在此生成并透传（跨查关联键）。

任务完成情况查询的口径（2026-09-04 补充）：

- 前端「排队中 / 执行中 / 已完成 / 失败 / 死信」视图 = 代理 `GET /v1/runs?status=…`（各状态过滤，含 request_id/action/时间组合），单任务详情 = `GET /v1/tasks/{id}`——**数据源是 job_runs（业务语义、终身历史），不是 asynqmon**（那是 M4 运维看板，看队列内部态，无业务过滤、无历史，不能面向用户查询）；
- 若前端需要各状态计数总览卡片：现有端点需按 status 各查一次；可选增强 = taskrunner 加轻量聚合端点（如 `GET /v1/runs/stats` 返回各状态计数），做页面时按需启用，暂不预先实现。

### 2.4 部门可见性策略【必做，对齐 M2/M3】

- 「哪个部门 / 角色能看、能管哪些归属标签」的**策略模型 + 存储（zhuzhao DB 自有表，与用户/部门/角色模型放一起）+ 管理功能**；
- 由三层校验消费——决定传给 taskrunner 的 `dept` 过滤参数与写权限放行；
- 创建 job 定义时写入归属标签；策略只存在于 zhuzhao，taskrunner 不感知。

### 2.5 任务提交日志【必做，对齐 §7 日志边界】

`{ action, task_id, request_id }` 落 zhuzhao（提交凭证，薄）；执行细节不回传，靠 request_id 跨查。

### 2.6 终败通知端点【后置】

`POST /internal/notifications/task-dead` 接收死信通知——仅当 taskrunner 侧启用终败通知（taskrunner.md §4，触发条件：无人盯守的周期任务需业务方知情）时需要，当前不做。

## 3. 里程碑对齐建议

| taskrunner 里程碑 | zhuzhao 侧需要就绪 |
|---|---|
| M2 HTTP API | 2.2 选型定案；2.3 / 2.4 开发中（联调依赖） |
| M3 首个预置动作 | 2.1（`audit_archive` handler + 端点）；2.5 提交日志；对齐 zhuzhao `docs/phase3/03-audit-l2.md`（B11②） |
