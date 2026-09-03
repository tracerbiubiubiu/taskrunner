# taskrunner — 事件/任务总线（异步触发预置动作）

> 独立仓库、独立部署、独立 Redis、独立 DB。zhuzhao 的「异步执行侧」：zhuzhao 只作网关（鉴权/编排/业务审计），taskrunner 负责可靠调度、触发与执行回调，并提供内部 HTTP API（提交任务 / 查询执行记录 / 死信管理）。
> 设计 SSOT = 本文档；决策依据 = [ADR-002](./ADR-002-asynq-async-task-executor.md)（快照，zhuzhao 侧为 SSOT）。
> 2026-09-03 收敛 + 形态定稿；同日增补定稿：提交方式（调 API）、任务三层模型与定义 API 管理、HTTP API v1（含 jobs 组与结果查询语义）、job_runs 落库与两路日志分工、审计透传与归属过滤、asynqmon 库嵌入、实施计划 M1–M4、zhuzhao 侧配套清单。

---

## 1. 定位

**taskrunner = 事件/任务总线**：把"异步触发预置动作"做成独立能力。业务点在 zhuzhao 显式提交任务，taskrunner 保证任务**可靠执行**（不丢、可重试、可定时、可查），动作本身（业务 handler）仍在 zhuzhao 侧实现。

一句话：**zhuzhao 负责"决定要做什么"，taskrunner 负责"可靠地去做"。**

## 2. 与 zhuzhao 的关系

```
[用户] → zhuzhao（网关：鉴权/编排/业务审计）
              │ 调 taskrunner API 提交任务（✅ 定稿，见 §5）
              ▼
      taskrunner（独立仓库/部署/Redis/DB）
       ├── HTTP API server（任务定义管理 / 提交 / 查询 / 取消 / 重试 / 死信）
       ├── Asynq worker + Scheduler
       ├── 消费任务 → HTTP 回调 zhuzhao 内网端点（携带 task_id + 参数 + request_id）
       ├── 失败重试 / 超时 / 死信
       └── 自维护 job_runs（独立 DB，执行记录，可经 API 查询）
```

- zhuzhao = 网关 + 业务归属（预置动作 handler 在 zhuzhao）；taskrunner = 通用调度/触发/重试运行时；
- **不反向依赖**：taskrunner 不 import zhuzhao 代码，只通过 HTTP 回调执行预置动作、通过 API 接收任务；
- 任务提交方式 ✅ **定稿（2026-09-03）**：**调 taskrunner API 提交**（原 Open Question「直连 Redis Enqueue vs API」随「taskrunner 提供内部 HTTP API」一并落定；直连 Redis 不作为对外契约）。

## 3. 职责边界

**做**：
- 可靠任务队列（Asynq，Redis AOF 持久化，重启不丢）；
- 定时调度（Scheduler / PeriodicTask，cron）；
- 触发 → HTTP 回调 zhuzhao 内网端点执行预置动作；
- 重试 / 超时 / 阻塞策略 / 死信；
- 自维护 `job_runs`（独立 DB：执行细节、重试次数、耗时、结果、回调目标，见 §6）；
- 内部 HTTP API（服务级鉴权）：提交、查询、取消/重试、死信管理、健康检查（见 §5）。

**不做**（2026-09-03 收敛）：
- ~~用户上传自定义脚本（python/shell）~~ → **🚦 降级**，按需再评估（选型参考 zhuzhao 仓库 `docs/phase3/15-script-platform-dagu-vs-inhouse.md`，见 §11）；
- ~~业务 handler~~ → 回调 zhuzhao，业务代码留在 zhuzhao；
- ~~事件源/事件流~~ → 事件事实源仍是 zhuzhao 的 L1 `ticket_events`（ADR-001），taskrunner 是执行侧不是事件源；
- 不替代任何消息中间件选型（Redis Pub/Sub 不持久化已排除、Kafka 对当前量级过度，见 ADR-001/002）。

## 4. 预置动作与任务定义（三层模型，2026-09-03 定稿）

"预置 task" 拆成三层，各归其位：

| 层 | 是什么 | 归属 | 变更方式 |
|---|---|---|---|
| **动作（action）** | 业务 handler，全局唯一 `action_id` | zhuzhao 代码：内网端点 `/internal/jobs/<action_id>` + handler 注册表 | 改代码发版 |
| **任务定义（job）** | `action_id + 触发方式（cron / 手动）+ params + enabled + 归属标签（如 dept，zhuzhao 写入）` | taskrunner **DB**（定稿：API 管理，不再用配置文件） | 运行时调 API（前端经网关操作） |
| **执行实例（run）** | 每次实际执行（含每次重试） | taskrunner `job_runs`（§6） | 自动产生 |

- 两层靠 **`action_id` 关联**：taskrunner 只认 id 和调度元数据，不感知函数；zhuzhao 侧用「接口 + 唯一 id 注册表」实现动作分发——

  ```
  // zhuzhao 侧（示意）
  type JobHandler interface{ Handle(ctx, params) error }
  var registry = map[action_id]JobHandler{ "audit_archive": …, … }
  // /internal/jobs/<action_id> → 查表分发执行
  ```

- **触发链路**：cron 到点 / `trigger` / `POST /v1/tasks` → taskrunner 按 job 定义组装回调（action_id + params + request_id）→ zhuzhao 查注册表执行 handler → 结果记 `job_runs`；
- **前端入口**（若有）：执行 / 加定时 / 启停的页面在 zhuzhao，链路 = 前端 → 网关（鉴权 + 业务审计）→ taskrunner API；cron 自动触发无用户参与，只记 `job_runs`；
- **调用人上下文透传（2026-09-03 补充）**：zhuzhao 调写接口（提交 / 建改定义 / 触发）时显式携带 `actor`（工号）与 `source_ip` 等原始信息，taskrunner **原样存档**（job 定义记 `created_by`；执行记录记 `submitted_by` / `source_ip`，cron 触发为空）并随 slog 打点（caller + actor + IP + request_id）——仅作审计归因，taskrunner 不校验、不据其做权限判断（信任边界在网关）；
- **归属过滤（2026-09-03 补充）**：job 定义的归属标签（如 `dept`）对 taskrunner 是不透明字符串，`GET /v1/jobs?dept=…` 按值过滤，实现「不同部门看到不同预置任务」；部门语义与「能看 / 能管哪些」的权限全归 zhuzhao（策略存 zhuzhao 自有 DB，见 §11 配套清单），taskrunner 不建组织模型；跨部门**写保护**随多调用方时代再启用（同 §5 `created_by` 口径）；
- **新增一个可调度动作**：① zhuzhao 新增 handler 并注册进注册表（发版一次）；② 调 `POST /v1/jobs` 建任务定义（运行时，无需再动 taskrunner）；
- 内置例外（后置，按需再启）：与业务无关的通用动作（如 ping URL、清理自身数据）可在 taskrunner 内实现同款「接口 + id 注册表」，进程内直接执行、不回调 zhuzhao。

### 回调契约（2026-09-03 补充）：
- **幂等**：任务重试可能重复回调，zhuzhao handler 须按 `request_id`/`task_id` 幂等（先查重再执行）；对「导出 + 删除」类有副作用的动作尤其必要；
- **语义**：at-least-once——幂等是 zhuzhao 侧义务，不是 taskrunner 承诺；
- **结果归属**：回调的 HTTP 响应即本次执行结果，handler 执行侧当场感知成败（仅此一次同步感知）；对提交方**不推送**，任务级执行结果一律经 §5 查询接口获取，详细过程只在 taskrunner `job_runs`，不回传、不复制（§7）；
- **终败通知（后置，按需启用）**：默认不做任何结果推送——执行结果一律由 zhuzhao 按需查询（§5）；死信的知情路径默认是 M4 死信告警（运维侧）与 `/monitor` 看板。仅当出现「无人盯守的周期任务、失败需业务方主动知情」的场景时，再启用死信回调通知（如 `POST /internal/notifications/task-dead`，带 task_id / request_id / 最后错误）；
- **重试判定**：5xx / 超时 → 按 Asynq 退避策略重试；4xx → 不重试，直接判失败；HTTP 2xx = 受理成功，业务级失败用响应体状态字段表达（避免"200 但失败"无法归因，字段定义落地时定）；
- **超时**：默认值落地时定（建议 30s），可按 action 覆盖；
- **L1 边界**：回调执行产生的业务事件（如 SLA 违约）仍由 zhuzhao 侧 handler 落 L1 `ticket_events`；taskrunner 只触发回调、**不写业务事实**；
- **安全边界**：独立部署下需明确内网可达性（如同 VPC）与回调鉴权（token/签名，防伪造回调），具体机制随首个预置动作落地细化。

## 5. HTTP API（内部，v1 范围定稿）

taskrunner 形态 = **Asynq worker + 常驻 HTTP server**（同进程部署，拆分留待需要时再议）。

- 暴露边界：**仅内网 + 服务级鉴权**（✅ 已实现：静态 Bearer token，env `TASKRUNNER_API_TOKEN` 注入，常量时间比较；未设置时拒绝启动）。分工：**用户权限（zhuzhao 三层校验）全部收敛在网关**——前端所有操作先过网关授权，通过后才代理调 taskrunner API；taskrunner 只认证「调用方服务」，不认证用户、不复刻权限模型；
- 调用方身份：按调用方发 credential（token 携带 caller id；当前仅 zhuzhao，设计预留多服务）；API 写操作（建/改定义、触发、取消、重试）在 slog 打点归因（caller + actor + IP + request_id）；`job` 表记 `created_by`（工号，§4 透传，审计归因）与 `owner_service`（来自 credential，当前恒为 zhuzhao）——跨服务归属约束待第二个调用方出现再启用，当前不实现；
- 响应结构 / 错误码复用 zhuzhao-utils `errcode` + `response`，与网关风格一致。

| 端点 | 用途 |
|---|---|
| `POST /v1/tasks` | 提交任务；**受理语义**：校验通过 + 写入队列（Redis AOF）即返回 task_id，「受理 ≠ 执行成功」；**幂等**：接受调用方生成的 `task_id`，重复提交去重 |
| `GET /v1/tasks/{id}` | 查任务状态 |
| `GET /v1/runs?request_id=&action=&status=&from=&to=` | 查执行记录（§7 日志边界的「request_id 跨查」即此） |
| `GET /v1/jobs?dept=&action=&enabled=` / `POST /v1/jobs` | 列出（支持按归属标签等过滤）/ 新增任务定义（action_id + cron 或手动 + params + enabled + 归属标签，§4） |
| `PATCH /v1/jobs/{id}` | 修改任务定义：cron / params / 启停 |
| `POST /v1/jobs/{id}/trigger` | 手动执行一次（按定义提交任务，前端「立即执行」按钮） |
| `POST /v1/tasks/{id}/cancel` | 取消未开始的任务 |
| `POST /v1/tasks/{id}/retry` | 重试失败/死信任务 |
| `GET /v1/dead-letters` | 死信列表（配合 Asynq Inspector 重放，死信处理闭环） |
| `GET /healthz` | 健康检查 |

**结果获取模型（2026-09-03 定稿）**：提交响应只回答「是否受理」；执行是否成功，一律通过 `GET /v1/tasks/{id}` / `GET /v1/runs` **按需查询**——查询是执行结果的唯一出口，taskrunner 不主动推送（终败通知为后置项，§4）。

**明确后置**（不算缺口）：全文检索、聚合统计、深分页——当前无日志平台，临时查库；未来上 ES 等平台时归平台（演进路径见 §6）。

## 6. job_runs 存储与日志分工（2026-09-03 定稿）

**执行记录落库**（不存文件、不依赖 Asynq/Redis 自带记录——Redis 只保队列运转，撑不起按 request_id/action/时间段的查询）：

- 存储：**独立 DB**，SQLite 起步（嵌入式、单机零运维、当前量级足够）；量级/部署升级再迁 Postgres，表结构不变。起步直接 `database/sql` + 驱动，不为 SQLite 抽公共包；
- 最小 schema：`task_id`、`request_id`、`action`、`status`（pending / running / succeeded / failed / dead）、`attempts`、`callback_url`、`error`、`duration_ms`、`submitted_by` / `source_ip`（zhuzhao 透传的原始调用人，cron 触发为空，仅审计归因）、`enqueued_at` / `started_at` / `finished_at`；
- 存储层代码放**本仓库 `internal/store`**：job_runs 是 taskrunner 领域 schema，**不放 zhuzhao-utils**（utils 只收通用件；出现第二个同类消费者再考虑下沉）；
- 保留策略 ⚠️ 落地时定（建议：保留期可配置 + 定时清理，思路同审计归档；`submitted_by` / `source_ip` 属个人信息，同受保留期约束）。

**两路日志分工**（"日志"是两件事）：

| 路 | 内容 | 载体 | 消费方式 |
|---|---|---|---|
| 执行事实 | task_id / request_id / action / 状态 / 重试 / 耗时 / 结果摘要 | `job_runs` 表 | 查询接口（§5） |
| 过程细节 | 回调请求/响应报文、堆栈、debug 输出 | 应用日志文件（zhuzhao-utils `logger`：slog JSON + lumberjack 轮转） | 人工排障（grep 文件）；后续采集入 ES（见下） |

- 关联约定：任务执行链路统一 `slog.With("request_id", …, "run_id", …)` 打点——表里查到一条记录，拿 id 即可在日志文件中定位完整细节；
- **ES 演进路径（2026-09-03 确认：当前无日志平台，先落文件）**：过程日志即 slog **JSON Lines**——一行一条结构化记录，字段即索引结构，上 ES 时只需加 Filebeat / Vector 等 shipper tail 日志文件（处理轮转）→ ES / Loki，**taskrunner 应用代码零改动**；前提是打点字段保持稳定命名（`request_id` / `run_id` / `action` / `task_id`），M1 起即按此约束写打点。

## 7. 日志边界（2026-09-03 确认）

| 层 | 记什么 | 归属 |
|---|---|---|
| 业务审计 | 用户/网关侧业务操作 | zhuzhao `audit_logs` |
| 任务提交日志 | `{ action, task_id, request_id }` | zhuzhao（提交凭证，薄） |
| 任务运行日志 | 执行细节/重试/耗时/结果 | taskrunner 自维护 `job_runs`（独立 DB，见 §6），**不传回 zhuzhao** |

- 两边以 **`request_id` 关联**（zhuzhao 提交任务时生成/透传，taskrunner 在 job_runs 记下）；
- 需要追溯时跨查：zhuzhao 以 request_id 调 taskrunner `GET /v1/runs?request_id=…`（§5），**不复制**对方数据；
- 跨查接口只暴露内网，用户侧查询经 zhuzhao 网关代理。

## 8. 部署形态（2026-09-03 定稿）

- **独立仓库**（本仓库，可独立部署）；
- **独立部署**（独立进程/容器，不与 zhuzhao 布一块——zhuzhao 只作网关调用各能力、拉起各任务）；
- **独立 Redis**（Asynq 队列归属 taskrunner，能力自包含，同 activelist 独立库原则）。口径：指 Redis 命名空间/库独立归属 taskrunner，**不必然新增一套 Redis 部署**，与 ADR-002「复用现有 Redis、不新增基础设施」不冲突，按部署环境落地；
- **独立 DB**（`job_runs` 归 taskrunner 自有，SQLite 起步——嵌入式单文件，无新增部署负担；同独立库原则）；
- **Docker 部署（2026-09-03 确认）**：单容器单进程（API server + Asynq worker + Scheduler 同进程，§5）；挂载卷：SQLite 数据目录 + 日志目录（轮转文件持久在宿主，供排障与后续采集）；配置走环境变量。SQLite 单写者 → **单副本部署**，需水平扩容（多副本 worker）时先迁 Postgres；`/monitor` 随容器同端口暴露，仅内网可达；
- 公共工具统一引自 [zhuzhao-utils](https://github.com/tracerbiubiubiu/zhuzhao-utils)：`logger`（应用日志）、`postgres`（迁 PG 时）、`errcode` + `response`（API 统一响应）；`redis` 包用不上（Asynq 走自己的 `RedisClientOpt`）。依赖 utils（独立通用工具库）**不属于**「不反向依赖」的禁止范围。

## 9. 首个预置动作（M-E 验收入口）

> 代号说明：B11② = zhuzhao 排期中的功能项编号；M-E = 里程碑验收入口。出处见 §11 关联文档。

- **审计归档（B11②）**：定时（如每日）回调 zhuzhao，导出 `audit_logs` 超期数据为 JSONL 并删除（保留期可配置）；
- 退出标准：归档任务按周期跑通；预置动作「触发 → 回调执行 → 失败重试」闭环；关联验收见 zhuzhao 仓库 `docs/phase3/03-audit-l2.md`（B11② 设计，见 §11 关联文档）。

## 10. 实施计划（2026-09-03 整理）

整体排期 SSOT 仍是 zhuzhao `docs/phase3/13-implementation-plan.md`（M-E 行）；本表只定**模块内顺序**。

| 里程碑 | 内容 | 出口标准 |
|---|---|---|
| **M1 核心运行时** | Asynq worker + Scheduler；回调客户端（超时 / 5xx·4xx 判定 / 退避重试）；`job_runs` 落库（SQLite + `internal/store`）；`/healthz`；入队暂以 CLI / 测试入口触发 | 任务能从入队走到回调并正确记录，失败按策略重试，死信可查 |
| **M2 HTTP API** | §5 全部 v1 端点（含任务定义 jobs 组、归属标签过滤）；内网 credential 鉴权；调用人上下文字段（`actor` / `source_ip`）；`errcode`/`response` 统一响应；提交幂等（task_id 去重） | zhuzhao 可走 API 建任务定义、提交任务，并按 request_id 查到执行记录 || **M3 首个预置动作** | 审计归档（B11②）：创建任务定义（job）+ zhuzhao `/internal/jobs/<action>` 端点 | 「触发 → 回调执行 → 失败重试」按周期闭环跑通（对齐 zhuzhao `docs/phase3/03-audit-l2.md`） |
| **M4 运维完善** | 指标（队列深度/成功率/回调延迟）；死信告警；asynqmon **以库嵌入 API server**（挂 `/monitor`，置于内网 token 之后，read-only 起步，前端随包内嵌；入队配 `asynq.Retention` 短期留观——Redis 里那份只作近期观察，长期事实以 `job_runs` 为准）；`job_runs` 保留清理 | 异常可感知、死信有出口、存储有界；运维看板可用且不裸暴露 |

模块内顺序 **M1 → M2 → M3**，M4 可与 M3 并行。

⚠️ 随 M2/M3 落地细化（M2 已定案部分随实现入档）：
- ~~动态 cron 实现方式~~ ✅ M2 定案：**分钟级 tick 扫 DB**（`cronloop`，默认 30s 轮询）——定义是 DB 数据、增改停启下个 tick 生效；宕机错失的触发重启后至多补一次。未选 asynq Scheduler 热重注册：静态 payload 撑不起「每次触发生成新 task_id + 审计字段」；
- ~~credential 具体形式~~ ✅ M2 定案：**静态 Bearer token**（`TASKRUNNER_API_TOKEN`）；多调用方时代再升级签名 / mTLS；
- ~~`GET /v1/tasks/{id}` 状态数据源~~ ✅ M2 定案：**job_runs 为主 + Asynq Inspector 补充 in-flight 实时态**（pending/active/retry/scheduled 只在 Redis，以 `live_state` 字段并返回）；
- ~~action 校验方式~~ ✅ M2 定案：**不做前置校验**——不存在 / 未注册的 action 经回调 4xx 快速失败（non-retryable，failed 可见）；zhuzhao 清单端点方案保留为可选增强；
- 回调超时默认值：实现取 30s（env `TASKRUNNER_CALLBACK_TIMEOUT` 可改，载荷可按任务覆盖）——随 M3 验证后转正式口径；
- `job_runs` 保留期：仍待定（M4 随清理任务落地）。

## 11. 关联文档

| 文档 | 位置 | 关系 |
|---|---|---|
| [ADR-002](./ADR-002-asynq-async-task-executor.md) | 本仓库 docs/（快照） | Asynq 执行器决策，zhuzhao 侧为 SSOT |
| [zhuzhao-utils](https://github.com/tracerbiubiubiu/zhuzhao-utils) | 独立仓库 | 通用工具库（logger / postgres / errcode+response），非反向依赖 |
| zhuzhao `docs/adr/ADR-001-event-mechanism-l1-steady-state.md` | zhuzhao 仓库 | L1 事件源（taskrunner 不替代） |
| zhuzhao `docs/phase3/13-implementation-plan.md` | zhuzhao 仓库 | 排期 SSOT（M-E 行 + 部署注记） |
| zhuzhao `docs/phase3/14-planning-overview.md` | zhuzhao 仓库 | 规划视图 |
| zhuzhao `docs/phase3/15-script-platform-dagu-vs-inhouse.md` | zhuzhao 仓库 | 脚本任务选型调研（🚦 按需再启参考） |
| zhuzhao `docs/phase3/03-audit-l2.md` | zhuzhao 仓库 | 审计归档（B11②）设计 |
| [zhuzhao-integration.md](./zhuzhao-integration.md) | 本仓库 docs/ | **zhuzhao 侧配套需求清单**（六项展开 + 契约摘要，见下） |

### zhuzhao 侧配套需求（跨仓库依赖，勿遗漏）

本设计引出的 zhuzhao 侧工作项已**拆出独立文档**：[zhuzhao-integration.md](./zhuzhao-integration.md)（动作注册表与内网端点、动作清单校验端点、任务管理功能、部门可见性策略及其存储、任务提交日志、后置的通知接收端点，含交互总览与回调契约摘要）。设计与排期以 zhuzhao 仓库为 SSOT，该文档供 zhuzhao 侧排期与实现参考。

## 12. 状态与变更记录

| 日期 | 变更 |
|---|---|
| 2026-09-03 | 建仓 + 建档：从 zhuzhao 13/14/15/ADR-002 提炼本设计文档；形态定稿（独立仓库/部署/Redis + 回调模型 + 日志边界 request_id 关联） |
| 2026-09-03 | 修订：明确独立 Redis 口径；补充回调幂等 / L1 边界 / 安全边界；job_runs 存储注记；提交方式标为 Open Question；代号说明；README 状态表述修正 |
| 2026-09-03 | 设计增补（运行时与契约）：提交方式定稿「调 taskrunner API」（关闭 Open Question）；新增 §5 HTTP API v1（端点集 + 内网鉴权边界 + 结果获取模型「受理 vs 查询唯一出口，不主动推送」）；§6 job_runs 落库（独立 DB / SQLite 起步 / `internal/store`，取代「默认随独立 Redis 落盘」注记）+ 两路日志分工（表存事实 / 文件存细节 / slog 关联）；§4 回调契约补全（at-least-once / 重试判定 / 超时 / 响应约定；终败通知曾提议后列为后置）；§10 实施计划 M1–M4 |
| 2026-09-03 | 设计增补（模型与治理）：§4 三层模型（动作 action / 任务定义 job / 执行实例 run），job 管理定稿「存 taskrunner DB + API 管理」（关闭「配置文件 vs API」待定项）；角色分工澄清——发起 / 下发 cron / 查日志由 zhuzhao 发起，调度·触发·重试·记录归 taskrunner，zhuzhao 在执行环节仅被动接收回调；审计透传（`actor` 工号 / `source_ip`）与归属标签过滤（`dept`，部门可见性策略存 zhuzhao 侧）；§5 调用方身份（per-caller credential + slog 归因，`created_by` / `owner_service` 字段）；§8 部署形态补独立 DB 与 zhuzhao-utils 依赖；M4 定稿 asynqmon 以库嵌入 `/monitor`（read-only 起步） |
| 2026-09-03 | 补充 §11 zhuzhao 侧配套清单（动作注册表与内网端点、动作清单校验端点、任务管理功能、部门可见性策略及其在 zhuzhao DB 的存储、任务提交日志、后置的通知接收端点），防跨仓库工作项遗漏 |
| 2026-09-03 | 全文整理：修正 ADR-002 注记章节引用（§6→§8）；统一 `created_by` / `owner_service` 字段口径；对齐「结果归属」与「结果获取模型」表述；M3 去除 `job_config` 旧术语；M1/M2 补触发方式与透传字段；待细化项集中至 §10 ⚠️；变更记录按主题合并 |
| 2026-09-03 | 环境决策与配套拆分：确认无日志平台（过程日志先落文件，§6 入档 ES 演进路径——slog JSON Lines，shipper 采集零改应用，字段稳定命名自 M1 约束）；部署形态定 **Docker**（单容器单进程、卷挂载 SQLite/日志、SQLite 单写者 → 单副本约束入档）；zhuzhao 侧配套需求拆出独立文档 [zhuzhao-integration.md](./zhuzhao-integration.md)，§11 改为链接；M4 补 `asynq.Retention` 留观口径 |
| 2026-09-03 | M1 完成（feat/m1-runtime）：核心运行时落地——Asynq worker + 回调客户端（2xx/4xx/5xx·超时判定）+ job_runs SQLite（WAL）+ healthz + enqueue CLI + Dockerfile；端到端冒烟通过（成功 / 4xx 死信 / 幂等重提）；实现中修正：store 自动建目录、先入队后落库防孤儿行 |
| 2026-09-03 | M2 完成（feat/m2-api）：HTTP API v1 全量落地——gin + utils `errcode`/`response` 统一响应；静态 Bearer 鉴权（未设 token 拒绝启动）；提交 / 查询（runs 条件分页 + live_state）/ jobs CRUD（dept 过滤、cron 校验）/ trigger / cancel / retry / 死信列表；`cronloop` 分钟级 tick 触发 cron 定义（§10 定案入档）；job_runs 增 `job_id` 列关联任务定义；API 层选型：token 取消用 asynq Scheduler 改自研 tick（静态 payload 限制）；单测 19 个 + 真 Redis 端到端冒烟（含 cron 到点自动触发、停用即不触发）|
