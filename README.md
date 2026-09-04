# taskrunner

**事件/任务总线**：zhuzhao 网关下的「异步执行侧」——可靠地调度、触发并执行预置动作（HTTP 回调 zhuzhao 内网端点），并提供内部 HTTP API；独立仓库、独立部署、独立 Redis、独立 DB。

## 定位

| | |
|---|---|
| 做什么 | 可靠任务队列（Asynq）+ 定时调度 + 触发回调 + 重试/超时/死信 + 自维护 `job_runs`（落库，SQLite 起步）+ 内部 HTTP API（任务定义管理 / 提交 / 查询 / 取消 / 重试 / 死信） |
| 不做什么 | 业务 handler（回调 zhuzhao）、用户上传脚本（🚦 降级）、事件源（L1 在 zhuzhao） |
| 底座 | [Asynq](./docs/ADR-002-asynq-async-task-executor.md)（Go，Redis 队列，重启不丢） |

## 与 zhuzhao 的关系

- zhuzhao = 网关（鉴权/编排/业务审计），**调 taskrunner API 提交任务 + 记任务提交日志**；
- taskrunner = 通用调度/触发运行时，消费任务后 **HTTP 回调 zhuzhao 内网端点**执行预置动作；
- 日志边界：zhuzhao 记「任务提交日志 + 业务审计」，taskrunner 自维护 `job_runs` 执行细节、**不传回 zhuzhao**，以 `request_id` 关联跨查（`GET /v1/runs?request_id=…`）。

## 文档

- [设计文档](./docs/taskrunner.md) —— SSOT：定位/职责/回调模型/HTTP API/日志边界/部署形态/实施计划
- [zhuzhao 侧配套需求](./docs/zhuzhao-integration.md) —— 本设计引出的 zhuzhao 侧工作项清单（注册表/任务管理/部门策略等）
- [ADR-002 契约快照](./docs/ADR-002-asynq-async-task-executor.md) —— Asynq 执行器决策（zhuzhao 侧为 SSOT）

## 状态

## 快速开始

```bash
cp configs/config.yaml my.yaml   # 或纯 env（TASKRUNNER_* 全量兼容）
export TASKRUNNER_CALLER_ZHUZHAO_SK=<zhuzhao侧分配的SK>   # 必填（API 验签）
export TASKRUNNER_SELF_SK=<zhuzhao侧为本服务分配的SK>       # 必填（回调签名）
./bin/taskrunner serve my.yaml   # 或纯 env 直接 serve
```

## 状态

- 2026-09-03：建仓 + 设计建档（独立仓库/部署/Redis，回调模型，日志边界定稿）；
- 2026-09-03：设计增补定稿（提交走 API；任务三层模型与定义管理 API；结果以查询接口为唯一出口；job_runs 落库 SQLite；审计透传与部门归属过滤；回调契约；asynqmon 库嵌入；实施计划 M1–M4；zhuzhao 侧配套清单）；
- 2026-09-03：环境决策（无日志平台——过程日志落文件、ES 演进路径入档；部署形态 Docker 单容器）；zhuzhao 侧配套需求拆出 [独立文档](./docs/zhuzhao-integration.md)；
- 2026-09-03：M1 核心运行时完成（worker/回调/job_runs 落库/healthz/CLI/Dockerfile）；M2 HTTP API 完成（v1 全量端点 + cronloop 定时触发；鉴权后随结构重构迁 AK/SK）；
- 2026-09-04：**微服务结构重构**（Wire DI / handler→service→repository 分层 / yaml+env 配置 / 统一 Makefile 门禁；C1/C2/C4/C5/C6/C9 收口——API 验签 AK/SK、/readyz、TZ、回调签名）。zhuzhao 侧预置动作体系已就绪（jobs Registry + audit_archive + /internal 回调端点 + 任务管理代理端点）；**下一步 M3 部署联调**（建 cron 定义 + 两侧对拉 + C3 网络拓扑/C7 迁 PG）；
- 2026-09-04：代码评审修复（提交幂等扩为终身；cancel 竞态三道防护，fix/idempotency-and-cancel-race）；§1 补「什么算一个任务」判定标准；
- 首个预置动作：审计归档（B11②，定时回调 zhuzhao 导出 audit_logs JSONL）；
- 状态：M1/M2 已完成，M3 待做；工作区状态见 git status。
