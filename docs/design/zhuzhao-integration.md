# 与 zhuzhao 接入

当前 zhuzhao 代理用户操作并作认证、路由授权和业务审计；taskrunner 接收签名请求，保存执行事实并回调固定的能力端点。两仓不跨库访问、不互相 import 业务代码。调用字段见 [HTTP契约](../api/contract.md)，运行边界见 [设计](taskrunner.md)；完成语义、后续身份/结果/调度合同按 [平台集成](platform-integration.md) 承接，尚未因文档落档实现。

## 接入顺序

1. 在能力属主实现统一回调入口。zhuzhao 当前为 `POST /internal/jobs/callback`，body.action 在其 jobs.Registry 分发；同一task_id须可重入/幂等，业务失败按2xx/4xx/5xx语义返回。
2. 部署双方到专用共享网络，配置调用方与回调方密钥环，并由taskrunner配置固定 CallbackTargetURL。用户提交不携带callback_url；密钥不写入文档或Git。
3. zhuzhao经API提交/建定义/trigger，并保存提交凭证与参数快照；请求归因通过签名头X-Operator，执行事实保留submitted_by/source_ip，request_id跨库关联。
4. 验证受理、执行成功、可重试失败、不可重试失败及重复delivery；数据库写栅栏不替代动作幂等。日志/接收凭证失败应按各仓现行runbook处理，不宣称跨服务事务。

## 权限与通知

任务中心当前是共享作业队列；dept仅标签，submitted_by仅“只看我提交”的筛选。没有部署中的部门可见性策略表或per-caller隔离，不要求空dept作为fail-closed授权条件。用户授权由网关维护，taskrunner只验证服务身份。

终败notifier已可选实现：TASKRUNNER_NOTIFY_*指定目标，zhuzhao接收 `/internal/notify/dead-letter` 后按通知配置分发webhook。发送尽力而为、失败记日志，不重建业务执行队列；正常任务结果仍按需GET查询。

## 部署与历史边界

[部署](../ops/deployment.md)明确两组SK对值、网络与18080探针；[备份](../ops/backup.md)明确Redis丢失时可自动恢复的状态。zhuzhao本地调度audit_archive时停止旧远端同名cron，避免双跑。

“审批通过→activelist加值→任务操作”仍是未来集成场景，不代表已有审批引擎或业务L2 Outbox。其原设计、M2/M3清单和曾经的地址协议见 [历史需求](../review/archive/zhuzhao-integration-history.md)。今后实施须先核实际业务事件面及activelist的 [ADR-003](https://github.com/tracerbiubiubiu/activelist/blob/codex/phase5-audit/docs/adr/adr-003-integration-contract.md)，不以历史“零架构改动”代替设计复核。
