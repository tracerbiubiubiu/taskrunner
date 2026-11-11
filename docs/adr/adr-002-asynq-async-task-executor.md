# ADR-002: 引入 Asynq 作为"异步任务执行器"，不替代 L1 事件源

> 2026-11-08 文档整理：本决策按原编号保留。当前运行边界见 [运行设计](../design/taskrunner.md)，HTTP见 [契约](../api/contract.md)。下方评审/实施计划保留原时点，已落地结果与尚未实施M4不得混为一谈。


> **快照注记**：本 ADR 的 **SSOT 在 zhuzhao 仓**，当前入口为 [ADR-002](https://github.com/tracerbiubiubiu/zhuzhao/blob/phase5/docs/adr/adr-002-asynq-async-task-executor.md)。
> taskrunner 侧不维护完整副本（去重——原 190 行快照与 zhuzhao 侧逐字相同）。
>
> 本仓补充口径（zhuzhao 快照未涵盖）：
> - **独立 Redis**（2026-09-03 形态定稿）：部署使用独立 Redis 实例（不与 zhuzhao 共享）；当前拓扑见 [部署](../ops/deployment.md)，原 `taskrunner.md §8` 见[设计历史](../review/archive/taskrunner-design-history.md)。
> - 队列设计、worker 并发、重试策略等实现细节见 [taskrunner.md](../design/taskrunner.md)。
