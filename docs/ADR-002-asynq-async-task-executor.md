# ADR-002: 引入 Asynq 作为"异步任务执行器"，不替代 L1 事件源

> **快照注记**：本 ADR 的 **SSOT 在 zhuzhao 仓** `docs/adr/ADR-002-asynq-async-task-executor.md`。
> taskrunner 侧不维护完整副本（去重——原 190 行快照与 zhuzhao 侧逐字相同）。
>
> 本仓补充口径（zhuzhao 快照未涵盖）：
> - **独立 Redis**（2026-09-03 形态定稿）：taskrunner 使用独立 Redis 实例（不与 zhuzhao 共享），详见 [taskrunner.md §8](./taskrunner.md)。
> - 队列设计、worker 并发、重试策略等实现细节见 [taskrunner.md](./taskrunner.md)。
