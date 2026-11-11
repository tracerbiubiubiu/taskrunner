# taskrunner

独立部署的事件/任务执行服务：HTTP受理、DB-first记录、Redis/Asynq调度、固定目标回调、重试和死信查询。业务handler由能力属主持有，用户权限由zhuzhao网关判定。

从 [文档导航](docs/README.md) 开始。接入查 [HTTP契约](docs/api/contract.md)，部署查 [部署](docs/ops/deployment.md) 和 [备份恢复](docs/ops/backup.md)，开发查 [验证与提交](docs/guides/development.md)。

当前候选源码不等于已合主线或部署；能力目录已批准但未实施，`/monitor`尚不存在。历史演进和审计结论在docs/review/archive保留。

许可：[MIT](LICENSE)。
