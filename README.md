# taskrunner

独立部署的任务调度执行服务：HTTP受理、DB-first记录、Redis/Asynq调度、固定目标回调、重试和死信查询。业务handler由能力属主持有，用户权限由zhuzhao网关判定。

从 [文档导航](docs/README.md) 开始。接入查 [HTTP契约](docs/api/contract.md)，后续能力/身份/结果与专业平台协同查 [平台集成合同](docs/design/platform-integration.md)。部署查 [部署](docs/ops/deployment.md) 和 [备份恢复](docs/ops/backup.md)，开发查 [验证与提交](docs/guides/development.md)。

源码整合、测试与实际部署分别留证；能力目录尚未实施，旧方案由重规划承接，`/monitor`尚不存在。历史演进和审计结论在docs/review/archive保留。

许可：[MIT](LICENSE)。
