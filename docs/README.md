# taskrunner 文档

当前候选行为与未来计划分开维护；文档不代替精确HEAD门禁或部署收据。

1. 新读者先看 [运行设计](design/taskrunner.md)，理解DB-first和动作归属。
2. 接口调用查 [HTTP与回调契约](api/contract.md)，跨仓接入看 [zhuzhao集成](design/zhuzhao-integration.md)。
3. 本地开发看 [指南](guides/README.md)，部署/升级/恢复看 [运维](ops/README.md)。
4. 原因追溯看 [ADR](adr/README.md)，原失败与旧里程碑看 [历史](review/README.md)。

能力目录 [已批准但尚未实施](design/capability-registry.md)。当前没有monitor路由；dept仅筛选标签，回调目标由服务配置固定。
