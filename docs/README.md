# taskrunner文档导航

> **用途**：按任务选择阅读入口。**设计状态**：现行运行解释与后续集成合同分列。**实施状态**：候选、已验证、已部署分别核对，文档不代替精确HEAD门禁或运行回执。

## 当前实现与接口

- [运行设计](design/taskrunner.md)：DB-first、调度/投递写主及当前限制。
- [HTTP与回调契约](api/contract.md)、[zhuzhao接入](design/zhuzhao-integration.md)：现行字段和执行callback；callback不是结果已回贴的证明。当前没有monitor路由，dept只是筛选标签，回调目标由服务配置固定。

## 目标设计与计划

- [设计导航](design/README.md)区分现行解释、目标合同和旧方案；[平台集成合同](design/platform-integration.md)承接结果、身份、调度恢复及专业协同。
- 排期、状态和新增范围只读主仓[唯一总账](../../zhuzhao/docs/planning/planning-backlog.md)及[互通增量](../../zhuzhao/docs/planning/capability-interconnect-supplement.md)，不在本仓复制预算。

## 开发、运行与验证

[开发指南](guides/README.md)给工具、测试先行、验证和提交入口，[AGENTS](../AGENTS.md)给全仓协作约束；[运维](ops/README.md)给部署、配置和恢复。health/ready通过只证明探针，不等于真实受理→执行→回调链通过。

## 决策与历史

[ADR](adr/README.md)保留决定与后继关系；[历史](review/README.md)保存原失败、旧里程碑和冻结报告。[能力目录旧方案](design/capability-registry.md)尚未实施，其业务定义/执行路由归属按现行合同承接，不能据“已批准”推导当前已有API。

## 统一入口与Git保留范围

全项目阅读从主仓[文档总入口](../../zhuzhao/docs/README.md)开始，当前计划统一在[planning](../../zhuzhao/docs/planning/README.md)；本仓继续维护随源码版本变化的合同、指南和正式结论。原始调研、执行卡、原日志、截图、备份与纯操作收据归仓库外项目档案，短期执行产物可留ignored `.codex/`；材料ID与保留决定见[材料目录](../../zhuzhao/docs/materials.md)。机器台账、冻结来源、Schema样例和必要验证依据继续Git跟踪，不按扩展名批量忽略。
