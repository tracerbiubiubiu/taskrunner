# taskrunner设计导航

> **用途**：区分运行解释、未来合同和旧方案。**设计状态**：后续执行/恢复/能力合同为候选。**实施状态**：现有实现限制不因规划文本消失，新增机制仍待实施。

## 当前实现解释

先读[运行设计](taskrunner.md)，再读[zhuzhao接入](zhuzhao-integration.md)；准确wire字段转[HTTP契约](../api/contract.md)。TR管理调度、投递及执行观察，业务handler和效果由能力属主承担。

## 目标设计与实施窗口

[平台集成合同](platform-integration.md)承接C03/D12/I01–I03/R03/R04的状态写主、授权、失败及验收边界；排期、依赖和状态归主仓[唯一总账](../../../zhuzhao/docs/review/planning-backlog.md)，本页不维护第二份进度。

所有入口复用主仓[统一调用合同](../../../zhuzhao/docs/design/capability-invocation-contract.md)；本仓[承接边界](platform-integration.md#统一能力通道中的tr边界)说明不把同步查询/本地事务全改成任务，也不直写其他仓库表。新增互通范围见[增量册](../../../zhuzhao/docs/review/capability-interconnect-supplement.md)，未复估、不自动包含在原预算。

## 运行验证与历史

实际部署/恢复证据从[运维](../ops/README.md)进入；[ADR](../adr/README.md)和[历史](../review/README.md)保留固定时点。[能力目录](capability-registry.md)是尚未实施的旧批准方案及重规划输入，不能作为今天已存在的接口。
