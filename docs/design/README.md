# 设计

先读 [当前运行设计](taskrunner.md)，再读 [zhuzhao接入](zhuzhao-integration.md)。[平台集成合同](platform-integration.md)承接 C03/D12/I01–I03/R03/R04 的状态写主、权限、失败与验收边界；排期归主仓唯一总账。[能力目录](capability-registry.md)保留旧批准方案作为重规划输入，尚未实施，不能作为今天的API。

所有业务能力的统一逻辑入口、API/事件/结果和渐进数据闭环，以主仓[统一调用合同](../../../zhuzhao/docs/design/capability-invocation-contract.md)为权威；本仓的[承接边界](platform-integration.md#统一能力通道中的tr边界)说明TR只管已批准任务的调度/投递，不把全部同步请求改成任务，也不直接回写其他仓库表。设计增量未复估，不隐含改写原预算。
