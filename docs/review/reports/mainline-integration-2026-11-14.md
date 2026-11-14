# 主线集成记录 — 规划与升级的事实后记

日期：2026-11-14；仓库：`taskrunner`；记录对象为本次实际main合并，不表示生产发布。

## 已合入的候选

用户已明确授权“规划与已验证升级一起合入”；本仓最新规划及既有升级候选已通过原PR进入main。
原来源分支：`codex/capability-interconnect-design`。
原source/head：`dc586e1958a2bb634002f14593a49d5a499589e7`。
原PR：[本次规划与升级合入](https://github.com/tracerbiubiubiu/taskrunner/pull/2)；合并时间：`2026-11-14T07:13:09Z`。
实际merge SHA：`62268b60f6328d9209a51147af9e003612291839`。
独立复核确认merge tree与已验证候选tree一致；规划中的风险联动、权限扩展等新业务没有因此变成已实现。

## 历史检查失败与证据

本次合并未显式指定`--subject`，GitHub默认`Merge pull request ...`标题不符合现行`type(scope): 非空摘要`格式，导致该merge的`commit-policy`失败。
准确失败记录：[原main提交规范检查](https://github.com/tracerbiubiubiu/taskrunner/actions/runs/43570050293/job/132372248272)；本次不重写其历史结果。
该merge的产品检查均已完成且成功：[test](https://github.com/tracerbiubiubiu/taskrunner/actions/runs/43570050293/job/132372248438)；没有其它失败或pending检查，整次历史运行仍因提交规范为失败。
保留原merge、失败日志与固定接入baseline；`range --strict`仍可检出历史违规，不豁免merge、不降低规则、不强推改史。

## 本次后记与后继边界

此次仅新增本事实后记，产品代码、依赖锁、CI规则和接入baseline均不修改；本次写入未重跑产品测试、未提交或创建新PR。
后续规范消息的文档PR只能使当前新增窗口与其当前main CI恢复可验；不能宣称历史违规清除。新PR号、head/main最终CI结果须另以实际PR和收据确认，不在此预填绿色。
未部署、未切换共享环境、未启动规划中的业务实现，也未修改任何消费者的utils依赖pin；合入库main不等于消费者已消费新版本。
