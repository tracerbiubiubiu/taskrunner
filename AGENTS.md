# AGENTS.md — taskrunner协作入口

适用于本仓全部文件和每次AI/人工协作。长规则只维护权威来源，本页不另立一套公约。

## 职责与权威

- 本仓负责任务受理、计划调度、投递、重试及执行账本；业务handler/效果归能力属主，用户授权归主仓。
- [文档导航](docs/README.md)区分现行实现、目标设计、运行验证与历史；当前wire字段看[HTTP/回调契约](docs/api/contract.md)和实际源码。
- 全生态工程、API、安全和测试纪律遵[主仓standards](../zhuzhao/docs/standards.md)；新需求/状态/估算只回填[唯一总账](../zhuzhao/docs/review/planning-backlog.md)。
- [平台集成合同](docs/design/platform-integration.md)含C03/D12/I01–I03等未实施目标；执行callback不等于结果已回贴，2xx不能抹掉现有完成语义缺口。

## 开发与验证

- 开工先`make hooks`，核对分支、工作树、在途文件及DB/Redis/queue/端口归属；写任务独立worktree。
- 保留standards §11测试先行：当前批准批全量正/负/边界/权限用例先写，取得目标行为的红证据，再分批实现到绿。
- 修复必带回归；并发修复遵§9/§12移除修复后连续至少8次稳定变红，不以`-race`全绿替代；红证据不等于允许提交红测试。
- 现有命令从本仓根目录运行：`make lint`、`make test`、`make build`；真实链另跑`make e2e`（需Docker及独占端口）。
- PG测试按[开发指南](docs/guides/development.md)设置专属`TASKRUNNER_TEST_PG_DSN`，执行CI同类的`go test -race -count=1 ./...`；未具备环境而跳过须披露。
- cron E2E使用已有`E2E_INCLUDE_CRON=1 make e2e`；同任务稳定身份、重复/迟到、unknown、恢复及取消边界按变更范围验证。
- 完整适用门禁未绿不标完成；mock/contract/real、首次失败、重试、skip及NOT_RUN分列，不削弱断言制造绿灯。

## 交付与边界

- 交付固定三节：改动摘要、影响面、验证证据；沿用[短审阅卡](../zhuzhao/docs/phase6/implementation-handoff.md#单维护者短审阅卡)，同步受影响合同/模块和主仓控制台。
- 仅提交本批明确路径，使用`git commit -F <消息文件> -- <paths>`等显式路径形态；禁`git add -A`、`git commit -a`和裸提交。
- 提交前核归属，提交后核`git show --stat`；全绿后提交，不擅自amend/reset/rebase他人历史。
- 合入走已确认的PR/单维护者策略及实际生效规则；默认不push、合main、发版或切共享环境，不声称未启用的远端保护已生效。
- TR不直写工单/风险/AL库，不自建用户IAM或专业SOAR；服务签名与actor/dept标签不授业务执行权。
- 文档、代码、mock均不得引入公司真实名称、人员、接口、地址或凭据；合成样例与私有adapter隔离，秘密不入Git/日志。
- 不顺手改仓库/module/API/配置/队列/卷名；运行与兼容迁移另核消费者、在途任务和恢复边界。

启动与资源保留见[开发指南](docs/guides/development.md)和[运维入口](docs/ops/README.md)；不清理未知归属的数据库、队列、容器或卷。
