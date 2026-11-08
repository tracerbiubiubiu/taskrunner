# 开发与验证

从仓库根目录执行以下命令。先安装 Go（版本由 `go.mod` 指定）与 Make；需要数据库或 Docker 的测试在独立测试环境运行。

```sh
make hooks
make lint
make test
make build
```

PostgreSQL 集成使用 `TASKRUNNER_TEST_PG_DSN`；`make e2e` 需要 Docker，cron 场景由 `E2E_INCLUDE_CRON=1` 启用。
预期为命令退出码 0；门禁通过只证明该次指定环境，不表示候选已合并或部署。

## 提交规范


遵 zhuzhao `docs/standards.md` §12-9 / §12-10；关联主仓复核报告
`deliverables/software-company/zhuzhao-report-followup-2026-11-08.md` 的 **RR-GIT-01**（2026-11-08）。
本仓脚本与主仓同源、自包含，仅依赖 Git + Python 3 标准库，不依赖主仓目录或新增 Go/npm 包。
主题为 `type(scope): 摘要`（scope 可省略），摘要去前缀后最多 50 个 Unicode 字符、不得以句号结尾；
主题与正文间须空行，正文不得为空或只有空白。所有新 merge 提交也须显式提供规范主题与正文。
修复正文仍按公约填写「改动摘要 / 影响面 / 验证证据」与同类扫描，硬检查①–⑤不替代人工验证证据。

```sh
make hooks              # 新 clone / 新工作树开工时安装
make commit-lint        # origin/当前分支..HEAD；初推无同名远端时从启用基线检查
make test-commit-lint    # 临时独立仓库黑盒反例，不触碰业务栈
# 显式历史审计（完整 SHA；不豁免历史）
python3 scripts/commit-policy.py range --before <完整SHA> --after <完整SHA> --strict
```

`make hooks` 启用 Git 的 worktree 配置，仅在当前工作树设置相对 `.githooks`；common
`core.hooksPath` 保留，尚未承接本批的 main/其它工作树不被改装。保留在途显影与跨主题纯重题
amend 保护，`commit-msg` 先挡消息违规，`pre-push` 按 Git 提供的每个 ref 新旧 SHA 检查完整窗口。
`GIT_INDEX_FILE` 保持原值，兼容显式 pathspec 提交，不暂存或重写其它会话文件。

固定启用基线见 `scripts/commit-policy-baseline`：仅其祖先为已推送存量，不改史、不自动前移。
CI push 使用事件 before/after，PR 使用真实 base/head SHA，初推与 force push 均有覆盖；
缺对象精确获取仍失败、非法 SHA 或浅克隆一律拒绝，不回退近 20 笔。现有代码测试 CI 保持。
