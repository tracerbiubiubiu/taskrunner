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

## 宿主启动与验证

开发形态可用 SQLite 保存执行事实，但队列仍需要 Redis；它不是无需外部依赖的单进程模式。生产 Compose 使用独立 PG/Redis，见 [部署](../ops/deployment.md)，不要把未发布宿主端口的容器栈当作宿主 Redis。

先按主仓[开发依赖与宿主联调](https://github.com/tracerbiubiubiu/zhuzhao/blob/phase5/docs/ops/deployment.md)确认端口、Redis 实例/DB/queue、SQLite 文件和回调接收方的归属。沿用已分配环境时保留原文件与连接；新隔离环境另选专属文件、队列和端口。不要为了试启动修改冻结的 `configs/config.yaml` 或替换存量数据源。

从本仓根目录，在本机专属终端或未跟踪的环境文件中注入下列配置；表中是变量含义，不是可直接使用的密钥：

- `TASKRUNNER_HTTP_ADDR`：本次已分配的监听地址，例如 `127.0.0.1:18080`；改端口时同步网关 `TASKRUNNER_BASE_URL`。
- `TASKRUNNER_DB_DRIVER=sqlite` 与 `TASKRUNNER_DB_PATH`：本次确认的 SQLite 文件；日志目录由 `TASKRUNNER_LOG_DIR` 指定。
- `TASKRUNNER_REDIS_ADDR` / `TASKRUNNER_REDIS_PASSWORD` / `TASKRUNNER_REDIS_DB` / `TASKRUNNER_QUEUE`：已确认的队列数据源与命名空间。
- `TASKRUNNER_CALLER_ZHUZHAO_SK`：与主仓出站 `TASKRUNNER_SK` 对值；`TASKRUNNER_SELF_SK`：与主仓入站 `INTERNAL_JOBS_SK` 对值。
- `TASKRUNNER_CALLBACK_TARGET_URL`：宿主可达的固定回调地址；主仓宿主默认端口形态为 `http://127.0.0.1:33333/internal/jobs/callback`。容器内的 `zhuzhao` DNS 名不能直接当作宿主地址。

```sh
# 已在本终端注入上述配置后执行；命令不会替你创建或选择 Redis 实例
make build
./bin/taskrunner serve configs/config.yaml
```

入口用法见 `cmd/taskrunner/main.go`，配置加载与启动校验见 `internal/config/config.go` 的 `Load`。正常结果是进程保持运行，日志无配置/数据库/队列初始化失败。另开终端，对实际分配地址检查 `GET /healthz` 与 `GET /readyz`，例如在确认使用18080时：

```sh
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
```

两者预期 HTTP200；ready 仅表示 DB/Redis 探针通过。再经已配置网关验证受理→执行→回调链，才能判断联调可用。退出宿主进程使用其终端的 Ctrl-C，保留 SQLite、Redis 和日志，不执行清卷命令。

## 提交规范


遵 zhuzhao `docs/standards.md` §12-9 / §12-10；关联主仓[复核报告的 RR-GIT-01](https://github.com/tracerbiubiubiu/zhuzhao/blob/phase5/docs/review/reports/zhuzhao-report-followup-2026-11-08.md)（2026-11-08）。
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
