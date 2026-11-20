# taskrunner 部署（M3/D 阶段交付物）

## 主线承接状态（2026-11-17，已合入main）

本批已由[TR #8](https://github.com/tracerbiubiubiu/taskrunner/pull/8)合入main，分支/PR及功能main适用CI通过，未部署。准确主线状态统一见[主线合入报告](../../../zhuzhao/docs/review/reports/audit-baseline-main-merge-2026-11-17.md)，材料ID`maintenance/audit-baseline-main-merge-20261117/`。

本页保留本地验证和支持画像；精确PR、head、merge及各CI在上述主报告统一维护，不复制第二份状态表。

下文“本地候选”的验证记录及旧材料ID保留原时点，首失败、重试和helper skip边界继续可追溯。主线承接沿用单实例画像；此前组合验证中的同机PG/S3/JSONL恢复演练不等于跨介质一致快照、PITR或异机灾备，现有HA及完整receipt后继边界不变。

## 执行身份升级（ECO-B07-F05，2026-11-06）

先备份DB、排空或停止全部旧taskrunner写实例，再启动新版本。`repository.migrateColumns`按既有启动迁移惯例给PG/SQLite的`job_runs`添加`execution_id TEXT NOT NULL DEFAULT ''`；保留存量任务/状态/attempt，旧行首次被新worker取得时写新身份。该列仅为内部并发栅栏，不进入API或载荷。每次delivery重新生成身份，终态写入同时匹配status/attempt/identity；旧实例忽略新谓词，混跑不满足修复条件。

同库有多个 PostgreSQL schema 时，启动按连接的 `search_path` 判定和补齐目标 `job_runs` 的列。同库其他 schema 的同名表不参与判断。升级验证须检查实际运行连接所用的表，并确认历史状态和投递标识未被重写。

回滚须先停全部新writer再换回旧二进制。新增列可保留，旧代码忽略它；如确需删除列，须在停写和备份后按所用数据库能力单独回退，禁止运行期删列。回滚将恢复旧的同attempt竞态边界，不得把旧代码保留该列视为仍具防护。备份/恢复应包含列和数据，回调动作的业务幂等依旧由属主服务保证。

> 本文命令从仓库根目录进入 `deploy/` 后运行；compose文件仍在 `deploy/compose.yaml`，不是docs目录。

## 取消清理代次升级（2026-11-20）

使用既有单实例停写升级流程。启动时给 SQLite 和 PG 的 `job_runs` 补 `cancel_cleanup_generation BIGINT NOT NULL DEFAULT 0`；存量取消行保留当前清理域和 queued 值。迟到投递为同一任务登记更新的清理代次，探针只确认采样时的代次。旧版忽略代次，混跑无法提供这一保证。回退前停止新写者，新增列可留给后继升级，仍按人工对账口径处理遗留队列。

## 运行基线升级（2026-11-12 候选）

应用构建使用 Go 1.27.4 / Alpine 3.24，运行镜像 Alpine 3.24.4；PostgreSQL 18.6 使用 Debian trixie 官方镜像，备份工具与服务端同版本。镜像均钉完整版本和多架构摘要；本候选不自动修改已有实例。版本选择、依赖变化和验证边界见 [升级记录](version-upgrade.md)。

PG18 的 `PGDATA=/var/lib/postgresql/18/docker`，Compose 将新的 `pgdata18` 卷挂到 `/var/lib/postgresql`。旧 PG15 `pgdata` 不能直接挂给 PG18；新卷为空不代表旧数据已迁移。已有开发/部署栈先盘点项目、容器、卷、备份与写者，停写后用逻辑备份恢复至独立 PG18，核验数据和业务再切连接。保留旧卷和旧环境；不要执行 `down -v`。切换后若新库已接收写入，回指旧库会丢新写，需重新安排数据迁移，不能称为无损回滚。

队列使用 Redis 8.2.11 Extended / Alpine 3.22 的独立 `redisdata82` 卷，旧 `redisdata` 保留。PG 与 Redis 必须作为一致的迁移窗口处理：先停止新任务受理和全部 worker，核对 queued/pending/running、回调和死信，再按已核验的 Redis 升级路径迁移数据。不能把新空 Redis 当作已迁移队列；DB 快照不能自动恢复所有在途任务，沿用下文人工对账边界。

## 快速开始

```sh
cd deploy

# 0. 预建共享网（zhuzhao 栈所在；一次性）
docker network create zhuzhao_to_taskrunner

# 1. 注入必填变量（本地文件 .env，已 gitignore；五项均 fail-closed，缺失拒建——
#    对齐 activelist 与主仓 W0 三件套纪律，勿再引入弱缺省）
cat > .env <<'EOF'
TASKRUNNER_PG_PASSWORD=<本机生成的强口令>
TASKRUNNER_REDIS_PASSWORD=<本机生成的强口令>
# 两个 SK 须与 zhuzhao 侧对值（本地栈=zhuzhao dev 同值；生产必换强值）
TASKRUNNER_CALLER_ZHUZHAO_SK=<与网关TASKRUNNER_SK同值>
TASKRUNNER_SELF_SK=<与主仓INTERNAL_JOBS_SK同值>
TASKRUNNER_CALLBACK_TARGET_URL=http://zhuzhao:33333/internal/jobs/callback
EOF

# 2. 起栈（首次会构建镜像；非 root + 可写目录已预授权）
docker compose up -d --build

# 3. zhuzhao 侧无须额外配置——其 deployments/docker-compose.yaml 默认
#    TASKRUNNER_BASE_URL=http://taskrunner:18080（共享网服务名）
```

## 拓扑与隔离（C3，对齐 activelist D5）

| 服务 | 网络 | 宿主端口 |
|------|------|----------|
| taskrunner | taskrunner_internal + zhuzhao_to_taskrunner | **不发布** |
| postgres | taskrunner_internal（仅本栈） | **不发布** |
| redis | taskrunner_internal（仅本栈） | **不发布** |

- zhuzhao → taskrunner：`http://taskrunner:18080`（共享网，AK/SK 出站签名）
- taskrunner → zhuzhao 回调：`http://zhuzhao:33333`（zhuzhao 在共享网注册别名；callback URL 由本服务 TASKRUNNER_CALLBACK_TARGET_URL 固定配置，用户值拒收）
- AK/SK 对值纪律：本栈 `TASKRUNNER_CALLER_ZHUZHAO_SK` = zhuzhao 侧 `TASKRUNNER_SK`；本栈 `TASKRUNNER_SELF_SK` = zhuzhao 侧 `INTERNAL_JOBS_SK`（**compose 无弱缺省，fail-closed 拒建**——2026-10-01 硬化，dev 对值经 .env 注入）

## 存储

- job_runs / jobs 落本栈专用 PG（C7 拍板：联调只用 PG；表结构应用启动自建）
- asynq 队列落本栈 Redis（DB 0，独立实例不与 zhuzhao 共享；队列不备份——载荷快照在 job_runs
  `enqueue_payload` 兜底。Redis 丢失仅 queued=0+pending 可自动补投；queued=1+pending/running 须按日志人工对账，不能承诺全自动恢复）
- **人工对账口径（2026-11-20）**：`queued=1+pending` 沿现有待投账本差异告警。各扫描取有限 ID 轮次，持续新增时仍会回卷。
  对宽限外的 `running`，看见队列归档或具体任务不存在后才复核账本仍为同一执行，再记录任务及队列状态。
  运行对账重复快照按时窗抑制，探针故障按批记录。队列注册缺失可能遮住任务本体，此时保留取消清理等待，
  pending/running 对账记录探针故障。恢复队列观测后重新查证。
  人工处理先向能力属主核对业务回执和实际效果，再决定补录或以新 task_id 重新提交。对账只读。
  取消侧登记每次迟到投递的清理代次，入队响应不明确时也登记已取消的任务。
  成功撤销残留或确认具体任务不存在后，探针才关闭自己观察到的这一轮。
  补投扫描对快照与权威列语义不符的存量行（P2-9，2026-11-20）按 `snapshot/ledger mismatch` 聚合告警
  （连续 ≥3 升级 Error 并点名不符字段）；该行保持 pending+queued=0 原地复查，不会被补投或改写。
  处置先核对两侧哪一侧是事实（落库后篡改、备份恢复或迁移损伤都可能），修正任一侧后行按域轮转自然恢复；
  不能确认前不得以快照覆盖账本或反向覆盖。cron 原时隙被同身份异内容的存量发生次占用时，
  该定义整笔回滚并逐轮重试原 due（`cron occurrence does not match the stored execution`），
  人工核对存量发生次后再处置，自动跳过/推进归 C03 调度恢复政策。
- **备份已就位（2026-10-01）**：pgbackup 侧车每日 02:00 `pg_dump -Fc`、保留 14 份
  （恢复步骤见 [备份与恢复](backup.md)；job_runs 保留期口径 M4 一并拍，
  调整=改 pgbackup 的 `RETAIN_COUNT`）

## 停止预算（F0，2026-11-20）

`deploy/compose.yaml` 的 taskrunner 服务配 `stop_grace_period: 90s`：`App.Run` 顺序收尾
HTTP 排空（`TASKRUNNER_SHUTDOWN_TIMEOUT` 默认 30s）与 asynq worker 排空（同配置上限），
Compose 默认 10s 宽限会把慢请求和未完成收尾提前 SIGKILL。上调排空配置时须同步上调该值
（≥ 2×排空上限 + 余量）。补偿扫描收到停止信号后停止处理新的扫描行；已返回的投递可登记当前清理责任后退出。

## 非 root 与可写目录（R5）

镜像内 `/data`、`/logs` 已预建并归属运行用户（uid 100）；named volume 首挂继承镜像属主。
改用宿主 bind mount 时，属主不受镜像控制——**启动期可写探针 fail-closed**（不可写即拒启并
指引属主排查；负向实测：`TASKRUNNER_LOG_DIR=/etc` → `log 目录不可写 … uid 100`）。

2026-11-06 ECO-B07-F11：容器/环境缺省/冻结yaml统一既定18080，生产compose探针与共享网地址均对齐；无宿主ports发布。覆盖监听端口时须同步探针与调用方，环境差异用env，不改冻结yaml。

## 存量任务链修复候选的升级

2026-11-17合入前本地验收记录（当时尚未合入main或部署）：`go test -race -count=1 ./...`有226项通过，另2项仅供父测试启动的子进程helper在顶层跳过，其父测试已实际执行；PG/Redis路径未因缺环境跳过。`E2E_INCLUDE_CRON=1 make e2e`的20项通过，lint、test、build通过，source及本轮新编二进制的govulncheck均无发现。完整范围、首次失败和收据见[本轮正式报告](../../../zhuzhao/docs/review/reports/audit-baseline-repair-2026-11-17.md)，原材料ID为`maintenance/audit-baseline-repair-20261117/`；以下升级约束仍按单实例画像适用，不据此宣称TR HA已实现。

本候选无新增DDL，沿原jobs/job_runs/Redis数据切换。先停旧scheduler及writer、备份执行库并核对pending/running/failed/dead；新旧loop不能混跑，因为旧loop为同一due生成随机task_id。新loop的原due稳定ID和CAS事务不重命名历史run。回退前同样停新writer/排空并保留备份，回退会失去该发生次保护。

`security.trusted_operator_callers`默认`[zhuzhao]`，可用`TASKRUNNER_TRUSTED_OPERATOR_CALLERS`覆盖；每个受信caller须已有验签key。显式空列表关闭真人声明，只记录服务主体。首次升级核对主仓签名operator；旧body.created_by/owner_service/submitted_by/actor不再覆盖可信归因。历史归因不批量改写，不能拿历史可疑created_by作为执行授权。

恢复时如原时隙已有完全相符的执行记录和入队快照，只推进时间游标并保留原投递。原记录与候选快照不同则保留原时隙和记录并告警，由维护者按原记录核对。旧随机标识的时隙在交接时人工对账。

本候选`App.Run`在HTTP/ready、worker和cron/reclaim前取得事实库单例：PG持专用session advisory锁，SQLite对规范化同文件旁的`.instance.lock`取本机flock（symlink同源也互斥）；失败直接退出，不切换DB或新建备用SQLite。运行期每500ms核对session仍持锁/文件inode，单次核验上限2s；丢失或不可证明时取消后台，并先停止worker拉取新任务，再排空HTTP和既有任务，不能让在途HTTP阻塞期间继续拉新。锁文件不作为可删除临时文件，进程退出由内核释放flock；升级/备份按固定DB来源，不能运行中删除或替换锁文件。

生命周期取消后，新HTTP请求（包括`/readyz`）在进入业务路由前返回503及错误码10008，并携带`Retry-After: 1`；已经进入的请求及任务按上述排空处理。停止worker拉取本身等待时，这道受理闸也保持关闭，不以监听端口尚可连接表示实例仍可接业务。

共享同一事实库的双loop防重和启动互斥不代表多主机、独立SQLite文件或完整TR HA支持。PG锁只在同一事实DB内、文件锁只在同机同文件域内生效；复制出独立DB不是同一事实源，部署须固定来源。按本批单实例画像运行，主仓本地归档与TR归档只启一个。联合切换时先确认主仓在途/未确认终态回503，旧主仓的在途2xx不能靠TR单侧升级弥补。
