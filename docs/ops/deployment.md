# taskrunner 部署（M3/D 阶段交付物）

## 执行身份升级（ECO-B07-F05，2026-11-06）

先备份DB、排空或停止全部旧taskrunner写实例，再启动新版本。`repository.migrateColumns`按既有启动迁移惯例给PG/SQLite的`job_runs`添加`execution_id TEXT NOT NULL DEFAULT ''`；保留存量任务/状态/attempt，旧行首次被新worker取得时写新身份。该列仅为内部并发栅栏，不进入API或载荷。每次delivery重新生成身份，终态写入同时匹配status/attempt/identity；旧实例忽略新谓词，混跑不满足修复条件。

回滚须先停全部新writer再换回旧二进制。新增列可保留，旧代码忽略它；如确需删除列，须在停写和备份后按所用数据库能力单独回退，禁止运行期删列。回滚将恢复旧的同attempt竞态边界，不得把旧代码保留该列视为仍具防护。备份/恢复应包含列和数据，回调动作的业务幂等依旧由属主服务保证。

> 本文命令从仓库根目录进入 `deploy/` 后运行；compose文件仍在 `deploy/compose.yaml`，不是docs目录。

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
- **备份已就位（2026-10-01）**：pgbackup 侧车每日 02:00 `pg_dump -Fc`、保留 14 份
  （恢复步骤见 [备份与恢复](backup.md)；job_runs 保留期口径 M4 一并拍，
  调整=改 pgbackup 的 `RETAIN_COUNT`）

## 非 root 与可写目录（R5）

镜像内 `/data`、`/logs` 已预建并归属运行用户（uid 100）；named volume 首挂继承镜像属主。
改用宿主 bind mount 时，属主不受镜像控制——**启动期可写探针 fail-closed**（不可写即拒启并
指引属主排查；负向实测：`TASKRUNNER_LOG_DIR=/etc` → `log 目录不可写 … uid 100`）。

2026-11-06 ECO-B07-F11：容器/环境缺省/冻结yaml统一既定18080，生产compose探针与共享网地址均对齐；无宿主ports发布。覆盖监听端口时须同步探针与调用方，环境差异用env，不改冻结yaml。
