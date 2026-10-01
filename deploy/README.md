# taskrunner 部署（M3/D 阶段交付物）

## 快速开始

```sh
# 0. 预建共享网（zhuzhao 栈所在；一次性）
docker network create zhuzhao_to_taskrunner

# 1. 注入必填变量（本地文件 .env，已 gitignore；五项均 fail-closed，缺失拒建——
#    对齐 activelist 与主仓 W0 三件套纪律，勿再引入弱缺省）
cat > .env <<'EOF'
TASKRUNNER_PG_PASSWORD=taskrunner-dev
TASKRUNNER_REDIS_PASSWORD=taskrunner-dev
# 两个 SK 须与 zhuzhao 侧对值（本地栈=zhuzhao dev 同值；生产必换强值）
TASKRUNNER_CALLER_ZHUZHAO_SK=dev-taskrunner-sk
TASKRUNNER_SELF_SK=dev-callback-sk
TASKRUNNER_CALLBACK_TARGET_URL=http://zhuzhao:33333/internal/jobs/callback
EOF

# 2. 起栈（首次会构建镜像；非 root + 可写目录已预授权）
docker compose up -d --build

# 3. zhuzhao 侧无须额外配置——其 deployments/docker-compose.yaml 默认
#    TASKRUNNER_BASE_URL=http://taskrunner:8080（共享网服务名）
```

## 拓扑与隔离（C3，对齐 activelist D5）

| 服务 | 网络 | 宿主端口 |
|------|------|----------|
| taskrunner | taskrunner_internal + zhuzhao_to_taskrunner | **不发布** |
| postgres | taskrunner_internal（仅本栈） | **不发布** |
| redis | taskrunner_internal（仅本栈） | **不发布** |

- zhuzhao → taskrunner：`http://taskrunner:8080`（共享网，AK/SK 出站签名）
- taskrunner → zhuzhao 回调：`http://zhuzhao:33333`（zhuzhao 在共享网注册别名；callback URL 由 zhuzhao 下发）
- AK/SK 对值纪律：本栈 `TASKRUNNER_CALLER_ZHUZHAO_SK` = zhuzhao 侧 `TASKRUNNER_SK`；本栈 `TASKRUNNER_SELF_SK` = zhuzhao 侧 `INTERNAL_JOBS_SK`（**compose 无弱缺省，fail-closed 拒建**——2026-10-01 硬化，dev 对值经 .env 注入）

## 存储

- job_runs / jobs 落本栈专用 PG（C7 拍板：联调只用 PG；表结构应用启动自建）
- asynq 队列落本栈 Redis（DB 0，独立实例不与 zhuzhao 共享；队列不备份——载荷快照在 job_runs
  `enqueue_payload` 兜底，Redis 丢失由 asynq 重试/死信语义消化）
- **备份已就位（2026-10-01）**：pgbackup 侧车每日 02:00 `pg_dump -Fc`、保留 14 份
  （恢复步骤见 [backup/README.md](backup/README.md)；job_runs 保留期口径 M4 一并拍，
  调整=改 pgbackup 的 `RETAIN_COUNT`）

## 非 root 与可写目录（R5）

镜像内 `/data`、`/logs` 已预建并归属运行用户（uid 100）；named volume 首挂继承镜像属主。
改用宿主 bind mount 时，属主不受镜像控制——**启动期可写探针 fail-closed**（不可写即拒启并
指引属主排查；负向实测：`TASKRUNNER_LOG_DIR=/etc` → `log 目录不可写 … uid 100`）。
