# taskrunner 部署（M3/D 阶段交付物）

## 快速开始

```sh
# 0. 预建共享网（zhuzhao 栈所在；一次性）
docker network create zhuzhao_to_taskrunner

# 1. 注入 PG 口令（本地文件，已 gitignore）
echo 'TASKRUNNER_PG_PASSWORD=taskrunner-dev' > .env

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
- AK/SK 对值纪律：本栈 `TASKRUNNER_CALLER_ZHUZHAO_SK` = zhuzhao 侧 `TASKRUNNER_SK`；本栈 `TASKRUNNER_SELF_SK` = zhuzhao 侧 `INTERNAL_JOBS_SK`（dev 默认值仅供本地起栈）

## 存储

- job_runs / jobs 落本栈专用 PG（C7 拍板：联调只用 PG；表结构应用启动自建）
- asynq 队列落本栈 Redis（DB 0，独立实例不与 zhuzhao 共享）
- **备份口径未定**（job_runs 保留期 M4 一并拍）——pgbackup 侧车暂缺位

## 非 root 与可写目录（R5）

镜像内 `/data`、`/logs` 已预建并归属运行用户（uid 100）；named volume 首挂继承镜像属主。
改用宿主 bind mount 时，属主不受镜像控制——**启动期可写探针 fail-closed**（不可写即拒启并
指引属主排查；负向实测：`TASKRUNNER_LOG_DIR=/etc` → `log 目录不可写 … uid 100`）。
