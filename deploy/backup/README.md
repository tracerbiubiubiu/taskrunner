# taskrunner 备份与恢复（对齐 activelist deploy/backup 模式，无 WAL 层）

## 策略

| 层 | 机制 | 频率 | 保留 |
|---|------|------|------|
| 逻辑备份 | `pg_dump -Fc`（pgbackup 服务，每日 02:00） | 每日 | 14 份（`RETAIN_COUNT` 可调） |

- 备份对象：本栈专用 PG 的 `job_runs` / `jobs`（C7：存储只在 PG；asynq 队列在 Redis，**队列不备份**——任务载荷快照在 job_runs 有 `enqueue_payload` 兜底，Redis 丢失=在途任务失投，由 asynq 重试/死信语义消化）。
- 与 activelist 的差异：无 WAL 归档层（job_runs 为追加型执行史，RPO=日级可接受；保留期/归档升级随 M4 保留期拍板一并评估）。

## 恢复步骤（pg_dump → 全量恢复）

```sh
# 均在 deploy/ 目录执行
# 1. 停写（缩容或停 taskrunner 服务）
docker compose stop taskrunner
# 2. 恢复（dump → 库；-c 传入连接串）
docker compose exec -T postgres pg_restore -U taskrunner -d taskrunner --clean --if-exists < ../<备份文件>.dump
#    备份文件从 backups 卷取出：
docker run --rm -v taskrunner_backups:/backups -v "$PWD":/out alpine cp /backups/tr-<日期>.dump /out/
# 3. 重启
docker compose start taskrunner
```
