# taskrunner 备份与恢复（对齐 activelist deploy/backup 模式，无 WAL 层）

## 策略

| 层 | 机制 | 频率 | 保留 |
|---|------|------|------|
| 逻辑备份 | `pg_dump -Fc`（pgbackup 服务，每日 02:00） | 每日 | 14 份（`RETAIN_COUNT` 可调） |

- 备份对象：本栈专用 PG 的 `job_runs` / `jobs`（C7：存储只在 PG；asynq队列在Redis，**队列不备份**）。Redis全丢后，没有队列消息可供asynq自动重试：仅`queued=0 AND status='pending'`会由reclaim快照重建；`queued=1 AND status='pending'`只Error告警、绝不自动重投，`running`不在三域内，须结合回调执行日志人工对账，再决定是否以**新task_id**受控重放。`enqueue_payload`是重建素材，不代表所有在途任务均能自动恢复。既有边界以[ADR-004的第二轮及后果⑥](../../docs/ADR-004-db-first-outbox-reclaim.md)为准。
- 与 activelist 的差异：无 WAL 归档层（job_runs 为追加型执行史，RPO=日级可接受；保留期/归档升级随 M4 保留期拍板一并评估）。

## 恢复步骤（pg_dump → 全量恢复）

```sh
# 均在 deploy/ 目录执行
# 1. 停写（缩容或停 taskrunner 服务）
docker compose stop taskrunner
# 2. 恢复（dump → 库；--clean删除dump中对象后重建）
docker compose exec -T postgres pg_restore -U taskrunner -d taskrunner --clean --if-exists < ../<备份文件>.dump
#    备份文件从 backups 卷取出：
docker run --rm -v taskrunner_backups:/backups -v "$PWD":/out alpine cp /backups/tr-<日期>.dump /out/
# 3. 重启
docker compose start taskrunner
```
