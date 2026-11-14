# taskrunner 备份与恢复（对齐 activelist deploy/backup 模式，无 WAL 层）

## 策略

| 层 | 机制 | 频率 | 保留 |
|---|------|------|------|
| 逻辑备份 | `pg_dump -Fc`（pgbackup 服务，每日 02:00） | 每日 | 14 份（`RETAIN_COUNT` 可调） |

- 备份对象：本栈专用 PG 的 `job_runs` / `jobs`（C7：存储只在 PG；asynq队列在Redis，**队列不备份**）。Redis全丢后，没有队列消息可供asynq自动重试：仅`queued=0 AND status='pending'`会由reclaim快照重建；`queued=1 AND status='pending'`只Error告警、绝不自动重投，`running`不在三域内，须结合回调执行日志人工对账，再决定是否以**新task_id**受控重放。`enqueue_payload`是重建素材，不代表所有在途任务均能自动恢复。既有边界以[ADR-004的第二轮及后果⑥](../adr/adr-004-db-first-outbox-reclaim.md)为准。
- 与 activelist 的差异：无 WAL 归档层（job_runs 为追加型执行史，RPO=日级可接受；保留期/归档升级随 M4 保留期拍板一并评估）。

## 恢复步骤（pg_dump → 全量恢复）

先确认本次恢复目标的 Compose 项目、`.env` 和数据库归属，保留原卷及恢复前备份。以下从仓库根 `cd deploy` 后执行，目标是该项目的 PG；不同环境使用自己已确认的项目参数。每一步成功后再进入下一步，不能在恢复命令失败后继续开写。

```sh
cd deploy
# 1. 先查看已完成备份，再将实际文件复制到本机忽略目录；不要选 .part 临时文件
docker compose exec -T pgbackup sh -c 'ls -lh /backups/tr-*.dump'
backup_file=tr-YYYYMMDD.dump  # 替换为上一步已确认的实际文件名
restore_dir="$PWD/../.codex/restore-check"
mkdir -p "$restore_dir"
docker compose cp "pgbackup:/backups/$backup_file" "$restore_dir/$backup_file"
test -s "$restore_dir/$backup_file"
docker compose exec -T postgres pg_restore --list < "$restore_dir/$backup_file" \
  > "$restore_dir/$backup_file.list"

# 2. 核对对象清单/备份日期；确认该目标可恢复后停全部writer与备份循环
docker compose stop taskrunner pgbackup
# --clean只重建dump内对象；恢复目标仍须核对无不应保留的额外对象
docker compose exec -T postgres pg_restore -U taskrunner -d taskrunner \
  --clean --if-exists --no-owner --exit-on-error < "$restore_dir/$backup_file"

# 3. 恢复无错后再启服务，查看就绪及任务/定义计数，随后恢复备份循环
docker compose start taskrunner
docker compose exec -T taskrunner wget -qO- http://127.0.0.1:18080/readyz
docker compose start pgbackup
```

`compose cp` 从实际项目的 pgbackup 服务取出 `/backups` 文件，无需猜宿主目录或 `taskrunner_backups` 卷名。`pg_restore --list` 仅验证归档可解析并提供对象清单，不能代替恢复后数据抽验。恢复执行史不等于回滚外部业务动作；恢复旧库后仍按上文 DB/Redis 状态边界对账，避免盲目重放。


PG18候选使用新的pgdata18/backups18与Redis的redisdata82，历史卷保留；版本迁移不能只换镜像，见部署升级。命令工作目录与compose项目名不变。恢复前核对实际项目的卷路径，停写并保留原卷；同时备份新增execution_id列。升级/回滚耦合见 [部署](deployment.md)。
