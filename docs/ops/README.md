# 运维

先读 [部署与执行身份升级](deployment.md)，再读 [备份恢复](backup.md)。命令仍在deploy目录，Redis丢失可重建的范围仅queued=0且pending；不要从DB-first推导任意状态都会自动重放。

当前依赖与容器基线、存量卷迁移限制见 [版本升级记录](version-upgrade.md)。
