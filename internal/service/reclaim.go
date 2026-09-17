// reclaim 扫描器（ADR-003 决策 4）：job_runs 兼任 outbox 的补偿回路。
// 三域职责（仿 cron.go 范式：Run 薄壳 + 同步单轮方法；单 goroutine 串行，停机 = ctx 取消；
// 多副本安全——同 task_id 竞投被 asynq TaskID 冲突挡住，无需分布式锁，F30）：
//
//	第一轮  自动修复  queued=0 AND status='pending' 宽限外 → 快照同参重投 + 条件标记（F32 三分）
//	第二轮  仅告警    status='pending' AND queued=1 宽限外 → Inspector 查证按 State 分流，绝不重投
//	第三域  仅清理    status='canceled' AND queued=0 宽限外 → DeleteTask 探针，确认消亡才出域
//
// 进程内状态（重启清零/回卷——F30/F46）：「已投未标记」集合、两级失败计数、两把游标。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// ReclaimLoop 三域扫描器。
type ReclaimLoop struct {
	Store      *repository.Store
	Client     Enqueuer      // 复用 asynq Client（第一轮重投）
	Inspector  TaskInspector // 第二轮查证 / 第三域探针，绝不投递
	Logger     *slog.Logger
	Tick       time.Duration
	StaleAfter time.Duration
	Retention  time.Duration // 重投 opts 同参（F61/F6）
	BatchSize  int
	Queue      string
	MaxRetry   int
	Now        func() time.Time

	// ---- 进程内状态（非持久化；游标重启回卷 F46，计数器/集合重启清零 F30）----
	unmarked       map[string]time.Time // 「已投未标记」集合（F3）：写者唯一为本扫描器（F33）；值 = 最近检视时间——成员行经其它路径离开 pending（worker 取走/取消）后条目由 TTL 回收（实现记录 §8，评审七·1）
	enqueueFailCnt map[string]*failStat // 第一轮逐行非冲突失败计数（≥3 → Error，TTL 回收）
	probeFailCnt   map[string]*failStat // 第三域探针失败计数（F65，TTL 回收）
	payloadSkipCnt map[string]*failStat // 空/损坏快照跳过计数（≥3 → Error 升级，TTL 回收）
	stuckCursor    int64                // 第二轮游标（行主键 id），0 = 从头扫（F36/F46/F56——时间列绑定格式不可作游标，见实现记录）
	cancelCursor   int64                // 第三域游标，同款
}

// Run ticker 薄壳（cron.go 同款生命周期与零值防御）。
func (l *ReclaimLoop) Run(ctx context.Context) {
	tick := l.Tick
	if tick <= 0 {
		tick = 10 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.ReclaimStalePending(ctx)
		}
	}
}

// ReclaimStalePending 一次扫描（三域顺序执行）。同步单轮方法，测试直驱。
func (l *ReclaimLoop) ReclaimStalePending(ctx context.Context) {
	l.defaults()
	now := l.now()
	// 计数条目与集合孤儿 TTL 回收（实现记录 §6/§8）：行经非成功路径离开域后不再驻留
	prune(l.enqueueFailCnt, now)
	prune(l.probeFailCnt, now)
	prune(l.payloadSkipCnt, now)
	pruneSeen(l.unmarked, now)
	before := now.Add(-l.StaleAfter)
	l.requeueUnqueued(ctx, before)
	l.alertStuckPending(ctx, before)
	l.probeCanceled(ctx, before)
}

// defaults 零值防御（F10：配置层已回退默认，此处兜底直构实例的测试/误用场景）
// 并惰性初始化进程内状态。
func (l *ReclaimLoop) defaults() {
	if l.BatchSize <= 0 {
		l.BatchSize = 100
	}
	if l.StaleAfter <= 0 {
		l.StaleAfter = 30 * time.Second
	}
	if l.Retention < 0 {
		l.Retention = 0
	}
	if l.enqueueFailCnt == nil {
		l.enqueueFailCnt = map[string]*failStat{}
	}
	if l.probeFailCnt == nil {
		l.probeFailCnt = map[string]*failStat{}
	}
	if l.payloadSkipCnt == nil {
		l.payloadSkipCnt = map[string]*failStat{}
	}
	if l.unmarked == nil {
		l.unmarked = map[string]time.Time{}
	}
}

func (l *ReclaimLoop) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

// sampleIDs 聚合日志的样本 task_id（F62：最多 3 个，防长样本刷屏）。
func sampleIDs(ids []string) string {
	if len(ids) > 3 {
		return strings.Join(append(append([]string{}, ids[:3]...), "..."), ",")
	}
	return strings.Join(ids, ",")
}

// failStat 失败/跳过计数 + 最近一次时间。条目按 TTL 回收（prune）：行经非成功路径
// 离开域（取消/被取走/手工修数/M4 清理）后计数不再驻留——业界 TTL map 做法，
// 防「只增不删」的内存微漏（实现记录 §6）。
type failStat struct {
	count int
	last  time.Time
}

const failStatTTL = 10 * time.Minute // 连续性窗口：超窗视为新一轮计数

// bump 递增计数，返回是否达到阈值。
func bump(m map[string]*failStat, id string, now time.Time, threshold int) bool {
	st, ok := m[id]
	if !ok {
		st = &failStat{}
		m[id] = st
	}
	st.count++
	st.last = now
	return st.count >= threshold
}

// prune 回收超过 TTL 未见失败的条目。
func prune(m map[string]*failStat, now time.Time) {
	for id, st := range m {
		if now.Sub(st.last) > failStatTTL {
			delete(m, id)
		}
	}
}

// pruneSeen 集合条目回收（实现记录 §8）：成员路径每次检视会重盖时间戳，
// 超过 TTL 未被检视的条目 = 行已离开域的孤儿（或域内积压挤出批位的极端情形——
// 后果为该行重投一次撞冲突自愈，在 at-least-once 残余内）。
func pruneSeen(m map[string]time.Time, now time.Time) {
	for id, seen := range m {
		if now.Sub(seen) > failStatTTL {
			delete(m, id)
		}
	}
}

const reclaimThreshold = 3 // 连续失败升级阈值（一轮/二轮告警与第三域探针共用）

// ---- 第一轮：自动修复（仅 queued=0 AND status='pending' 安全域）----

// requeueUnqueued 快照同参重投 + 条件标记。无需游标——Redis 全断时全批失败、恢复即逐批
// 自愈；DB 写故障期批位被集合成员占据（仅重试标记），两种占位都无正确性损失（F66）。
func (l *ReclaimLoop) requeueUnqueued(ctx context.Context, before time.Time) {
	rows, err := l.Store.ListUnqueued(ctx, before, l.BatchSize)
	if err != nil {
		l.Logger.Error("reclaim: list unqueued failed", slog.Any("err", err))
		return
	}
	now := l.now()
	var (
		warned        []string // 本 tick 内 1–2 次失败的行（聚合 Warn，F62）
		errored       []string // 连续 ≥3 次失败的行（聚合 Error）
		markFailed    []string // 条件标记执行失败（DB 写故障，F3/F33）
		payloadSkip   []string // 空/损坏快照跳过（1–2 次，聚合 Warn）
		payloadBusted []string // 空/损坏快照连续 ≥3 次（聚合 Error 升级，评审五·3）
	)
	for _, r := range rows {
		if _, isMember := l.unmarked[r.TaskID]; isMember {
			// 集合成员：只重试标记、跳过重投（F3——任务已在 Redis，重投纯属制造重复）；
			// 检视即重盖时间戳（TTL 回收只针对行已离开域的孤儿条目，实现记录 §8）
			l.unmarked[r.TaskID] = now
			n, err := l.Store.MarkQueued(ctx, r.TaskID)
			if err != nil {
				markFailed = append(markFailed, r.TaskID)
				continue
			}
			if n == 0 {
				// 成员行已离开 pending（F39 三义，最典型并发取消）→ 补偿 + 出集
				//（F34：否则成员永久驻留、集合无界增长）
				l.compensateDelete(ctx, r.TaskID, "unmarked member left pending")
				delete(l.unmarked, r.TaskID)
				continue
			}
			delete(l.unmarked, r.TaskID) // 标记成功出集
			continue
		}
		// 判空/反序列化失败防御（F7/F66）：空/损坏快照跳过——按 tick 聚合 Warn，
		// 连续 ≥3 次升级聚合 Error（正常不可达；真出现 = 手工改库/迁移问题，需人工）
		var p task.Payload
		perr := json.Unmarshal([]byte(r.EnqueuePayload), &p)
		if r.EnqueuePayload == "" || perr != nil {
			if bump(l.payloadSkipCnt, r.TaskID, now, reclaimThreshold) {
				payloadBusted = append(payloadBusted, r.TaskID)
			} else {
				payloadSkip = append(payloadSkip, r.TaskID)
			}
			continue
		}
		_, err := l.Client.Enqueue(asynq.NewTask(task.TypeCallback, []byte(r.EnqueuePayload)),
			buildEnqueueOpts(p, l.Queue, l.MaxRetry, l.Retention)...)
		switch {
		case err == nil || errors.Is(err, asynq.ErrTaskIDConflict):
			// 冲突不是异常，而是「入队成功、标记前崩溃」的主恢复信号（F32）——不计失败计数
			delete(l.enqueueFailCnt, r.TaskID)
			n, merr := l.Store.MarkQueued(ctx, r.TaskID)
			if merr != nil {
				// 标记执行失败（DB 写故障）→ 入「已投未标记」集合（F3），下轮只重试标记
				l.unmarked[r.TaskID] = now
				markFailed = append(markFailed, r.TaskID)
				continue
			}
			if n == 0 {
				// 行在标记前离开 pending（F39/F43 三义）→ DeleteTask 补偿撤销
				l.compensateDelete(ctx, r.TaskID, "left pending before mark")
				continue
			}
			l.Logger.Info("reclaim: re-enqueued", slog.String("task_id", r.TaskID))
		default:
			// 非冲突失败（Redis 不可用等）：计数，同一行 ≥3 → Error（F62 聚合打点）
			if bump(l.enqueueFailCnt, r.TaskID, now, reclaimThreshold) {
				errored = append(errored, r.TaskID)
			} else {
				warned = append(warned, r.TaskID)
			}
		}
	}
	if len(payloadSkip) > 0 {
		l.Logger.Warn("reclaim: enqueue payload empty/corrupt, skipped (escalates after 3 consecutive)",
			slog.Int("rows", len(payloadSkip)), slog.String("sample_task_ids", sampleIDs(payloadSkip)))
	}
	if len(payloadBusted) > 0 {
		l.Logger.Error("reclaim: enqueue payload empty/corrupt persistently (>=3) — manual data repair needed",
			slog.Int("rows", len(payloadBusted)), slog.String("sample_task_ids", sampleIDs(payloadBusted)))
	}
	if len(warned) > 0 {
		l.Logger.Warn("reclaim: enqueue failed (will retry next tick)",
			slog.Int("rows", len(warned)), slog.String("sample_task_ids", sampleIDs(warned)))
	}
	if len(errored) > 0 {
		l.Logger.Error("reclaim: enqueue persistently failing (>=3 consecutive)",
			slog.Int("rows", len(errored)), slog.String("sample_task_ids", sampleIDs(errored)))
	}
	if len(markFailed) > 0 {
		l.Logger.Warn("reclaim: mark queued failed (rows held in unmarked set, mark-only retry next tick)",
			slog.Int("rows", len(markFailed)), slog.String("sample_task_ids", sampleIDs(markFailed)))
	}
}

// compensateDelete 补偿撤销（F19/F44/F64）：行已离开 pending 时的在途投递回收。
// 容忍键消亡；active = worker 已取走（不可再缩残余——MarkRunning 复活拒绝 +
// 决策 7 终态守卫兜底）；其余瞬时错误由第三域探针（canceled 行）或正常执行覆盖。
func (l *ReclaimLoop) compensateDelete(ctx context.Context, taskID, why string) {
	if l.Inspector == nil {
		return
	}
	err := l.Inspector.DeleteTask(l.Queue, taskID)
	switch {
	case err == nil || isKeyGoneErr(err):
		l.Logger.Info("reclaim: compensated in-flight enqueue (redis task deleted)",
			slog.String("task_id", taskID), slog.String("why", why))
	case isDeleteActiveErr(err):
		l.Logger.Warn("reclaim: compensation delete hit active task, give up (at-least-once residual)",
			slog.String("task_id", taskID))
	default:
		l.Logger.Warn("reclaim: compensation delete failed (canceled rows covered by domain-3 probe)",
			slog.String("task_id", taskID), slog.Any("err", err))
	}
}

// ---- 第二轮：仅告警，绝不重投 ----

// alertStuckPending pending AND queued=1 宽限外 → Inspector 查证按 State 分流（F20）。
// 复用 batch + id 游标轮转（F36：在途行占批位时保证 eventual 全覆盖）。
func (l *ReclaimLoop) alertStuckPending(ctx context.Context, before time.Time) {
	rows, err := l.Store.ListStuckPending(ctx, before, l.stuckCursor, l.BatchSize)
	if err != nil {
		l.Logger.Error("reclaim: list stuck pending failed", slog.Any("err", err))
		return
	}
	for _, r := range rows {
		l.inspectStuck(r)
	}
	// F36/F46：游标按批推进（在途行也推进——否则每轮占批位）；不足一批 = 耗尽回卷
	l.advanceCursor(rows, l.BatchSize, &l.stuckCursor)
}

func (l *ReclaimLoop) inspectStuck(r *repository.Run) {
	info, err := l.Inspector.GetTaskInfo(l.Queue, r.TaskID)
	switch {
	case err != nil && !isKeyGoneErr(err):
		// 查证瞬时失败：静默跳过（Redis 全断时第一轮已在告警），游标照常推进
		return
	case isKeyGoneErr(err):
		// F64：队列注册集被清（FLUSHDB/删队列）同归 absent——「任务不在」恰是事实
		l.Logger.Error("reclaim: stuck pending row, task absent from queue (redis loss/mark residual); "+
			"manual action: resubmit with new task_id or wait for M4 reconciliation",
			slog.String("task_id", r.TaskID))
		return
	}
	switch info.State {
	case asynq.TaskStateCompleted:
		// F20：completed =「已执行完但 job_runs 未落终态」的直接指纹，立即告警而非等 Retention 过期
		l.Logger.Error("reclaim: task completed but job_runs row stuck pending — "+
			"mark/terminal write fault fingerprint, manual repair needed",
			slog.String("task_id", r.TaskID))
	case asynq.TaskStateArchived:
		l.Logger.Error("reclaim: task archived (asynq gave up retrying) but job_runs row still pending, never advances by itself",
			slog.String("task_id", r.TaskID))
	default:
		// pending/scheduled/retry/active/aggregating：在途，静默跳过
	}
}

// ---- 第三域：仅清理，绝不重投 ----

// probeCanceled canceled AND queued=0 宽限外 → DeleteTask 探针，确认键消亡才置 queued=1
// 出域（F45/F48/F49）；探针失败计数，同一行 ≥3 → Error（F65，防漏配容忍集/脏态/Internal
// 错误的永久静默循环）。
func (l *ReclaimLoop) probeCanceled(ctx context.Context, before time.Time) {
	if l.Inspector == nil {
		return
	}
	rows, err := l.Store.ListCanceledUnqueued(ctx, before, l.cancelCursor, l.BatchSize)
	if err != nil {
		l.Logger.Error("reclaim: list canceled unqueued failed", slog.Any("err", err))
		return
	}
	now := l.now()
	var errored []string
	var dirty []string // 脏态行（数据不一致指纹，F65 分类面）——按 tick 聚合 Warn（F62 精神，评审七·补）
	for _, r := range rows {
		err := l.Inspector.DeleteTask(l.Queue, r.TaskID)
		switch {
		case err == nil || isKeyGoneErr(err):
			// 键消亡确认 → 出域（MarkCleanupDone 不可复用 MarkQueued，F48）
			if _, uerr := l.Store.MarkCleanupDone(ctx, r.TaskID); uerr != nil {
				// 置位失败：留域下轮重探（键已删，下轮探消亡快速出域）
				l.Logger.Warn("reclaim: mark cleanup done failed, will re-probe",
					slog.String("task_id", r.TaskID), slog.Any("err", uerr))
			} else {
				delete(l.probeFailCnt, r.TaskID)
			}
		default:
			// active / Lua 脏态 / 瞬时错误：不出域，下轮重探（F49：无条件出域会放走
			// 仍在 Redis 的已取消任务，重开 F45 空洞）
			if isRedisDirtyStateErr(err) {
				dirty = append(dirty, r.TaskID)
			}
			if bump(l.probeFailCnt, r.TaskID, now, reclaimThreshold) {
				errored = append(errored, r.TaskID)
			}
		}
	}
	if len(dirty) > 0 {
		l.Logger.Warn("reclaim: cancel-cleanup probe hit dirty task state (redis data inconsistent), will re-probe",
			slog.Int("rows", len(dirty)), slog.String("sample_task_ids", sampleIDs(dirty)))
	}
	if len(errored) > 0 {
		l.Logger.Error("reclaim: cancel-cleanup probe persistently failing (>=3 consecutive)",
			slog.Int("rows", len(errored)), slog.String("sample_task_ids", sampleIDs(errored)))
	}
	l.advanceCursor(rows, l.BatchSize, &l.cancelCursor)
}

// advanceCursor 游标按批推进；不足一批 = 耗尽回卷（F36/F46：重启回卷已点明）。
// 游标键 = 行主键 id——时间列绑定含驱动内部格式后缀（time.Time.String() 的 m=+…），
// 等值/边界比较不可靠，实测记录见 ADR-003 实现记录。
func (l *ReclaimLoop) advanceCursor(rows []*repository.Run, batch int, cursor *int64) {
	if len(rows) < batch {
		*cursor = 0
		return
	}
	if len(rows) > 0 {
		*cursor = rows[len(rows)-1].ID
	}
}
