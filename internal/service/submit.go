// submit 统一「受理一个任务」的路径（CLI / HTTP API / 手动触发 / cron 触发共用）。
// DB-first（ADR-004：job_runs 兼任 outbox）：记录先行——GetByTaskID → InsertPending(queued=0+快照)
// → Enqueue。行即事实源（SSOT），Redis 任务是可重建投影：插行失败 = 500（Redis 无任务、
// DB 无行，干净）；入队失败 = 行在（可见、可修、可查），由 reclaim 扫描器修复后执行。
// 幂等语义（§5 契约）：task_id 终身唯一——先查 job_runs（覆盖任务完成后 Asynq 释放 ID 的
// 窗口），再靠 A2 的 UNIQUE 复查与 Asynq TaskID 冲突兜底（覆盖并发同提）。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// Enqueuer Asynq 入队能力（*asynq.Client 即满足；抽出接口便于单测）。
type Enqueuer interface {
	Enqueue(t *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

// TaskDelete 撤销已取消任务的残留投递（ADR-004 决策 3 A3）。
type TaskDelete interface {
	DeleteTask(queue, id string) error
}

type Service struct {
	Store     *repository.Store
	Client    Enqueuer
	Inspector TaskDelete   // A3 已确认取消的残留投递；nil 时跳过撤销
	Logger    *slog.Logger // A3 标记失败 Warn（nil 时静默——reclaim 冲突分支兜底）
	Queue     string
	MaxRetry  int
	Retention time.Duration // 入队留观（ADR-004 决策 5：占住 TaskID 挡迟到重投与完成释放竞态）
}

// buildEnqueueOpts A3 与 reclaim 重投共用的入队参数（F61：同参唯一来源，防两处漂移）。
func buildEnqueueOpts(p task.Payload, queue string, maxRetry int, retention time.Duration) []asynq.Option {
	opts := []asynq.Option{
		asynq.TaskID(p.TaskID),
		asynq.MaxRetry(maxRetry),
		asynq.Queue(queue),
	}
	// 任务级 deadline（审计 M2 双保险）：无 Timeout 的 handler goroutine 可被
	// 超长 timeout_secs 占死全部并发槽位并阻塞优雅停机；0 = asynq 默认（无限制）
	if p.TimeoutSecs > 0 {
		capSecs := p.TimeoutSecs
		if capSecs > 86400 {
			capSecs = 86400
		}
		opts = append(opts, asynq.Timeout(time.Duration(capSecs)*time.Second))
	}
	// Retention：完成任务在 Redis 留观占住 TaskID，挡扫描器迟到重投（决策 5）。
	// 注意 asynq 以整秒承载（F67）——config 层已做整秒化校验。
	opts = append(opts, asynq.Retention(retention))
	return opts
}

// Submit受理任务。accepted=false且无error仅表示同task_id/action/params幂等重提。
// 同task_id异action/params返回ConflictError，保留原任务且零新增执行。
// 入队失败（Redis 抖动）返回 warning：行已落库即 SSOT，reclaim 修复后执行——
// 200 语义 =「已受理，待入队」；warning 仅进日志不回传响应体（F8）。
func (s *Service) Submit(ctx context.Context, p task.Payload) (accepted bool, warning error, err error) {
	if err := validateTaskID(p.TaskID); err != nil {
		return false, nil, err
	}
	if err := validateParameters(p.Params); err != nil {
		return false, nil, err
	}
	// A1：先查 job_runs。Asynq 的 TaskID 冲突只在任务还留在 Redis 时成立（succeeded 后 ID 被释放），
	// job_runs 的行是终身的——以它为准关掉「完成后同 task_id 重提导致重复执行 + 覆盖历史」的窗口。
	if existing, err := s.Store.GetByTaskID(ctx, p.TaskID); err == nil {
		return false, nil, sameInput(existing, p)
	} else if !errors.Is(err, repository.ErrNotFound) {
		return false, nil, fmt.Errorf("submit: check existing: %w", err)
	}

	// A2：记录先行。快照与 asynq 载荷同源（F9：一次 Marshal，同一份 bytes 双用）。
	payload, err := json.Marshal(p)
	if err != nil {
		return false, nil, err
	}
	params := string(p.Params)
	if params == "" {
		params = "{}" // 与 schema 默认一致（jobs 路径存定义 params 的 JSON 文本）
	}
	if err := s.Store.InsertPending(ctx, repository.Run{
		TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, JobID: p.JobID,
		Dept: p.Dept, Params: params, CallbackURL: p.CallbackURL, SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP,
		EnqueuedAt: time.Now(), EnqueuePayload: string(payload),
	}); err != nil {
		// UNIQUE 并发重复采用驱动无关判定（F23）：失败后复查 GetByTaskID——
		// 命中 ⇒ 并发重提（幂等 200）；未命中（含复查失败）⇒ 真错误 → 500
		//（此刻 Redis 无任务、DB 无行，干净失败）。
		if existing, cerr := s.Store.GetByTaskID(ctx, p.TaskID); cerr == nil {
			return false, nil, sameInput(existing, p)
		}
		return false, nil, fmt.Errorf("submit: insert pending: %w", err)
	}

	return s.enqueuePersisted(ctx, p, payload)
}

// enqueuePersisted projects a committed pending outbox row to Redis. Both
// ordinary submissions and atomically claimed cron occurrences use this path.
func (s *Service) enqueuePersisted(ctx context.Context, p task.Payload, payload []byte) (bool, error, error) {
	// A3：入队。行已落库即 SSOT——此刻起任何失败都有可见、可修的记录。
	if _, err := s.Client.Enqueue(asynq.NewTask(task.TypeCallback, payload),
		buildEnqueueOpts(p, s.Queue, s.MaxRetry, s.Retention)...); err != nil {
		if !errors.Is(err, asynq.ErrTaskIDConflict) {
			// An enqueue error can arrive after Redis accepted the task.
			// Only a canceled row needs another cleanup obligation.
			compensateCanceledDelivery(ctx, s.Store, s.Inspector, s.Logger, s.Queue, p.TaskID)
			return true, err, nil
		}
		// 冲突 = 同 ID 任务已在 Redis（并发同提兜底）——本行照常条件置位
	}
	// 条件标记（F19）：按 status 条件是取消检测器——0 行 = 行在入队期间离开 pending
	n, merr := s.Store.MarkQueued(ctx, p.TaskID)
	if merr != nil || n == 0 {
		compensateCanceledDelivery(ctx, s.Store, s.Inspector, s.Logger, s.Queue, p.TaskID)
	}
	if merr != nil {
		// 标记执行失败（DB 写故障，F33）：行留 queued=0，任务已在 Redis，
		// 由 reclaim 第一轮的冲突分支收敛补课（F32）。至少 Warn 留痕——
		// 与扫描器 markFailed 聚合对称（评审六·2），不能静默吞
		if s.Logger != nil {
			s.Logger.Warn("submit: enqueued but mark queued failed, reclaim conflict branch will retry",
				slog.String("task_id", p.TaskID), slog.Any("err", merr))
		}
		return true, nil, nil
	}
	return true, nil, nil
}

// A worker may execute or schedule a retry before MarkQueued. Cancel is final,
// so the Redis copy may be withdrawn only after the ledger confirms cancellation.
func compensateCanceledDelivery(ctx context.Context, store *repository.Store, inspector TaskDelete, logger *slog.Logger, queue, taskID string) {
	// The queue call has already happened. A disconnected submitter must not
	// discard cleanup responsibility for a delivery that can still exist.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	pending, err := store.MarkCleanupPending(cleanup, taskID)
	if err != nil {
		if logger != nil {
			logger.Error("outbox: cannot register canceled delivery cleanup",
				slog.String("task_id", taskID), slog.Any("err", err))
		}
		return
	}
	if pending == 0 || inspector == nil {
		return
	}
	if err := inspector.DeleteTask(queue, taskID); err != nil && !errors.Is(err, asynq.ErrTaskNotFound) && logger != nil {
		logger.Warn("outbox: could not withdraw canceled delivery",
			slog.String("task_id", taskID), slog.Any("err", err))
	}
}

func sameInput(existing *repository.Run, p task.Payload) error {
	equal, err := task.EqualJSON([]byte(existing.Params), p.Params)
	if err != nil {
		return conflict("task_id 已存在且原参数无法安全比对")
	}
	if existing.Action != p.Action || existing.JobID != p.JobID || !equal {
		return conflict("task_id 已存在且 action/params 不一致")
	}
	return nil
}

// SubmitCron persists the original due slot and its next_run advance in one
// local transaction before any queue call. Losing loop instances do no work.
func (s *Service) SubmitCron(ctx context.Context, j repository.Job, p task.Payload, next, now time.Time) (bool, error, error) {
	if err := validateParameters(p.Params); err != nil {
		return false, nil, err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return false, nil, err
	}
	params := string(p.Params)
	if params == "" {
		params = "{}"
	}
	claimed, err := s.Store.ClaimCronOccurrence(ctx, j, next, now, repository.Run{
		TaskID: p.TaskID, Action: p.Action, JobID: p.JobID, Dept: p.Dept, Params: params, CallbackURL: p.CallbackURL,
		SubmittedBy: p.SubmittedBy, EnqueuedAt: now, EnqueuePayload: string(payload),
	})
	if err != nil || !claimed {
		return false, nil, err
	}
	return s.enqueuePersisted(ctx, p, payload)
}

// SubmitJob freezes the current enabled job execution snapshot and the original
// task identity in one DB transaction before projecting it to the queue.
func (s *Service) SubmitJob(ctx context.Context, j repository.Job, p task.Payload) (bool, error, error) {
	equal, compareErr := task.EqualJSON([]byte(j.Params), p.Params)
	if compareErr != nil || !equal || p.Action != j.ActionID || p.JobID != j.JobID || p.Dept != j.Dept || p.TimeoutSecs != j.TimeoutSecs {
		return false, nil, conflict("冻结载荷与任务定义不一致")
	}

	if err := validateTaskID(p.TaskID); err != nil {
		return false, nil, err
	}
	if err := validateParameters(p.Params); err != nil {
		return false, nil, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return false, nil, err
	}
	params := string(p.Params)
	if params == "" {
		params = "{}"
	}
	inserted, err := s.Store.ClaimJobRun(ctx, j, repository.Run{TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, JobID: p.JobID, Dept: p.Dept, Params: params, CallbackURL: p.CallbackURL, SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP, EnqueuedAt: time.Now(), EnqueuePayload: string(raw)})
	if errors.Is(err, repository.ErrJobSnapshotChanged) {
		return false, nil, conflict("任务定义已变化，请重新核验后触发")
	}
	if err != nil {
		return false, nil, err
	}
	if !inserted {
		existing, err := s.Store.GetByTaskID(ctx, p.TaskID)
		if err != nil {
			return false, nil, err
		}
		return false, nil, sameInput(existing, p)
	}
	return s.enqueuePersisted(ctx, p, raw)
}
