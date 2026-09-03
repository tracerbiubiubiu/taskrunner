// Package enqueue 统一「受理一个任务」的路径（CLI / HTTP API / 手动触发 / cron 触发共用）。
// 幂等语义（§5 契约）：task_id 终身唯一——先查 job_runs（覆盖任务完成后 Asynq 释放 ID 的窗口），
// 再靠 Asynq TaskID 冲突兜底（覆盖并发同提）；两条都过才真正入队。
package enqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// Enqueuer Asynq 入队能力（*asynq.Client 即满足；抽出接口便于单测）。
type Enqueuer interface {
	Enqueue(t *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

type Service struct {
	Store    *store.Store
	Client   Enqueuer
	Queue    string
	MaxRetry int
}

// Submit 受理任务。accepted=false 表示 task_id 已存在（幂等重提，非错误）。
// 入队成功但落库失败时返回 warning（不影响执行，worker 侧容忍缺行）。
func (s *Service) Submit(ctx context.Context, p task.Payload) (accepted bool, warning error, err error) {
	// A1：先查 job_runs。Asynq 的 TaskID 冲突只在任务还留在 Redis 时成立（succeeded 后 ID 被释放），
	// job_runs 的行是终身的——以它为准关掉「完成后同 task_id 重提导致重复执行 + 覆盖历史」的窗口。
	if _, err := s.Store.GetByTaskID(ctx, p.TaskID); err == nil {
		return false, nil, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, nil, fmt.Errorf("enqueue: check existing: %w", err)
	}

	payload, err := json.Marshal(p)
	if err != nil {
		return false, nil, err
	}
	if _, err := s.Client.Enqueue(
		asynq.NewTask(task.TypeCallback, payload),
		asynq.TaskID(p.TaskID),
		asynq.MaxRetry(s.MaxRetry),
		asynq.Queue(s.Queue),
	); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			return false, nil, nil // 并发同提的兜底
		}
		return false, nil, err
	}
	if err := s.Store.InsertPending(ctx, store.Run{
		TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, JobID: p.JobID,
		CallbackURL: p.CallbackURL, SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP,
		EnqueuedAt: time.Now(),
	}); err != nil {
		return true, err, nil // 已入队：只提示记录缺失
	}
	return true, nil, nil
}
