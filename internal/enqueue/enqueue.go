// Package enqueue 统一「受理一个任务」的路径（CLI / HTTP API / 手动触发 / cron 触发共用）：
// 先入 Asynq 队列、后写 job_runs；task_id 冲突 = 幂等受理。
package enqueue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type Service struct {
	Store    *store.Store
	Client   *asynq.Client
	Queue    string
	MaxRetry int
}

// Submit 受理任务。accepted=false 表示 task_id 已存在（幂等重提，非错误）。
// 入队成功但落库失败时返回 warning（不影响执行，worker 侧容忍缺行）。
func (s *Service) Submit(ctx context.Context, p task.Payload) (accepted bool, warning error, err error) {
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
			return false, nil, nil
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
