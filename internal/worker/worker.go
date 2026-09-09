// Package worker 消费 Asynq 任务：执行回调并维护 job_runs 状态流转。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type Deps struct {
	Store    *repository.Store
	Callback *callback.Client
	Logger   *slog.Logger
}

// NewMux 注册任务处理器（cron 触发由 service.Loop 分钟级 tick 扫 DB，§10 定案）。
func NewMux(d Deps) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	mux.HandleFunc(task.TypeCallback, d.handleCallback)
	return mux
}

// handleCallback 单次尝试的执行流程：running → 回调 → succeeded / failed / dead。
// 终态判定（设计文档 §4）：
//   - 回调成功 → succeeded；
//   - 4xx（ErrNonRetryable）→ failed 终态，SkipRetry；
//   - 5xx / 超时，重试未耗尽 → failed（等 Asynq 退避重试）；
//   - 5xx / 超时，本次已是最后一次 → dead（死信）。
func (d Deps) handleCallback(ctx context.Context, t *asynq.Task) error {
	var p task.Payload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		// 载荷损坏不可重试：记日志后放弃（无 task_id 可回写 job_runs）
		d.Logger.Error("payload unmarshalable, dropping task", slog.String("type", t.Type()), slog.Any("err", err))
		return fmt.Errorf("worker: bad payload: %w: %w", err, asynq.SkipRetry)
	}

	logger := d.Logger.With(
		slog.String("request_id", p.RequestID),
		slog.String("task_id", p.TaskID),
		slog.String("action", p.Action),
	)
	attempt := 1
	if n, ok := asynq.GetRetryCount(ctx); ok {
		attempt = n + 1
	}
	maxRetry := 0
	if n, ok := asynq.GetMaxRetry(ctx); ok {
		maxRetry = n
	}
	now := time.Now()

	if err := d.Store.MarkRunning(ctx, p.TaskID, attempt, now); err != nil {
		// job_runs 缺行（如提交后未及落库）：照常执行回调，仅记日志
		logger.Warn("mark running failed, continuing without job_runs update", slog.Any("err", err))
	}

	err := d.Callback.Do(ctx, p)
	switch {
	case err == nil:
		if ferr := d.Store.Finish(ctx, p.TaskID, repository.StatusSucceeded, "", attempt, now, time.Now()); ferr != nil {
			logger.Error("persist succeeded failed", slog.Any("err", ferr))
		}
		logger.Info("task succeeded", slog.Int("attempt", attempt))
		return nil

	case errors.Is(err, callback.ErrNonRetryable):
		if ferr := d.Store.Finish(ctx, p.TaskID, repository.StatusFailed, err.Error(), attempt, now, time.Now()); ferr != nil {
			logger.Error("persist failed(4xx) failed", slog.Any("err", ferr))
		}
		logger.Error("task failed terminally (non-retryable 4xx)", slog.Int("attempt", attempt), slog.Any("err", err))
		return fmt.Errorf("%w: %w", err, asynq.SkipRetry)

	default:
		final := maxRetry > 0 && attempt >= maxRetry
		status := repository.StatusFailed
		if final {
			status = repository.StatusDead
		}
		if ferr := d.Store.Finish(ctx, p.TaskID, status, err.Error(), attempt, now, time.Now()); ferr != nil {
			logger.Error("persist failure state failed", slog.Any("err", ferr))
		}
		if final {
			logger.Error("task dead (retries exhausted)", slog.Int("attempt", attempt), slog.Any("err", err))
		} else {
			logger.Warn("task failed, will retry", slog.Int("attempt", attempt), slog.Any("err", err))
		}
		return err // 交还 Asynq 按退避策略重试
	}
}
