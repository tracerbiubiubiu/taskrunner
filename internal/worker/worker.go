// Package worker 消费 Asynq 任务：执行回调并维护 job_runs 状态流转。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/notify"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// CallbackClient 回调能力（*callback.Client 即满足；接口化便于单测注入）。
type CallbackClient interface {
	Do(ctx context.Context, p task.Payload) error
}

type Deps struct {
	Store    *repository.Store
	Callback CallbackClient
	Logger   *slog.Logger
	Notifier DeadLetterNotifier // P4-2 死信告警（nil=关闭——App 装配处按配置注入）
}

// DeadLetterNotifier 死信通知出口（最小接口——notify.Notifier 实现；nil 安全）
type DeadLetterNotifier interface {
	NotifyDeadLetter(p notify.Payload)
}

// NewMux 注册任务处理器（cron 触发由 service.Loop 分钟级 tick 扫 DB，§10 定案）。
func NewMux(d Deps) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	mux.HandleFunc(task.TypeCallback, d.handleCallback)
	return mux
}

// finishCtx 终态落库专用 ctx：asynq 在任务超时/停机窗口会 cancel handler ctx——
// 若用原 ctx，超时瞬间的 Finish 直接报 context canceled，job_runs 永久停留 running
// 且 RetryTask 不受理 running（无法自愈）。终态必须落库，故脱离取消链，仅保留
// 有限时长防 DB 挂起泄漏（C4）。
func finishCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// finish 终态落库：失败必须返回队列处理者，不将未确认终态当成功。
// store侧status/attempt/execution_id谓词拒绝过期写者，包含同attempt停机重投。
func (d Deps) finish(ctx context.Context, logger *slog.Logger, taskID, status, errMsg string, attempt int, startedAt time.Time, executionID string) error {
	fctx, cancel := finishCtx(ctx)
	defer cancel()
	if ferr := d.Store.Finish(fctx, taskID, status, errMsg, attempt, startedAt, time.Now(), executionID); ferr != nil {
		logger.Error("persist terminal state failed", slog.String("status", status), slog.Any("err", ferr))
		return ferr
	}
	return nil
}

// handleCallback 单次尝试的执行流程：running → 回调 → succeeded / failed / dead。
// 终态判定（设计文档 §4）：
//   - 回调成功 → succeeded；
//   - 不可重试错误（ErrNonRetryable：4xx / callback_url 非法等本地构造错误）→ failed 终态，SkipRetry；
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
	maxRetry := -1 // 未知（不在 asynq 处理上下文）按终败处理——失败必须留终态记录
	if n, ok := asynq.GetMaxRetry(ctx); ok {
		maxRetry = n
	}
	now := time.Now()
	executionID := uuid.NewString() // one invocation, including equal-attempt shutdown redelivery

	if err := d.Store.MarkRunning(ctx, p.TaskID, attempt, now, executionID); err != nil {
		if errors.Is(err, repository.ErrTerminalRejected) || errors.Is(err, repository.ErrDeliverySuperseded) {
			// 终态守卫（ADR-004 决策 7）：行已 succeeded/canceled——重复投递或已取消任务
			// 被取走，消费端拦截副作用：不执行回调、不写终态（行已是终态），SkipRetry 丢弃。
			// F45/F58 窗口的「已取消任务产生回调」由此结构性关闭。
			logger.Error("terminal-state guard: job_runs row already terminal, dropping delivery",
				slog.Any("err", err))
			if errors.Is(err, repository.ErrTerminalRejected) {
				// A late delivery may be observed here even if its producer
				// could not register it with the cancellation sweep.
				d.reopenCancelCleanup(ctx, logger, p.TaskID)
			}
			return fmt.Errorf("worker: terminal row rejects resurrection: %w: %w", err, asynq.SkipRetry)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// DB-first 下缺行或DB故障都不能证明本次取得执行权；
		// 返回错误，恢复后重投，不允许无账callback。
		logger.Error("mark running failed; callback not executed", slog.Any("err", err))
		return fmt.Errorf("worker: claim required before callback: %w", err)
	}

	err := d.callCallback(callback.WithDeliveryID(ctx, executionID), p, logger)
	switch {
	case err == nil:
		if err := d.finish(ctx, logger, p.TaskID, repository.StatusSucceeded, "", attempt, now, executionID); err != nil {
			return completionError(err)
		}
		logger.Info("task succeeded", slog.Int("attempt", attempt))
		return nil

	case errors.Is(err, callback.ErrNonRetryable):
		if ferr := d.finish(ctx, logger, p.TaskID, repository.StatusFailed, err.Error(), attempt, now, executionID); ferr != nil {
			return completionError(ferr)
		}
		logger.Error("task failed terminally (non-retryable: 4xx or malformed request)", slog.Int("attempt", attempt), slog.Any("err", err))
		return fmt.Errorf("%w: %w", err, asynq.SkipRetry)

	default:
		// asynq 语义 = 第 maxRetry+1 次失败才归档：attempt > maxRetry 才是真终态；
		// `>=` 会把非末次失败提前判 dead，随后 MarkRunning 又复活为 running（M1）
		final := maxRetry < 0 || attempt > maxRetry // maxRetry=0（显式不重试）时首败即 dead
		status := repository.StatusFailed
		if final {
			status = repository.StatusDead
		}
		if ferr := d.finish(ctx, logger, p.TaskID, status, err.Error(), attempt, now, executionID); ferr != nil {
			return completionError(ferr)
		}
		if final {
			logger.Error("task dead (retries exhausted)", slog.Int("attempt", attempt), slog.Any("err", err))
			// P4-2 死信告警：异步尽力而为（nil/未配置=空操作；失败不阻塞死信落库路径）
			if d.Notifier != nil {
				d.Notifier.NotifyDeadLetter(notify.Payload{
					TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action,
					JobID: p.JobID, Dept: p.Dept, Error: err.Error(),
					Attempts: attempt, FailedAt: now.Format(time.RFC3339),
				})
			}
		} else {
			logger.Warn("task failed, will retry", slog.Int("attempt", attempt), slog.Any("err", err))
		}
		return err // 交还 Asynq 按退避策略重试
	}
}

// reopenCancelCleanup 迟到已取消投递的清理重入（F2a 取消对账，2026-11-20 复核）：
// 清理确认出域（queued=1）的取消行仍可能收到 at-least-once 迟到投递——该行已不在
// reclaim 第三域扫描（ListCanceledUnqueued 不再选中），残留副本本无收敛出口。此处把
// canceled 行放回清理域（queued=0），由 probeCanceled 下轮重探撤残留或确认消亡；
// 取消终态不动，succeeded 行的条件写恒 0 行（合法留观副本不受影响）。写失败仅留痕：
// 投递照常丢弃，行为退回重入前口径，后续迟到投递可再次重入。
func (d Deps) reopenCancelCleanup(ctx context.Context, logger *slog.Logger, taskID string) {
	fctx, cancel := finishCtx(ctx)
	defer cancel()
	n, err := d.Store.MarkCleanupPending(fctx, taskID)
	switch {
	case err != nil:
		logger.Error("cancel-cleanup re-admission failed",
			slog.String("task_id", taskID), slog.Any("err", err))
	case n > 0:
		logger.Debug("late canceled delivery re-admitted to cancel-cleanup sweep",
			slog.String("task_id", taskID))
	}
}

// Catch a callback panic before Asynq's outer recovery: the error must pass the
// same failed/dead/retry and execution fence paths as an ordinary callback 5xx.
// Panics outside the callback boundary remain visible to Asynq's recovery.
func (d Deps) callCallback(ctx context.Context, p task.Payload, logger *slog.Logger) (err error) {
	defer func() {
		if v := recover(); v != nil {
			logger.Error("callback panic", slog.Any("panic", v), slog.String("stack", string(debug.Stack())))
			err = fmt.Errorf("worker: callback panic: %v", v)
		}
	}()
	return d.Callback.Do(ctx, p)
}

func completionError(err error) error {
	if errors.Is(err, repository.ErrDeliverySuperseded) {
		return fmt.Errorf("worker: stale completion: %w: %w", err, asynq.SkipRetry)
	}
	return fmt.Errorf("worker: terminal persistence unconfirmed: %w", err)
}
