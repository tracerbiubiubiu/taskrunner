// submit 统一「受理一个任务」的路径（CLI / HTTP API / 手动触发 / cron 触发共用）。
// DB-first（ADR-003：job_runs 兼任 outbox）：记录先行——GetByTaskID → InsertPending(queued=0+快照)
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
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// Enqueuer Asynq 入队能力（*asynq.Client 即满足；抽出接口便于单测）。
type Enqueuer interface {
	Enqueue(t *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

// TaskDelete 补偿撤销能力（ADR-003 决策 3 A3：入队后行已离开 pending 时立即撤销刚投递的
// 任务；*asynq.Inspector 即满足；最小接口便于单测注入）。
type TaskDelete interface {
	DeleteTask(queue, id string) error
}

type Service struct {
	Store     *repository.Store
	Client    Enqueuer
	Inspector TaskDelete // A3 补偿撤销（0 行三义处置，F39/F44）；nil 时跳过撤销
	Queue     string
	MaxRetry  int
	Retention time.Duration // 入队留观（ADR-003 决策 5：占住 TaskID 挡迟到重投与完成释放竞态）
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

// Submit 受理任务。accepted=false 表示 task_id 已存在（幂等重提，非错误）。
// 入队失败（Redis 抖动）返回 warning：行已落库即 SSOT，reclaim 修复后执行——
// 200 语义 =「已受理，待入队」；warning 仅进日志不回传响应体（F8）。
func (s *Service) Submit(ctx context.Context, p task.Payload) (accepted bool, warning error, err error) {
	// A1：先查 job_runs。Asynq 的 TaskID 冲突只在任务还留在 Redis 时成立（succeeded 后 ID 被释放），
	// job_runs 的行是终身的——以它为准关掉「完成后同 task_id 重提导致重复执行 + 覆盖历史」的窗口。
	if _, err := s.Store.GetByTaskID(ctx, p.TaskID); err == nil {
		return false, nil, nil
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
		if _, cerr := s.Store.GetByTaskID(ctx, p.TaskID); cerr == nil {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("submit: insert pending: %w", err)
	}

	// A3：入队。行已落库即 SSOT——此刻起任何失败都有可见、可修的记录。
	if _, err := s.Client.Enqueue(asynq.NewTask(task.TypeCallback, payload),
		buildEnqueueOpts(p, s.Queue, s.MaxRetry, s.Retention)...); err != nil {
		if !errors.Is(err, asynq.ErrTaskIDConflict) {
			// Redis 抖动：行留 queued=0，reclaim 第一轮修复后执行
			return true, err, nil
		}
		// 冲突 = 同 ID 任务已在 Redis（并发同提兜底）——本行照常条件置位
	}
	// 条件标记（F19）：按 status 条件是取消检测器——0 行 = 行在入队期间离开 pending
	n, merr := s.Store.MarkQueued(ctx, p.TaskID)
	if merr != nil {
		// 标记执行失败（DB 写故障，F33）：行留 queued=0，任务已在 Redis，
		// 由 reclaim 第一轮的冲突分支收敛补课（F32）
		return true, nil, nil
	}
	if n == 0 {
		// F39 三义：并发取消（撤销即取消生效）/ 已被 worker 取走 running（active 撤销失败，
		// 无害放弃——MarkRunning 复活拒绝 + 决策 7 终态守卫兜底）/ 已达终态（仅清 Retention
		// 留观副本）。容忍 ErrTaskNotFound/ErrQueueNotFound（F44/F64）；其余撤销失败同样
		// 不影响受理口径——canceled 行由第三域探针兜底，running/终态行本就会正常执行。
		if s.Inspector != nil {
			_ = s.Inspector.DeleteTask(s.Queue, p.TaskID)
		}
	}
	return true, nil, nil
}
