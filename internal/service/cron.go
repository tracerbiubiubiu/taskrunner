// cron 定时触发器（动态 cron 的实现定稿：分钟级 tick 扫 DB，设计文档 §10）。
// 职责归属说明：本文件在 service 包（业务编排层：触发决策依赖 jobs 表数据），
// 与 internal/worker（队列消费执行层：回调执行）分层不同、非不对称——前者决定
// 「何时发起」，后者负责「可靠执行」。
// 每 tick 读出到期定义，以原due稳定身份，在同库事务内认领、推进并写pending，再投队列。
// 定义经 API 增改停启后，下一个 tick 天然生效，无需维护内存注册表；进程宕机期间的错失触发
// 在重启后首个 tick 至多补一次（next_run 已过期即触发），随后按新周期推进。
package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type Submitter interface {
	Submit(ctx context.Context, p task.Payload) (accepted bool, warning error, err error)
}

type CronSubmitter interface {
	SubmitCron(context.Context, repository.Job, task.Payload, time.Time, time.Time) (bool, error, error)
}

type Loop struct {
	Store  *repository.Store
	Submit CronSubmitter
	Logger *slog.Logger
	// CallbackTargetURL W0b：cron 触发的出站回调目标——忽略 jobs 列存量值
	//（十七批「读侧覆盖不迁存量」），一律配置值。
	CallbackTargetURL string
	Tick              time.Duration // 默认 30s（cron 最细粒度 1 分钟，30s 轮询足够）
	Now               func() time.Time
}

// ParseSpec 解析并校验 cron 表达式（5 段标准格式）。
func ParseSpec(spec string) (cron.Schedule, error) {
	return cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(spec)
}

// NextRun 算下次触发时间；disabled 或 manual 返回零值（存 NULL）。
func NextRun(spec string, enabled bool, from time.Time) (time.Time, bool) {
	if !enabled {
		return time.Time{}, false
	}
	sched, err := ParseSpec(spec)
	if err != nil {
		return time.Time{}, false
	}
	next := sched.Next(from)
	return next, !next.IsZero()
}

func (l *Loop) Run(ctx context.Context) {
	tick := l.Tick
	if tick <= 0 {
		tick = 30 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runBackgroundTick(l.Logger, "cron", func() { l.FireDue(ctx) })
		}
	}
}

// FireDue 触发所有到期任务（一次扫描）。单条失败只记日志，不阻塞其他任务。
func (l *Loop) FireDue(ctx context.Context) {
	now := l.now()
	due, err := l.Store.ListDueCronJobs(ctx, now)
	if err != nil {
		l.Logger.Error("cronloop: list due jobs failed", slog.Any("err", err))
		return
	}

	for _, j := range due {
		next, ok := NextRun(j.CronSpec, true, now)
		if !ok {
			if err := l.Store.DisableCronSnapshot(ctx, *j, now); err != nil {
				l.Logger.Error("cronloop: disable invalid spec failed", "job_id", j.JobID, "err", err)
			}
			continue
		}
		p := task.Payload{TaskID: CronTaskID(j.JobID, j.NextRun.Time), Action: j.ActionID, JobID: j.JobID, Dept: j.Dept,
			CallbackURL: l.CallbackTargetURL, Params: []byte(j.Params), TimeoutSecs: j.TimeoutSecs}
		claimed, warn, err := l.Submit.SubmitCron(ctx, *j, p, next, now)
		if err != nil {
			l.Logger.Error("cronloop: occurrence transaction failed; retry original due slot", "job_id", j.JobID, "err", err)
			continue
		}
		if !claimed {
			continue
		}
		if warn != nil {
			l.Logger.Warn("cronloop: occurrence persisted; reclaim will enqueue", "task_id", p.TaskID, "err", warn)
		}
		l.Logger.Info("cronloop: occurrence committed", "job_id", j.JobID, "task_id", p.TaskID)
	}
}

// CronTaskID is stable for the original scheduled slot, including process
// restart and delayed ticks. Its 41 bytes fit the core's existing 64-byte key.
func CronTaskID(jobID string, due time.Time) string {
	return "cron:" + uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s\x00%s", jobID, due.UTC().Format(time.RFC3339Nano)))).String()
}

func (l *Loop) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}
