// cron 定时触发器（动态 cron 的实现定稿：分钟级 tick 扫 DB，设计文档 §10）。
// 每 tick 读出到期（next_run <= now）的 enabled cron 定义 → 走统一提交路径（新 task_id）→ 推进 next_run。
// 定义经 API 增改停启后，下一个 tick 天然生效，无需维护内存注册表；进程宕机期间的错失触发
// 在重启后首个 tick 至多补一次（next_run 已过期即触发），随后按新周期推进。
package service

import (
	"context"
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

type Loop struct {
	Store     *repository.Store
	Submit    Submitter
	Logger    *slog.Logger
	Tick      time.Duration // 默认 30s（cron 最细粒度 1 分钟，30s 轮询足够）
	NewTaskID func() string
	Now       func() time.Time
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
	return sched.Next(from), true
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
			l.FireDue(ctx)
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
		newID := l.newTaskID()
		p := task.Payload{
			TaskID: newID,
			Action: j.ActionID, JobID: j.JobID, Dept: j.Dept,
			CallbackURL: j.CallbackURL,
			Params:      []byte(j.Params),
			TimeoutSecs: j.TimeoutSecs,
			// cron 触发无用户参与：request_id / submitted_by / source_ip 留空（§4）
		}
		if _, warn, err := l.Submit.Submit(ctx, p); err != nil {
			l.Logger.Error("cronloop: submit failed",
				slog.String("job_id", j.JobID), slog.String("action", j.ActionID), slog.Any("err", err))
			continue // 不推进 next_run：下个 tick 重试本次触发
		} else if warn != nil {
			l.Logger.Warn("cronloop: submitted but job_runs insert failed",
				slog.String("job_id", j.JobID), slog.String("task_id", newID), slog.Any("err", warn))
		}
		next, ok := NextRun(j.CronSpec, true, now)
		if !ok {
			// spec 已失效（不该发生，创建/更新时已校验）：停用并告警
			l.Logger.Error("cronloop: invalid spec, disabling job", slog.String("job_id", j.JobID), slog.String("spec", j.CronSpec))
			next, ok = time.Time{}, false
		}
		if uerr := l.Store.UpdateJobNextRun(ctx, j.JobID, next); uerr != nil {
			// 推进失败 → 下个 tick 以新 task_id 重触发同一动作（task_id 幂等拦不住），
			// 显式告警供运维介入
			l.Logger.Error("cronloop: advance next_run failed, duplicate fire risk",
				slog.String("job_id", j.JobID), slog.Any("err", uerr))
		}
		l.Logger.Info("cronloop: job fired",
			slog.String("job_id", j.JobID), slog.String("action", j.ActionID), slog.String("task_id", newID))
	}
}

func (l *Loop) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

func (l *Loop) newTaskID() string {
	if l.NewTaskID != nil {
		return l.NewTaskID()
	}
	return uuid.NewString()
}
