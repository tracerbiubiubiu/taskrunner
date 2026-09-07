// task 业务服务层（微服务结构重构 2026-09-04：原 handler 内业务下沉）。
// handler 只做绑定/映射；本层持有全部业务规则（幂等受理、状态机干预、
// 任务定义校验与 next_run 推进、A2 取消三道防护）。
package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// TaskInspector Asynq 检视能力（死信/取消/重试/实时态）。生产实现 = *asynq.Inspector。
type TaskInspector interface {
	GetTaskInfo(queue, id string) (*asynq.TaskInfo, error)
	DeleteTask(queue, id string) error
	RunTask(queue, id string) error
	ListArchivedTasks(queue string, opts ...asynq.ListOption) ([]*asynq.TaskInfo, error)
}

// 业务错误（handler 映射 HTTP；不携带 HTTP 语义）。
var (
	ErrTaskNotFound  = errors.New("service: task not found")
	ErrJobNotFound   = errors.New("service: job not found")
	ErrStateConflict = errors.New("service: state conflict")
)

// InvalidError 参数/校验错误（→ 400）。
type InvalidError struct{ Msg string }

func (e *InvalidError) Error() string { return e.Msg }

func invalid(format string, a ...any) error { return &InvalidError{Msg: fmt.Sprintf(format, a...)} }

// ConflictError 状态冲突（→ 409，带人话原因）。
type ConflictError struct{ Msg string }

func (e *ConflictError) Error() string { return e.Msg }

func conflict(format string, a ...any) error { return &ConflictError{Msg: fmt.Sprintf(format, a...)} }

// TaskService 任务域服务。
type TaskService struct {
	repo      *repository.Store
	submitter Submitter
	inspector TaskInspector
	queue     string
}

func NewTaskService(repo *repository.Store, submitter Submitter, inspector TaskInspector, queue string) *TaskService {
	return &TaskService{repo: repo, submitter: submitter, inspector: inspector, queue: queue}
}

// ---- 提交 ----

// SubmitInput 一次性任务提交（受理语义：accepted ≠ 执行成功）。
type SubmitInput struct {
	TaskID      string
	RequestID   string
	Action      string
	Dept        string // 一次性任务的归属标签（zhuzhao 携带，C11 快照）
	CallbackURL string
	Params      json.RawMessage
	SubmittedBy string
	SourceIP    string
	TimeoutSecs int
}

// SubmitOutput 受理结果。
type SubmitOutput struct {
	TaskID   string `json:"task_id"`
	Accepted bool   `json:"accepted"`
}

func (s *TaskService) Submit(ctx context.Context, in SubmitInput) (*SubmitOutput, error) {
	if in.Action == "" || in.CallbackURL == "" {
		return nil, invalid("action 与 callback_url 必填")
	}
	if in.TaskID == "" {
		in.TaskID = uuid.NewString()
	}
	accepted, _, err := s.submitter.Submit(ctx, task.Payload{
		TaskID: in.TaskID, RequestID: in.RequestID, Action: in.Action, Dept: in.Dept,
		CallbackURL: in.CallbackURL, Params: in.Params,
		SubmittedBy: in.SubmittedBy, SourceIP: in.SourceIP, TimeoutSecs: in.TimeoutSecs,
	})
	if err != nil {
		return nil, fmt.Errorf("submit: %w", err)
	}
	return &SubmitOutput{TaskID: in.TaskID, Accepted: accepted}, nil
}

// ---- 查询 ----

// TaskView 任务全景（job_runs 为主 + live_state 补 in-flight 实时态，§10 定案）。
type TaskView struct {
	TaskID      string     `json:"task_id"`
	RequestID   string     `json:"request_id"`
	Action      string     `json:"action"`
	JobID       string     `json:"job_id"`
	Dept        string     `json:"dept"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	Error       string     `json:"error"`
	DurationMS  int64      `json:"duration_ms"`
	SubmittedBy string     `json:"submitted_by"`
	SourceIP    string     `json:"source_ip"`
	EnqueuedAt  time.Time  `json:"enqueued_at"`
	StartedAt   *time.Time `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at"`
	LiveState   string     `json:"live_state,omitempty"`
}

// RunView 执行记录行。
type RunView struct {
	TaskID      string     `json:"task_id"`
	RequestID   string     `json:"request_id"`
	Action      string     `json:"action"`
	JobID       string     `json:"job_id"`
	Dept        string     `json:"dept"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	Error       string     `json:"error"`
	DurationMS  int64      `json:"duration_ms"`
	SubmittedBy string     `json:"submitted_by"`
	EnqueuedAt  time.Time  `json:"enqueued_at"`
	StartedAt   *time.Time `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at"`
}

func runView(r *repository.Run) RunView {
	return RunView{
		TaskID: r.TaskID, RequestID: r.RequestID, Action: r.Action, JobID: r.JobID, Dept: r.Dept,
		Status: r.Status, Attempts: r.Attempts, Error: r.Error, DurationMS: r.DurationMS,
		SubmittedBy: r.SubmittedBy, EnqueuedAt: r.EnqueuedAt,
		StartedAt: nullTime(r.StartedAt), FinishedAt: nullTime(r.FinishedAt),
	}
}

func nullTime(t sql.NullTime) *time.Time {
	if t.Valid {
		return &t.Time
	}
	return nil
}

func (s *TaskService) GetTask(ctx context.Context, taskID string) (*TaskView, error) {
	run, err := s.repo.GetByTaskID(ctx, taskID)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	v := &TaskView{
		TaskID: run.TaskID, RequestID: run.RequestID, Action: run.Action, JobID: run.JobID, Dept: run.Dept,
		Status: run.Status, Attempts: run.Attempts, Error: run.Error, DurationMS: run.DurationMS,
		SubmittedBy: run.SubmittedBy, SourceIP: run.SourceIP,
		EnqueuedAt: run.EnqueuedAt, StartedAt: nullTime(run.StartedAt), FinishedAt: nullTime(run.FinishedAt),
	}
	if info, err := s.inspector.GetTaskInfo(s.queue, taskID); err == nil {
		v.LiveState = info.State.String()
	}
	return v, nil
}

// RunQuery 执行记录查询（全部可选；from/to 为 RFC3339）。
type RunQuery struct {
	RequestID, Action, Status, JobID string
	Depts                            []string
	From, To                         *time.Time
	Page, PageSize                   int
}

func (s *TaskService) ListRuns(ctx context.Context, q RunQuery) ([]RunView, int64, error) {
	runs, total, err := s.repo.ListRuns(ctx, repository.RunFilter{
		RequestID: q.RequestID, Action: q.Action, Status: q.Status, JobID: q.JobID, Depts: q.Depts,
		From: q.From, To: q.To, Page: q.Page, PageSize: q.PageSize,
	})
	if err != nil {
		return nil, 0, err
	}
	out := make([]RunView, 0, len(runs))
	for _, r := range runs {
		out = append(out, runView(r))
	}
	return out, total, nil
}

// DeadLetterView 死信行（asynq archived）。
type DeadLetterView struct {
	TaskID       string    `json:"task_id"`
	Type         string    `json:"type"`
	State        string    `json:"state"`
	LastError    string    `json:"last_error"`
	Retried      int       `json:"retried"`
	MaxRetry     int       `json:"max_retry"`
	LastFailedAt time.Time `json:"last_failed_at"`
}

func (s *TaskService) ListDeadLetters(ctx context.Context, pageSize int) ([]DeadLetterView, error) {
	if pageSize <= 0 {
		pageSize = 50
	}
	tasks, err := s.inspector.ListArchivedTasks(s.queue, asynq.PageSize(pageSize))
	if err != nil {
		return nil, err
	}
	out := make([]DeadLetterView, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, DeadLetterView{
			TaskID: t.ID, Type: t.Type, State: t.State.String(),
			LastError: t.LastErr, Retried: t.Retried, MaxRetry: t.MaxRetry, LastFailedAt: t.LastFailedAt,
		})
	}
	return out, nil
}

// ---- 干预 ----

// CancelTask 取消未开始的任务。竞态防护（A2 三道关卡）：
// ① 删队列前 GetTaskInfo 确认非 active；② DeleteTask 报 active 错误映射冲突（①之后被取走的兜底）；
// ③ MarkCanceledIfPending 条件更新，状态已变即冲突——保证不出现「回了已取消、实际跑完了」。
func (s *TaskService) CancelTask(ctx context.Context, taskID string) error {
	run, err := s.repo.GetByTaskID(ctx, taskID)
	if errors.Is(err, repository.ErrNotFound) {
		return ErrTaskNotFound
	}
	if err != nil {
		return err
	}
	if run.Status != repository.StatusPending {
		return conflict("仅未开始的任务可取消，当前状态: %s", run.Status)
	}
	if info, err := s.inspector.GetTaskInfo(s.queue, taskID); err == nil {
		if info.State == asynq.TaskStateActive {
			return conflict("任务正在执行，无法取消")
		}
	} else if !errors.Is(err, asynq.ErrTaskNotFound) {
		return err
	}
	// ErrTaskNotFound：DB 为 pending 但队列已无此任务（提交后入队失败的残留），直接标 canceled
	if err := s.inspector.DeleteTask(s.queue, taskID); err != nil && !errors.Is(err, asynq.ErrTaskNotFound) {
		if strings.Contains(err.Error(), "active state") { // 检查后瞬间被 worker 取走
			return conflict("任务正在执行，无法取消")
		}
		return err
	}
	ok, err := s.repo.MarkCanceledIfPending(ctx, taskID, "canceled via API", time.Now())
	if err != nil {
		return err
	}
	if !ok {
		return conflict("任务状态已变化（可能刚开始执行），请刷新后重试")
	}
	return nil
}

// RetryTask 重试终败/死信：Asynq archived → pending，job_runs 同步重置。
func (s *TaskService) RetryTask(ctx context.Context, taskID string) error {
	run, err := s.repo.GetByTaskID(ctx, taskID)
	if errors.Is(err, repository.ErrNotFound) {
		return ErrTaskNotFound
	}
	if err != nil {
		return err
	}
	if run.Status != repository.StatusFailed && run.Status != repository.StatusDead {
		return conflict("仅失败/死信任务可重试，当前状态: %s", run.Status)
	}
	if err := s.inspector.RunTask(s.queue, taskID); err != nil {
		if errors.Is(err, asynq.ErrTaskNotFound) {
			return conflict("队列中已无该任务（可能已被清理），无法重试")
		}
		return err
	}
	return s.repo.ResetPending(ctx, taskID)
}

// ---- 任务定义 ----

// JobInput 建定义入参。
type JobInput struct {
	ActionID     string
	CallbackURL  string
	TriggerType  string // cron | manual
	CronSpec     string
	Params       json.RawMessage
	Dept         string
	Enabled      *bool
	TimeoutSecs  int
	Description  string
	CreatedBy    string
	OwnerService string
}

// JobPatch 修改定义（指针语义：nil = 不改）。
type JobPatch struct {
	CallbackURL *string
	CronSpec    *string
	Params      json.RawMessage
	Dept        *string
	Enabled     *bool
	TimeoutSecs *int
	Description *string
}

// JobView 定义视图。
type JobView struct {
	JobID        string          `json:"job_id"`
	ActionID     string          `json:"action_id"`
	TriggerType  string          `json:"trigger_type"`
	CronSpec     string          `json:"cron_spec"`
	CallbackURL  string          `json:"callback_url"`
	Params       json.RawMessage `json:"params"`
	Dept         string          `json:"dept"`
	Enabled      bool            `json:"enabled"`
	TimeoutSecs  int             `json:"timeout_secs"`
	Description  string          `json:"description"`
	CreatedBy    string          `json:"created_by"`
	OwnerService string          `json:"owner_service"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	NextRun      *time.Time      `json:"next_run,omitempty"`
}

func jobView(j *repository.Job) JobView {
	v := JobView{
		JobID: j.JobID, ActionID: j.ActionID, TriggerType: j.TriggerType,
		CronSpec: j.CronSpec, CallbackURL: j.CallbackURL, Params: json.RawMessage(j.Params),
		Dept: j.Dept, Enabled: j.Enabled, TimeoutSecs: j.TimeoutSecs, Description: j.Description,
		CreatedBy: j.CreatedBy, OwnerService: j.OwnerService,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt,
	}
	if j.NextRun.Valid {
		t := j.NextRun.Time
		v.NextRun = &t
	}
	return v
}

func (s *TaskService) CreateJob(ctx context.Context, in JobInput) (*JobView, error) {
	if in.ActionID == "" || in.CallbackURL == "" || in.TriggerType == "" {
		return nil, invalid("action_id / callback_url / trigger_type 必填")
	}
	if in.TriggerType != repository.TriggerCron && in.TriggerType != repository.TriggerManual {
		return nil, invalid("trigger_type 仅支持 cron | manual")
	}
	if in.TriggerType == repository.TriggerCron {
		if _, err := ParseSpec(in.CronSpec); err != nil {
			return nil, invalid("cron_spec 非法: %s", err.Error())
		}
	}
	enabled := in.Enabled == nil || *in.Enabled
	params := "{}"
	if len(in.Params) > 0 {
		params = string(in.Params)
	}
	if in.OwnerService == "" {
		in.OwnerService = "zhuzhao"
	}
	now := time.Now()
	next, hasNext := NextRun(in.CronSpec, enabled && in.TriggerType == repository.TriggerCron, now)
	j := repository.Job{
		JobID: uuid.NewString(), ActionID: in.ActionID, TriggerType: in.TriggerType,
		CallbackURL: in.CallbackURL, CronSpec: in.CronSpec, Params: params, Dept: in.Dept,
		Enabled: enabled, TimeoutSecs: in.TimeoutSecs, Description: in.Description,
		CreatedBy: in.CreatedBy, OwnerService: in.OwnerService, CreatedAt: now, UpdatedAt: now,
	}
	if hasNext {
		j.NextRun.Valid, j.NextRun.Time = true, next
	}
	if err := s.repo.CreateJob(ctx, j); err != nil {
		return nil, err
	}
	v := jobView(&j)
	return &v, nil
}

func (s *TaskService) ListJobs(ctx context.Context, f repository.JobFilter) ([]JobView, int64, error) {
	jobs, total, err := s.repo.ListJobs(ctx, f)
	if err != nil {
		return nil, 0, err
	}
	out := make([]JobView, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobView(j))
	}
	return out, total, nil
}

func (s *TaskService) UpdateJob(ctx context.Context, jobID string, p JobPatch) (*JobView, error) {
	j, err := s.repo.GetJob(ctx, jobID)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}
	if p.CallbackURL != nil {
		j.CallbackURL = *p.CallbackURL
	}
	if p.CronSpec != nil {
		if j.TriggerType != repository.TriggerCron {
			return nil, invalid("仅 cron 类型定义可改 cron_spec")
		}
		if _, err := ParseSpec(*p.CronSpec); err != nil {
			return nil, invalid("cron_spec 非法: %s", err.Error())
		}
		j.CronSpec = *p.CronSpec
	}
	if p.Params != nil {
		j.Params = string(p.Params)
	}
	if p.Dept != nil {
		j.Dept = *p.Dept
	}
	if p.Enabled != nil {
		j.Enabled = *p.Enabled
	}
	if p.TimeoutSecs != nil {
		j.TimeoutSecs = *p.TimeoutSecs
	}
	if p.Description != nil {
		j.Description = *p.Description
	}
	j.UpdatedAt = time.Now()
	next, hasNext := NextRun(j.CronSpec, j.Enabled && j.TriggerType == repository.TriggerCron, j.UpdatedAt)
	j.NextRun.Valid, j.NextRun.Time = hasNext, next
	if err := s.repo.UpdateJob(ctx, *j); err != nil {
		return nil, err
	}
	v := jobView(j)
	return &v, nil
}

// TriggerInput 手动触发入参（body 可空）。
type TriggerInput struct {
	RequestID string
	Actor     string
	SourceIP  string
}

// TriggerJob 手动执行一次：按定义组装参数走统一提交路径（「立即执行」按钮）。
func (s *TaskService) TriggerJob(ctx context.Context, jobID string, in TriggerInput) (*SubmitOutput, error) {
	j, err := s.repo.GetJob(ctx, jobID)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}
	if !j.Enabled {
		return nil, conflict("任务定义已停用")
	}
	taskID := uuid.NewString()
	accepted, _, err := s.submitter.Submit(ctx, task.Payload{
		TaskID: taskID, RequestID: in.RequestID, Action: j.ActionID, JobID: j.JobID, Dept: j.Dept,
		CallbackURL: j.CallbackURL, Params: []byte(j.Params),
		SubmittedBy: in.Actor, SourceIP: in.SourceIP, TimeoutSecs: j.TimeoutSecs,
	})
	if err != nil {
		return nil, err
	}
	return &SubmitOutput{TaskID: taskID, Accepted: accepted}, nil
}
