// Package api taskrunner 内部 HTTP API v1（设计文档 §5）。
// 仅内网 + 服务级鉴权（静态 Bearer token）；用户权限全部收敛在 zhuzhao 网关。
// 响应结构 / 错误码复用 zhuzhao-utils errcode + response，与网关一致。
package api

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/zhuzhao-utils/errcode"
	"github.com/tracerbiubiubiu/zhuzhao-utils/response"

	"github.com/tracerbiubiubiu/taskrunner/internal/cronloop"
	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// TaskInspector Asynq 检视能力（死信 / 取消 / 重试 / 实时态）。生产实现 = *asynq.Inspector。
type TaskInspector interface {
	GetTaskInfo(queue, id string) (*asynq.TaskInfo, error)
	DeleteTask(queue, id string) error
	RunTask(queue, id string) error
	ListArchivedTasks(queue string, opts ...asynq.ListOption) ([]*asynq.TaskInfo, error)
}

type Submitter interface {
	Submit(ctx context.Context, p task.Payload) (accepted bool, warning error, err error)
}

type Deps struct {
	Store     *store.Store
	Submitter Submitter
	Inspector TaskInspector
	Queue     string
	Token     string // 内网 credential（静态 Bearer）
	Logger    *slog.Logger
}

func New(d Deps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/v1", auth(d.Token))
	{
		v1.POST("/tasks", d.submitTask)
		v1.GET("/tasks/:id", d.getTask)
		v1.POST("/tasks/:id/cancel", d.cancelTask)
		v1.POST("/tasks/:id/retry", d.retryTask)
		v1.GET("/runs", d.listRuns)
		v1.GET("/dead-letters", d.listDeadLetters)
		v1.GET("/jobs", d.listJobs)
		v1.POST("/jobs", d.createJob)
		v1.PATCH("/jobs/:id", d.patchJob)
		v1.POST("/jobs/:id/trigger", d.triggerJob)
	}
	return r
}

// auth 静态 Bearer token（常量时间比较防时序侧信道）。
func auth(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		got, ok := strings.CutPrefix(h, "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			response.Unauthorized(c, "invalid or missing bearer token")
			c.Abort()
			return
		}
		c.Set("caller", "zhuzhao") // 当前唯一调用方；多调用方时代改由 credential 映射
		c.Next()
	}
}

// ---- 提交 ----

type submitReq struct {
	TaskID      string          `json:"task_id"`
	RequestID   string          `json:"request_id"`
	Action      string          `json:"action" binding:"required"`
	CallbackURL string          `json:"callback_url" binding:"required"`
	Params      json.RawMessage `json:"params"`
	SubmittedBy string          `json:"submitted_by"` // 工号（审计归因）
	SourceIP    string          `json:"source_ip"`
	TimeoutSecs int             `json:"timeout_secs"`
}

// submitTask 受理语义（§5）：校验 + 入队（Redis AOF）即返回；受理 ≠ 执行成功。
func (d *Deps) submitTask(c *gin.Context) {
	var req submitReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "action 与 callback_url 必填")
		return
	}
	if req.TaskID == "" {
		req.TaskID = uuid.NewString()
	}
	accepted, warning, err := d.Submitter.Submit(c.Request.Context(), task.Payload{
		TaskID: req.TaskID, RequestID: req.RequestID, Action: req.Action,
		CallbackURL: req.CallbackURL, Params: req.Params,
		SubmittedBy: req.SubmittedBy, SourceIP: req.SourceIP, TimeoutSecs: req.TimeoutSecs,
	})
	if err != nil {
		d.Logger.Error("api: submit failed", slog.Any("err", err), slog.String("action", req.Action))
		response.InternalError(c, "提交失败")
		return
	}
	if warning != nil {
		d.Logger.Warn("api: submitted but job_runs insert failed",
			slog.String("task_id", req.TaskID), slog.Any("err", warning))
	}
	d.logOp(c, "submit", req.TaskID)
	response.OK(c, gin.H{"task_id": req.TaskID, "accepted": accepted})
}

// ---- 查询 ----

// getTask 状态数据源定稿（§10）：job_runs 为主，Asynq 实时态补充 in-flight（pending/active/retry/scheduled 只在 Redis）。
func (d *Deps) getTask(c *gin.Context) {
	id := c.Param("id")
	run, err := d.Store.GetByTaskID(c.Request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		response.NotFound(c, "任务不存在")
		return
	}
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	out := gin.H{
		"task_id": run.TaskID, "request_id": run.RequestID, "action": run.Action, "job_id": run.JobID,
		"status": run.Status, "attempts": run.Attempts, "error": run.Error, "duration_ms": run.DurationMS,
		"submitted_by": run.SubmittedBy, "source_ip": run.SourceIP,
		"enqueued_at": run.EnqueuedAt, "started_at": nullTime(run.StartedAt), "finished_at": nullTime(run.FinishedAt),
	}
	if info, err := d.Inspector.GetTaskInfo(d.Queue, id); err == nil {
		out["live_state"] = info.State.String()
	}
	response.OK(c, out)
}

// listRuns GET /v1/runs（§7 request_id 跨查即此）。
func (d *Deps) listRuns(c *gin.Context) {
	f := store.RunFilter{
		RequestID: c.Query("request_id"),
		Action:    c.Query("action"),
		Status:    c.Query("status"),
		JobID:     c.Query("job_id"),
		Page:      atoi(c.Query("page"), 1),
		PageSize:  atoi(c.Query("page_size"), 20),
	}
	if v := c.Query("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.From = &t
		} else {
			response.BadRequest(c, "from 需为 RFC3339 时间")
			return
		}
	}
	if v := c.Query("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.To = &t
		} else {
			response.BadRequest(c, "to 需为 RFC3339 时间")
			return
		}
	}
	runs, total, err := d.Store.ListRuns(c.Request.Context(), f)
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	list := make([]gin.H, 0, len(runs))
	for _, r := range runs {
		list = append(list, gin.H{
			"task_id": r.TaskID, "request_id": r.RequestID, "action": r.Action, "job_id": r.JobID,
			"status": r.Status, "attempts": r.Attempts, "error": r.Error, "duration_ms": r.DurationMS,
			"submitted_by": r.SubmittedBy, "enqueued_at": r.EnqueuedAt,
			"started_at": nullTime(r.StartedAt), "finished_at": nullTime(r.FinishedAt),
		})
	}
	response.OKPage(c, list, total, f.Page, f.PageSize)
}

// listDeadLetters 死信列表（asynq archived；配合 retry 端点形成死信处理闭环）。
func (d *Deps) listDeadLetters(c *gin.Context) {
	tasks, err := d.Inspector.ListArchivedTasks(d.Queue, asynq.PageSize(atoi(c.Query("page_size"), 50)))
	if err != nil {
		d.Logger.Error("api: list archived failed", slog.Any("err", err))
		response.InternalError(c, "查询失败")
		return
	}
	list := make([]gin.H, 0, len(tasks))
	for _, t := range tasks {
		list = append(list, gin.H{
			"task_id": t.ID, "type": t.Type, "state": t.State.String(),
			"last_error": t.LastErr, "retried": t.Retried, "max_retry": t.MaxRetry,
			"last_failed_at": t.LastFailedAt,
		})
	}
	response.OK(c, list)
}

// ---- 干预 ----

// cancelTask 取消未开始的任务（pending/scheduled）；执行中 → 409。
func (d *Deps) cancelTask(c *gin.Context) {
	id := c.Param("id")
	run, err := d.Store.GetByTaskID(c.Request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		response.NotFound(c, "任务不存在")
		return
	}
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	if run.Status != store.StatusPending {
		response.Fail(c, http.StatusConflict, errcode.ErrConflict.Code, "仅未开始的任务可取消，当前状态: "+run.Status)
		return
	}
	if err := d.Inspector.DeleteTask(d.Queue, id); err != nil && !errors.Is(err, asynq.ErrTaskNotFound) {
		d.Logger.Error("api: cancel delete failed", slog.String("task_id", id), slog.Any("err", err))
		response.InternalError(c, "取消失败")
		return
	}
	if err := d.Store.MarkCanceled(c.Request.Context(), id, "canceled via API", time.Now()); err != nil {
		response.InternalError(c, "取消失败")
		return
	}
	d.logOp(c, "cancel", id)
	response.OKWithMessage(c, "已取消", gin.H{"task_id": id, "status": store.StatusCanceled})
}

// retryTask 重试终败/死信任务：Asynq archived → pending，job_runs 同步重置。
func (d *Deps) retryTask(c *gin.Context) {
	id := c.Param("id")
	run, err := d.Store.GetByTaskID(c.Request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		response.NotFound(c, "任务不存在")
		return
	}
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	if run.Status != store.StatusFailed && run.Status != store.StatusDead {
		response.Fail(c, http.StatusConflict, errcode.ErrConflict.Code, "仅失败/死信任务可重试，当前状态: "+run.Status)
		return
	}
	if err := d.Inspector.RunTask(d.Queue, id); err != nil {
		if errors.Is(err, asynq.ErrTaskNotFound) {
			response.Fail(c, http.StatusConflict, errcode.ErrConflict.Code, "队列中已无该任务（可能已被清理），无法重试")
			return
		}
		d.Logger.Error("api: retry run failed", slog.String("task_id", id), slog.Any("err", err))
		response.InternalError(c, "重试失败")
		return
	}
	if err := d.Store.ResetPending(c.Request.Context(), id); err != nil {
		response.InternalError(c, "重试失败")
		return
	}
	d.logOp(c, "retry", id)
	response.OKWithMessage(c, "已重新入队", gin.H{"task_id": id, "status": store.StatusPending})
}

// ---- 任务定义 ----

type createJobReq struct {
	ActionID     string          `json:"action_id" binding:"required"`
	CallbackURL  string          `json:"callback_url" binding:"required"`
	TriggerType  string          `json:"trigger_type" binding:"required"` // cron | manual
	CronSpec     string          `json:"cron_spec"`
	Params       json.RawMessage `json:"params"`
	Dept         string          `json:"dept"`
	Enabled      *bool           `json:"enabled"`
	TimeoutSecs  int             `json:"timeout_secs"`
	Description  string          `json:"description"`
	CreatedBy    string          `json:"created_by"` // 工号
	SourceIP     string          `json:"source_ip"`
}

func (d *Deps) createJob(c *gin.Context) {
	var req createJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "action_id / callback_url / trigger_type 必填")
		return
	}
	if req.TriggerType != store.TriggerCron && req.TriggerType != store.TriggerManual {
		response.BadRequest(c, "trigger_type 仅支持 cron | manual")
		return
	}
	if req.TriggerType == store.TriggerCron {
		if _, err := cronloop.ParseSpec(req.CronSpec); err != nil {
			response.BadRequest(c, "cron_spec 非法: "+err.Error())
			return
		}
	}
	enabled := req.Enabled == nil || *req.Enabled
	params := "{}"
	if len(req.Params) > 0 {
		params = string(req.Params)
	}
	now := time.Now()
	next, hasNext := cronloop.NextRun(req.CronSpec, enabled && req.TriggerType == store.TriggerCron, now)
	j := store.Job{
		JobID: uuid.NewString(), ActionID: req.ActionID, TriggerType: req.TriggerType,
		CallbackURL: req.CallbackURL, CronSpec: req.CronSpec, Params: params, Dept: req.Dept,
		Enabled: enabled, TimeoutSecs: req.TimeoutSecs, Description: req.Description,
		CreatedBy: req.CreatedBy, OwnerService: "zhuzhao", CreatedAt: now, UpdatedAt: now,
	}
	if hasNext {
		j.NextRun.Valid, j.NextRun.Time = true, next
	}
	if err := d.Store.CreateJob(c.Request.Context(), j); err != nil {
		d.Logger.Error("api: create job failed", slog.Any("err", err))
		response.InternalError(c, "创建失败")
		return
	}
	d.logOp(c, "create_job", j.JobID)
	response.OK(c, jobView(&j, next, hasNext))
}

func (d *Deps) listJobs(c *gin.Context) {
	f := store.JobFilter{Dept: c.Query("dept"), Action: c.Query("action_id")}
	if v := c.Query("enabled"); v != "" {
		b := v == "true" || v == "1"
		f.Enabled = &b
	}
	jobs, err := d.Store.ListJobs(c.Request.Context(), f)
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	list := make([]gin.H, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, jobView(j, j.NextRun.Time, j.NextRun.Valid))
	}
	response.OK(c, list)
}

type patchJobReq struct {
	CallbackURL *string         `json:"callback_url"`
	CronSpec    *string         `json:"cron_spec"`
	Params      json.RawMessage `json:"params"`
	Dept        *string         `json:"dept"`
	Enabled     *bool           `json:"enabled"`
	TimeoutSecs *int            `json:"timeout_secs"`
	Description *string         `json:"description"`
	UpdatedBy   string          `json:"updated_by"` // 工号
}

// patchJob PATCH 语义：改 cron / 改参数 / 启停；cron 生效时间 = 下一个 cronloop tick（≤30s）。
func (d *Deps) patchJob(c *gin.Context) {
	id := c.Param("id")
	j, err := d.Store.GetJob(c.Request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		response.NotFound(c, "任务定义不存在")
		return
	}
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	var req patchJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if req.CallbackURL != nil {
		j.CallbackURL = *req.CallbackURL
	}
	if req.CronSpec != nil {
		if j.TriggerType != store.TriggerCron {
			response.BadRequest(c, "仅 cron 类型定义可改 cron_spec")
			return
		}
		if _, err := cronloop.ParseSpec(*req.CronSpec); err != nil {
			response.BadRequest(c, "cron_spec 非法: "+err.Error())
			return
		}
		j.CronSpec = *req.CronSpec
	}
	if req.Params != nil {
		j.Params = string(req.Params)
	}
	if req.Dept != nil {
		j.Dept = *req.Dept
	}
	if req.Enabled != nil {
		j.Enabled = *req.Enabled
	}
	if req.TimeoutSecs != nil {
		j.TimeoutSecs = *req.TimeoutSecs
	}
	if req.Description != nil {
		j.Description = *req.Description
	}
	j.UpdatedAt = time.Now()
	next, hasNext := cronloop.NextRun(j.CronSpec, j.Enabled && j.TriggerType == store.TriggerCron, j.UpdatedAt)
	j.NextRun.Valid, j.NextRun.Time = hasNext, next
	if err := d.Store.UpdateJob(c.Request.Context(), *j); err != nil {
		d.Logger.Error("api: patch job failed", slog.Any("err", err))
		response.InternalError(c, "更新失败")
		return
	}
	d.logOp(c, "patch_job", id)
	response.OK(c, jobView(j, next, hasNext))
}

type triggerReq struct {
	RequestID string `json:"request_id"`
	Actor     string `json:"actor"`     // 工号
	SourceIP  string `json:"source_ip"`
}

// triggerJob 手动执行一次：按定义组装参数，走统一提交路径（§4 前端「立即执行」按钮）。
func (d *Deps) triggerJob(c *gin.Context) {
	id := c.Param("id")
	j, err := d.Store.GetJob(c.Request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		response.NotFound(c, "任务定义不存在")
		return
	}
	if err != nil {
		response.InternalError(c, "查询失败")
		return
	}
	if !j.Enabled {
		response.Fail(c, http.StatusConflict, errcode.ErrConflict.Code, "任务定义已停用")
		return
	}
	var req triggerReq
	_ = c.ShouldBindJSON(&req) // body 可空
	taskID := uuid.NewString()
	accepted, warning, err := d.Submitter.Submit(c.Request.Context(), task.Payload{
		TaskID: taskID, RequestID: req.RequestID, Action: j.ActionID, JobID: j.JobID,
		CallbackURL: j.CallbackURL, Params: []byte(j.Params),
		SubmittedBy: req.Actor, SourceIP: req.SourceIP, TimeoutSecs: j.TimeoutSecs,
	})
	if err != nil {
		d.Logger.Error("api: trigger failed", slog.Any("err", err), slog.String("job_id", id))
		response.InternalError(c, "触发失败")
		return
	}
	if warning != nil {
		d.Logger.Warn("api: triggered but job_runs insert failed", slog.String("task_id", taskID), slog.Any("err", warning))
	}
	d.logOp(c, "trigger_job", id)
	response.OK(c, gin.H{"task_id": taskID, "accepted": accepted})
}

// ---- 工具 ----

func jobView(j *store.Job, next time.Time, hasNext bool) gin.H {
	h := gin.H{
		"job_id": j.JobID, "action_id": j.ActionID, "trigger_type": j.TriggerType,
		"cron_spec": j.CronSpec, "callback_url": j.CallbackURL, "params": json.RawMessage(j.Params),
		"dept": j.Dept, "enabled": j.Enabled, "timeout_secs": j.TimeoutSecs, "description": j.Description,
		"created_by": j.CreatedBy, "owner_service": j.OwnerService,
		"created_at": j.CreatedAt, "updated_at": j.UpdatedAt,
	}
	if hasNext {
		h["next_run"] = next
	}
	return h
}

// logOp API 写操作归因打点（§5：caller + 对象；actor 在各 handler 的业务日志中）。
func (d *Deps) logOp(c *gin.Context, op, object string) {
	d.Logger.Info("api op",
		slog.String("op", op), slog.String("object", object), slog.String("caller", c.GetString("caller")))
}

func nullTime(t sql.NullTime) any {
	if t.Valid {
		return t.Time
	}
	return nil
}

func atoi(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
