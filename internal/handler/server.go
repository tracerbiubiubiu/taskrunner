// Package handler HTTP 层（微服务结构重构 2026-09-04：只做绑定/映射，
// 业务在 service.TaskService）。路由挂载于 app 装配：全局 RequestID + AccessLog（C1），
// /v1 组 AK/SK 验签（C2）；/healthz /readyz 裸露（探针）。
package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tracerbiubiubiu/zhuzhao-utils/errcode"
	"github.com/tracerbiubiubiu/zhuzhao-utils/response"

	"github.com/tracerbiubiubiu/taskrunner/internal/middleware"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/service"
)

type Deps struct {
	Tasks  *service.TaskService
	Ready  func() error // readyz 探针（检 Redis/DB 依赖；nil = 恒就绪）
	Keys   map[string][]byte
	Logger *slog.Logger
}

func New(d Deps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), middleware.RequestID(), middleware.AccessLog(d.Logger))

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", func(c *gin.Context) {
		if d.Ready != nil {
			if err := d.Ready(); err != nil {
				// 探针免鉴权：不回传内部错误细节（避免泄露拓扑），详情进日志
				d.Logger.Error("readyz: dependency unavailable", "err", err)
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unready"})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	v1 := r.Group("/v1", middleware.AKSKAuth(d.Keys))
	{
		v1.POST("/tasks", d.submitTask)
		v1.GET("/tasks/:id", d.getTask)
		v1.POST("/tasks/cancel", d.cancelTask)
		v1.POST("/tasks/retry", d.retryTask)
		v1.GET("/runs", d.listRuns)
		v1.GET("/dead-letters", d.listDeadLetters)
		v1.GET("/jobs", d.listJobs)
		v1.POST("/jobs", d.createJob)
		v1.POST("/jobs/update", d.patchJob)
		v1.POST("/jobs/trigger", d.triggerJob)
	}
	return r
}

// ---- 提交 ----

type submitReq struct {
	TaskID      string          `json:"task_id"`
	RequestID   string          `json:"request_id"`
	Action      string          `json:"action" binding:"required"`
	Dept        string          `json:"dept"` // 一次性任务归属标签（zhuzhao E-⑤ 携带，C11 快照列）
	CallbackURL string          `json:"callback_url" binding:"required"`
	Params      json.RawMessage `json:"params"`
	SubmittedBy string          `json:"submitted_by"` // 工号（审计归因）
	SourceIP    string          `json:"source_ip"`
	TimeoutSecs int             `json:"timeout_secs"`
}

func (d *Deps) submitTask(c *gin.Context) {
	var req submitReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "action 与 callback_url 必填")
		return
	}
	// request_id 兜底：zhuzhao client 签名头携带 X-Request-ID，body 可不带——
	// 取中间件读到的头值，保证 job_runs.request_id 与跨查链贯通
	if req.RequestID == "" {
		req.RequestID = c.GetString("request_id")
	}
	// params 上限（与 zhuzhao 提交入口对称；防大参数滥用）
	if len(req.Params) > 64<<10 {
		response.BadRequest(c, "params 超过上限（64KB）")
		return
	}
	if len(req.Params) > 0 && !json.Valid(req.Params) {
		response.BadRequest(c, "params 不是合法 JSON")
		return
	}
	out, err := d.Tasks.Submit(c.Request.Context(), service.SubmitInput{
		Dept:   req.Dept,
		TaskID: req.TaskID, RequestID: req.RequestID, Action: req.Action,
		CallbackURL: req.CallbackURL, Params: req.Params,
		SubmittedBy: req.SubmittedBy, SourceIP: req.SourceIP, TimeoutSecs: req.TimeoutSecs,
	})
	if err != nil {
		d.fail(c, err, "提交失败")
		return
	}
	d.logOp(c, "submit", out.TaskID)
	response.OK(c, out)
}

// ---- 查询 ----

func (d *Deps) getTask(c *gin.Context) {
	v, err := d.Tasks.GetTask(c.Request.Context(), c.Param("id"))
	if err != nil {
		d.fail(c, err, "查询失败")
		return
	}
	response.OK(c, v)
}

func (d *Deps) listRuns(c *gin.Context) {
	q := service.RunQuery{
		RequestID: c.Query("request_id"), Action: c.Query("action"),
		Status: c.Query("status"), JobID: c.Query("job_id"), Depts: c.QueryArray("dept"),
		Page: atoi(c.Query("page"), 1), PageSize: atoi(c.Query("page_size"), 20),
	}
	for k, dest := range map[string]**time.Time{"from": &q.From, "to": &q.To} {
		if v := c.Query(k); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				response.BadRequest(c, k+" 需为 RFC3339 时间")
				return
			}
			*dest = &t
		}
	}
	runs, total, err := d.Tasks.ListRuns(c.Request.Context(), q)
	if err != nil {
		d.fail(c, err, "查询失败")
		return
	}
	response.OKPage(c, runs, total, clampPage(q.Page), clampSize(q.PageSize, 20))
}

func (d *Deps) listDeadLetters(c *gin.Context) {
	list, err := d.Tasks.ListDeadLetters(c.Request.Context(), atoi(c.Query("page_size"), 50))
	if err != nil {
		d.fail(c, err, "查询失败")
		return
	}
	response.OK(c, list)
}

// ---- 干预 ----

// taskIDReq 干预类操作的请求体（C10：POST URL 不携带业务信息，标识全在 body）。
type taskIDReq struct {
	TaskID string `json:"task_id" binding:"required"`
}

func (d *Deps) cancelTask(c *gin.Context) {
	var req taskIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "task_id 必填")
		return
	}
	if err := d.Tasks.CancelTask(c.Request.Context(), req.TaskID); err != nil {
		d.fail(c, err, "取消失败")
		return
	}
	d.logOp(c, "cancel", req.TaskID)
	response.OKWithMessage(c, "已取消", gin.H{"task_id": req.TaskID, "status": "canceled"})
}

func (d *Deps) retryTask(c *gin.Context) {
	var req taskIDReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "task_id 必填")
		return
	}
	if err := d.Tasks.RetryTask(c.Request.Context(), req.TaskID); err != nil {
		d.fail(c, err, "重试失败")
		return
	}
	d.logOp(c, "retry", req.TaskID)
	response.OKWithMessage(c, "已重新入队", gin.H{"task_id": req.TaskID, "status": "pending"})
}

// ---- 任务定义 ----

type createJobReq struct {
	ActionID     string          `json:"action_id" binding:"required"`
	CallbackURL  string          `json:"callback_url" binding:"required"`
	TriggerType  string          `json:"trigger_type" binding:"required"`
	CronSpec     string          `json:"cron_spec"`
	Params       json.RawMessage `json:"params"`
	Dept         string          `json:"dept"`
	Enabled      *bool           `json:"enabled"`
	TimeoutSecs  int             `json:"timeout_secs"`
	Description  string          `json:"description"`
	CreatedBy    string          `json:"created_by"`
	OwnerService string          `json:"owner_service"`
}

func (d *Deps) createJob(c *gin.Context) {
	var req createJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "action_id / callback_url / trigger_type 必填")
		return
	}
	if !validTimeoutSecs(req.TimeoutSecs) {
		response.BadRequest(c, "timeout_secs 须在 0–86400 秒")
		return
	}
	// params 上限（与 zhuzhao 提交入口对称；防大参数滥用）
	if len(req.Params) > 64<<10 {
		response.BadRequest(c, "params 超过上限（64KB）")
		return
	}
	if len(req.Params) > 0 && !json.Valid(req.Params) {
		response.BadRequest(c, "params 不是合法 JSON")
		return
	}
	v, err := d.Tasks.CreateJob(c.Request.Context(), service.JobInput{
		ActionID: req.ActionID, CallbackURL: req.CallbackURL, TriggerType: req.TriggerType,
		CronSpec: req.CronSpec, Params: req.Params, Dept: req.Dept, Enabled: req.Enabled,
		TimeoutSecs: req.TimeoutSecs, Description: req.Description,
		CreatedBy: req.CreatedBy, OwnerService: req.OwnerService,
	})
	if err != nil {
		d.fail(c, err, "创建失败")
		return
	}
	d.logOp(c, "create_job", v.JobID)
	response.OK(c, v)
}

func (d *Deps) listJobs(c *gin.Context) {
	f := jobFilterOf(c)
	jobs, total, err := d.Tasks.ListJobs(c.Request.Context(), f)
	if err != nil {
		d.fail(c, err, "查询失败")
		return
	}
	response.OKPage(c, jobs, total, clampPage(f.Page), clampSize(f.PageSize, 50))
}

type patchJobReq struct {
	JobID       string          `json:"job_id" binding:"required"`
	CallbackURL *string         `json:"callback_url"`
	CronSpec    *string         `json:"cron_spec"`
	Params      json.RawMessage `json:"params"`
	Dept        *string         `json:"dept"`
	Enabled     *bool           `json:"enabled"`
	TimeoutSecs *int            `json:"timeout_secs"`
	Description *string         `json:"description"`
}

func (d *Deps) patchJob(c *gin.Context) {
	var req patchJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "job_id 必填")
		return
	}
	if req.TimeoutSecs != nil && !validTimeoutSecs(*req.TimeoutSecs) {
		response.BadRequest(c, "timeout_secs 须在 0–86400 秒")
		return
	}
	if len(req.Params) > 64<<10 {
		response.BadRequest(c, "params 超过上限（64KB）")
		return
	}
	if len(req.Params) > 0 && !json.Valid(req.Params) {
		response.BadRequest(c, "params 不是合法 JSON")
		return
	}
	v, err := d.Tasks.UpdateJob(c.Request.Context(), req.JobID, service.JobPatch{
		CallbackURL: req.CallbackURL, CronSpec: req.CronSpec, Params: req.Params,
		Dept: req.Dept, Enabled: req.Enabled, TimeoutSecs: req.TimeoutSecs, Description: req.Description,
	})
	if err != nil {
		d.fail(c, err, "更新失败")
		return
	}
	d.logOp(c, "patch_job", v.JobID)
	response.OK(c, v)
}

type triggerReq struct {
	JobID     string `json:"job_id" binding:"required"`
	RequestID string `json:"request_id"`
	Actor     string `json:"actor"`
	SourceIP  string `json:"source_ip"`
}

func (d *Deps) triggerJob(c *gin.Context) {
	var req triggerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "job_id 必填")
		return
	}
	if req.RequestID == "" {
		req.RequestID = c.GetString("request_id") // 同 submitTask：头值兜底
	}
	out, err := d.Tasks.TriggerJob(c.Request.Context(), req.JobID, service.TriggerInput{
		RequestID: req.RequestID, Actor: req.Actor, SourceIP: req.SourceIP,
	})
	if err != nil {
		d.fail(c, err, "触发失败")
		return
	}
	d.logOp(c, "trigger_job", req.JobID)
	response.OK(c, out)
}

// ---- 工具 ----

// fail service 错误 → HTTP 映射：NotFound→404 / Invalid→400 / Conflict→409 / 其他→500。
func (d *Deps) fail(c *gin.Context, err error, internalMsg string) {
	var inv *service.InvalidError
	var con *service.ConflictError
	switch {
	case errors.Is(err, service.ErrTaskNotFound), errors.Is(err, service.ErrJobNotFound):
		response.NotFound(c, "对象不存在")
	case errors.As(err, &inv):
		response.BadRequest(c, inv.Msg)
	case errors.As(err, &con):
		response.Fail(c, http.StatusConflict, errcode.ErrConflict.Code, con.Msg)
	default:
		d.Logger.Error("handler: service error", "err", err, "op", c.Request.Method+" "+c.Request.URL.Path)
		response.InternalError(c, internalMsg)
	}
}

// logOp 写操作归因打点（C1 访问日志之外的定向补充：op + 对象 + caller + request_id）。
func (d *Deps) logOp(c *gin.Context, op, object string) {
	d.Logger.Info("api op", "op", op, "object", object,
		"caller", c.GetString("caller"), "request_id", c.GetString("request_id"))
}

func jobFilterOf(c *gin.Context) repository.JobFilter {
	f := repository.JobFilter{Dept: c.Query("dept"), Action: c.Query("action_id")}
	if v := c.Query("enabled"); v != "" {
		b := v == "true" || v == "1"
		f.Enabled = &b
	}
	f.Page = atoi(c.Query("page"), 1)
	f.PageSize = atoi(c.Query("page_size"), 50)
	return f
}

// clampPage / clampSize 与 repository 钳制口径一致，回显生效值防响应元数据失真。
func clampPage(p int) int {
	if p < 1 {
		return 1
	}
	return p
}

func clampSize(size, def int) int {
	if size < 1 || size > 200 {
		return def
	}
	return size
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

// validTimeoutSecs 回调/任务超时上界（审计 M2）：0 = 沿用默认 30s；
// 正值上限 1 天——无上界的 timeout 会经 asynq 占死 worker 槽位并阻塞优雅停机。
func validTimeoutSecs(secs int) bool { return secs >= 0 && secs <= 86400 }
