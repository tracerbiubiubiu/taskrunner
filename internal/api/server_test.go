package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeSubmitter 记录提交，可注入幂等/错误行为；持有 store 时模拟真实 Service 的落库。
type fakeSubmitter struct {
	submitted []task.Payload
	dup       bool
	err       error
	st        *store.Store
}

func (f *fakeSubmitter) Submit(ctx context.Context, p task.Payload) (bool, error, error) {
	if f.err != nil {
		return false, nil, f.err
	}
	if f.dup {
		return false, nil, nil
	}
	f.submitted = append(f.submitted, p)
	if f.st != nil {
		_ = f.st.InsertPending(ctx, store.Run{
			TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, JobID: p.JobID,
			CallbackURL: p.CallbackURL, SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP,
			EnqueuedAt: time.Now(),
		})
	}
	return true, nil, nil
}

// fakeInspector 模拟 asynq Inspector。
type fakeInspector struct {
	live    map[string]*asynq.TaskInfo // task_id → live 态（nil = 不存在）
	list    []*asynq.TaskInfo
	delErr  error
	runErr  error
	deleted []string
	rerun   []string
}

func (f *fakeInspector) GetTaskInfo(_, id string) (*asynq.TaskInfo, error) {
	if t, ok := f.live[id]; ok {
		return t, nil
	}
	return nil, asynq.ErrTaskNotFound
}
func (f *fakeInspector) DeleteTask(_, id string) error {
	if f.delErr != nil {
		return f.delErr
	}
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *fakeInspector) RunTask(_, id string) error {
	if f.runErr != nil {
		return f.runErr
	}
	f.rerun = append(f.rerun, id)
	return nil
}
func (f *fakeInspector) ListArchivedTasks(string, ...asynq.ListOption) ([]*asynq.TaskInfo, error) {
	return f.list, nil
}

type fixture struct {
	engine *gin.Engine
	st     *store.Store
	sub    *fakeSubmitter
	insp   *fakeInspector
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	sub := &fakeSubmitter{st: st}
	insp := &fakeInspector{live: map[string]*asynq.TaskInfo{}}
	d := Deps{Store: st, Submitter: sub, Inspector: insp, Queue: "jobs", Token: "tok", Logger: slog.Default()}
	return &fixture{engine: New(d), st: st, sub: sub, insp: insp}
}

func (f *fixture) do(t *testing.T, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	f.engine.ServeHTTP(w, req)
	return w
}

func decode(t *testing.T, w *httptest.ResponseRecorder) (int, map[string]any) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode response %q: %v", w.Body.String(), err)
	}
	return int(m["code"].(float64)), m
}

func TestAuth(t *testing.T) {
	f := newFixture(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	f.engine.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
	// 无 header 同样 401
	req2 := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	w2 := httptest.NewRecorder()
	f.engine.ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 without token, got %d", w2.Code)
	}
	// healthz 不需要鉴权
	w3 := httptest.NewRecorder()
	f.engine.ServeHTTP(w3, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w3.Code != http.StatusOK {
		t.Fatalf("healthz want 200, got %d", w3.Code)
	}
}

func TestSubmitTask(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, http.MethodPost, "/v1/tasks", map[string]any{
		"task_id": "tk-1", "request_id": "req-1", "action": "audit_archive",
		"callback_url": "http://zhu.internal/internal/jobs/audit_archive",
		"params": map[string]any{"retain_days": 90}, "submitted_by": "10086", "source_ip": "10.0.0.9",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	code, m := decode(t, w)
	data := m["data"].(map[string]any)
	if code != 0 || data["accepted"] != true {
		t.Fatalf("unexpected: code=%v accepted=%v", code, data["accepted"])
	}
	// 落库 pending + 审计字段
	run, err := f.st.GetByTaskID(context.Background(), "tk-1")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != store.StatusPending || run.SubmittedBy != "10086" || run.RequestID != "req-1" {
		t.Fatalf("unexpected run: %+v", run)
	}
	// 幂等重提
	f.sub.dup = true
	w2 := f.do(t, http.MethodPost, "/v1/tasks", map[string]any{
		"task_id": "tk-1", "action": "a", "callback_url": "http://x",
	})
	if _, m2 := decode(t, w2); m2["data"].(map[string]any)["accepted"] != false {
		t.Fatalf("dup submit should be accepted=false, got %v", m2["data"])
	}
	// 缺必填
	w3 := f.do(t, http.MethodPost, "/v1/tasks", map[string]any{"action": "a"})
	if w3.Code != http.StatusBadRequest {
		t.Fatalf("missing callback_url want 400, got %d", w3.Code)
	}
}

func TestGetTaskLiveState(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.st.InsertPending(ctx, store.Run{TaskID: "tk-2", Action: "a", EnqueuedAt: time.Now()})
	f.insp.live["tk-2"] = &asynq.TaskInfo{ID: "tk-2", State: asynq.TaskStatePending}

	w := f.do(t, http.MethodGet, "/v1/tasks/tk-2", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	_, m := decode(t, w)
	data := m["data"].(map[string]any)
	if data["live_state"] != "pending" {
		t.Fatalf("live_state: %v", data["live_state"])
	}
	// 不存在
	w2 := f.do(t, http.MethodGet, "/v1/tasks/nope", nil)
	if w2.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w2.Code)
	}
}

func TestListRunsFilter(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	now := time.Now()
	f.st.InsertPending(ctx, store.Run{TaskID: "r1", RequestID: "rq", Action: "a1", EnqueuedAt: now})
	f.st.InsertPending(ctx, store.Run{TaskID: "r2", RequestID: "rq2", Action: "a2", EnqueuedAt: now})
	f.st.MarkRunning(ctx, "r1", 1, now)
	f.st.Finish(ctx, "r1", store.StatusSucceeded, "", 1, now, now.Add(time.Second))

	w := f.do(t, http.MethodGet, "/v1/runs?request_id=rq&status=succeeded", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var resp struct {
		Data struct {
			List []map[string]any `json:"list"`
			Total int64           `json:"total"`
		} `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Data.Total != 1 || len(resp.Data.List) != 1 || resp.Data.List[0]["task_id"] != "r1" {
		t.Fatalf("unexpected list: %+v", resp.Data)
	}
}

func TestCancelAndRetry(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	now := time.Now()
	f.st.InsertPending(ctx, store.Run{TaskID: "c1", Action: "a", EnqueuedAt: now})
	f.st.InsertPending(ctx, store.Run{TaskID: "c2", Action: "a", EnqueuedAt: now})
	f.st.MarkRunning(ctx, "c2", 1, now) // c2 running → 不可取消

	w := f.do(t, http.MethodPost, "/v1/tasks/c1/cancel", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("cancel want 200, got %d: %s", w.Code, w.Body.String())
	}
	if run, _ := f.st.GetByTaskID(ctx, "c1"); run.Status != store.StatusCanceled {
		t.Fatalf("c1 status: %s", run.Status)
	}
	if len(f.insp.deleted) != 1 || f.insp.deleted[0] != "c1" {
		t.Fatalf("inspector delete: %v", f.insp.deleted)
	}

	w2 := f.do(t, http.MethodPost, "/v1/tasks/c2/cancel", nil)
	if w2.Code != http.StatusConflict {
		t.Fatalf("cancel running want 409, got %d", w2.Code)
	}

	// c1 已 canceled → 不可重试；造一个 dead 再重试
	w3 := f.do(t, http.MethodPost, "/v1/tasks/c1/retry", nil)
	if w3.Code != http.StatusConflict {
		t.Fatalf("retry canceled want 409, got %d", w3.Code)
	}
	f.st.InsertPending(ctx, store.Run{TaskID: "d1", Action: "a", EnqueuedAt: now})
	f.st.MarkRunning(ctx, "d1", 2, now)
	f.st.Finish(ctx, "d1", store.StatusDead, "exhausted", 2, now, now)
	w4 := f.do(t, http.MethodPost, "/v1/tasks/d1/retry", nil)
	if w4.Code != http.StatusOK {
		t.Fatalf("retry dead want 200, got %d: %s", w4.Code, w4.Body.String())
	}
	if run, _ := f.st.GetByTaskID(ctx, "d1"); run.Status != store.StatusPending {
		t.Fatalf("d1 after retry: %s", run.Status)
	}
}

func TestJobsCRUDAndTrigger(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, http.MethodPost, "/v1/jobs", map[string]any{
		"action_id": "audit_archive", "callback_url": "http://zhu.internal/internal/jobs/audit_archive",
		"trigger_type": "cron", "cron_spec": "0 3 * * *", "dept": "audit",
		"params": map[string]any{"retain_days": 90}, "created_by": "10086",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("create job want 200, got %d: %s", w.Code, w.Body.String())
	}
	_, m := decode(t, w)
	data := m["data"].(map[string]any)
	jobID := data["job_id"].(string)
	if data["next_run"] == nil {
		t.Fatal("cron job should compute next_run")
	}

	// 非法 cron
	w2 := f.do(t, http.MethodPost, "/v1/jobs", map[string]any{
		"action_id": "x", "callback_url": "http://x", "trigger_type": "cron", "cron_spec": "not-a-cron",
	})
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("bad spec want 400, got %d", w2.Code)
	}

	// dept 过滤
	w3 := f.do(t, http.MethodGet, "/v1/jobs?dept=audit", nil)
	if w3.Code != http.StatusOK {
		t.Fatalf("list jobs want 200, got %d", w3.Code)
	}
	var resp struct {
		Data []map[string]any `json:"data"`
	}
	json.Unmarshal(w3.Body.Bytes(), &resp)
	if len(resp.Data) != 1 {
		t.Fatalf("dept filter: %d rows", len(resp.Data))
	}

	// PATCH 停用 → next_run 置空
	w4 := f.do(t, http.MethodPatch, "/v1/jobs/"+jobID, map[string]any{"enabled": false})
	if w4.Code != http.StatusOK {
		t.Fatalf("patch want 200, got %d: %s", w4.Code, w4.Body.String())
	}
	j, _ := f.st.GetJob(context.Background(), jobID)
	if j.Enabled || j.NextRun.Valid {
		t.Fatalf("disabled job should drop next_run: %+v", j)
	}

	// 停用后 trigger → 409
	w5 := f.do(t, http.MethodPost, "/v1/jobs/"+jobID+"/trigger", nil)
	if w5.Code != http.StatusConflict {
		t.Fatalf("trigger disabled want 409, got %d", w5.Code)
	}

	// 重新启用 + 手动触发 → 提交成功且带上定义参数
	f.do(t, http.MethodPatch, "/v1/jobs/"+jobID, map[string]any{"enabled": true})
	w6 := f.do(t, http.MethodPost, "/v1/jobs/"+jobID+"/trigger", map[string]any{
		"request_id": "req-9", "actor": "10086", "source_ip": "10.0.0.9",
	})
	if w6.Code != http.StatusOK {
		t.Fatalf("trigger want 200, got %d: %s", w6.Code, w6.Body.String())
	}
	if len(f.sub.submitted) != 1 {
		t.Fatalf("submitted: %+v", f.sub.submitted)
	}
	p := f.sub.submitted[0]
	if p.Action != "audit_archive" || p.JobID != jobID || p.SubmittedBy != "10086" || p.RequestID != "req-9" {
		t.Fatalf("trigger payload: %+v", p)
	}
	if string(p.Params) == "" || p.Params[0] != '{' {
		t.Fatalf("params not carried: %s", p.Params)
	}
}

func TestDeadLetters(t *testing.T) {
	f := newFixture(t)
	f.insp.list = []*asynq.TaskInfo{{ID: "dz-1", State: asynq.TaskStateArchived, LastErr: "boom", Retried: 5, MaxRetry: 5}}
	w := f.do(t, http.MethodGet, "/v1/dead-letters", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var resp struct {
		Data []map[string]any `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data) != 1 || resp.Data[0]["task_id"] != "dz-1" {
		t.Fatalf("dead letters: %+v", resp.Data)
	}
}
