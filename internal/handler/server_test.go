package handler

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

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/service"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeSubmitter 记录提交，可注入幂等/错误行为；持有 store 时模拟真实 Service 的落库。
type fakeSubmitter struct {
	submitted []task.Payload
	dup       bool
	err       error
	st        *repository.Store
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
		fakeParams := string(p.Params)
		if fakeParams == "" {
			fakeParams = "{}"
		}
		_ = f.st.InsertPending(ctx, repository.Run{
			TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, JobID: p.JobID,
			Dept: p.Dept, Params: fakeParams, CallbackURL: p.CallbackURL, SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP,
			EnqueuedAt: time.Now(),
		})
	}
	return true, nil, nil
}

// fakeInspector 模拟 asynq Inspector。
type fakeInspector struct {
	live    map[string]*asynq.TaskInfo
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

const (
	callerAK = "zhuzhao"
	callerSK = "sk-zhuzhao-test"
)

type fixture struct {
	engine *gin.Engine
	st     *repository.Store
	sub    *fakeSubmitter
	insp   *fakeInspector
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := repository.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	sub := &fakeSubmitter{st: st}
	insp := &fakeInspector{live: map[string]*asynq.TaskInfo{}}
	svc := service.NewTaskService(st, sub, insp, "jobs")
	d := Deps{Tasks: svc, Keys: map[string][]byte{callerAK: []byte(callerSK)}, Logger: slog.Default()}
	return &fixture{engine: New(d), st: st, sub: sub, insp: insp}
}

// do 构造带 aksk 签名的请求（模拟 zhuzhao client：签名 + rid/operator 透传）。
func (f *fixture) do(t *testing.T, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	aksk.Sign(req, raw, aksk.SignOptions{AK: callerAK, SK: []byte(callerSK),
		RequestID: "req-test", Operator: "10086"})
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

	t.Run("unsigned 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader([]byte(`{}`)))
		w := httptest.NewRecorder()
		f.engine.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("want 401, got %d", w.Code)
		}
	})
	t.Run("wrong secret 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		aksk.Sign(req, nil, aksk.SignOptions{AK: callerAK, SK: []byte("sk-other")})
		w := httptest.NewRecorder()
		f.engine.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("want 401 wrong sk, got %d", w.Code)
		}
	})
	t.Run("healthz/readyz 不需要鉴权 + rid 回显", func(t *testing.T) {
		w := httptest.NewRecorder()
		f.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("healthz want 200, got %d", w.Code)
		}
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		req.Header.Set("X-Request-ID", "req-inbound-1")
		w2 := httptest.NewRecorder()
		f.engine.ServeHTTP(w2, req)
		if w2.Code != http.StatusOK {
			t.Fatalf("readyz want 200, got %d", w2.Code)
		}
		if got := w2.Header().Get("X-Request-ID"); got != "req-inbound-1" {
			t.Fatalf("rid echo = %q", got)
		}
	})
}

func TestSubmitTask(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, http.MethodPost, "/v1/tasks", map[string]any{
		"task_id": "tk-1", "request_id": "req-1", "action": "audit_archive",
		"callback_url": "http://zhu.internal/internal/jobs/audit_archive",
		"params":       map[string]any{"retain_days": 90}, "submitted_by": "10086", "source_ip": "10.0.0.9",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	code, m := decode(t, w)
	data := m["data"].(map[string]any)
	if code != 0 || data["accepted"] != true {
		t.Fatalf("unexpected: code=%v accepted=%v", code, data["accepted"])
	}
	run, err := f.st.GetByTaskID(context.Background(), "tk-1")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != repository.StatusPending || run.SubmittedBy != "10086" || run.RequestID != "req-1" {
		t.Fatalf("unexpected run: %+v", run)
	}
	// 幂等重提
	f.sub.dup = true
	w2 := f.do(t, http.MethodPost, "/v1/tasks", map[string]any{
		"task_id": "tk-1", "action": "a", "callback_url": "http://x",
	})
	if w2.Code != http.StatusOK {
		t.Fatalf("idempotent resubmit want 200, got %d", w2.Code)
	}
	_, m2 := decode(t, w2)
	if m2["data"].(map[string]any)["accepted"] != false {
		t.Fatalf("accepted should be false on dup: %v", m2)
	}
}

func TestGetTaskLiveState(t *testing.T) {
	f := newFixture(t)
	f.do(t, http.MethodPost, "/v1/tasks", map[string]any{
		"task_id": "tk-live", "action": "a", "callback_url": "http://x"})
	f.insp.live["tk-live"] = &asynq.TaskInfo{ID: "tk-live", State: asynq.TaskStateRetry}

	w := f.do(t, http.MethodGet, "/v1/tasks/tk-live", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	_, m := decode(t, w)
	d := m["data"].(map[string]any)
	if d["live_state"] != asynq.TaskStateRetry.String() {
		t.Fatalf("live_state = %v", d["live_state"])
	}

	// 不存在 → 404
	w404 := f.do(t, http.MethodGet, "/v1/tasks/nope", nil)
	if w404.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w404.Code)
	}
}

func TestListRunsFilter(t *testing.T) {
	f := newFixture(t)
	f.do(t, http.MethodPost, "/v1/tasks", map[string]any{"task_id": "r1", "request_id": "rqA", "action": "a1", "callback_url": "http://x"})
	f.do(t, http.MethodPost, "/v1/tasks", map[string]any{"task_id": "r2", "request_id": "rqB", "action": "a2", "callback_url": "http://x"})

	w := f.do(t, http.MethodGet, "/v1/runs?request_id=rqA", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	_, m := decode(t, w)
	d := m["data"].(map[string]any)
	if d["total"] != float64(1) {
		t.Fatalf("total = %v", d["total"])
	}
	if d["list"].([]any)[0].(map[string]any)["task_id"] != "r1" {
		t.Fatalf("filter wrong row: %v", d["list"])
	}

	// C11：dept 多值过滤（job 定义的归属标签约束执行记录可见性）
	var jaID, jbID string
	for _, job := range []struct{ id, dept string }{{"j-a", "deptA"}, {"j-b", "deptB"}} {
		wj := f.do(t, http.MethodPost, "/v1/jobs", map[string]any{
			"action_id": "a1", "callback_url": "http://x", "trigger_type": "cron",
			"cron_spec": "0 4 * * *", "dept": job.dept,
		})
		if wj.Code != http.StatusOK {
			t.Fatalf("create job %s: %s", job.id, wj.Body.String())
		}
		_, mj := decode(t, wj)
		jid := mj["data"].(map[string]any)["job_id"].(string)
		if job.id == "j-a" {
			jaID = jid
		} else {
			jbID = jid
		}
	}
	// 带 dept 的 run 经 trigger 产生（直接提交的一次性任务 job_id 为空，不挂 dept）
	f.do(t, http.MethodPost, "/v1/jobs/trigger", map[string]any{"job_id": jaID, "actor": "10086"})
	f.do(t, http.MethodPost, "/v1/jobs/trigger", map[string]any{"job_id": jbID, "actor": "10086"})

	wd := f.do(t, http.MethodGet, "/v1/runs?dept=deptA", nil)
	_, md := decode(t, wd)
	list := md["data"].(map[string]any)["list"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["dept"] != "deptA" {
		t.Fatalf("dept=deptA 应只含 deptA 的执行记录：%v", list)
	}
}

// TestC10NegativeBindings C10 负向：标识缺失 → 400；旧路由（path 带标识）→ 404 防混布错配。
// TestSubmitParamsRoundTrip params 快照往返（P0-1 回归锚点）：提交带 params →
// job_runs.params 落库（非空串）→ task/runs 查询回显。
func TestSubmitParamsRoundTrip(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, http.MethodPost, "/v1/tasks", map[string]any{
		"task_id": "p-rt-1", "action": "a", "callback_url": "http://x",
		"dept": "d1", "params": map[string]any{"retain_days": 7},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("submit: %s", w.Body.String())
	}
	// TaskView 回显
	wg := f.do(t, http.MethodGet, "/v1/tasks/p-rt-1", nil)
	_, mg := decode(t, wg)
	d := mg["data"].(map[string]any)
	if d["params"] != `{"retain_days":7}` {
		t.Fatalf("task params 回显错误：%v", d["params"])
	}
	if d["dept"] != "d1" {
		t.Fatalf("task dept 回显错误：%v", d["dept"])
	}
	// runs 列表回显
	wr := f.do(t, http.MethodGet, "/v1/runs?request_id=", nil)
	_ = wr
	_, mr := decode(t, f.do(t, http.MethodGet, "/v1/runs", nil))
	list := mr["data"].(map[string]any)["list"].([]any)
	found := false
	for _, it := range list {
		row := it.(map[string]any)
		if row["task_id"] == "p-rt-1" {
			found = true
			if row["params"] != `{"retain_days":7}` {
				t.Fatalf("run params 回显错误：%v", row["params"])
			}
		}
	}
	if !found {
		t.Fatal("runs 列表未见 p-rt-1")
	}
}

func TestC10NegativeBindings(t *testing.T) {
	f := newFixture(t)

	cases := []struct {
		name, method, path string
		body               map[string]any
	}{
		{"cancel 缺 task_id", http.MethodPost, "/v1/tasks/cancel", map[string]any{}},
		{"retry 缺 task_id", http.MethodPost, "/v1/tasks/retry", map[string]any{}},
		{"trigger 缺 job_id", http.MethodPost, "/v1/jobs/trigger", map[string]any{}},
		{"update 缺 job_id", http.MethodPost, "/v1/jobs/update", map[string]any{"enabled": true}},
	}
	for _, tc := range cases {
		w := f.do(t, tc.method, tc.path, tc.body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s want 400, got %d: %s", tc.name, w.Code, w.Body.String())
		}
	}

	// 旧路由（信息在 URL 的形态）应已不存在——404 而非被静默匹配
	for _, old := range []struct{ method, path string }{
		{http.MethodPatch, "/v1/jobs/anything"},
		{http.MethodPost, "/v1/jobs/anything/trigger"},
		{http.MethodPost, "/v1/tasks/anything/cancel"},
		{http.MethodPost, "/v1/tasks/anything/retry"},
	} {
		w := f.do(t, old.method, old.path, map[string]any{})
		if w.Code != http.StatusNotFound {
			t.Fatalf("旧路由 %s %s 应 404，got %d", old.method, old.path, w.Code)
		}
	}
}

// TestC11DeptFilterNegative C11 负向/边界：不存在的 dept → 空；多值并集；task 响应带 dept。
func TestC11DeptFilterNegative(t *testing.T) {
	f := newFixture(t)
	wj := f.do(t, http.MethodPost, "/v1/jobs", map[string]any{
		"action_id": "a1", "callback_url": "http://x", "trigger_type": "cron",
		"cron_spec": "0 4 * * *", "dept": "deptA",
	})
	if wj.Code != http.StatusOK {
		t.Fatalf("create job: %s", wj.Body.String())
	}
	_, mj := decode(t, wj)
	jid := mj["data"].(map[string]any)["job_id"].(string)
	wt := f.do(t, http.MethodPost, "/v1/jobs/trigger", map[string]any{"job_id": jid, "actor": "10086"})
	if wt.Code != http.StatusOK {
		t.Fatalf("trigger: %s", wt.Body.String())
	}
	_, mt := decode(t, wt)
	tid := mt["data"].(map[string]any)["task_id"].(string)

	// task 响应应回显 dept（C11 对外承诺）
	wg := f.do(t, http.MethodGet, "/v1/tasks/"+tid, nil)
	_, mg := decode(t, wg)
	if mg["data"].(map[string]any)["dept"] != "deptA" {
		t.Fatalf("task 响应应带 dept=deptA：%v", mg["data"])
	}

	// 不存在的 dept → 空列表（fail-closed 语义：无归属即不可见）
	wn := f.do(t, http.MethodGet, "/v1/runs?dept=nonexistent", nil)
	_, mn := decode(t, wn)
	if got := mn["data"].(map[string]any)["total"]; got != float64(0) {
		t.Fatalf("不存在 dept 应 0 条，got %v", got)
	}
}

func TestCancelAndRetry(t *testing.T) {
	f := newFixture(t)
	f.do(t, http.MethodPost, "/v1/tasks", map[string]any{"task_id": "c1", "action": "a", "callback_url": "http://x"})

	// 取消 pending（C10：POST URL 不携带业务信息，标识在 body）
	w := f.do(t, http.MethodPost, "/v1/tasks/cancel", map[string]any{"task_id": "c1"})
	if w.Code != http.StatusOK {
		t.Fatalf("cancel pending want 200, got %d: %s", w.Code, w.Body.String())
	}
	// 已取消再取消 → 409
	w2 := f.do(t, http.MethodPost, "/v1/tasks/cancel", map[string]any{"task_id": "c1"})
	if w2.Code != http.StatusConflict {
		t.Fatalf("double cancel want 409, got %d", w2.Code)
	}

	// retry 非 failed → 409
	w3 := f.do(t, http.MethodPost, "/v1/tasks/retry", map[string]any{"task_id": "c1"})
	if w3.Code != http.StatusConflict {
		t.Fatalf("retry non-failed want 409, got %d", w3.Code)
	}

	// retry failed 任务：置 failed 后重试成功
	f.do(t, http.MethodPost, "/v1/tasks", map[string]any{"task_id": "c2", "action": "a", "callback_url": "http://x"})
	if err := f.st.Finish(context.Background(), "c2", repository.StatusFailed, "boom", 1, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	w4 := f.do(t, http.MethodPost, "/v1/tasks/retry", map[string]any{"task_id": "c2"})
	if w4.Code != http.StatusOK {
		t.Fatalf("retry failed want 200, got %d: %s", w4.Code, w4.Body.String())
	}
	if len(f.insp.rerun) != 1 || f.insp.rerun[0] != "c2" {
		t.Fatalf("inspector rerun = %v", f.insp.rerun)
	}
	run, _ := f.st.GetByTaskID(context.Background(), "c2")
	if run.Status != repository.StatusPending {
		t.Fatalf("after retry status = %s", run.Status)
	}
}

func TestJobsCRUDAndTrigger(t *testing.T) {
	f := newFixture(t)
	w := f.do(t, http.MethodPost, "/v1/jobs", map[string]any{
		"action_id": "audit_archive", "callback_url": "http://zhu.internal/internal/jobs/audit_archive",
		"trigger_type": "cron", "cron_spec": "0 3 * * *", "params": map[string]any{"retain_days": 90},
		"dept": "audit", "created_by": "10086",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("create job want 200, got %d: %s", w.Code, w.Body.String())
	}
	_, m := decode(t, w)
	job := m["data"].(map[string]any)
	jobID := job["job_id"].(string)
	if job["next_run"] == nil {
		t.Fatal("cron job must have next_run")
	}

	// 非法 cron → 400
	wBad := f.do(t, http.MethodPost, "/v1/jobs", map[string]any{
		"action_id": "a", "callback_url": "http://x", "trigger_type": "cron", "cron_spec": "bad"})
	if wBad.Code != http.StatusBadRequest {
		t.Fatalf("bad cron want 400, got %d", wBad.Code)
	}

	// 列表过滤
	wList := f.do(t, http.MethodGet, "/v1/jobs?dept=audit", nil)
	_, mList := decode(t, wList)
	// OKPage 形态：data = {list, total, page, page_size}
	if mList["data"].(map[string]any)["list"].([]any)[0].(map[string]any)["job_id"] != jobID {
		t.Fatalf("list filter wrong: %v", mList)
	}

	// POST /jobs/update 启停（C10：方法仅 POST，标识在 body）
	wPatch := f.do(t, http.MethodPost, "/v1/jobs/update", map[string]any{"job_id": jobID, "enabled": false})
	if wPatch.Code != http.StatusOK {
		t.Fatalf("update want 200, got %d: %s", wPatch.Code, wPatch.Body.String())
	}
	// 停用后 trigger → 409
	wTrig := f.do(t, http.MethodPost, "/v1/jobs/trigger", map[string]any{"job_id": jobID, "actor": "10086"})
	if wTrig.Code != http.StatusConflict {
		t.Fatalf("trigger disabled want 409, got %d", wTrig.Code)
	}
	// 重新启用后 trigger 成功
	f.do(t, http.MethodPost, "/v1/jobs/update", map[string]any{"job_id": jobID, "enabled": true})
	wTrig2 := f.do(t, http.MethodPost, "/v1/jobs/trigger", map[string]any{"job_id": jobID, "actor": "10086", "request_id": "req-t"})
	if wTrig2.Code != http.StatusOK {
		t.Fatalf("trigger enabled want 200, got %d: %s", wTrig2.Code, wTrig2.Body.String())
	}
	_, mTrig := decode(t, wTrig2)
	if mTrig["data"].(map[string]any)["accepted"] != true {
		t.Fatalf("trigger result: %v", mTrig)
	}
	if len(f.sub.submitted) != 1 || f.sub.submitted[0].JobID != jobID {
		t.Fatalf("submitted payloads: %+v", f.sub.submitted)
	}
}

func TestDeadLetters(t *testing.T) {
	f := newFixture(t)
	f.insp.list = []*asynq.TaskInfo{{ID: "dead-1", State: asynq.TaskStateArchived, LastErr: "boom"}}
	w := f.do(t, http.MethodGet, "/v1/dead-letters", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	_, m := decode(t, w)
	list := m["data"].([]any)
	if list[0].(map[string]any)["task_id"] != "dead-1" {
		t.Fatalf("dead letters: %v", list)
	}
}
