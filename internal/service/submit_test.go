package service

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeEnqueuer 可编程入队假件（ADR-004 F31：全仓原先无 Enqueuer 假件）：
// 记录调用；err/conflict 控制返回；hook 在 Enqueue 内执行（模拟入队期间的并发翻转）。
type fakeEnqueuer struct {
	mu       sync.Mutex
	enqueues []task.Payload
	err      error
	hook     func(p task.Payload)
}

func (f *fakeEnqueuer) Enqueue(t *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	var p task.Payload
	_ = json.Unmarshal(t.Payload(), &p)
	f.mu.Lock()
	f.enqueues = append(f.enqueues, p)
	hook, e := f.hook, f.err
	f.mu.Unlock()
	if hook != nil {
		hook(p)
	}
	if e != nil {
		return nil, e
	}
	return &asynq.TaskInfo{}, nil
}

func (f *fakeEnqueuer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.enqueues)
}

// fakeTaskDelete 补偿撤销假件。
type fakeTaskDelete struct {
	mu      sync.Mutex
	deletes []string
	err     error
}

func (f *fakeTaskDelete) DeleteTask(_, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, id)
	return f.err
}

func (f *fakeTaskDelete) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deletes)
}

func newSubmitSvc(t *testing.T) (*Service, *repository.Store, *fakeEnqueuer, *fakeTaskDelete) {
	t.Helper()
	st, err := repository.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	fake := &fakeEnqueuer{}
	del := &fakeTaskDelete{}
	return &Service{Store: st, Client: fake, Inspector: del, Queue: "jobs",
		MaxRetry: 3, Retention: time.Hour}, st, fake, del
}

func submitPayload(id string) task.Payload {
	return task.Payload{TaskID: id, Action: "a", CallbackURL: "http://zhu.internal/internal/jobs/callback"}
}

// A1：已有行 → 幂等重提，零入队。
func TestSubmitA1Idempotent(t *testing.T) {
	svc, st, fake, _ := newSubmitSvc(t)
	ctx := context.Background()
	if err := st.InsertPending(ctx, repository.Run{TaskID: "a1", Action: "a", EnqueuedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	accepted, warning, err := svc.Submit(ctx, submitPayload("a1"))
	if accepted || warning != nil || err != nil {
		t.Fatalf("want (false,nil,nil), got (%v,%v,%v)", accepted, warning, err)
	}
	if fake.count() != 0 {
		t.Fatalf("idempotent resubmit must not enqueue, got %d", fake.count())
	}
}

// A3 成功 / 冲突：都条件置位 queued=1（冲突是并发同提兜底，行照常标记）。
// TimeoutSecs>0 走 buildEnqueueOpts 的 Timeout 分支（F61 同参构造的另一半）。
func TestSubmitA3SuccessAndConflict(t *testing.T) {
	for _, tc := range []struct {
		name   string
		enqErr error
	}{
		{"success", nil},
		{"conflict", asynq.ErrTaskIDConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, st, fake, del := newSubmitSvc(t)
			fake.err = tc.enqErr
			p := submitPayload("a3")
			p.TimeoutSecs = 3600
			accepted, warning, err := svc.Submit(context.Background(), p)
			if !accepted || warning != nil || err != nil {
				t.Fatalf("want (true,nil,nil), got (%v,%v,%v)", accepted, warning, err)
			}
			if q, gerr := st.GetQueued(context.Background(), "a3"); gerr != nil || !q {
				t.Fatalf("queued must be set: %v %v", q, gerr)
			}
			if del.count() != 0 {
				t.Fatal("no compensation expected")
			}
		})
	}
}

// A3 入队失败（Redis 抖动）：200 = 已受理待入队，warning 返回、行留 queued=0。
func TestSubmitA3EnqueueFailure(t *testing.T) {
	svc, st, fake, _ := newSubmitSvc(t)
	fake.err = errors.New("dial tcp: connection refused")
	accepted, warning, err := svc.Submit(context.Background(), submitPayload("a3f"))
	if !accepted || warning == nil || err != nil {
		t.Fatalf("want (true,warning,nil), got (%v,%v,%v)", accepted, warning, err)
	}
	if q, _ := st.GetQueued(context.Background(), "a3f"); q {
		t.Fatal("queued must stay 0 on enqueue failure")
	}
}

// F19 核心用例：A3 条件标记 0 行（入队期间行被并发取消）→ DeleteTask 补偿被调用。
func TestSubmitA3ZeroRowsCompensates(t *testing.T) {
	svc, st, fake, del := newSubmitSvc(t)
	ctx := context.Background()
	// hook 在 Enqueue 内把行翻成 canceled（模拟「刚入队即被取消」的并发窗口）
	fake.hook = func(p task.Payload) {
		ok, err := st.MarkCanceledIfPending(ctx, &repository.Run{TaskID: p.TaskID}, "canceled via API", time.Now(), false)
		if !ok || err != nil {
			t.Errorf("hook cancel: %v %v", ok, err)
		}
	}
	accepted, warning, err := svc.Submit(ctx, submitPayload("f19"))
	if !accepted || warning != nil || err != nil {
		t.Fatalf("受理口径不受补偿影响，want (true,nil,nil), got (%v,%v,%v)", accepted, warning, err)
	}
	if del.count() != 1 || del.deletes[0] != "f19" {
		t.Fatalf("compensation DeleteTask must be called once, got %+v", del.deletes)
	}
	if r, _ := st.GetByTaskID(ctx, "f19"); r.Status != repository.StatusCanceled {
		t.Fatalf("row must stay canceled: %s", r.Status)
	}
}

func TestSubmitReopensCancellationWhenLateDeliveryCannotBeWithdrawn(t *testing.T) {
	svc, st, queue, inspector := newSubmitSvc(t)
	ctx := context.Background()
	inspector.err = errors.New("queue unavailable")
	queue.hook = func(p task.Payload) {
		canceled, err := st.MarkCanceledIfPending(ctx, &repository.Run{TaskID: p.TaskID}, "canceled", time.Now(), false)
		if err != nil || !canceled {
			t.Fatalf("cancel before late delivery: %v %v", canceled, err)
		}
		if n, err := st.MarkCleanupDone(ctx, p.TaskID, 0); n != 1 || err != nil {
			t.Fatalf("confirm absent queue key before late delivery: %d %v", n, err)
		}
	}

	accepted, _, err := svc.Submit(ctx, submitPayload("late-submit"))
	if err != nil || !accepted || inspector.count() != 1 {
		t.Fatalf("late submit and withdrawal: accepted=%v, err=%v, deletes=%d", accepted, err, inspector.count())
	}
	rows, err := st.ListCanceledUnqueued(ctx, time.Now().Add(time.Hour), 0, math.MaxInt64, 10)
	if err != nil || len(rows) != 1 || rows[0].TaskID != "late-submit" {
		t.Fatalf("unwithdrawn delivery must be visible to cleanup before any worker runs: %v %v", rows, err)
	}
}

func TestSubmitA3KeepsRetryableDelivery(t *testing.T) {
	svc, st, fake, inspector := newSubmitSvc(t)
	ctx := context.Background()
	fake.hook = func(p task.Payload) {
		markFixtureDeliveryFailed(t, ctx, st, p.TaskID)
	}
	accepted, warning, err := svc.Submit(ctx, submitPayload("retryable"))
	if !accepted || warning != nil || err != nil {
		t.Fatalf("submit: got (%v, %v, %v)", accepted, warning, err)
	}
	if inspector.count() != 0 {
		t.Fatal("a worker's retryable delivery must stay in the queue")
	}
}

func markFixtureDeliveryFailed(t *testing.T, ctx context.Context, st *repository.Store, taskID string) {
	t.Helper()
	now := time.Now()
	if err := st.MarkRunning(ctx, taskID, 1, now, "fixture-delivery"); err != nil {
		t.Fatal(err)
	}
	if err := st.Finish(ctx, taskID, repository.StatusFailed, "transient error", 1, now, now, "fixture-delivery"); err != nil {
		t.Fatal(err)
	}
}

// A2：InsertPending 失败且复查失败 → 真错误 → 500 干净失败（Redis 无任务、DB 无行）。
// 复查命中分支由 TestSubmitConcurrentSameTaskID 覆盖——输者走 A1 命中或 A2 复查命中，
// 两条路径返回值相同（幂等 200），竞态结果断言确定。
func TestSubmitStoreFailureCleanError(t *testing.T) {
	st, err := repository.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeEnqueuer{}
	svc := &Service{Store: st, Client: fake, Inspector: &fakeTaskDelete{}, Queue: "jobs"}
	st.Close()
	accepted, _, err := svc.Submit(context.Background(), submitPayload("a2"))
	if err == nil {
		t.Fatal("store failure must error (clean 500)")
	}
	if accepted {
		t.Fatal("must not accept on store failure")
	}
	if fake.count() != 0 {
		t.Fatal("no enqueue on store failure")
	}
}

// 并发同提（F23 竞态）：恰好一个受理、恰一次入队、恰一行；输者必为幂等 200
// （A1 命中或 A2 复查命中——两条路径返回值相同，结果断言确定）。
func TestSubmitConcurrentSameTaskID(t *testing.T) {
	svc, st, fake, _ := newSubmitSvc(t)
	const n = 8
	type result struct {
		accepted bool
		warning  error
		err      error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			a, w, e := svc.Submit(context.Background(), submitPayload("race"))
			results[idx] = result{a, w, e}
		}(i)
	}
	wg.Wait()
	acceptedCount := 0
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("并发重提不允许错误（UNIQUE 竞提应收敛为幂等 200）: %v", r.err)
		}
		if r.accepted {
			acceptedCount++
		}
	}
	if acceptedCount != 1 {
		t.Fatalf("exactly one accepted, got %d", acceptedCount)
	}
	if fake.count() != 1 {
		t.Fatalf("exactly one enqueue, got %d", fake.count())
	}
	// UNIQUE 约束保证至多一行；acceptedCount==1 保证至少一行
	if _, err := st.GetByTaskID(context.Background(), "race"); err != nil {
		t.Fatalf("row must exist: %v", err)
	}
}

func TestSubmitInputConflictExactJSON(t *testing.T) {
	for _, tc := range []struct {
		name, action, params string
		conflict             bool
	}{
		{"key order", "a", `{"z":1.0,"id":9007199254740992}`, false},
		{"different action", "b", `{"id":9007199254740992,"z":1}`, true},
		{"adjacent large integer", "a", `{"id":9007199254740993,"z":1}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, enq, _ := newSubmitSvc(t)
			p := submitPayload("exact")
			p.Params = []byte(`{"id":9007199254740992,"z":1}`)
			if _, _, err := svc.Submit(context.Background(), p); err != nil {
				t.Fatal(err)
			}
			p.Action = tc.action
			p.Params = []byte(tc.params)
			accepted, _, err := svc.Submit(context.Background(), p)
			var conflict *ConflictError
			if tc.conflict && !errors.As(err, &conflict) {
				t.Fatalf("different immutable input must conflict; accepted=%v err=%v", accepted, err)
			}
			if !tc.conflict && (err != nil || accepted) {
				t.Fatalf("equivalent input must be idempotent: %v %v", accepted, err)
			}
			if enq.count() != 1 {
				t.Fatalf("conflict/idempotent must not enqueue again: %d", enq.count())
			}
		})
	}
}

func TestConcurrentSubmitDifferentInput(t *testing.T) {
	svc, st, enq, _ := newSubmitSvc(t)
	type result struct {
		p        task.Payload
		accepted bool
		err      error
	}
	out := make(chan result, 8)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			p := submitPayload("competing-input")
			if i%2 == 0 {
				p.Params = []byte(`{"id":9007199254740992}`)
			} else {
				p.Params = []byte(`{"id":9007199254740993}`)
			}
			a, _, err := svc.Submit(context.Background(), p)
			out <- result{p, a, err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(out)
	row, err := st.GetByTaskID(context.Background(), "competing-input")
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for result := range out {
		if result.accepted {
			accepted++
		}
		same := string(result.p.Params) == row.Params
		var conflict *ConflictError
		if !same && !errors.As(result.err, &conflict) {
			t.Fatalf("losing input did not conflict: %v", result.err)
		}
		if same && result.err != nil {
			t.Fatalf("same winning input failed: %v", result.err)
		}
	}
	if accepted != 1 || enq.count() != 1 {
		t.Fatalf("one immutable operation: accepted=%d enqueue=%d", accepted, enq.count())
	}
}

func TestNewTaskIDMatchesCoreCharacterWidth(t *testing.T) {
	svc, _, enq, _ := newSubmitSvc(t)
	for _, n := range []int{64, 65} {
		p := submitPayload(strings.Repeat("字", n))
		accepted, _, err := svc.Submit(context.Background(), p)
		if n == 64 && (!accepted || err != nil) {
			t.Fatalf("64 Unicode characters are valid: %v %v", accepted, err)
		}
		if n == 65 && (accepted || err == nil) {
			t.Fatalf("65 characters must fail before persistence: %v %v", accepted, err)
		}
	}
	if enq.count() != 1 {
		t.Fatalf("oversize ID reached queue: %d", enq.count())
	}
}

func TestSubmitB3SameIdentityRequiresFrozenDeliveryInput(t *testing.T) {
	svc, st, queue, _ := newSubmitSvc(t)
	ctx := context.Background()
	original := submitPayload("b3-task")
	original.Action = "ticket.evidence.report.generate"
	original.Params = []byte(`{"operation_id":"op-original","count":2}`)
	original.TimeoutSecs = 18
	original.SubmittedBy = "ticket-operator"
	original.Dept = "sample-dept"
	if accepted, warning, err := svc.Submit(ctx, original); !accepted || warning != nil || err != nil {
		t.Fatalf("first submission: accepted=%v warning=%v err=%v", accepted, warning, err)
	}

	idempotent := original
	idempotent.RequestID = "later-trace"
	idempotent.SourceIP = "127.0.0.2"
	idempotent.Params = []byte(`{"count":2.0,"operation_id":"op-original"}`)
	if accepted, warning, err := svc.Submit(ctx, idempotent); accepted || warning != nil || err != nil {
		t.Fatalf("equivalent retry: accepted=%v warning=%v err=%v", accepted, warning, err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*task.Payload)
	}{
		{"different timeout", func(p *task.Payload) { p.TimeoutSecs++ }},
		{"different submitter", func(p *task.Payload) { p.SubmittedBy = "other-operator" }},
		{"different department", func(p *task.Payload) { p.Dept = "other-dept" }},
		{"different target", func(p *task.Payload) { p.CallbackURL += "-other" }},
		{"different operation", func(p *task.Payload) { p.Params = []byte(`{"count":2,"operation_id":"op-other"}`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conflicting := original
			tc.mutate(&conflicting)
			var conflictErr *ConflictError
			if accepted, warning, err := svc.Submit(ctx, conflicting); accepted || warning != nil || !errors.As(err, &conflictErr) {
				t.Fatalf("conflicting retry: accepted=%v warning=%v err=%v", accepted, warning, err)
			}
		})
	}
	row, err := st.GetByTaskID(ctx, original.TaskID)
	if err != nil || row.Params != string(original.Params) || queue.count() != 1 {
		t.Fatalf("only the original task may be stored and delivered: row=%#v enqueued=%d err=%v", row, queue.count(), err)
	}
}

func TestSubmitB3RejectsMissingOperationID(t *testing.T) {
	for _, raw := range []string{`{}`, `{"operation_id":42}`, `{"Operation_ID":"other"}`, `{"operation_id":" "}`} {
		t.Run(raw, func(t *testing.T) {
			svc, st, queue, _ := newSubmitSvc(t)
			p := submitPayload("b3-task")
			p.Action = "activelist.document.update"
			p.Params = []byte(raw)
			ctx := context.Background()
			var invalidErr *InvalidError
			if accepted, warning, err := svc.Submit(ctx, p); accepted || warning != nil || !errors.As(err, &invalidErr) {
				t.Errorf("malformed operation accepted=%v warning=%v err=%v", accepted, warning, err)
			}
			if _, err := st.GetByTaskID(ctx, p.TaskID); !errors.Is(err, repository.ErrNotFound) || queue.count() != 0 {
				t.Fatalf("malformed operation reached storage or Redis: enqueues=%d err=%v", queue.count(), err)
			}
		})
	}
}
