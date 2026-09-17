package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeEnqueuer 可编程入队假件（ADR-003 F31：全仓原先无 Enqueuer 假件）：
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
		ok, err := st.MarkCanceledIfPending(ctx, p.TaskID, "canceled via API", time.Now())
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
