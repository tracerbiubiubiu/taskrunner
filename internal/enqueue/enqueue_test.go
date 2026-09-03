package enqueue

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeEnqueuer 模拟 asynq.Client：记录调用，可注入 TaskID 冲突。
type fakeEnqueuer struct {
	calls    int
	conflict bool
}

func (f *fakeEnqueuer) Enqueue(*asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error) {
	f.calls++
	if f.conflict {
		return nil, asynq.ErrTaskIDConflict
	}
	return &asynq.TaskInfo{ID: "x"}, nil
}

func newSvc(t *testing.T) (*Service, *fakeEnqueuer) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	f := &fakeEnqueuer{}
	return &Service{Store: st, Client: f, Queue: "jobs", MaxRetry: 5}, f
}

func pl(id string) task.Payload {
	return task.Payload{TaskID: id, Action: "a", CallbackURL: "http://x"}
}

// A1 核心：job_runs 行已存在（即使任务早已 succeeded、Asynq 已释放 ID）→ 幂等拒绝，且不再触碰队列。
func TestSubmitIdempotentAfterCompletion(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()
	st := svc.Store

	st.InsertPending(ctx, store.Run{TaskID: "t1", Action: "a", EnqueuedAt: time.Now()})
	st.MarkRunning(ctx, "t1", 1, time.Now())
	st.Finish(ctx, "t1", store.StatusSucceeded, "", 1, time.Now(), time.Now().Add(time.Second))

	accepted, _, err := svc.Submit(ctx, pl("t1"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if accepted {
		t.Fatal("task_id 已有终态行，必须幂等拒绝（accepted=false），否则重复执行并覆盖历史")
	}
	if f.calls != 0 {
		t.Fatalf("已存在行时不应再入队，got %d calls", f.calls)
	}
}

func TestSubmitNewTask(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()

	accepted, warning, err := svc.Submit(ctx, pl("t2"))
	if err != nil || warning != nil || !accepted {
		t.Fatalf("new task: accepted=%v warning=%v err=%v", accepted, warning, err)
	}
	if f.calls != 1 {
		t.Fatalf("enqueue calls: %d", f.calls)
	}
	if run, err := svc.Store.GetByTaskID(ctx, "t2"); err != nil || run.Status != store.StatusPending {
		t.Fatalf("pending row missing: %+v %v", run, err)
	}
}

// 并发同提的兜底：DB 查无此行、Asynq 撞 TaskID 冲突（另一路刚入队未落库）→ 幂等拒绝。
func TestSubmitConcurrentConflictFallback(t *testing.T) {
	svc, f := newSvc(t)
	f.conflict = true
	accepted, _, err := svc.Submit(context.Background(), pl("t3"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if accepted {
		t.Fatal("TaskID 冲突应返回 accepted=false")
	}
}

func TestSubmitStoreError(t *testing.T) {
	svc, _ := newSvc(t)
	svc.Store.Close() // 查库报错应向上冒泡，不能被当幂等
	if _, _, err := svc.Submit(context.Background(), pl("t4")); err == nil {
		t.Fatal("store error should propagate")
	}
}
