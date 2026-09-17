package service

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeInspector 可编程 asynq 检视（干预路径竞态验证）。
type fakeInspector struct {
	state   asynq.TaskState // GetTaskInfo 返回的状态
	exists  bool
	getErr  error // GetTaskInfo 返回的错误（模拟 Redis 可用性故障 / ErrQueueNotFound，F58/F64）
	delErr  error
	runErr  error
	deleted []string // DeleteTask 调用记录
}

func (f *fakeInspector) GetTaskInfo(_, _ string) (*asynq.TaskInfo, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if !f.exists {
		return nil, asynq.ErrTaskNotFound
	}
	return &asynq.TaskInfo{State: f.state}, nil
}
func (f *fakeInspector) DeleteTask(_, id string) error {
	f.deleted = append(f.deleted, id)
	return f.delErr
}
func (f *fakeInspector) RunTask(_, _ string) error { return f.runErr }
func (f *fakeInspector) ListArchivedTasks(string, ...asynq.ListOption) ([]*asynq.TaskInfo, error) {
	return nil, nil
}

type fakeSubmitter struct{}

func (fakeSubmitter) Submit(_ context.Context, _ task.Payload) (bool, error, error) {
	return true, nil, nil
}

func timeNow() time.Time { return time.Now() }

// raceInspector RunTask 时注入「worker 瞬间完成」的中间态（竞态验证）。
type raceInspector struct {
	TaskInspector
	afterRun func()
}

func (r raceInspector) RunTask(queue, id string) error {
	r.afterRun()
	return nil
}

func newSvc(t *testing.T, ins *fakeInspector) *TaskService {
	t.Helper()
	st, err := repository.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewTaskService(st, nil, ins, "jobs", slog.Default())
}

func row(t *testing.T, s *TaskService, id, status string) {
	t.Helper()
	ctx := context.Background()
	if err := s.repo.InsertPending(ctx, repository.Run{TaskID: id, Action: "a", EnqueuedAt: timeNow()}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	switch status {
	case repository.StatusRunning:
		s.repo.MarkRunning(ctx, id, 1, timeNow())
	case repository.StatusFailed, repository.StatusDead:
		s.repo.MarkRunning(ctx, id, 1, timeNow())
		s.repo.Finish(ctx, id, status, "boom", 1, timeNow(), timeNow())
	}
}

func isConflict(err error) bool {
	var ce *ConflictError
	return errors.As(err, &ce)
}

func wantConflict(t *testing.T, err error) {
	t.Helper()
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("want ConflictError, got %v", err)
	}
}

func TestCancelRaces(t *testing.T) {
	ctx := context.Background()

	// 已在执行（active）→ 409
	ins := &fakeInspector{exists: true, state: asynq.TaskStateActive}
	s := newSvc(t, ins)
	row(t, s, "ra", repository.StatusPending)
	if err := s.CancelTask(ctx, "ra"); !isConflict(err) {
		t.Fatalf("active cancel want conflict, got %v", err)
	}

	// 检查后被取走（DeleteTask 报 active state）→ 409
	ins2 := &fakeInspector{exists: true, state: asynq.TaskStatePending,
		delErr: errors.New("asynq: INTERNAL_ERROR: cannot delete task in active state. use CancelProcessing instead.")}
	s2 := newSvc(t, ins2)
	row(t, s2, "rb", repository.StatusPending)
	if err := s2.CancelTask(ctx, "rb"); !isConflict(err) {
		t.Fatalf("delete-active want conflict, got %v", err)
	}

	// running（DB 视角）→ 409
	ins3 := &fakeInspector{exists: true, state: asynq.TaskStateActive}
	s3 := newSvc(t, ins3)
	row(t, s3, "rc", repository.StatusRunning)
	if err := s3.CancelTask(ctx, "rc"); !isConflict(err) {
		t.Fatalf("running cancel want conflict, got %v", err)
	}

	// 正常取消 → canceled
	ins4 := &fakeInspector{exists: true, state: asynq.TaskStatePending}
	s4 := newSvc(t, ins4)
	row(t, s4, "rd", repository.StatusPending)
	if err := s4.CancelTask(ctx, "rd"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if r, _ := s4.repo.GetByTaskID(ctx, "rd"); r.Status != repository.StatusCanceled {
		t.Fatalf("status: %s", r.Status)
	}
}

// F35：Retention 留观中的 completed / archived 命中 → 409 且不删留观
// （删了会抹掉「已执行」告警指纹并写 canceled 假终态）。
func TestCancelTerminalStateFingerprints409(t *testing.T) {
	ctx := context.Background()
	for _, st := range []asynq.TaskState{asynq.TaskStateCompleted, asynq.TaskStateArchived} {
		ins := &fakeInspector{exists: true, state: st}
		s := newSvc(t, ins)
		row(t, s, "f35", repository.StatusPending)
		wantConflict(t, s.CancelTask(ctx, "f35"))
		if len(ins.deleted) != 0 {
			t.Fatalf("%v: must not delete the retention copy", st)
		}
		if r, _ := s.repo.GetByTaskID(ctx, "f35"); r.Status != repository.StatusPending {
			t.Fatalf("%v: row must stay pending (fingerprint preserved), got %s", st, r.Status)
		}
	}
}

// F58：queued=0 行遇 Redis 可用性错误 → 降级取消（200，不触 Redis 删除，第三域探针收尾）；
// queued=1 行同错误 → 原样返回（取消必须 Redis 配合）。
func TestCancelDegradedByQueuedFlag(t *testing.T) {
	ctx := context.Background()
	down := errors.New("dial tcp: connection refused")

	ins := &fakeInspector{getErr: down}
	s := newSvc(t, ins)
	row(t, s, "dg0", repository.StatusPending) // InsertPending 默认 queued=0
	if err := s.CancelTask(ctx, "dg0"); err != nil {
		t.Fatalf("queued=0 cancel must degrade to success, got %v", err)
	}
	if r, _ := s.repo.GetByTaskID(ctx, "dg0"); r.Status != repository.StatusCanceled {
		t.Fatalf("degraded cancel must mark canceled, got %s", r.Status)
	}
	if len(ins.deleted) != 0 {
		t.Fatal("degraded cancel must not call DeleteTask")
	}

	ins2 := &fakeInspector{getErr: down}
	s2 := newSvc(t, ins2)
	row(t, s2, "dg1", repository.StatusPending)
	if _, err := s2.repo.MarkQueued(ctx, "dg1"); err != nil {
		t.Fatal(err)
	}
	err := s2.CancelTask(ctx, "dg1")
	if err == nil || isConflict(err) {
		t.Fatalf("queued=1 availability error must surface as-is, got %v", err)
	}
	if r, _ := s2.repo.GetByTaskID(ctx, "dg1"); r.Status != repository.StatusPending {
		t.Fatalf("queued=1 row must stay pending, got %s", r.Status)
	}
}

// F64：ErrQueueNotFound（队列注册集被清，如 FLUSHDB）= 键消亡确认——取消照常完成。
func TestCancelQueueNotFoundIsKeyGone(t *testing.T) {
	ctx := context.Background()
	ins := &fakeInspector{getErr: asynq.ErrQueueNotFound}
	s := newSvc(t, ins)
	row(t, s, "qnf", repository.StatusPending)
	if err := s.CancelTask(ctx, "qnf"); err != nil {
		t.Fatalf("queue-not-found must be tolerated, got %v", err)
	}
	if r, _ := s.repo.GetByTaskID(ctx, "qnf"); r.Status != repository.StatusCanceled {
		t.Fatalf("status: %s", r.Status)
	}
}

func TestRetryRaces(t *testing.T) {
	ctx := context.Background()

	// RunTask 后 worker 瞬间完成（终态已写）→ 条件更新 0 行 → 409
	ins := &fakeInspector{exists: true}
	s := newSvc(t, ins)
	row(t, s, "r1", repository.StatusDead)
	// 模拟 worker 在 RunTask 与 ResetPending 之间完成：RunTask 回调里写终态
	s.inspector = raceInspector{TaskInspector: ins, afterRun: func() {
		ctx := context.Background()
		_ = s.repo.MarkRunning(ctx, "r1", 2, timeNow())
		_ = s.repo.Finish(ctx, "r1", repository.StatusSucceeded, "", 2, timeNow(), timeNow())
	}}
	if err := s.RetryTask(ctx, "r1"); !isConflict(err) {
		t.Fatalf("retry race want conflict, got %v", err)
	}

	// 队列无任务（RunTask 返回 ErrTaskNotFound）→ 409
	ins2 := &fakeInspector{runErr: asynq.ErrTaskNotFound}
	s2 := newSvc(t, ins2)
	row(t, s2, "r2", repository.StatusDead)
	if err := s2.RetryTask(ctx, "r2"); !isConflict(err) {
		t.Fatalf("not-found retry want conflict, got %v", err)
	}

	// 正常重试 → pending
	ins3 := &fakeInspector{exists: true}
	s3 := newSvc(t, ins3)
	row(t, s3, "r3", repository.StatusDead)
	if err := s3.RetryTask(ctx, "r3"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if r, _ := s3.repo.GetByTaskID(ctx, "r3"); r.Status != repository.StatusPending {
		t.Fatalf("status: %s", r.Status)
	}
}
