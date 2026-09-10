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
	state  asynq.TaskState // GetTaskInfo 返回的状态
	exists bool
	delErr error
	runErr error
}

func (f *fakeInspector) GetTaskInfo(_, _ string) (*asynq.TaskInfo, error) {
	if !f.exists {
		return nil, asynq.ErrTaskNotFound
	}
	return &asynq.TaskInfo{State: f.state}, nil
}
func (f *fakeInspector) DeleteTask(_, _ string) error { return f.delErr }
func (f *fakeInspector) RunTask(_, _ string) error    { return f.runErr }
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
