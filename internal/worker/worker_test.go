package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// fakeCB 可编程回调结果。
type fakeCB struct{ err error }

func (f fakeCB) Do(context.Context, task.Payload) error { return f.err }

func newDeps(t *testing.T, cb fakeCB) (Deps, *asynq.ServeMux) {
	t.Helper()
	st, err := repository.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	d := Deps{Store: st, Callback: cb, Logger: slog.Default()}
	return d, NewMux(d)
}

func payload(t *testing.T, id string) *asynq.Task {
	b, _ := json.Marshal(task.Payload{TaskID: id, RequestID: "r-" + id, Action: "a",
		CallbackURL: "http://zhu.internal/internal/jobs/callback"})
	return asynq.NewTask(task.TypeCallback, b)
}

func TestHandleSuccess(t *testing.T) {
	d, mux := newDeps(t, fakeCB{})
	ctx := context.Background()
	d.Store.InsertPending(ctx, repository.Run{TaskID: "ok", Action: "a", EnqueuedAt: time.Now()})

	if err := mux.ProcessTask(ctx, payload(t, "ok")); err != nil {
		t.Fatalf("success path: %v", err)
	}
	r, _ := d.Store.GetByTaskID(ctx, "ok")
	if r.Status != repository.StatusSucceeded || r.Attempts != 1 {
		t.Fatalf("row: %+v", r)
	}
}

func TestHandle4xxTerminalSkipRetry(t *testing.T) {
	d, mux := newDeps(t, fakeCB{err: fmt.Errorf("%w: status=404", callback.ErrNonRetryable)})
	ctx := context.Background()
	d.Store.InsertPending(ctx, repository.Run{TaskID: "t4", Action: "a", EnqueuedAt: time.Now()})

	err := mux.ProcessTask(ctx, payload(t, "t4"))
	if !errors.Is(err, callback.ErrNonRetryable) || !strings.Contains(fmt.Sprint(err), "skip retry") {
		t.Fatalf("want SkipRetry-wrapped non-retryable, got %v", err)
	}
	r, _ := d.Store.GetByTaskID(ctx, "t4")
	if r.Status != repository.StatusFailed {
		t.Fatalf("status: %s", r.Status)
	}
}

func TestHandle5xxUnknownRetryBudgetGoesDead(t *testing.T) {
	// 直接调 handler 时不在 asynq 处理上下文内（maxRetry 未知）——按终败处理：
	// 失败必须有终态记录，防「failed 永不晋升 dead」的脱节
	d, mux := newDeps(t, fakeCB{err: errors.New("callback: do: dial timeout")})
	ctx := context.Background()
	d.Store.InsertPending(ctx, repository.Run{TaskID: "t5", Action: "a", EnqueuedAt: time.Now()})

	if err := mux.ProcessTask(ctx, payload(t, "t5")); err == nil {
		t.Fatal("5xx should return error to asynq")
	}
	r, _ := d.Store.GetByTaskID(ctx, "t5")
	if r.Status != repository.StatusDead {
		t.Fatalf("unknown retry budget → dead, got %s", r.Status)
	}
}

func TestHandleMissingRowStillExecutes(t *testing.T) {
	d, mux := newDeps(t, fakeCB{})
	ctx := context.Background()
	if err := mux.ProcessTask(ctx, payload(t, "ghost")); err != nil {
		t.Fatalf("missing row should not fail the task: %v", err)
	}
	if _, err := d.Store.GetByTaskID(ctx, "ghost"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("row must stay missing, got %v", err)
	}
}

func TestHandleBadPayloadDropped(t *testing.T) {
	_, mux := newDeps(t, fakeCB{})
	ctx := context.Background()
	err := mux.ProcessTask(ctx, asynq.NewTask(task.TypeCallback, []byte("not-json")))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("bad payload want SkipRetry, got %v", err)
	}
}
