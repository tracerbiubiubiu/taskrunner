package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/tracerbiubiubiu/taskrunner/internal/notify"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type panicCallbackFunc func(context.Context, task.Payload) error

func (f panicCallbackFunc) Do(ctx context.Context, p task.Payload) error { return f(ctx, p) }

type panicNotifier struct{ calls int }

func (n *panicNotifier) NotifyDeadLetter(notify.Payload) { n.calls++ }

func TestCallbackPanicCannotFinishNewDelivery(t *testing.T) {
	st := panicStore(t, "sqlite")
	ctx := context.Background()
	if err := st.InsertPending(ctx, repository.Run{TaskID: "panic-stale", Action: "audit", EnqueuedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	n := &panicNotifier{}
	d := Deps{Store: st, Logger: slog.Default(), Notifier: n, Callback: panicCallbackFunc(func(ctx context.Context, _ task.Payload) error {
		if err := st.MarkRunning(ctx, "panic-stale", 1, time.Now(), "newer-delivery"); err != nil {
			t.Fatal(err)
		}
		panic("audit-stale-callback")
	})}
	err := NewMux(d).ProcessTask(ctx, payload(t, "panic-stale"))
	if !errors.Is(err, asynq.SkipRetry) || !errors.Is(err, repository.ErrDeliverySuperseded) {
		t.Fatalf("stale panic must skip: %v", err)
	}
	r, err := st.GetByTaskID(ctx, "panic-stale")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != repository.StatusRunning || n.calls != 0 {
		t.Fatalf("stale panic wrote/notified: %+v notifications=%d", r, n.calls)
	}
}

type panicCallback struct {
	count     atomic.Int32
	firstOnly bool
}

func (c *panicCallback) Do(context.Context, task.Payload) error {
	n := c.count.Add(1)
	if n == 1 || !c.firstOnly {
		panic("audit-callback-injected")
	}
	return nil
}

// The broker's own panic recovery does not persist job_runs terminal state.
// Use an actual Asynq server to verify both final failure and a successful retry.
func TestCallbackPanicAsynqLifecycle(t *testing.T) {
	addr := os.Getenv("TASKRUNNER_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("requires dedicated Redis")
	}
	drivers := []string{"sqlite"}
	if os.Getenv("TASKRUNNER_TEST_PG_DSN") != "" {
		drivers = append(drivers, "pg")
	}
	for _, driver := range drivers {
		t.Run(driver, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				maxRetry  int
				firstOnly bool
				status    string
				attempts  int
			}{
				{"final", 0, false, repository.StatusDead, 1},
				{"retry_success", 1, true, repository.StatusSucceeded, 2},
				{"retry_exhausted", 1, false, repository.StatusDead, 2},
			} {
				t.Run(tc.name, func(t *testing.T) {
					st := panicStore(t, driver)
					ctx := context.Background()
					id, queue := "panic-"+uuid.NewString(), "panic-"+uuid.NewString()
					if err := st.InsertPending(ctx, repository.Run{TaskID: id, Action: "audit", EnqueuedAt: time.Now().Add(-time.Hour)}); err != nil {
						t.Fatal(err)
					}
					cb := &panicCallback{firstOnly: tc.firstOnly}
					var logs bytes.Buffer
					deps := Deps{Store: st, Callback: cb, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
					opt := asynq.RedisClientOpt{Addr: addr}
					server := asynq.NewServer(opt, asynq.Config{Concurrency: 1, Queues: map[string]int{queue: 1},
						ShutdownTimeout: time.Second, TaskCheckInterval: 5 * time.Millisecond, DelayedTaskCheckInterval: 20 * time.Millisecond,
						RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return time.Millisecond }})
					if err := server.Start(NewMux(deps)); err != nil {
						t.Fatal(err)
					}
					client := asynq.NewClient(opt)
					defer client.Close()
					inspector := asynq.NewInspector(opt)
					defer inspector.Close()
					defer func() {
						server.Shutdown()
						if _, err := inspector.DeleteAllArchivedTasks(queue); err != nil {
							t.Errorf("cleanup archived: %v", err)
						}
					}()
					raw, _ := json.Marshal(task.Payload{TaskID: id, Action: "audit"})
					if _, err := client.Enqueue(asynq.NewTask(task.TypeCallback, raw), asynq.Queue(queue), asynq.TaskID(id), asynq.MaxRetry(tc.maxRetry)); err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(10 * time.Second)
					for time.Now().Before(deadline) {
						r, err := st.GetByTaskID(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						if r.Status == tc.status && r.Attempts == tc.attempts {
							break
						}
						info, e := inspector.GetTaskInfo(queue, id)
						if e == nil && info.State == asynq.TaskStateArchived && r.Status == repository.StatusRunning {
							before := time.Now().Add(time.Hour)
							a, _ := st.ListUnqueued(ctx, before, 100)
							b, _ := st.ListStuckPending(ctx, before, 0, 100)
							c, _ := st.ListCanceledUnqueued(ctx, before, 0, 100)
							t.Fatalf("panic archived but DB still running; reclaim domains=%d/%d/%d attempts=%d", len(a), len(b), len(c), r.Attempts)
						}
						time.Sleep(5 * time.Millisecond)
					}
					server.Shutdown() // wait for worker/log writes before reading the buffer
					r, err := st.GetByTaskID(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if r.Status != tc.status || r.Attempts != tc.attempts {
						t.Fatalf("panic lifecycle: %+v", r)
					}
					if !strings.Contains(logs.String(), "callback panic") || !strings.Contains(logs.String(), "stack") {
						t.Fatalf("missing correlated panic diagnostics: %s", logs.String())
					}
				})
			}
		})
	}
}

func panicStore(t *testing.T, driver string) *repository.Store {
	t.Helper()
	if driver == "sqlite" {
		st, err := repository.Open(filepath.Join(t.TempDir(), "panic.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	}
	dsn := os.Getenv("TASKRUNNER_TEST_PG_DSN")
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "panic_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(`CREATE DATABASE "` + name + `"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, e := admin.Exec(`DROP DATABASE "` + name + `"`); e != nil {
			t.Errorf("cleanup PG: %v", e)
		}
		admin.Close()
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	st, err := repository.OpenPG(u.String())
	if err != nil {
		t.Fatal(fmt.Errorf("open fixture PG: %w", err))
	}
	t.Cleanup(func() { st.Close() })
	return st
}
