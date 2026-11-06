package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/notify"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type blockedDeliveryCallback struct {
	count                       atomic.Int32
	firstStarted, secondStarted chan struct{}
	firstRelease, secondRelease chan struct{}
	firstErr                    error
}

func (c *blockedDeliveryCallback) Do(_ context.Context, _ task.Payload) error {
	if c.count.Add(1) == 1 {
		close(c.firstStarted)
		<-c.firstRelease
		return c.firstErr
	} else {
		close(c.secondStarted)
		<-c.secondRelease
	}
	return nil
}

// An actual Asynq shutdown requeues an unfinished delivery without incrementing
// retry count. A callback port deliberately ignores cancellation to expose the
// still-running invocation; old completion must not win the newer DB claim.
func TestDeliveryFence_AsynqShutdownRedelivery(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"nonretryable", fmt.Errorf("%w: synthetic 400", callback.ErrNonRetryable)},
		{"dead", fmt.Errorf("synthetic 500")},
	} {
		t.Run(tc.name, func(t *testing.T) { testDeliveryShutdown(t, tc.err) })
	}
}

type fenceNotifier struct{ calls atomic.Int32 }

func (n *fenceNotifier) NotifyDeadLetter(notify.Payload) { n.calls.Add(1) }

func testDeliveryShutdown(t *testing.T, firstErr error) {
	dsn, redisAddr := os.Getenv("TASKRUNNER_TEST_PG_DSN"), os.Getenv("TASKRUNNER_TEST_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		t.Skip("requires dedicated PG and Redis")
	}
	// Repository PG tests intentionally DROP their fixture tables. Use a distinct
	// database per worker test so normal parallel package execution cannot race.
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	dbName := "fence_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(`CREATE DATABASE "` + dbName + `"`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(`DROP DATABASE "` + dbName + `"`); err != nil {
			t.Errorf("cleanup worker fixture database: %v", err)
		}
	}()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + dbName
	dsn = u.String()
	st, err := repository.OpenPG(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	id := "delivery-" + uuid.NewString()
	queue := "fence-" + uuid.NewString()
	if err = st.InsertPending(ctx, repository.Run{TaskID: id, Action: "synthetic", EnqueuedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	cb := &blockedDeliveryCallback{firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), firstRelease: make(chan struct{}), secondRelease: make(chan struct{})}
	cb.firstErr = firstErr
	var firstOnce, secondOnce sync.Once
	releaseFirst := func() { firstOnce.Do(func() { close(cb.firstRelease) }) }
	releaseSecond := func() { secondOnce.Do(func() { close(cb.secondRelease) }) }
	defer releaseFirst()
	defer releaseSecond()
	notifier := &fenceNotifier{}
	deps := Deps{Store: st, Callback: cb, Logger: slog.Default(), Notifier: notifier}
	opt := asynq.RedisClientOpt{Addr: redisAddr}
	cfg := asynq.Config{Concurrency: 1, Queues: map[string]int{queue: 1}, ShutdownTimeout: time.Second, TaskCheckInterval: 10 * time.Millisecond}
	oldFinished := make(chan struct{})
	first := asynq.NewServer(opt, cfg)
	if err = first.Start(asynq.HandlerFunc(func(c context.Context, at *asynq.Task) error {
		defer close(oldFinished)
		return NewMux(deps).ProcessTask(c, at)
	})); err != nil {
		t.Fatal(err)
	}
	defer first.Shutdown()
	client := asynq.NewClient(opt)
	defer client.Close()
	raw, _ := json.Marshal(task.Payload{TaskID: id, Action: "synthetic", RequestID: "E800112"})
	if _, err = client.Enqueue(asynq.NewTask(task.TypeCallback, raw), asynq.Queue(queue), asynq.TaskID(id), asynq.MaxRetry(0), asynq.Retention(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waitFenceSignal(t, cb.firstStarted, "first callback")
	first.Shutdown() // budget expires: actual broker requeue, old callback remains in flight
	second := asynq.NewServer(opt, cfg)
	if err = second.Start(NewMux(deps)); err != nil {
		t.Fatal(err)
	}
	defer func() { releaseSecond(); second.Shutdown() }()
	waitFenceSignal(t, cb.secondStarted, "second callback")
	run, err := st.GetByTaskID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Attempts != 1 || run.Status != repository.StatusRunning {
		t.Fatalf("redelivery must keep retry count=1 while new callback blocked: %+v", run)
	}
	releaseFirst()
	waitFenceSignal(t, oldFinished, "old handler completion")
	run, err = st.GetByTaskID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("real_Asynq_shutdown_requeue=true real_PG=true callbacks=%d same_attempt=%d after_old_finish=%s", cb.count.Load(), run.Attempts, run.Status)
	if run.Status != repository.StatusRunning {
		t.Fatalf("old completion overwrote in-flight new delivery: %s", run.Status)
	}
	if notifier.calls.Load() != 0 {
		t.Fatal("stale delivery emitted a dead-letter notification")
	}
	releaseSecond()
	deadline := time.Now().Add(3 * time.Second)
	for {
		run, err = st.GetByTaskID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == repository.StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("current delivery did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ins := asynq.NewInspector(opt)
	defer ins.Close()
	defer ins.DeleteQueue(queue, true)
}

func waitFenceSignal(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(10 * time.Second):
		t.Fatal(fmt.Sprintf("timeout waiting for %s", what))
	}
}
