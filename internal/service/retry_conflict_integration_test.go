package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
)

type captureRetryInspector struct {
	TaskInspector
	err error
}

func (i *captureRetryInspector) RunTask(queue, id string) error {
	i.err = i.TaskInspector.RunTask(queue, id)
	return fmt.Errorf("actual inspector: %w", i.err)
}

// Keep the actual broker state fixed while the DB still reflects the previous
// failed delivery. This deterministically covers the forwarder/worker race and
// pins the otherwise unexported Asynq v0.26.0 precondition-error contract.
func TestRetryBrokerStateConflictRealRedis(t *testing.T) {
	addr := os.Getenv("TASKRUNNER_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("requires dedicated Redis")
	}
	for _, state := range []asynq.TaskState{asynq.TaskStatePending, asynq.TaskStateActive} {
		t.Run(state.String(), func(t *testing.T) {
			opt := asynq.RedisClientOpt{Addr: addr}
			inspector := asynq.NewInspector(opt)
			client := asynq.NewClient(opt)
			queue, id := "retry-conflict-"+uuid.NewString(), "retry-"+uuid.NewString()
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var server *asynq.Server
			t.Cleanup(func() {
				once.Do(func() { close(release) })
				if server != nil {
					server.Shutdown()
				}
				if err := inspector.DeleteTask(queue, id); err != nil && !errors.Is(err, asynq.ErrTaskNotFound) && !errors.Is(err, asynq.ErrQueueNotFound) {
					t.Errorf("cleanup task: %v", err)
				}
				if err := inspector.DeleteQueue(queue, false); err != nil && !errors.Is(err, asynq.ErrQueueNotFound) {
					t.Errorf("cleanup queue: %v", err)
				}
				if err := client.Close(); err != nil {
					t.Errorf("close client: %v", err)
				}
				if err := inspector.Close(); err != nil {
					t.Errorf("close inspector: %v", err)
				}
			})
			if state == asynq.TaskStateActive {
				server = asynq.NewServer(opt, asynq.Config{Concurrency: 1, Queues: map[string]int{queue: 1}, TaskCheckInterval: 5 * time.Millisecond, ShutdownTimeout: time.Second})
				if err := server.Start(asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
					close(started)
					<-release
					return nil
				})); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := client.Enqueue(asynq.NewTask("synthetic:retry-conflict", nil), asynq.Queue(queue), asynq.TaskID(id), asynq.MaxRetry(0), asynq.Retention(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if state == asynq.TaskStateActive {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("dedicated worker did not claim task")
				}
			}
			beforeBroker, err := inspector.GetTaskInfo(queue, id)
			if err != nil {
				t.Fatal(err)
			}
			if beforeBroker.State != state {
				t.Fatalf("fixture broker state=%s, want %s", beforeBroker.State, state)
			}
			for _, status := range []string{repository.StatusFailed, repository.StatusDead} {
				t.Run(status, func(t *testing.T) {
					s := newSvc(t, &fakeInspector{})
					captured := &captureRetryInspector{TaskInspector: inspector}
					s.inspector, s.queue = captured, queue
					row(t, s, id, status)
					ctx := context.Background()
					before, err := s.repo.GetByTaskID(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					err = s.RetryTask(ctx, id)
					t.Logf("broker_state=%s DB_status=%s actual_run_error=%v", state, status, captured.err)
					wantText := "asynq: FAILED_PRECONDITION: task is already in pending state"
					if state == asynq.TaskStateActive {
						wantText = "asynq: FAILED_PRECONDITION: task is already running"
					}
					if captured.err == nil || captured.err.Error() != wantText {
						t.Fatalf("pinned Asynq error changed: got %v, want %s", captured.err, wantText)
					}
					if !isConflict(err) {
						t.Errorf("actual broker state conflict must map to ConflictError, got %v", err)
					}
					after, err := s.repo.GetByTaskID(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, after) {
						t.Errorf("rejected broker retry changed record: before=%+v after=%+v", before, after)
					}
					afterBroker, err := inspector.GetTaskInfo(queue, id)
					if err != nil {
						t.Fatal(err)
					}
					if afterBroker.State != beforeBroker.State || afterBroker.Retried != beforeBroker.Retried || afterBroker.MaxRetry != beforeBroker.MaxRetry {
						t.Errorf("rejected retry changed broker state/budget: before=%+v after=%+v", beforeBroker, afterBroker)
					}
				})
			}
		})
	}
}
