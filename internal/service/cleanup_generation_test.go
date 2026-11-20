package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type reclaimProbeHooks struct {
	*reclaimInspector
	delete func(string) error
	info   func(string) (*asynq.TaskInfo, error)
}

func (h reclaimProbeHooks) DeleteTask(queue, id string) error {
	if h.delete != nil {
		return h.delete(id)
	}
	return h.reclaimInspector.DeleteTask(queue, id)
}

func (h reclaimProbeHooks) GetTaskInfo(queue, id string) (*asynq.TaskInfo, error) {
	if h.info != nil {
		return h.info(id)
	}
	return h.reclaimInspector.GetTaskInfo(queue, id)
}

func TestReclaimOldAbsenceCannotCloseNewCancellationCleanup(t *testing.T) {
	l, st, queue, inspector, _ := newReclaim(t)
	ctx := context.Background()
	id := "late-between-probe-and-confirmation"
	seedCanceled(t, st, id, 0)
	deletes := 0
	l.Inspector = reclaimProbeHooks{reclaimInspector: inspector, delete: func(got string) error {
		if got != id {
			t.Fatalf("unexpected deletion %s", got)
		}
		deletes++
		if deletes == 1 {
			// Redis observes no key, then a separate late delivery reopens
			// cleanup before this older probe's result reaches the ledger.
			if _, err := st.MarkCleanupPending(ctx, id); err != nil {
				t.Fatal(err)
			}
			return asynq.ErrTaskNotFound
		}
		return nil
	}}

	l.ReclaimStalePending(ctx)
	if closed, err := st.GetQueued(ctx, id); err != nil || closed {
		t.Fatalf("older confirmation retired the new cleanup: %v %v", closed, err)
	}
	l.ReclaimStalePending(ctx)
	if closed, err := st.GetQueued(ctx, id); err != nil || !closed || deletes != 2 || queue.count() != 0 {
		t.Fatalf("cleanup did not converge without replay: closed=%v err=%v deletes=%d enqueues=%d",
			closed, err, deletes, queue.count())
	}
}

func TestReclaimRunningObservationChecksExecutionAfterQueueProbe(t *testing.T) {
	l, st, _, inspector, logs := newReclaim(t)
	ctx := context.Background()
	seedOld(t, st, "normal-terminal", "", 0)
	if err := st.MarkRunning(ctx, "normal-terminal", 1, reclaimBase, "execution-1"); err != nil {
		t.Fatal(err)
	}
	l.Inspector = reclaimProbeHooks{reclaimInspector: inspector, info: func(id string) (*asynq.TaskInfo, error) {
		if err := st.Finish(ctx, id, repository.StatusDead, "synthetic", 1, reclaimBase, reclaimBase.Add(time.Minute), "execution-1"); err != nil {
			return nil, fmt.Errorf("fixture finish: %w", err)
		}
		return &asynq.TaskInfo{State: asynq.TaskStateArchived}, nil
	}}
	l.ReclaimStalePending(ctx)
	row, err := st.GetByTaskID(ctx, "normal-terminal")
	if err != nil || row.Status != repository.StatusDead {
		t.Fatalf("fixture failed to finish the current execution: %v %v", row, err)
	}
	if got := logs.count(slog.LevelError, "stuck running"); got != 0 {
		t.Fatalf("normal completion became a reconciliation error: %d", got)
	}
}

func TestLateCanceledDeliveryWithUncertainQueueResponse(t *testing.T) {
	for _, source := range []string{"submit", "sweep"} {
		t.Run(source, func(t *testing.T) {
			l, store, queue, inspector, _ := newReclaim(t)
			l.defaults()
			ctx := context.Background()
			id := "possibly-delivered"
			keyExists := false
			queue.err = errors.New("queue response lost")
			queue.hook = func(p task.Payload) {
				row := &repository.Run{TaskID: p.TaskID}
				if ok, err := store.MarkCanceledIfPending(ctx, row, "fixture canceled", reclaimBase, false); err != nil || !ok {
					t.Fatalf("cancel before delivery: %v %v", ok, err)
				}
				if n, err := store.MarkCleanupDone(ctx, id, 0); err != nil || n != 1 {
					t.Fatalf("old probe must confirm the key is absent: %v %v", n, err)
				}
				keyExists = true
			}
			withdrawals := 0
			l.Inspector = reclaimProbeHooks{reclaimInspector: inspector, delete: func(string) error {
				withdrawals++
				if withdrawals == 1 {
					return errors.New("temporary cleanup failure")
				}
				keyExists = false
				return nil
			}}
			if source == "submit" {
				service := &Service{Store: store, Client: queue, Inspector: l.Inspector, Queue: l.Queue}
				if accepted, warning, err := service.Submit(ctx, submitPayload(id)); err != nil || warning == nil || !accepted {
					t.Fatalf("acceptance must preserve ambiguous delivery warning: %v %v %v", accepted, warning, err)
				}
			} else {
				seedOld(t, store, id, `{"task_id":"possibly-delivered","action":"a"}`, 0)
				l.requeueUnqueued(ctx, reclaimBase)
			}
			closed, err := store.GetQueued(ctx, id)
			if err != nil || closed || withdrawals != 1 || !keyExists {
				t.Fatalf("ambiguous response lost cleanup: closed=%v withdrawals=%d key=%v err=%v", closed, withdrawals, keyExists, err)
			}
			l.probeCanceled(ctx, time.Now().Add(time.Hour))
			if closed, err = store.GetQueued(ctx, id); err != nil || !closed || withdrawals != 2 || keyExists || queue.count() != 1 {
				t.Fatalf("unconfirmed delivery failed to converge: closed=%v withdrawals=%d key=%v enqueues=%d err=%v",
					closed, withdrawals, keyExists, queue.count(), err)
			}
		})
	}
}

func TestReclaimScansAgainUnderContinuousArrivals(t *testing.T) {
	l, store, _, inspector, logs := newReclaim(t)
	l.BatchSize = 1
	ctx := context.Background()
	seedCanceled(t, store, "first-cancel", 0)
	inspector.delErr["first-cancel"] = errors.New("temporary queue fault")
	seedRunning := func(id string) {
		seedOld(t, store, id, "", 0)
		if err := store.MarkRunning(ctx, id, 1, reclaimBase, id); err != nil {
			t.Fatal(err)
		}
		inspector.states[id] = asynq.TaskStateActive
	}
	seedRunning("first-running")
	l.ReclaimStalePending(ctx)
	delete(inspector.delErr, "first-cancel")
	inspector.states["first-running"] = asynq.TaskStateArchived
	for i := 0; i < 8; i++ {
		seedCanceled(t, store, fmt.Sprintf("more-canceled-%d", i), 0)
		seedRunning(fmt.Sprintf("more-running-%d", i))
		l.ReclaimStalePending(ctx)
	}
	if closed, err := store.GetQueued(ctx, "first-cancel"); err != nil || !closed {
		t.Fatalf("sustained arrivals starved an older cancellation: %v %v", closed, err)
	}
	if got := logs.count(slog.LevelError, "stuck running"); got != 1 {
		t.Fatalf("sustained arrivals starved an older execution: %d", got)
	}
}

func TestReclaimMissingQueueRegistrationIsInconclusive(t *testing.T) {
	l, store, _, inspector, logs := newReclaim(t)
	ctx := context.Background()
	seedCanceled(t, store, "cancel-with-missing-queue", 0)
	inspector.delErr["cancel-with-missing-queue"] = asynq.ErrQueueNotFound
	seedOld(t, store, "running-with-missing-queue", "", 0)
	if err := store.MarkRunning(ctx, "running-with-missing-queue", 1, reclaimBase, "execution"); err != nil {
		t.Fatal(err)
	}
	inspector.getErrs["running-with-missing-queue"] = asynq.ErrQueueNotFound
	l.ReclaimStalePending(ctx)
	if closed, err := store.GetQueued(ctx, "cancel-with-missing-queue"); err != nil || closed {
		t.Errorf("missing queue registry retired cleanup without checking the task: %v %v", closed, err)
	}
	if got := logs.count(slog.LevelError, "stuck running"); got != 0 {
		t.Errorf("missing queue registry was treated as an absent task key: %d", got)
	}
}
