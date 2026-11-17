package service

import (
	"context"
	"database/sql"
	"errors"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func cronFixture(t *testing.T) (*Service, *repository.Store, *fakeEnqueuer, *sql.DB, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cron.db")
	st, err := repository.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	now := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	if err := st.CreateJob(context.Background(), repository.Job{JobID: "atomic", ActionID: "a", TriggerType: repository.TriggerCron, CronSpec: "* * * * *", Params: "{}", Enabled: true, NextRun: sql.NullTime{Valid: true, Time: now.Add(-time.Minute)}, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	enq := &fakeEnqueuer{}
	svc := &Service{Store: st, Client: enq, Queue: "jobs", Retention: time.Hour}
	return svc, st, enq, raw, now
}
func TestCronAdvanceFailureRollsBackOccurrence(t *testing.T) {
	svc, st, enq, raw, now := cronFixture(t)
	if _, err := raw.Exec(`CREATE TRIGGER fail_next BEFORE UPDATE OF next_run ON jobs BEGIN SELECT RAISE(ABORT,'injected advance failure'); END`); err != nil {
		t.Fatal(err)
	}
	loop := &Loop{Store: st, Submit: svc, Logger: slog.Default(), Now: func() time.Time { return now }}
	loop.FireDue(context.Background())
	loop.FireDue(context.Background())
	_, total, err := st.ListRuns(context.Background(), repository.RunFilter{})
	if err != nil || total != 0 || enq.count() != 0 {
		t.Fatalf("failed occurrence transaction must have zero runs/enqueue: total=%d enq=%d err=%v", total, enq.count(), err)
	}
	if _, err := raw.Exec(`DROP TRIGGER fail_next`); err != nil {
		t.Fatal(err)
	}
	loop.FireDue(context.Background())
	loop.FireDue(context.Background())
	if enq.count() != 1 {
		t.Fatalf("recovered same due must enqueue once: %d", enq.count())
	}
}
func TestCronTwoLoopsClaimBeforeEnqueue(t *testing.T) {
	svc, st, enq, _, now := cronFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var n atomic.Int32
	enq.hook = func(task.Payload) {
		if n.Add(1) == 1 {
			close(entered)
			<-release
		}
	}
	loop := func() *Loop {
		return &Loop{Store: st, Submit: svc, Logger: slog.Default(), Now: func() time.Time { return now }}
	}
	go func() { defer close(done); loop().FireDue(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first loop did not enqueue")
	}
	loop().FireDue(context.Background())
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("first loop stuck")
	}
	if enq.count() != 1 {
		t.Fatalf("same occurrence executed by both loops: enq=%d", enq.count())
	}
}

// Existing SQLite jobs may contain a monotonic suffix and named local timezone
// in the stored timestamp. A decoded time.Time cannot reproduce that SQL token.
func TestCronSQLiteLegacyClockSnapshot(t *testing.T) {
	svc, st, enq, raw, _ := cronFixture(t)
	zone := time.FixedZone("CST", 8*60*60)
	due := time.Date(2026, 11, 17, 10, 0, 0, 0, zone)
	updated := "2026-11-17 09:00:00 +0800 CST m=+12.123456789"
	if _, err := raw.Exec(`UPDATE jobs SET next_run=?,updated_at=? WHERE job_id='atomic'`, due.String(), updated); err != nil {
		t.Fatal(err)
	}
	loop := &Loop{Store: st, Submit: svc, Logger: slog.Default(), Now: func() time.Time { return due.Add(time.Second) }}
	loop.FireDue(context.Background())
	loop.FireDue(context.Background())
	if enq.count() != 1 {
		t.Fatalf("legacy stored clock tokens must claim exactly once: %d", enq.count())
	}
}

func TestManualJobStaleSnapshotHasNoRun(t *testing.T) {
	svc, st, enq, _, now := cronFixture(t)
	ctx := context.Background()
	stale, err := st.GetJob(ctx, "atomic")
	if err != nil {
		t.Fatal(err)
	}
	changed := *stale
	changed.Params = `{"changed":true}`
	changed.UpdatedAt = now.Add(time.Second)
	if err := st.UpdateJob(ctx, changed); err != nil {
		t.Fatal(err)
	}
	p := task.Payload{TaskID: "core-before-trigger", Action: stale.ActionID, JobID: stale.JobID, Dept: stale.Dept, Params: []byte(stale.Params), TimeoutSecs: stale.TimeoutSecs}
	accepted, _, err := svc.SubmitJob(ctx, *stale, p)
	var conflict *ConflictError
	if accepted || !errors.As(err, &conflict) || enq.count() != 0 {
		t.Fatalf("stale job snapshot reached queue: %v %v %d", accepted, err, enq.count())
	}
	_, total, err := st.ListRuns(ctx, repository.RunFilter{})
	if err != nil || total != 0 {
		t.Fatalf("stale snapshot created run: %d %v", total, err)
	}
	current, err := st.GetJob(ctx, "atomic")
	if err != nil {
		t.Fatal(err)
	}
	p.Params = []byte(current.Params)
	if a, _, err := svc.SubmitJob(ctx, *current, p); !a || err != nil {
		t.Fatalf("current snapshot should submit: %v %v", a, err)
	}
	if a, _, err := svc.SubmitJob(ctx, *current, p); a || err != nil {
		t.Fatalf("same identity should be idempotent: %v %v", a, err)
	}
	if enq.count() != 1 {
		t.Fatalf("manual repeated delivery: %d", enq.count())
	}
}
