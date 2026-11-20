package service

import (
	"context"
	"database/sql"
	"encoding/json"
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
func TestCronAlreadyRecordedOccurrenceAdvancesWithoutAnotherDelivery(t *testing.T) {
	svc, st, enq, _, now := cronFixture(t)
	ctx := context.Background()
	job, err := st.GetJob(ctx, "atomic")
	if err != nil {
		t.Fatal(err)
	}
	p := task.Payload{TaskID: CronTaskID(job.JobID, job.NextRun.Time), JobID: job.JobID, Action: job.ActionID, Params: []byte(job.Params)}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.InsertPending(ctx, repository.Run{
		TaskID: p.TaskID, JobID: p.JobID, Action: p.Action, Params: job.Params, EnqueuePayload: string(raw), EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	loop := &Loop{Store: st, Submit: svc, Logger: slog.Default(), Now: func() time.Time { return now }}
	loop.FireDue(ctx)
	current, err := st.GetJob(ctx, job.JobID)
	if err != nil || !current.NextRun.Time.After(now) {
		t.Fatalf("known occurrence should advance its schedule: %v %v", current, err)
	}
	loop.FireDue(ctx)
	_, total, err := st.ListRuns(ctx, repository.RunFilter{})
	if err != nil || total != 1 || enq.count() != 0 {
		t.Fatalf("keep original occurrence without another delivery: rows=%d deliveries=%d err=%v", total, enq.count(), err)
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

// 时隙冲突/非法定义（P2-9 独立调度面，2026-11-20 续批钉行为）：两条现行合同边界——
// ① 非法存量定义由 FireDue 有界停用（CAS 失败下轮重试，可见 Error，零发生次）；
// ② 同一发生次身份被内容不符的存量占用时整笔回滚、原 due 保留、存量快照不被覆盖，
// 等待人工处置——自动跳过/推进归 C03 调度恢复政策，本候选不扩权。

// ① 非法存量定义：到期待触发 → 停用落库（enabled=false + next_run=NULL）→
// 离开到期列表，零发生次、零投递。
func TestFireDueDisablesInvalidLegacyDefinition(t *testing.T) {
	// 独立事实库：只放非法定义，排除 fixture 默认任务的合法发生次干扰。
	path := filepath.Join(t.TempDir(), "invalid-cron.db")
	st, err := repository.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	enq := &fakeEnqueuer{}
	svc := &Service{Store: st, Client: enq, Queue: "jobs", Retention: time.Hour}
	ctx := context.Background()
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	if err := st.CreateJob(ctx, repository.Job{JobID: "broken-spec", ActionID: "a", TriggerType: repository.TriggerCron,
		CronSpec: "not-a-cron", Params: "{}", Enabled: true,
		NextRun: sql.NullTime{Valid: true, Time: now.Add(-time.Minute)}, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	loop := &Loop{Store: st, Submit: svc, Logger: slog.Default(), Now: func() time.Time { return now }}
	loop.FireDue(ctx)
	job, err := st.GetJob(ctx, "broken-spec")
	if err != nil || job.Enabled || job.NextRun.Valid {
		t.Fatalf("invalid legacy definition must be disabled with next_run cleared: %+v %v", job, err)
	}
	if enq.count() != 0 {
		t.Fatalf("invalid definition must not deliver: %d", enq.count())
	}
	loop.FireDue(ctx) // 已停用 → 不再列入到期，无副作用
	if enq.count() != 0 {
		t.Fatalf("disabled definition must stay silent: %d", enq.count())
	}
}

// ② 同发生次异内容：task_id 由 job_id+原due 确定性生成，存量发生次以旧定义快照
// 占住该身份（人工回拨/备份恢复后的「不明历史」）。现行合同：匹配同一发生次才能
// 推进；不符 → 整笔回滚（next_run 保留原时隙）、零新增投递、存量快照原样保留。
func TestCronSlotIdentityConflictKeepsOriginalDueAndRollsBack(t *testing.T) {
	svc, st, enq, _, now := cronFixture(t)
	ctx := context.Background()
	job, err := st.GetJob(ctx, "atomic")
	if err != nil {
		t.Fatal(err)
	}
	due := job.NextRun.Time
	taskID := CronTaskID(job.JobID, due)

	// 存量发生次：同身份、旧参数快照
	older := task.Payload{TaskID: taskID, JobID: job.JobID, Action: job.ActionID, Params: []byte(`{"older":true}`)}
	rawOlder, err := json.Marshal(older)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.InsertPending(ctx, repository.Run{TaskID: taskID, JobID: job.JobID, Action: job.ActionID,
		Params: `{"older":true}`, EnqueuePayload: string(rawOlder), EnqueuedAt: now.Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}

	loop := &Loop{Store: st, Submit: svc, Logger: slog.Default(), Now: func() time.Time { return now }}
	loop.FireDue(ctx)
	loop.FireDue(ctx) // 下轮按合同重试原 due，仍回滚

	current, err := st.GetJob(ctx, "atomic")
	if err != nil || !current.NextRun.Valid || !current.NextRun.Time.Equal(due) {
		t.Fatalf("conflicted slot must keep the original due: %+v %v", current, err)
	}
	if enq.count() != 0 {
		t.Fatalf("conflicted occurrence must not deliver: %d", enq.count())
	}
	row, err := st.GetByTaskID(ctx, taskID)
	if err != nil || row.Params != `{"older":true}` || row.Status != repository.StatusPending {
		t.Fatalf("existing occurrence snapshot must not be auto-overwritten: %+v %v", row, err)
	}
	_, total, err := st.ListRuns(ctx, repository.RunFilter{})
	if err != nil || total != 1 {
		t.Fatalf("rollback must leave exactly the stored occurrence: %d %v", total, err)
	}
}
