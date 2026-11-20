// PG 集成测试（C7）：设置 TASKRUNNER_TEST_PG_DSN 才运行（如
// postgres://zhuzhao:***@127.0.0.1:5432/taskrunner_test?sslmode=disable）。
// 覆盖方言差异面：BIGSERIAL/BOOLEAN/TIMESTAMPTZ 建表、$n 占位符转换、
// 布尔字面量（WHERE enabled）、Timestamptz 扫描、条件更新、分页过滤。
package repository

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

func openTestStorePG(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TASKRUNNER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置，跳过 PG 集成测试")
	}
	// 每次运行清表，保证用例自包含（固定 task_id 可重入）
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open raw pg: %v", err)
	}
	if _, err := raw.Exec(`DROP TABLE IF EXISTS job_runs; DROP TABLE IF EXISTS jobs;`); err != nil {
		raw.Close()
		t.Fatalf("drop tables: %v", err)
	}
	raw.Close()

	s, err := OpenPG(dsn)
	if err != nil {
		t.Fatalf("OpenPG: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPGFullLifecycle(t *testing.T) {
	if os.Getenv("TASKRUNNER_TEST_PG_DSN") == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置")
	}
	s := openTestStorePG(t)
	ctx := context.Background()
	now := time.Now()

	// 生命周期：pending → running → succeeded
	if err := s.InsertPending(ctx, Run{
		TaskID: "pg-1", RequestID: "rpg-1", Action: "audit_archive", JobID: "j-pg", Dept: "audit",
		Params: `{"retain_days":90}`, CallbackURL: "http://zhu.internal/internal/jobs/callback",
		SubmittedBy: "10086", SourceIP: "10.0.0.9", EnqueuedAt: now,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.MarkRunning(ctx, "pg-1", 1, now.Add(time.Second), "fixture-delivery"); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.Finish(ctx, "pg-1", StatusSucceeded, "", 1, now.Add(time.Second), now.Add(3*time.Second), "fixture-delivery"); err != nil {
		t.Fatalf("finish: %v", err)
	}
	got, err := s.GetByTaskID(ctx, "pg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusSucceeded || got.Dept != "audit" || got.Params != `{"retain_days":90}` {
		t.Fatalf("unexpected row: %+v", got)
	}
	if !got.StartedAt.Valid || !got.FinishedAt.Valid {
		t.Fatal("timestamptz should round-trip")
	}

	// 终身幂等：同 task_id 重提 → 目录查到行，accepted=false 路径依赖本查询
	if err := s.InsertPending(ctx, Run{TaskID: "pg-1", Action: "a", EnqueuedAt: now}); err == nil {
		t.Fatal("duplicate task_id must violate UNIQUE")
	}

	// 条件更新：非 pending 不可取消
	ok, err := s.MarkCanceledIfPending(ctx, &Run{TaskID: "pg-1"}, "x", now, false)
	if err != nil || ok {
		t.Fatalf("cancel on succeeded should be no-op (ok=%v err=%v)", ok, err)
	}

	// jobs 建表 + BOOLEAN/Timestamptz 往返 + 到期扫描
	if err := s.CreateJob(ctx, Job{
		JobID: "j-pg", ActionID: "audit_archive", TriggerType: TriggerCron,
		CallbackURL: "http://zhu.internal/internal/jobs/callback", CronSpec: "0 3 * * *",
		Params: `{"retain_days":90}`, Dept: "audit", Enabled: true,
		CreatedBy: "10086", CreatedAt: now, UpdatedAt: now,
		NextRun: sql.NullTime{Valid: true, Time: now.Add(-time.Minute)}, // 已到期
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	due, err := s.ListDueCronJobs(ctx, now) // WHERE enabled（布尔字面量方言中立）
	if err != nil {
		t.Fatalf("list due: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("due jobs: %d", len(due))
	}
	if err := s.UpdateJobNextRun(ctx, "j-pg", now.Add(24*time.Hour), now); err != nil {
		t.Fatalf("advance: %v", err)
	}
	due, _ = s.ListDueCronJobs(ctx, now)
	if len(due) != 0 {
		t.Fatal("advanced next_run should not be due")
	}

	// NullTime 空值往返（manual 任务：next_run NULL）——pgx stdlib Valuer 链路
	if err := s.CreateJob(ctx, Job{
		JobID: "j-manual", ActionID: "audit_archive", TriggerType: TriggerManual,
		CallbackURL: "http://zhu.internal/internal/jobs/callback", Enabled: true,
		CreatedAt: now, UpdatedAt: now, // NextRun 零值 → NULL
	}); err != nil {
		t.Fatalf("create manual job: %v", err)
	}
	j, err := s.GetJob(ctx, "j-manual")
	if err != nil {
		t.Fatalf("get manual job: %v", err)
	}
	if j.NextRun.Valid {
		t.Fatal("manual job next_run should be NULL")
	}

	// 分页 + dept 多值过滤
	runs, total, err := s.ListRuns(ctx, RunFilter{Depts: []string{"audit"}, Status: StatusSucceeded, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if total != 1 || len(runs) != 1 || runs[0].TaskID != "pg-1" {
		t.Fatalf("filter result: total=%d len=%d", total, len(runs))
	}
}

// C6 回归（判定测试转正）：终态覆写/复活谓词——
// ① 过期 attempt 的 Finish 被拒（租约失效双执行的后到写者）
// ② succeeded 行不可被 MarkRunning 复活
func TestPGFinishPredicateRejectsStaleWriter(t *testing.T) {
	if os.Getenv("TASKRUNNER_TEST_PG_DSN") == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置")
	}
	s := openTestStorePG(t)
	ctx := context.Background()
	now := time.Now()
	id := "pg-pred-1"

	must := func(err error, what string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	must(s.InsertPending(ctx, Run{TaskID: id, RequestID: "r", Action: "a", EnqueuedAt: now}), "insert")
	must(s.MarkRunning(ctx, id, 1, now, "fixture-delivery"), "mark running 1")
	must(s.MarkRunning(ctx, id, 2, now, "fixture-delivery"), "mark running 2（模拟重投递的新执行推进 attempts）")

	// 过期写者（attempt=1）：等值谓词拒绝
	if err := s.Finish(ctx, id, StatusSucceeded, "stale", 1, now, now, "fixture-delivery"); err == nil {
		t.Fatal("过期 attempt 的 Finish 应被拒绝")
	}
	// 新执行终态（attempt=2）正常落库
	must(s.Finish(ctx, id, StatusSucceeded, "", 2, now, now, "fixture-delivery"), "finish attempt 2")
	// 终态后再次 Finish 亦被拒（status 已非 running）
	if err := s.Finish(ctx, id, StatusFailed, "late", 2, now, now, "fixture-delivery"); err == nil {
		t.Fatal("终态后的重复 Finish 应被拒绝")
	}
}

func TestPGMarkRunningRejectsSucceededResurrection(t *testing.T) {
	if os.Getenv("TASKRUNNER_TEST_PG_DSN") == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置")
	}
	s := openTestStorePG(t)
	ctx := context.Background()
	now := time.Now()
	id := "pg-pred-2"

	must := func(err error, what string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	must(s.InsertPending(ctx, Run{TaskID: id, RequestID: "r", Action: "a", EnqueuedAt: now}), "insert")
	must(s.MarkRunning(ctx, id, 1, now, "fixture-delivery"), "mark running")
	must(s.Finish(ctx, id, StatusSucceeded, "", 1, now, now, "fixture-delivery"), "finish succeeded")

	// succeeded 行不可被 MarkRunning 复活（failed→running 为重试周期正常路径，
	// 由 asynq 重投递驱动；手动 RetryTask 走 ResetPendingIfTerminal 先归 pending）
	if err := s.MarkRunning(ctx, id, 2, now, "fixture-delivery"); err == nil {
		t.Fatal("succeeded 行被 MarkRunning 复活——C6 防线失效")
	}
	got, err := s.GetByTaskID(ctx, id)
	must(err, "get")
	if got.Status != StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("终态应保持不变: %+v", got)
	}
}

// ADR-004：queued 标志 / 三域扫描查询 / id 游标 / MarkRunning 分类在 PG 下的等价性。
func TestPGReclaimColumnsAndQueries(t *testing.T) {
	if os.Getenv("TASKRUNNER_TEST_PG_DSN") == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置")
	}
	s := openTestStorePG(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	now := time.Now()

	// 新列写入 + 第一轮扫描域 + payload/权威列往返（P2-9 比对数据面）
	if err := s.InsertPending(ctx, Run{TaskID: "pg-rq1", RequestID: "req-rq1", Action: "a", JobID: "j-rq1",
		Dept: "audit", Params: `{"k":1}`, CallbackURL: "http://cb.invalid", SubmittedBy: "10086", SourceIP: "10.9.9.9",
		EnqueuedAt: base, EnqueuePayload: `{"task_id":"pg-rq1","action":"a"}`}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := s.ListUnqueued(ctx, now, 0, math.MaxInt64, 10)
	if err != nil || len(rows) != 1 || rows[0].TaskID != "pg-rq1" || rows[0].EnqueuePayload != `{"task_id":"pg-rq1","action":"a"}` {
		t.Fatalf("list unqueued: %+v err=%v", rows, err)
	}
	if r := rows[0]; r.RequestID != "req-rq1" || r.Action != "a" || r.JobID != "j-rq1" || r.Dept != "audit" ||
		r.Params != `{"k":1}` || r.CallbackURL != "http://cb.invalid" || r.SubmittedBy != "10086" || r.SourceIP != "10.9.9.9" {
		t.Fatalf("PG ledger columns round-trip broken: %+v", r)
	}
	if n, err := s.MarkQueued(ctx, "pg-rq1"); err != nil || n != 1 {
		t.Fatalf("mark queued: %d %v", n, err)
	}
	if q, err := s.GetQueued(ctx, "pg-rq1"); err != nil || !q {
		t.Fatalf("get queued: %v %v", q, err)
	}

	// 第二轮 id 游标：耗尽判定
	stuck, err := s.ListStuckPending(ctx, now, 0, math.MaxInt64, 10)
	if err != nil || len(stuck) != 1 || stuck[0].TaskID != "pg-rq1" {
		t.Fatalf("stuck: %+v err=%v", stuck, err)
	}
	if stuck2, err := s.ListStuckPending(ctx, now, stuck[0].ID, math.MaxInt64, 10); err != nil || len(stuck2) != 0 {
		t.Fatalf("cursor exhausted: %+v err=%v", stuck2, err)
	}

	// 第三域：取消 → 探针域入选 → 出域后离开
	if err := s.InsertPending(ctx, Run{TaskID: "pg-rq2", Action: "a", EnqueuedAt: base}); err != nil {
		t.Fatalf("insert rq2: %v", err)
	}
	if ok, err := s.MarkCanceledIfPending(ctx, &Run{TaskID: "pg-rq2"}, "x", now, false); !ok || err != nil {
		t.Fatalf("cancel: %v %v", ok, err)
	}
	crows, err := s.ListCanceledUnqueued(ctx, now, 0, math.MaxInt64, 10)
	if err != nil || len(crows) != 1 || crows[0].TaskID != "pg-rq2" {
		t.Fatalf("canceled domain: %+v err=%v", crows, err)
	}
	oldGeneration := crows[0].CancelCleanupGeneration
	if n, err := s.MarkCleanupPending(ctx, "pg-rq2"); err != nil || n != 1 {
		t.Fatalf("new cleanup obligation: %d %v", n, err)
	}
	if n, err := s.MarkCleanupDone(ctx, "pg-rq2", oldGeneration); err != nil || n != 0 {
		t.Fatalf("old probe closed a more recent obligation: %d %v", n, err)
	}
	crows, err = s.ListCanceledUnqueued(ctx, now, 0, math.MaxInt64, 10)
	if err != nil || len(crows) != 1 || crows[0].CancelCleanupGeneration <= oldGeneration {
		t.Fatalf("new cleanup generation: %+v %v", crows, err)
	}
	if n, err := s.MarkCleanupDone(ctx, "pg-rq2", crows[0].CancelCleanupGeneration); err != nil || n != 1 {
		t.Fatalf("cleanup done: %d %v", n, err)
	}
	if crows2, _ := s.ListCanceledUnqueued(ctx, now, 0, math.MaxInt64, 10); len(crows2) != 0 {
		t.Fatalf("出域后应离开扫描域: %+v", crows2)
	}

	// MarkRunning 分类（F59）：canceled 复活 → ErrTerminalRejected
	if err := s.MarkRunning(ctx, "pg-rq2", 1, now, "fixture-delivery"); !errors.Is(err, ErrTerminalRejected) {
		t.Fatalf("want ErrTerminalRejected, got %v", err)
	}
}

// 旧库列迁移（PG 路径，C7 补齐的对称能力）：旧库缺新列 → Open 补列 → 新列读写。
func TestPGLegacyColumnMigration(t *testing.T) {
	dsn := os.Getenv("TASKRUNNER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DROP TABLE IF EXISTS job_runs; DROP TABLE IF EXISTS jobs;
CREATE TABLE job_runs (
	id           BIGSERIAL PRIMARY KEY,
	task_id      TEXT NOT NULL UNIQUE,
	request_id   TEXT NOT NULL DEFAULT '',
	action       TEXT NOT NULL,
	callback_url TEXT NOT NULL DEFAULT '',
	status       TEXT NOT NULL DEFAULT 'pending',
	attempts     INTEGER NOT NULL DEFAULT 0,
	error        TEXT NOT NULL DEFAULT '',
	duration_ms  BIGINT NOT NULL DEFAULT 0,
	submitted_by TEXT NOT NULL DEFAULT '',
	source_ip    TEXT NOT NULL DEFAULT '',
	started_at   TIMESTAMPTZ,
	finished_at  TIMESTAMPTZ,
	enqueued_at  TIMESTAMPTZ NOT NULL,
	queued INTEGER NOT NULL DEFAULT 1
);`); err != nil {
		raw.Close()
		t.Fatalf("create legacy pg table: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO job_runs (task_id, action, status, attempts, enqueued_at, queued) VALUES
		('legacy-cancel', 'a', 'canceled', 0, '2026-11-19 12:00:00', 0),
		('legacy-pending', 'a', 'pending', 0, '2026-11-19 12:00:00', 0),
		('legacy-running', 'a', 'running', 2, '2026-11-19 12:00:00', 1)`); err != nil {
		raw.Close()
		t.Fatalf("seed legacy rows: %v", err)
	}
	raw.Close()

	s, err := OpenPG(dsn)
	if err != nil {
		t.Fatalf("OpenPG on legacy db: %v", err)
	}
	defer s.Close()

	checkUpgradedRuns(t, s)
	reopened, err := OpenPG(dsn)
	if err != nil {
		t.Fatalf("reopen upgraded store: %v", err)
	}
	defer reopened.Close()
	checkUpgradedRuns(t, reopened)

	ctx := context.Background()
	if err := s.InsertPending(ctx, Run{
		TaskID: "pg-lg", Action: "a", JobID: "j", Dept: "audit",
		Params: `{"k":1}`, CallbackURL: "http://x", EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("insert with new columns: %v", err)
	}
	if r, err := s.GetByTaskID(ctx, "pg-lg"); err != nil || r.Dept != "audit" || r.JobID != "j" {
		t.Fatalf("pg migrated columns round-trip: %+v %v", r, err)
	}
}

// Concurrent due claims use the real PG row lock and one atomic outbox/advance
// transaction; a trigger proves rollback when the pending insert cannot commit.
func TestPGCronOccurrenceAtomicClaim(t *testing.T) {
	st := openTestStorePG(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	j := Job{JobID: "pg-occurrence", ActionID: "a", TriggerType: TriggerCron, CronSpec: "* * * * *", Params: "{}", Enabled: true, NextRun: sql.NullTime{Valid: true, Time: now.Add(-time.Minute)}, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
	if err := st.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	r := Run{TaskID: "cron:fixed-original-due", Action: "a", JobID: j.JobID, Params: "{}", EnqueuedAt: now, EnqueuePayload: "{}"}
	if _, err := st.db.Exec(`CREATE FUNCTION reject_occurrence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected pending failure'; END $$; CREATE TRIGGER reject_pending BEFORE INSERT ON job_runs FOR EACH ROW EXECUTE FUNCTION reject_occurrence()`); err != nil {
		t.Fatal(err)
	}
	defer st.db.Exec(`DROP TRIGGER IF EXISTS reject_pending ON job_runs; DROP FUNCTION IF EXISTS reject_occurrence()`)
	if ok, err := st.ClaimCronOccurrence(ctx, j, now.Add(time.Minute), now, r); err == nil || ok {
		t.Fatalf("pending failure must reject claim: %v %v", ok, err)
	}
	after, err := st.GetJob(ctx, j.JobID)
	if err != nil || !after.NextRun.Time.Equal(j.NextRun.Time) {
		t.Fatalf("next_run advanced without pending: %+v %v", after, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER reject_pending ON job_runs; DROP FUNCTION reject_occurrence()`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	type outcome struct {
		ok  bool
		err error
	}
	out := make(chan outcome, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := st.ClaimCronOccurrence(ctx, j, now.Add(time.Minute), now, r)
			out <- outcome{ok, err}
		}()
	}
	wg.Wait()
	close(out)
	winners := 0
	for result := range out {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("one due occurrence must have one owner: %d", winners)
	}
	if _, err := st.GetByTaskID(ctx, r.TaskID); err != nil {
		t.Fatal(err)
	}
	_, total, err := st.ListRuns(ctx, RunFilter{})
	if err != nil || total != 1 {
		t.Fatalf("pending outbox count: %d %v", total, err)
	}
}

// P2-9 独立调度面（PG 方言）：同一发生次身份被内容不符的存量占用 → 整笔回滚、
// 原 due 保留、零新增；同内容重认领 → 匹配同一发生次才推进、零重复投递。
func TestPGCronOccurrenceIdentityConflictRollsBack(t *testing.T) {
	st := openTestStorePG(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	j := Job{JobID: "pg-conflict", ActionID: "a", TriggerType: TriggerCron, CronSpec: "* * * * *", Params: "{}", Enabled: true, NextRun: sql.NullTime{Valid: true, Time: now.Add(-time.Minute)}, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
	if err := st.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	due := j.NextRun.Time
	// 存量发生次：同身份、旧参数快照（人工回拨/备份恢复后的「不明历史」）
	older := Run{TaskID: "cron:pg-identity", Action: "a", JobID: j.JobID, Params: `{"older":true}`,
		EnqueuedAt: now.Add(-time.Minute), EnqueuePayload: `{"task_id":"cron:pg-identity","action":"a","job_id":"pg-conflict","params":{"older":true}}`}
	if err := st.InsertPending(ctx, older); err != nil {
		t.Fatal(err)
	}

	current := Run{TaskID: "cron:pg-identity", Action: "a", JobID: j.JobID, Params: "{}", EnqueuedAt: now,
		EnqueuePayload: `{"task_id":"cron:pg-identity","action":"a","job_id":"pg-conflict","params":{}}`}
	if _, err := st.ClaimCronOccurrence(ctx, j, now.Add(time.Minute), now, current); err == nil {
		t.Fatal("identity conflict must reject the claim")
	}
	after, err := st.GetJob(ctx, j.JobID)
	if err != nil || !after.NextRun.Time.Equal(due) {
		t.Fatalf("conflicted slot must keep the original due: %+v %v", after, err)
	}
	_, total, err := st.ListRuns(ctx, RunFilter{})
	if err != nil || total != 1 {
		t.Fatalf("rollback must leave exactly the stored occurrence: %d %v", total, err)
	}
	row, err := st.GetByTaskID(ctx, older.TaskID)
	if err != nil || row.Params != `{"older":true}` || row.Status != StatusPending {
		t.Fatalf("existing occurrence snapshot must not be auto-overwritten: %+v %v", row, err)
	}

	// 同内容重认领：匹配同一发生次 → 推进 next_run、零重复投递记录
	same := Run{TaskID: "cron:pg-identity", Action: "a", JobID: j.JobID, Params: `{"older":true}`,
		EnqueuedAt: now, EnqueuePayload: older.EnqueuePayload}
	claimed, err := st.ClaimCronOccurrence(ctx, j, now.Add(time.Minute), now, same)
	if err != nil || claimed {
		t.Fatalf("already-owned occurrence must advance without a new delivery: %v %v", claimed, err)
	}
	advanced, err := st.GetJob(ctx, j.JobID)
	if err != nil || !advanced.NextRun.Time.After(due) {
		t.Fatalf("matching occurrence must advance the schedule: %+v %v", advanced, err)
	}
	_, total, err = st.ListRuns(ctx, RunFilter{})
	if err != nil || total != 1 {
		t.Fatalf("matching occurrence must not duplicate: %d %v", total, err)
	}
}

func TestPGManualJobSnapshotCommit(t *testing.T) {
	st := openTestStorePG(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	j := Job{JobID: "manual-freeze", ActionID: "a", TriggerType: TriggerManual, Params: "{}", Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := st.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	stale, err := st.GetJob(ctx, j.JobID)
	if err != nil {
		t.Fatal(err)
	}
	changed := *stale
	changed.Params = `{"v":2}`
	changed.UpdatedAt = now.Add(time.Second)
	if err := st.UpdateJob(ctx, changed); err != nil {
		t.Fatal(err)
	}
	r := Run{TaskID: "preallocated", Action: "a", JobID: j.JobID, Params: "{}", EnqueuedAt: now, EnqueuePayload: "{}"}
	if ok, err := st.ClaimJobRun(ctx, *stale, r); ok || !errors.Is(err, ErrJobSnapshotChanged) {
		t.Fatalf("stale snapshot did not reject: %v %v", ok, err)
	}
	current, err := st.GetJob(ctx, j.JobID)
	if err != nil {
		t.Fatal(err)
	}
	r.Params = current.Params
	if ok, err := st.ClaimJobRun(ctx, *current, r); !ok || err != nil {
		t.Fatalf("current snapshot did not freeze: %v %v", ok, err)
	}
	if ok, err := st.ClaimJobRun(ctx, *current, r); ok || err != nil {
		t.Fatalf("same task should not freeze twice: %v %v", ok, err)
	}
}
