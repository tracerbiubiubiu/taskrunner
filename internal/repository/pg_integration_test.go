// PG 集成测试（C7）：设置 TASKRUNNER_TEST_PG_DSN 才运行（如
// postgres://zhuzhao:***@127.0.0.1:5432/taskrunner_test?sslmode=disable）。
// 覆盖方言差异面：BIGSERIAL/BOOLEAN/TIMESTAMPTZ 建表、$n 占位符转换、
// 布尔字面量（WHERE enabled）、Timestamptz 扫描、条件更新、分页过滤。
package repository

import (
	"context"
	"database/sql"
	"os"
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
	if err := s.MarkRunning(ctx, "pg-1", 1, now.Add(time.Second)); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.Finish(ctx, "pg-1", StatusSucceeded, "", 1, now.Add(time.Second), now.Add(3*time.Second)); err != nil {
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
	ok, err := s.MarkCanceledIfPending(ctx, "pg-1", "x", now)
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
	if err := s.UpdateJobNextRun(ctx, "j-pg", now.Add(24*time.Hour)); err != nil {
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
	must(s.MarkRunning(ctx, id, 1, now), "mark running 1")
	must(s.MarkRunning(ctx, id, 2, now), "mark running 2（模拟重投递的新执行推进 attempts）")

	// 过期写者（attempt=1）：等值谓词拒绝
	if err := s.Finish(ctx, id, StatusSucceeded, "stale", 1, now, now); err == nil {
		t.Fatal("过期 attempt 的 Finish 应被拒绝")
	}
	// 新执行终态（attempt=2）正常落库
	must(s.Finish(ctx, id, StatusSucceeded, "", 2, now, now), "finish attempt 2")
	// 终态后再次 Finish 亦被拒（status 已非 running）
	if err := s.Finish(ctx, id, StatusFailed, "late", 2, now, now); err == nil {
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
	must(s.MarkRunning(ctx, id, 1, now), "mark running")
	must(s.Finish(ctx, id, StatusSucceeded, "", 1, now, now), "finish succeeded")

	// succeeded 行不可被 MarkRunning 复活（failed→running 为重试周期正常路径，
	// 由 asynq 重投递驱动；手动 RetryTask 走 ResetPendingIfTerminal 先归 pending）
	if err := s.MarkRunning(ctx, id, 2, now); err == nil {
		t.Fatal("succeeded 行被 MarkRunning 复活——C6 防线失效")
	}
	got, err := s.GetByTaskID(ctx, id)
	must(err, "get")
	if got.Status != StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("终态应保持不变: %+v", got)
	}
}
