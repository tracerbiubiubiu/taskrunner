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

	// 分页 + dept 多值过滤
	runs, total, err := s.ListRuns(ctx, RunFilter{Depts: []string{"audit"}, Status: StatusSucceeded, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if total != 1 || len(runs) != 1 || runs[0].TaskID != "pg-1" {
		t.Fatalf("filter result: total=%d len=%d", total, len(runs))
	}
}
