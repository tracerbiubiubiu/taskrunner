package repository

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	enq := time.Now()

	// 入队 → pending
	if err := s.InsertPending(ctx, Run{
		TaskID: "t1", RequestID: "r1", Action: "audit_archive",
		CallbackURL: "http://zhuzhao.internal/internal/jobs/audit_archive",
		SubmittedBy: "10086", SourceIP: "10.0.0.1", EnqueuedAt: enq,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// task_id 唯一约束（幂等重提在 DB 侧的表现）
	if err := s.InsertPending(ctx, Run{TaskID: "t1", Action: "x", EnqueuedAt: enq}); err == nil {
		t.Fatal("duplicate task_id should fail")
	}

	// 取任务 → running（第 1 次尝试）
	got, err := s.GetByTaskID(ctx, "t1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusPending || got.SubmittedBy != "10086" || got.RequestID != "r1" {
		t.Fatalf("unexpected pending row: %+v", got)
	}
	if err := s.MarkRunning(ctx, "t1", 1, enq.Add(time.Second)); err != nil {
		t.Fatalf("mark running: %v", err)
	}

	// 成功 → succeeded，耗时按 started/finished 差值
	started := enq.Add(time.Second)
	fin := enq.Add(3 * time.Second)
	if err := s.Finish(ctx, "t1", StatusSucceeded, "", 1, started, fin); err != nil {
		t.Fatalf("finish: %v", err)
	}
	got, _ = s.GetByTaskID(ctx, "t1")
	if got.Status != StatusSucceeded || got.Attempts != 1 || got.Error != "" {
		t.Fatalf("unexpected succeeded row: %+v", got)
	}
	if got.DurationMS != 2000 { // started=enq+1s, finished=enq+3s
		t.Fatalf("unexpected duration: %dms", got.DurationMS)
	}
}

func TestRetryToDead(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	start := time.Now()
	s.InsertPending(ctx, Run{TaskID: "t2", Action: "a", EnqueuedAt: start})

	for attempt := 1; attempt <= 3; attempt++ {
		started := start.Add(time.Duration(attempt) * time.Second)
		s.MarkRunning(ctx, "t2", attempt, started)
		status := StatusFailed
		if attempt == 3 { // 最后一次：重试耗尽 → dead
			status = StatusDead
		}
		if err := s.Finish(ctx, "t2", status, "boom", attempt, started, started.Add(time.Second)); err != nil {
			t.Fatalf("finish attempt %d: %v", attempt, err)
		}
	}

	got, err := s.GetByTaskID(ctx, "t2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusDead || got.Attempts != 3 || got.Error != "boom" {
		t.Fatalf("unexpected dead row: %+v", got)
	}
	if !got.FinishedAt.Valid {
		t.Fatal("finished_at should be set")
	}
}

func TestNotFound(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.GetByTaskID(context.Background(), "nope"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestOpenMigratesOldSchema 旧库升级回归（P0-2）：CREATE TABLE IF NOT EXISTS 对旧库空转，
// Open 必须补齐 job_id/dept/params 三列，否则升级后 INSERT/SELECT 全量失败。
func TestOpenMigratesOldSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	oldSchema := `CREATE TABLE job_runs (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id       TEXT NOT NULL UNIQUE,
		request_id    TEXT NOT NULL DEFAULT '',
		action        TEXT NOT NULL,
		callback_url  TEXT NOT NULL DEFAULT '',
		status        TEXT NOT NULL DEFAULT 'pending',
		attempts      INTEGER NOT NULL DEFAULT 0,
		error         TEXT NOT NULL DEFAULT '',
		duration_ms   INTEGER NOT NULL DEFAULT 0,
		submitted_by  TEXT NOT NULL DEFAULT '',
		source_ip     TEXT NOT NULL DEFAULT '',
		enqueued_at   TIMESTAMP NOT NULL,
		started_at    TIMESTAMP,
		finished_at   TIMESTAMP
	);`
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	db.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open with migration: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.InsertPending(ctx, Run{
		TaskID: "upg-1", Action: "a", JobID: "j1", Dept: "d1", Params: `{"k":1}`,
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("insert after upgrade: %v", err)
	}
	run, err := st.GetByTaskID(ctx, "upg-1")
	if err != nil {
		t.Fatalf("get after upgrade: %v", err)
	}
	if run.JobID != "j1" || run.Dept != "d1" || run.Params != `{"k":1}` {
		t.Fatalf("upgraded row fields wrong: %+v", run)
	}
}

// 缺行可见性：入队成功但落库失败的残留任务，worker 侧 MarkRunning/Finish
// 必须报错（而非静默 0 行）——否则任务执行全程无 job_runs 记录且无日志。
func TestMissingRowVisible(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.MarkRunning(ctx, "ghost", 1, time.Now()); err == nil {
		t.Fatal("MarkRunning on missing row must error")
	}
	if err := s.Finish(ctx, "ghost", StatusFailed, "boom", 1, time.Now(), time.Now()); err == nil {
		t.Fatal("Finish on missing row must error")
	}
}
