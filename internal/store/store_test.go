package store

import (
	"context"
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
