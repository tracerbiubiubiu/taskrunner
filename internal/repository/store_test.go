package repository

import (
	"context"
	"database/sql"
	"errors"
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

// ---- ADR-003：queued 标志 / 三域扫描域 / MarkRunning 分类 ----

func seedRun(t *testing.T, s *Store, id string, enqueuedAt time.Time, payload string) {
	t.Helper()
	if err := s.InsertPending(t.Context(), Run{
		TaskID: id, Action: "a", CallbackURL: "http://x",
		EnqueuedAt: enqueuedAt, EnqueuePayload: payload,
	}); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

func TestQueuedFlagLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// 新库直接含新列（F29）：InsertPending 写 queued=0 + 快照
	seedRun(t, s, "q1", time.Now(), `{"task_id":"q1"}`)
	q, err := s.GetQueued(ctx, "q1")
	if err != nil || q {
		t.Fatalf("fresh row must be queued=0, got %v %v", q, err)
	}
	// MarkQueued：pending → 1 行
	if n, err := s.MarkQueued(ctx, "q1"); err != nil || n != 1 {
		t.Fatalf("mark queued: n=%d err=%v", n, err)
	}
	if q, _ := s.GetQueued(ctx, "q1"); !q {
		t.Fatal("queued should be 1 after MarkQueued")
	}

	// MarkQueued 条件标记（F19 回归）：canceled 行 → 0 行、queued 不被翻动
	seedRun(t, s, "q2", time.Now(), "")
	if ok, err := s.MarkCanceledIfPending(ctx, "q2", "canceled via API", time.Now()); !ok || err != nil {
		t.Fatalf("cancel: %v %v", ok, err)
	}
	if n, err := s.MarkQueued(ctx, "q2"); err != nil || n != 0 {
		t.Fatalf("mark queued on canceled must be 0 rows: n=%d err=%v", n, err)
	}
	if q, _ := s.GetQueued(ctx, "q2"); q {
		t.Fatal("canceled row must keep queued=0（F37：取消不翻标志）")
	}

	// MarkCleanupDone（F48 条件回归）：pending 行 → 0 行；canceled+queued=0 → 1 行；重复 → 0 行
	seedRun(t, s, "q3", time.Now(), "")
	if n, err := s.MarkCleanupDone(ctx, "q3"); err != nil || n != 0 {
		t.Fatalf("cleanup on pending must be 0 rows: n=%d err=%v", n, err)
	}
	if ok, err := s.MarkCanceledIfPending(ctx, "q3", "x", time.Now()); !ok {
		t.Fatalf("cancel q3: %v %v", ok, err)
	}
	if n, err := s.MarkCleanupDone(ctx, "q3"); err != nil || n != 1 {
		t.Fatalf("cleanup on canceled+queued=0: n=%d err=%v", n, err)
	}
	if n, _ := s.MarkCleanupDone(ctx, "q3"); n != 0 {
		t.Fatalf("second cleanup must be 0 rows, got %d", n)
	}
}

// 第一轮扫描域安全性（F1/F14）：canceled 行与 succeeded+queued=0 残留都不得入选。
func TestListUnqueuedSafetyDomain(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)

	seedRun(t, s, "u-ok", base, `{"k":1}`) // pending+queued=0 → 入选
	seedRun(t, s, "u-cancel", base, "")    // canceled+queued=0 → 排除（F1）
	if ok, _ := s.MarkCanceledIfPending(ctx, "u-cancel", "x", time.Now()); !ok {
		t.Fatal("cancel u-cancel")
	}
	// succeeded+queued=0 残留（F14 场景：内联标记失败 + DB 抖动在执行前恢复）
	seedRun(t, s, "u-done", base, "")
	if err := s.MarkRunning(ctx, "u-done", 1, base); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.Finish(ctx, "u-done", StatusSucceeded, "", 1, base, base); err != nil {
		t.Fatalf("finish: %v", err)
	}
	// pending+queued=1 → 排除（属于第二轮域）
	seedRun(t, s, "u-stuck", base, "")
	if _, err := s.MarkQueued(ctx, "u-stuck"); err != nil {
		t.Fatalf("mark queued: %v", err)
	}

	rows, err := s.ListUnqueued(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 || rows[0].TaskID != "u-ok" {
		t.Fatalf("safety domain broken: %+v", rows)
	}
	if rows[0].EnqueuePayload != `{"k":1}` {
		t.Fatalf("payload round-trip: %q", rows[0].EnqueuePayload)
	}

	// LIMIT 批位上界（F66）
	seedRun(t, s, "u-ok2", base, "")
	rows, _ = s.ListUnqueued(ctx, time.Now(), 1)
	if len(rows) != 1 {
		t.Fatalf("limit: %+v", rows)
	}
}

// 第二/三域游标轮转（F36/F46/F56）：id 游标推进、耗尽、回卷。
func TestListCursorRotation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	ids := []string{"c1", "c2", "c3"}
	for i, id := range ids {
		seedRun(t, s, id, base.Add(time.Duration(i)*time.Minute), "")
		s.MarkQueued(ctx, id) // pending+queued=1（第二轮域）
	}
	before := time.Now()

	rows, err := s.ListStuckPending(ctx, before, 0, 2)
	if err != nil || len(rows) != 2 || rows[0].TaskID != "c1" || rows[1].TaskID != "c2" {
		t.Fatalf("batch1: %+v err=%v", rows, err)
	}
	rows2, err := s.ListStuckPending(ctx, before, rows[len(rows)-1].ID, 2)
	if err != nil || len(rows2) != 1 || rows2[0].TaskID != "c3" {
		t.Fatalf("batch2: %+v err=%v", rows2, err)
	}
	rows3, _ := s.ListStuckPending(ctx, before, rows2[len(rows2)-1].ID, 2)
	if len(rows3) != 0 {
		t.Fatalf("exhausted: %+v", rows3)
	}
	rows4, _ := s.ListStuckPending(ctx, before, 0, 2)
	if len(rows4) != 2 || rows4[0].TaskID != "c1" {
		t.Fatalf("wrap around: %+v", rows4)
	}

	// 第三域同款（先出域的行不再入选）
	for i, id := range []string{"x1", "x2"} {
		seedRun(t, s, id, base.Add(time.Duration(i)*time.Minute), "")
		if ok, err := s.MarkCanceledIfPending(ctx, id, "x", time.Now()); !ok {
			t.Fatalf("cancel %s: %v %v", id, ok, err)
		}
	}
	rows, err = s.ListCanceledUnqueued(ctx, before, 0, 1)
	if err != nil || len(rows) != 1 || rows[0].TaskID != "x1" {
		t.Fatalf("cancel batch1: %+v err=%v", rows, err)
	}
	if _, err := s.MarkCleanupDone(ctx, "x1"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	rows, _ = s.ListCanceledUnqueued(ctx, before, 0, 1)
	if len(rows) != 1 || rows[0].TaskID != "x2" {
		t.Fatalf("cleanup 出域后应离开扫描域: %+v", rows)
	}
}

// MarkRunning 分类（F59）：canceled/succeeded 复活拒绝 → ErrTerminalRejected；
// 缺行 → ErrNotFound；worker 据此分流（守卫拦截 vs 旧口径继续）。
func TestMarkRunningClassification(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now()

	seedRun(t, s, "m-cancel", now, "")
	if ok, _ := s.MarkCanceledIfPending(ctx, "m-cancel", "x", now); !ok {
		t.Fatal("cancel")
	}
	if err := s.MarkRunning(ctx, "m-cancel", 1, now); !errors.Is(err, ErrTerminalRejected) {
		t.Fatalf("canceled want ErrTerminalRejected, got %v", err)
	}

	seedRun(t, s, "m-done", now, "")
	if err := s.MarkRunning(ctx, "m-done", 1, now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.Finish(ctx, "m-done", StatusSucceeded, "", 1, now, now); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if err := s.MarkRunning(ctx, "m-done", 2, now); !errors.Is(err, ErrTerminalRejected) {
		t.Fatalf("succeeded want ErrTerminalRejected, got %v", err)
	}

	err := s.MarkRunning(ctx, "m-ghost", 1, now)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing want ErrNotFound, got %v", err)
	}
	if errors.Is(err, ErrTerminalRejected) {
		t.Fatal("missing must not be classified as terminal rejection")
	}
}

// pgq 占位符转换钉住测试（C7）：? → $n 顺序编号；非 pg 驱动原样返回。
// 已知限制（文档化约定）：SQL 字面量中的 '?' 同样被替换——SQL 字符串常量禁用 '?'。
func TestPGQueryPlaceholderConversion(t *testing.T) {
	s := &Store{pg: true}
	if got := s.pgq("INSERT INTO t (a, b) VALUES (?, ?)"); got != "INSERT INTO t (a, b) VALUES ($1, $2)" {
		t.Fatalf("got %q", got)
	}
	if got := s.pgq("SELECT * FROM t WHERE a = ? AND b IN (?, ?)"); got != "SELECT * FROM t WHERE a = $1 AND b IN ($2, $3)" {
		t.Fatalf("got %q", got)
	}
	if got := (&Store{}).pgq("SELECT ?"); got != "SELECT ?" {
		t.Fatalf("非 pg 驱动应原样返回: %q", got)
	}
	if got := s.pgq("SELECT 'a?b'"); got != "SELECT 'a$1b'" {
		t.Fatalf("got %q", got)
	}
}

// 旧库列迁移（SQLite 路径）：模拟 C11 之前的旧库（无 job_id/dept/params 列），
// Open 后自动补列且读写含新列的完整行。
func TestLegacySQLiteColumnMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `
CREATE TABLE job_runs (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id      TEXT NOT NULL UNIQUE,
	request_id   TEXT NOT NULL DEFAULT '',
	action       TEXT NOT NULL,
	callback_url TEXT NOT NULL DEFAULT '',
	status       TEXT NOT NULL DEFAULT 'pending',
	attempts     INTEGER NOT NULL DEFAULT 0,
	error        TEXT NOT NULL DEFAULT '',
	duration_ms  INTEGER NOT NULL DEFAULT 0,
	submitted_by TEXT NOT NULL DEFAULT '',
	source_ip    TEXT NOT NULL DEFAULT '',
	started_at   TIMESTAMP,
	finished_at  TIMESTAMP,
	enqueued_at  TIMESTAMP NOT NULL
);`
	if _, err := raw.Exec(legacy); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	raw.Close()

	s, err := Open(path) // Open 应补齐 job_id/dept/params 三列
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.InsertPending(ctx, Run{
		TaskID: "lg-1", RequestID: "lr", Action: "a", JobID: "j1", Dept: "audit",
		Params: `{"k":1}`, CallbackURL: "http://x", EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("insert with new columns: %v", err)
	}
	r, err := s.GetByTaskID(ctx, "lg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if r.JobID != "j1" || r.Dept != "audit" || r.Params != `{"k":1}` {
		t.Fatalf("migrated columns round-trip: %+v", r)
	}
}
