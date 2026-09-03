// Package store 维护 job_runs（taskrunner 自有 SQLite，设计文档 §6）。
// 一行 = 一个任务的完整生命周期：入队 pending → running → succeeded / failed（可重试）/ dead（重试耗尽）。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// 任务状态（设计文档 §6 最小 schema）。
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusDead      = "dead"
)

var ErrNotFound = errors.New("store: job_run not found")

type Store struct {
	db *sql.DB
}

// Run job_runs 一行。
type Run struct {
	TaskID      string
	RequestID   string
	Action      string
	CallbackURL string
	Status      string
	Attempts    int
	Error       string
	DurationMS  int64
	SubmittedBy string
	SourceIP    string
	EnqueuedAt  time.Time
	StartedAt   sql.NullTime
	FinishedAt  sql.NullTime
}

const schema = `
CREATE TABLE IF NOT EXISTS job_runs (
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
);
CREATE INDEX IF NOT EXISTS idx_job_runs_request_id ON job_runs(request_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_action_status ON job_runs(action, status);
CREATE INDEX IF NOT EXISTS idx_job_runs_enqueued_at ON job_runs(enqueued_at);
`

// Open 打开（或创建）SQLite 库并建表。WAL + busy_timeout 适配单容器单进程部署。
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: create db dir: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite 单写者：串行写，读走 WAL
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// InsertPending 入队时写入 pending 行；task_id 重复（幂等重提）返回已存在错误。
func (s *Store) InsertPending(ctx context.Context, r Run) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO job_runs (task_id, request_id, action, callback_url, status, submitted_by, source_ip, enqueued_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.TaskID, r.RequestID, r.Action, r.CallbackURL, StatusPending, r.SubmittedBy, r.SourceIP, r.EnqueuedAt)
	if err != nil {
		return fmt.Errorf("store: insert job_run: %w", err)
	}
	return nil
}

func (s *Store) GetByTaskID(ctx context.Context, taskID string) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT task_id, request_id, action, callback_url, status, attempts, error, duration_ms,
       submitted_by, source_ip, enqueued_at, started_at, finished_at
FROM job_runs WHERE task_id = ?`, taskID)
	var r Run
	err := row.Scan(&r.TaskID, &r.RequestID, &r.Action, &r.CallbackURL, &r.Status, &r.Attempts,
		&r.Error, &r.DurationMS, &r.SubmittedBy, &r.SourceIP, &r.EnqueuedAt, &r.StartedAt, &r.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job_run: %w", err)
	}
	return &r, nil
}

// MarkRunning worker 取到任务时置 running 并记本次尝试序号与开始时间。
func (s *Store) MarkRunning(ctx context.Context, taskID string, attempt int, startedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE job_runs SET status = ?, attempts = ?, started_at = ?, error = '' WHERE task_id = ?`,
		StatusRunning, attempt, startedAt, taskID)
	if err != nil {
		return fmt.Errorf("store: mark running: %w", err)
	}
	return nil
}

// Finish 写终态：succeeded / failed（还将重试）/ dead（重试耗尽或 4xx 不重试）。
// duration 按本次尝试的 started→finished 计算（Go 侧算好传入，避免 SQLite 时间函数对绑定参数的解析差异）。
func (s *Store) Finish(ctx context.Context, taskID, status, errMsg string, attempts int, startedAt, finishedAt time.Time) error {
	durationMS := finishedAt.Sub(startedAt).Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE job_runs
SET status = ?, attempts = ?, error = ?, finished_at = ?, duration_ms = ?
WHERE task_id = ?`,
		status, attempts, errMsg, finishedAt, durationMS, taskID)
	if err != nil {
		return fmt.Errorf("store: finish job_run: %w", err)
	}
	return nil
}
