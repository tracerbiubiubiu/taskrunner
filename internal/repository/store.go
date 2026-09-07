// Package store 维护 job_runs（taskrunner 自有 SQLite，设计文档 §6）。
// 一行 = 一个任务的完整生命周期：入队 pending → running → succeeded / failed（可重试）/ dead（重试耗尽）。
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	StatusCanceled  = "canceled" // API 取消（仅未开始的任务）
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
	JobID       string // 经由哪个任务定义触发（一次性提交为空）
	Dept        string // 归属标签（冗余自 jobs.dept，C11 可见性过滤/展示）
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

var extraSchemas []string // 其他文件（如 jobs.go）注册自己的建表语句

const schema = `
CREATE TABLE IF NOT EXISTS job_runs (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id       TEXT NOT NULL UNIQUE,
	request_id    TEXT NOT NULL DEFAULT '',
	action        TEXT NOT NULL,
	job_id        TEXT NOT NULL DEFAULT '',
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
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
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
	for _, extra := range extraSchemas {
		if _, err := db.Exec(extra); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: migrate extra: %w", err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Ping 依赖就绪探针（C4 readyz：SQLite 可查询）。
func (s *Store) Ping() error {
	var one int
	return s.db.QueryRowContext(context.Background(), `SELECT 1`).Scan(&one)
}

// InsertPending 入队时写入 pending 行；task_id 重复（幂等重提）返回已存在错误。
func (s *Store) InsertPending(ctx context.Context, r Run) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO job_runs (task_id, request_id, action, job_id, callback_url, status, submitted_by, source_ip, enqueued_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.TaskID, r.RequestID, r.Action, r.JobID, r.CallbackURL, StatusPending, r.SubmittedBy, r.SourceIP, r.EnqueuedAt)
	if err != nil {
		return fmt.Errorf("store: insert job_run: %w", err)
	}
	return nil
}

func (s *Store) GetByTaskID(ctx context.Context, taskID string) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT r.task_id, r.request_id, r.action, r.job_id, COALESCE(j.dept, '') AS dept, r.callback_url, r.status, r.attempts, r.error, r.duration_ms,
       r.submitted_by, r.source_ip, r.enqueued_at, r.started_at, r.finished_at
FROM job_runs r LEFT JOIN jobs j ON j.job_id = r.job_id WHERE r.task_id = ?`, taskID)
	var r Run
	err := row.Scan(&r.TaskID, &r.RequestID, &r.Action, &r.JobID, &r.Dept, &r.CallbackURL, &r.Status, &r.Attempts,
		&r.Error, &r.DurationMS, &r.SubmittedBy, &r.SourceIP, &r.EnqueuedAt, &r.StartedAt, &r.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job_run: %w", err)
	}
	return &r, nil
}

// ResetPending 死信/终败任务经 API 重试时，回到 pending（Asynq RunTask 同步重置其重试计数）。
func (s *Store) ResetPending(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE job_runs SET status = ?, error = '', started_at = NULL, finished_at = NULL WHERE task_id = ?`,
		StatusPending, taskID)
	if err != nil {
		return fmt.Errorf("store: reset pending: %w", err)
	}
	return nil
}

// MarkCanceledIfPending 取消未开始的任务。条件更新（WHERE status=pending）：
// 影响行数为 0 说明检查后状态已被并发改变（如 worker 刚取走），返回 false 由调用方按冲突处理。
func (s *Store) MarkCanceledIfPending(ctx context.Context, taskID, errMsg string, finishedAt time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE job_runs SET status = ?, error = ?, finished_at = ? WHERE task_id = ? AND status = ?`,
		StatusCanceled, errMsg, finishedAt, taskID, StatusPending)
	if err != nil {
		return false, fmt.Errorf("store: mark canceled: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: mark canceled rows: %w", err)
	}
	return n > 0, nil
}

// RunFilter 执行记录查询条件（§5 GET /v1/runs，全部可选）。
type RunFilter struct {
	RequestID string
	Action    string
	Status    string
	JobID     string
	Depts     []string
	From      *time.Time
	To        *time.Time
	Page      int // 1 起
	PageSize  int
}

// toAny []string → []any（SQL 参数）。
func toAny(ss []string) []any {
	out := make([]any, 0, len(ss))
	for _, v := range ss {
		out = append(out, v)
	}
	return out
}

// ListRuns 按条件分页查询执行记录，返回行列表与总数。
// C11：JOIN jobs——支持按归属标签（dept）多值过滤，并在行内返回 dept（zhuzhao E-⑤ 可见性）。
func (s *Store) ListRuns(ctx context.Context, f RunFilter) ([]*Run, int64, error) {
	where, args := " WHERE 1=1", []any{}
	if f.RequestID != "" {
		where, args = where+" AND r.request_id = ?", append(args, f.RequestID)
	}
	if f.Action != "" {
		where, args = where+" AND r.action = ?", append(args, f.Action)
	}
	if f.Status != "" {
		where, args = where+" AND r.status = ?", append(args, f.Status)
	}
	if f.JobID != "" {
		where, args = where+" AND r.job_id = ?", append(args, f.JobID)
	}
	if len(f.Depts) > 0 {
		ph := strings.Repeat("?,", len(f.Depts))
		where, args = where+" AND j.dept IN ("+ph[:len(ph)-1]+")", append(args, toAny(f.Depts)...)
	}
	if f.From != nil {
		where, args = where+" AND r.enqueued_at >= ?", append(args, *f.From)
	}
	if f.To != nil {
		where, args = where+" AND r.enqueued_at <= ?", append(args, *f.To)
	}

	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_runs r LEFT JOIN jobs j ON j.job_id = r.job_id`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("store: count runs: %w", err)
	}

	page, size := f.Page, f.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 20
	}
	q := `SELECT r.task_id, r.request_id, r.action, r.job_id, COALESCE(j.dept, '') AS dept, r.callback_url, r.status, r.attempts, r.error, r.duration_ms,
       r.submitted_by, r.source_ip, r.enqueued_at, r.started_at, r.finished_at FROM job_runs r LEFT JOIN jobs j ON j.job_id = r.job_id` + where +
		` ORDER BY r.enqueued_at DESC LIMIT ? OFFSET ?`
	rows, err := s.db.QueryContext(ctx, q, append(args, size, (page-1)*size)...)
	if err != nil {
		return nil, 0, fmt.Errorf("store: list runs: %w", err)
	}
	defer rows.Close()

	var out []*Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.TaskID, &r.RequestID, &r.Action, &r.JobID, &r.Dept, &r.CallbackURL, &r.Status,
			&r.Attempts, &r.Error, &r.DurationMS, &r.SubmittedBy, &r.SourceIP,
			&r.EnqueuedAt, &r.StartedAt, &r.FinishedAt); err != nil {
			return nil, 0, fmt.Errorf("store: scan run: %w", err)
		}
		out = append(out, &r)
	}
	return out, total, rows.Err()
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
