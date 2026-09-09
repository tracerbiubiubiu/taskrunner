// Package store 维护 job_runs（taskrunner 自有存储，设计文档 §6；C7：SQLite / PostgreSQL 双驱动）。
// 查询统一 `?` 占位符编写，PG 路径执行前机械转换为 $n（两条驱动共用一套 SQL）。
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

	_ "github.com/jackc/pgx/v5/stdlib" // pgx stdlib 驱动注册（C7）
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

// 存储驱动（C7）。
const (
	DriverSQLite = "sqlite"
	DriverPG     = "pg"
)

type Store struct {
	db *sql.DB
	pg bool // true = PostgreSQL：执行前做 ?→$n 占位符转换
}

// Run job_runs 一行。
type Run struct {
	TaskID      string
	RequestID   string
	Action      string
	JobID       string // 经由哪个任务定义触发（一次性提交为空）
	Dept        string // 归属标签快照（C11：插入时从 Payload 定格；删 job/改 dept 不影响历史归属）
	Params      string // 执行入参快照（C11：JSON 原文；重试重放组装回调需要完整值）
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

// extraSchemas 其他文件（如 jobs.go）按驱动注册的建表语句。
var extraSchemas = map[string][]string{}

const schema = `
CREATE TABLE IF NOT EXISTS job_runs (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	task_id       TEXT NOT NULL UNIQUE,
	request_id    TEXT NOT NULL DEFAULT '',
	action        TEXT NOT NULL,
	job_id        TEXT NOT NULL DEFAULT '',
	dept          TEXT NOT NULL DEFAULT '',
	params        TEXT NOT NULL DEFAULT '{}',
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
`

// indexes 建表/补列之后执行——引用新列（job_id）的索引依赖列迁移先行。
const indexes = `
CREATE INDEX IF NOT EXISTS idx_job_runs_request_id ON job_runs(request_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_action_status ON job_runs(action, status);
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_enqueued_at ON job_runs(enqueued_at);
`

// Open 打开（或创建）SQLite 库并建表。WAL + busy_timeout 适配单容器单进程部署。
func Open(path string) (*Store, error) { return open(DriverSQLite, path) }

// OpenPG 打开 PostgreSQL 库并建表（C7：连接串如 postgres://user:pass@host:5432/dbname?sslmode=disable）。
// PG 为全新建库：schema 即最终形态，无 SQLite 旧库列迁移问题。
func OpenPG(dsn string) (*Store, error) { return open(DriverPG, dsn) }

func open(driver, target string) (*Store, error) {
	var db *sql.DB
	var err error
	switch driver {
	case DriverSQLite:
		if dir := filepath.Dir(target); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("store: create db dir: %w", err)
			}
		}
		// WAL + busy_timeout 适配单容器单进程部署
		db, err = sql.Open("sqlite", target+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1) // SQLite 单写者：串行写，读走 WAL
	case DriverPG:
		db, err = sql.Open("pgx", target)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(10)
	default:
		return nil, fmt.Errorf("store: unknown driver %q", driver)
	}

	for _, ddl := range []string{schemaFor(driver)} {
		if _, err := db.Exec(ddl); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: migrate: %w", err)
		}
	}
	for _, extra := range extraSchemas[driver] {
		if _, err := db.Exec(extra); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: migrate extra(%s): %w", driver, err)
		}
	}
	// 轻量列迁移仅 SQLite 存在（旧库升级）；PG 全新建库即最终形态
	if driver == DriverSQLite {
		if err := migrateColumns(db); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: migrate columns: %w", err)
		}
	}
	// 索引最后建：引用新列的索引（如 idx_job_runs_job_id）在旧库补列前会失败
	if _, err := db.Exec(indexes); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate indexes: %w", err)
	}
	return &Store{db: db, pg: driver == DriverPG}, nil
}

// schemaFor 按驱动返回建表 DDL（列集一致，仅自增/类型方言差异）。
func schemaFor(driver string) string {
	if driver == DriverPG {
		return `
CREATE TABLE IF NOT EXISTS job_runs (
	id            BIGSERIAL PRIMARY KEY,
	task_id       TEXT NOT NULL UNIQUE,
	request_id    TEXT NOT NULL DEFAULT '',
	action        TEXT NOT NULL,
	job_id        TEXT NOT NULL DEFAULT '',
	dept          TEXT NOT NULL DEFAULT '',
	params        TEXT NOT NULL DEFAULT '{}',
	callback_url  TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL DEFAULT 'pending',
	attempts      INTEGER NOT NULL DEFAULT 0,
	error         TEXT NOT NULL DEFAULT '',
	duration_ms   BIGINT NOT NULL DEFAULT 0,
	submitted_by  TEXT NOT NULL DEFAULT '',
	source_ip     TEXT NOT NULL DEFAULT '',
	enqueued_at   TIMESTAMPTZ NOT NULL,
	started_at    TIMESTAMPTZ,
	finished_at   TIMESTAMPTZ
);
`
	}
	return schema
}

// pgq PG 路径把 `?` 占位符顺序转换为 $1..$n（SQL 中无字面量 `?`，机械替换安全；
// SQLite 原生支持 `?`，原样返回）。单查询内含字符串字面量 `?` 时不可用——当前无此场景。
func (s *Store) pgq(query string) string {
	if !s.pg {
		return query
	}
	var b strings.Builder
	n := 0
	for _, c := range query {
		if c == '?' {
			n++
			b.WriteString(fmt.Sprintf("$%d", n))
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

func (s *Store) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.pgq(q), args...)
}

func (s *Store) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.pgq(q), args...)
}

func (s *Store) queryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.pgq(q), args...)
}

// migrateColumns 按列判存补齐旧库缺失列（幂等：新库全部命中跳过）。
// 依赖 SQLite ALTER TABLE ADD COLUMN 不带默认值时对 NOT NULL 约束的限制，
// 故新列均先加可空再回填默认值——本表默认均为 ”/'{}' 文本，直接带 DEFAULT 即可。
func migrateColumns(db *sql.DB) error {
	type colDef struct {
		table string
		col   string
		ddl   string
	}
	for _, c := range []colDef{
		{"job_runs", "job_id", "ALTER TABLE job_runs ADD COLUMN job_id TEXT NOT NULL DEFAULT ''"},
		{"job_runs", "dept", "ALTER TABLE job_runs ADD COLUMN dept TEXT NOT NULL DEFAULT ''"},
		{"job_runs", "params", "ALTER TABLE job_runs ADD COLUMN params TEXT NOT NULL DEFAULT '{}'"},
	} {
		rows, err := db.Query("SELECT 1 FROM pragma_table_info(?) WHERE name = ?", c.table, c.col)
		if err != nil {
			return err
		}
		has := rows.Next()
		rows.Close()
		if has {
			continue
		}
		if _, err := db.Exec(c.ddl); err != nil {
			return fmt.Errorf("add column %s.%s: %w", c.table, c.col, err)
		}
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

// Ping 依赖就绪探针（C4 readyz：SQLite 可查询）。
func (s *Store) Ping() error {
	var one int
	return s.db.QueryRowContext(context.Background(), `SELECT 1`).Scan(&one)
}

// InsertPending 入队时写入 pending 行；task_id 重复（幂等重提）返回已存在错误。
func (s *Store) InsertPending(ctx context.Context, r Run) error {
	_, err := s.exec(ctx, `
INSERT INTO job_runs (task_id, request_id, action, job_id, dept, params, callback_url, status, submitted_by, source_ip, enqueued_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.TaskID, r.RequestID, r.Action, r.JobID, r.Dept, r.Params, r.CallbackURL, StatusPending, r.SubmittedBy, r.SourceIP, r.EnqueuedAt)
	if err != nil {
		return fmt.Errorf("store: insert job_run: %w", err)
	}
	return nil
}

func (s *Store) GetByTaskID(ctx context.Context, taskID string) (*Run, error) {
	row := s.queryRow(ctx, `
SELECT task_id, request_id, action, job_id, dept, params, callback_url, status, attempts, error, duration_ms,
       submitted_by, source_ip, enqueued_at, started_at, finished_at
FROM job_runs WHERE task_id = ?`, taskID)
	var r Run
	err := row.Scan(&r.TaskID, &r.RequestID, &r.Action, &r.JobID, &r.Dept, &r.Params, &r.CallbackURL, &r.Status, &r.Attempts,
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
// 条件更新（WHERE status IN failed/dead）：RunTask 之后 worker 可能已完成并写入终态，
// 0 行 = 状态已被并发改变 → 返回 false，调用方按冲突处理（防止把 succeeded 覆盖回 pending）。
func (s *Store) ResetPendingIfTerminal(ctx context.Context, taskID string) (bool, error) {
	res, err := s.exec(ctx, `
UPDATE job_runs SET status = ?, error = '', started_at = NULL, finished_at = NULL
WHERE task_id = ? AND status IN (?, ?)`,
		StatusPending, taskID, StatusFailed, StatusDead)
	if err != nil {
		return false, fmt.Errorf("store: reset pending: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: reset pending rows: %w", err)
	}
	return n > 0, nil
}

// MarkCanceledIfPending 取消未开始的任务。条件更新（WHERE status=pending）：
// 影响行数为 0 说明检查后状态已被并发改变（如 worker 刚取走），返回 false 由调用方按冲突处理。
func (s *Store) MarkCanceledIfPending(ctx context.Context, taskID, errMsg string, finishedAt time.Time) (bool, error) {
	res, err := s.exec(ctx, `
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
// C11：按归属标签（dept 快照列）多值过滤，行内返回 dept（zhuzhao E-⑤ 可见性）——纯表查询不 JOIN jobs。
func (s *Store) ListRuns(ctx context.Context, f RunFilter) ([]*Run, int64, error) {
	where, args := " WHERE 1=1", []any{}
	if f.RequestID != "" {
		where, args = where+" AND request_id = ?", append(args, f.RequestID)
	}
	if f.Action != "" {
		where, args = where+" AND action = ?", append(args, f.Action)
	}
	if f.Status != "" {
		where, args = where+" AND status = ?", append(args, f.Status)
	}
	if f.JobID != "" {
		where, args = where+" AND job_id = ?", append(args, f.JobID)
	}
	if len(f.Depts) > 0 {
		ph := strings.Repeat("?,", len(f.Depts))
		where, args = where+" AND dept IN ("+ph[:len(ph)-1]+")", append(args, toAny(f.Depts)...)
	}
	if f.From != nil {
		where, args = where+" AND enqueued_at >= ?", append(args, *f.From)
	}
	if f.To != nil {
		where, args = where+" AND enqueued_at <= ?", append(args, *f.To)
	}

	var total int64
	if err := s.queryRow(ctx, `SELECT COUNT(*) FROM job_runs`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("store: count runs: %w", err)
	}

	page, size := f.Page, f.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 20
	}
	q := `SELECT task_id, request_id, action, job_id, dept, params, callback_url, status, attempts, error, duration_ms,
       submitted_by, source_ip, enqueued_at, started_at, finished_at FROM job_runs` + where +
		` ORDER BY enqueued_at DESC LIMIT ? OFFSET ?`
	rows, err := s.query(ctx, q, append(args, size, (page-1)*size)...)
	if err != nil {
		return nil, 0, fmt.Errorf("store: list runs: %w", err)
	}
	defer rows.Close()

	var out []*Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.TaskID, &r.RequestID, &r.Action, &r.JobID, &r.Dept, &r.Params, &r.CallbackURL, &r.Status,
			&r.Attempts, &r.Error, &r.DurationMS, &r.SubmittedBy, &r.SourceIP,
			&r.EnqueuedAt, &r.StartedAt, &r.FinishedAt); err != nil {
			return nil, 0, fmt.Errorf("store: scan run: %w", err)
		}
		out = append(out, &r)
	}
	return out, total, rows.Err()
}

// MarkRunning worker 取到任务时置 running 并记本次尝试序号与开始时间。
// 行不存在（入队成功但落库失败的残留）→ 返回错误——worker 侧据此记日志，
// 避免任务执行全程无任何 job_runs 记录的「隐形任务」静默发生。
func (s *Store) MarkRunning(ctx context.Context, taskID string, attempt int, startedAt time.Time) error {
	res, err := s.exec(ctx, `
UPDATE job_runs SET status = ?, attempts = ?, started_at = ?, error = '' WHERE task_id = ?`,
		StatusRunning, attempt, startedAt, taskID)
	if err != nil {
		return fmt.Errorf("store: mark running: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark running rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark running: job_runs row missing (task_id=%s)", taskID)
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
	res, err := s.exec(ctx, `
UPDATE job_runs
SET status = ?, attempts = ?, error = ?, finished_at = ?, duration_ms = ?
WHERE task_id = ?`,
		status, attempts, errMsg, finishedAt, durationMS, taskID)
	if err != nil {
		return fmt.Errorf("store: finish job_run: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: finish job_run rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("store: finish job_run: job_runs row missing (task_id=%s)", taskID)
	}
	return nil
}
