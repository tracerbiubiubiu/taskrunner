// Package repository 维护 job_runs / jobs（taskrunner 自有存储，设计文档 §6；C7：SQLite / PostgreSQL 双驱动）。
// 包名 repository 为 2026-09-04 微服务结构重构所改（原 store），对齐 handler/service/repository 分层。
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

// ErrTerminalRejected MarkRunning 复活拒绝（ADR-003 决策 7 终态守卫）：行已 succeeded/canceled
// ——重复投递或已取消任务被取走，消费端必须拦截副作用（不回调 + SkipRetry）。
var ErrTerminalRejected = errors.New("store: terminal row rejects resurrection")

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
	ID          int64 // 行主键（自增）——仅由扫描器列表方法填充，游标推进用（ADR-003 F36/F46/F56）
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
	Queued      bool // 投递状态标志（ADR-003，两义：已交付 Redis / 已了结）——仅由新方法填充，既有查询下恒为零值、不可作判据（F29）
	// EnqueuePayload 提交载荷快照（task.Payload JSON，ADR-003 决策 2）——reclaim 重投唯一来源，
	// 自带 timeout_secs 规避「job_runs 无超时覆盖」的重建缺口；与 asynq 载荷同源（F9）。
	EnqueuePayload string
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
	finished_at   TIMESTAMP,
	queued        INTEGER NOT NULL DEFAULT 1,
	enqueue_payload TEXT NOT NULL DEFAULT ''
);
`

// indexes 建表/补列之后执行——引用新列（job_id/queued）的索引依赖列迁移先行。
// 三个 partial index（ADR-003 实施计划 1）谓词与三域判据逐字对齐，与既有
// idx_job_runs_enqueued_at 正交、可并存；索引列 = id（游标键，F36/F46/F56——
// 时间列绑定含驱动内部格式后缀，等值/边界比较不可靠，游标必须走整数主键，见实现记录）。
const indexes = `
CREATE INDEX IF NOT EXISTS idx_job_runs_request_id ON job_runs(request_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_action_status ON job_runs(action, status);
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_enqueued_at ON job_runs(enqueued_at);
CREATE INDEX IF NOT EXISTS idx_job_runs_unqueued ON job_runs(id) WHERE queued = 0 AND status = 'pending';
CREATE INDEX IF NOT EXISTS idx_job_runs_stuck ON job_runs(id) WHERE status = 'pending' AND queued = 1;
CREATE INDEX IF NOT EXISTS idx_job_runs_cancel_cleanup ON job_runs(id) WHERE status = 'canceled' AND queued = 0;
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
	// 轻量列迁移（双驱动，C7 补齐 PG 路径）：CREATE TABLE IF NOT EXISTS 对已存在的
	// 旧库空转——新列（job_id/dept/params）必须判存补齐，否则升级后 INSERT/SELECT
	// 报 no column 全量失败。新增列在此登记（ALTER 语句两驱动通用）。
	if err := migrateColumns(db, driver); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate columns: %w", err)
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
	finished_at   TIMESTAMPTZ,
	queued        INTEGER NOT NULL DEFAULT 1,
	enqueue_payload TEXT NOT NULL DEFAULT ''
);
`
	}
	return schema
}

// pgq PG 路径把 `?` 占位符顺序转换为 $1..$n（SQL 中无字面量 `?`，机械替换安全；
// SQLite 原生支持 `?`，原样返回）。
// ⚠️ 约束：本机制要求 SQL 语句的字符串字面量中不得出现 `?`（会被误转为 $n 静默损坏）。
// 新增查询若需字面量问号：SQLite 用 `??` 转义、PG 用 position/chr(63) 表达，或改走
// 各自方言 DDL——评审时 grep 双检。当前全仓无此场景。
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

// migrateColumns 按列判存补齐旧库缺失列（幂等：新库全部命中跳过；双驱动——
// SQLite 走 pragma_table_info，PG 走 information_schema；ALTER 语句两驱动通用）。
func migrateColumns(db *sql.DB, driver string) error {
	existsQuery := map[string]string{
		DriverSQLite: "SELECT 1 FROM pragma_table_info(?) WHERE name = ?",
		DriverPG:     "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
	}
	type colDef struct {
		table string
		col   string
		ddl   string
	}
	for _, c := range []colDef{
		{"job_runs", "job_id", "ALTER TABLE job_runs ADD COLUMN job_id TEXT NOT NULL DEFAULT ''"},
		{"job_runs", "dept", "ALTER TABLE job_runs ADD COLUMN dept TEXT NOT NULL DEFAULT ''"},
		{"job_runs", "params", "ALTER TABLE job_runs ADD COLUMN params TEXT NOT NULL DEFAULT '{}'"},
		// ADR-003（F29）：历史行 DEFAULT 1 =「已尝试过投递」——旧世界行存在 ⟺ 入队成功过，
		// 事实正确；新行由 InsertPending 显式写 0。
		{"job_runs", "queued", "ALTER TABLE job_runs ADD COLUMN queued INTEGER NOT NULL DEFAULT 1"},
		{"job_runs", "enqueue_payload", "ALTER TABLE job_runs ADD COLUMN enqueue_payload TEXT NOT NULL DEFAULT ''"},
	} {
		rows, err := db.Query(existsQuery[driver], c.table, c.col)
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

// InsertPending 受理落库（ADR-003 记录先行）：写 pending 行 + queued=0（未投递）+
// enqueue_payload 快照（重投唯一来源）；task_id 重复（幂等重提/并发竞提）返回已存在错误。
// EnqueuedAt 先 Round(0) 剥离单调时钟——驱动按 time.Time.String() 落库，m=+ 后缀会让
// 同一时刻的等值/边界比较永假（实测记录，ADR-003 游标改 id 的同源原因）。
func (s *Store) InsertPending(ctx context.Context, r Run) error {
	_, err := s.exec(ctx, `
INSERT INTO job_runs (task_id, request_id, action, job_id, dept, params, callback_url, status, submitted_by, source_ip, enqueued_at, queued, enqueue_payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.TaskID, r.RequestID, r.Action, r.JobID, r.Dept, r.Params, r.CallbackURL, StatusPending, r.SubmittedBy, r.SourceIP, r.EnqueuedAt.Round(0), 0, r.EnqueuePayload)
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
UPDATE job_runs SET status = ?, attempts = ?, started_at = ?, error = ''
WHERE task_id = ? AND status NOT IN (?, ?)`,
		StatusRunning, attempt, startedAt, taskID, StatusSucceeded, StatusCanceled)
	if err != nil {
		return fmt.Errorf("store: mark running: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark running rows: %w", err)
	}
	if n == 0 {
		// 0 行 = 行缺失或 succeeded/canceled 复活被拒（C6：有意的终态不可被过期写者推翻；
		// failed→running 为 asynq 重试周期正常路径，dead→running 仅供手动重置后）。
		// 补查分类（ADR-003 F59：UPDATE 仍是唯一权威，补查仅作错误分类；补查失败按
		// 未知包装，worker 对未知维持「继续执行」旧口径）：
		run, gerr := s.GetByTaskID(ctx, taskID)
		switch {
		case gerr == nil:
			return fmt.Errorf("store: mark running: %w (status=%s, task_id=%s)", ErrTerminalRejected, run.Status, taskID)
		case errors.Is(gerr, ErrNotFound):
			return fmt.Errorf("store: mark running: %w (task_id=%s)", ErrNotFound, taskID)
		default:
			return fmt.Errorf("store: mark running: missing or terminal resurrection rejected (classification failed: %w, task_id=%s)", gerr, taskID)
		}
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
WHERE task_id = ? AND status = ? AND attempts = ?`,
		status, attempts, errMsg, finishedAt, durationMS, taskID, StatusRunning, attempts)
	if err != nil {
		return fmt.Errorf("store: finish job_run: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: finish job_run rows: %w", err)
	}
	if n == 0 {
		// 0 行 = 过期写者（C6：租约失效重投递后，新执行已推进 status/attempts，
		// 旧 goroutine 的终态写入被等值谓词拒绝）或行缺失
		return fmt.Errorf("store: finish job_run: no running row with matching attempt (task_id=%s)", taskID)
	}
	return nil
}

// ---- ADR-003：outbox 补偿（queued 标志 + 三域扫描器）----

// MarkQueued 入队成功后按状态条件置位（决策 3/理由）：`status='pending'` 条件是
// 取消检测器（F19）——0 行 = 行已离开 pending（并发取消/已被取走/已达终态，F39 三义），
// 调用方据此走 DeleteTask 补偿撤销；返回影响行数供调用方判定。
func (s *Store) MarkQueued(ctx context.Context, taskID string) (int64, error) {
	res, err := s.exec(ctx, `UPDATE job_runs SET queued = 1 WHERE task_id = ? AND status = ?`, taskID, StatusPending)
	if err != nil {
		return 0, fmt.Errorf("store: mark queued: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: mark queued rows: %w", err)
	}
	return n, nil
}

// MarkCleanupDone 第三域探针确认键消亡后的出域置位（F45/F48）：canceled 域专用——
// 不可复用 MarkQueued（其 status='pending' 条件是 F19 取消检测器，对 canceled 行恒 0 行）。
func (s *Store) MarkCleanupDone(ctx context.Context, taskID string) (int64, error) {
	res, err := s.exec(ctx, `UPDATE job_runs SET queued = 1 WHERE task_id = ? AND status = ? AND queued = 0`, taskID, StatusCanceled)
	if err != nil {
		return 0, fmt.Errorf("store: mark cleanup done: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: mark cleanup done rows: %w", err)
	}
	return n, nil
}

// GetQueued 窄查询投递标志（F58：CancelTask 降级判据）——不扩既有 SELECT/Scan（F29 约束不变）。
func (s *Store) GetQueued(ctx context.Context, taskID string) (bool, error) {
	var queued int
	err := s.queryRow(ctx, `SELECT queued FROM job_runs WHERE task_id = ?`, taskID).Scan(&queued)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("store: get queued: %w", err)
	}
	return queued == 1, nil
}

// reclaimListCols 扫描器三域共用的最小列集（行主键 + 任务 ID + 快照 + 入队时间；
// F29：新列只被新方法读取，既有查询零触碰）。
const reclaimListCols = `SELECT id, task_id, enqueue_payload, enqueued_at FROM job_runs`

func scanReclaimRuns(rows *sql.Rows) ([]*Run, error) {
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.TaskID, &r.EnqueuePayload, &r.EnqueuedAt); err != nil {
			return nil, fmt.Errorf("store: scan reclaim run: %w", err)
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}

// ListUnqueued 第一轮扫描域（F1/F14：queued=0 AND status='pending' 才是安全域），
// 显式批位上界（F66）。无需游标——论证见 ADR-003 决策 4 第一轮。
func (s *Store) ListUnqueued(ctx context.Context, before time.Time, limit int) ([]*Run, error) {
	rows, err := s.query(ctx, reclaimListCols+`
WHERE queued = 0 AND status = ? AND enqueued_at <= ?
ORDER BY id LIMIT ?`, StatusPending, before, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list unqueued: %w", err)
	}
	return scanReclaimRuns(rows)
}

// ListStuckPending 第二轮扫描域（pending AND queued=1），id 游标支持轮转
// （F36/F46/F56；cursorID=0 = 从头扫；游标键必须走整数主键——时间列绑定格式
// 含驱动内部后缀，等值/边界比较不可靠，见实现记录）。
func (s *Store) ListStuckPending(ctx context.Context, before time.Time, cursorID int64, limit int) ([]*Run, error) {
	rows, err := s.query(ctx, reclaimListCols+`
WHERE status = ? AND queued = 1 AND enqueued_at <= ? AND id > ?
ORDER BY id LIMIT ?`, StatusPending, before, cursorID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list stuck pending: %w", err)
	}
	return scanReclaimRuns(rows)
}

// ListCanceledUnqueued 第三域扫描域（canceled AND queued=0，F45），同款 id 游标 +
// 批位上界（F50/F56：队头取消行探针持续失败时不占死批位）。
func (s *Store) ListCanceledUnqueued(ctx context.Context, before time.Time, cursorID int64, limit int) ([]*Run, error) {
	rows, err := s.query(ctx, reclaimListCols+`
WHERE status = ? AND queued = 0 AND enqueued_at <= ? AND id > ?
ORDER BY id LIMIT ?`, StatusCanceled, before, cursorID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list canceled unqueued: %w", err)
	}
	return scanReclaimRuns(rows)
}
