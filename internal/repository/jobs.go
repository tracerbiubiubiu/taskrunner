// jobs 表：任务定义（三层模型的中间层，设计文档 §4）。
// cron 触发的 next_run 由 store 的调用方（cronloop）推进；本文件只管数据。
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// 触发方式。
const (
	TriggerCron   = "cron"
	TriggerManual = "manual"
)

// Job 任务定义一行。
type Job struct {
	JobID          string
	ActionID       string
	TriggerType    string // cron | manual
	CronSpec       string
	CallbackURL    string
	Params         string // JSON 原文
	Dept           string // 归属标签，不透明字符串（§4）
	Enabled        bool
	TimeoutSecs    int
	Description    string
	CreatedBy      string // 工号（审计归因）
	OwnerService   string // credential 调用方，当前恒为 zhuzhao
	NextRun        sql.NullTime
	CreatedAt      time.Time
	UpdatedAt      time.Time
	nextRunToken   sql.NullString
	updatedAtToken string
}

// 列集一致，自增/布尔/时间类型按驱动方言。
const (
	sqliteJobsSchema = `
CREATE TABLE IF NOT EXISTS jobs (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id        TEXT NOT NULL UNIQUE,
	action_id     TEXT NOT NULL,
	trigger_type  TEXT NOT NULL DEFAULT 'manual',
	callback_url  TEXT NOT NULL DEFAULT '',
	cron_spec     TEXT NOT NULL DEFAULT '',
	params        TEXT NOT NULL DEFAULT '{}',
	dept          TEXT NOT NULL DEFAULT '',
	enabled       INTEGER NOT NULL DEFAULT 1,
	timeout_secs  INTEGER NOT NULL DEFAULT 0,
	description   TEXT NOT NULL DEFAULT '',
	created_by    TEXT NOT NULL DEFAULT '',
	owner_service TEXT NOT NULL DEFAULT 'zhuzhao',
	next_run      TIMESTAMP,
	created_at    TIMESTAMP NOT NULL,
	updated_at    TIMESTAMP NOT NULL
);
`
	pgJobsSchema = `
CREATE TABLE IF NOT EXISTS jobs (
	id            BIGSERIAL PRIMARY KEY,
	job_id        TEXT NOT NULL UNIQUE,
	action_id     TEXT NOT NULL,
	trigger_type  TEXT NOT NULL DEFAULT 'manual',
	callback_url  TEXT NOT NULL DEFAULT '',
	cron_spec     TEXT NOT NULL DEFAULT '',
	params        TEXT NOT NULL DEFAULT '{}',
	dept          TEXT NOT NULL DEFAULT '',
	enabled       BOOLEAN NOT NULL DEFAULT TRUE,
	timeout_secs  INTEGER NOT NULL DEFAULT 0,
	description   TEXT NOT NULL DEFAULT '',
	created_by    TEXT NOT NULL DEFAULT '',
	owner_service TEXT NOT NULL DEFAULT 'zhuzhao',
	next_run      TIMESTAMPTZ,
	created_at    TIMESTAMPTZ NOT NULL,
	updated_at    TIMESTAMPTZ NOT NULL
);
`
)

// 两驱动共用索引（CREATE INDEX IF NOT EXISTS 方言中立）。
const jobsIndexes = `
CREATE INDEX IF NOT EXISTS idx_jobs_dept ON jobs(dept);
CREATE INDEX IF NOT EXISTS idx_jobs_next_run ON jobs(enabled, trigger_type, next_run);
`

const jobsCols = `job_id, action_id, trigger_type, callback_url, cron_spec, params, dept, enabled,
	timeout_secs, description, created_by, owner_service, next_run, created_at, updated_at, CAST(next_run AS TEXT), CAST(updated_at AS TEXT)`

func init() {
	extraSchemas[DriverSQLite] = append(extraSchemas[DriverSQLite], sqliteJobsSchema, jobsIndexes)
	extraSchemas[DriverPG] = append(extraSchemas[DriverPG], pgJobsSchema, jobsIndexes)
}

func scanJob(row interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	err := row.Scan(&j.JobID, &j.ActionID, &j.TriggerType, &j.CallbackURL, &j.CronSpec, &j.Params,
		&j.Dept, &j.Enabled, &j.TimeoutSecs, &j.Description, &j.CreatedBy, &j.OwnerService,
		&j.NextRun, &j.CreatedAt, &j.UpdatedAt, &j.nextRunToken, &j.updatedAtToken)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: scan job: %w", err)
	}
	return &j, nil
}

// CreateJob 新建任务定义。nextRun 由调用方算好传入（cron 且 enabled 时非空）。
func (s *Store) CreateJob(ctx context.Context, j Job) error {
	_, err := s.exec(ctx, `
INSERT INTO jobs (job_id, action_id, trigger_type, callback_url, cron_spec, params, dept,
                  enabled, timeout_secs, description, created_by, owner_service, next_run, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.JobID, j.ActionID, j.TriggerType, j.CallbackURL, j.CronSpec, j.Params, j.Dept,
		j.Enabled, j.TimeoutSecs, j.Description, j.CreatedBy, j.OwnerService, normalizedJobTime(j.NextRun), j.CreatedAt.UTC().Round(0), j.UpdatedAt.UTC().Round(0))
	if err != nil {
		return fmt.Errorf("store: create job: %w", err)
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return scanJob(s.queryRow(ctx, `SELECT `+jobsCols+` FROM jobs WHERE job_id = ?`, jobID))
}

// UpdateJob 更新任务定义（PATCH 语义：调用方已合并好整行）。nextRun 同 CreateJob。
func (s *Store) UpdateJob(ctx context.Context, j Job) error {
	res, err := s.exec(ctx, `
UPDATE jobs SET action_id=?, trigger_type=?, callback_url=?, cron_spec=?, params=?, dept=?,
                 enabled=?, timeout_secs=?, description=?, next_run=?, updated_at=? WHERE job_id=?`,
		j.ActionID, j.TriggerType, j.CallbackURL, j.CronSpec, j.Params, j.Dept,
		j.Enabled, j.TimeoutSecs, j.Description, normalizedJobTime(j.NextRun), j.UpdatedAt.UTC().Round(0), j.JobID)
	if err != nil {
		return fmt.Errorf("store: update job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// JobFilter 列表过滤与分页（全部可选，零值 = 不过滤；Page<1 视为 1，PageSize 越界回落 50）。
type JobFilter struct {
	Dept     string
	Action   string
	Enabled  *bool
	Page     int
	PageSize int
}

func (s *Store) ListJobs(ctx context.Context, f JobFilter) ([]*Job, int64, error) {
	where, args := " WHERE 1=1", []any{}
	if f.Dept != "" {
		where, args = where+" AND dept = ?", append(args, f.Dept)
	}
	if f.Action != "" {
		where, args = where+" AND action_id = ?", append(args, f.Action)
	}
	if f.Enabled != nil {
		where, args = where+" AND enabled = ?", append(args, *f.Enabled)
	}

	var total int64
	if err := s.queryRow(ctx, `SELECT COUNT(*) FROM jobs`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("store: count jobs: %w", err)
	}

	page, size := f.Page, f.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 50
	}
	q := `SELECT ` + jobsCols + ` FROM jobs` + where + ` ORDER BY created_at DESC LIMIT ? OFFSET ?`
	rows, err := s.query(ctx, q, append(args, size, (page-1)*size)...)
	if err != nil {
		return nil, 0, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, j)
	}
	return out, total, rows.Err()
}

// ListDueCronJobs 取出到期待触发的 cron 定义（cronloop 每 tick 调用）。
func (s *Store) ListDueCronJobs(ctx context.Context, now time.Time) ([]*Job, error) {
	query := `SELECT ` + jobsCols + ` FROM jobs WHERE enabled AND trigger_type='cron' AND next_run IS NOT NULL`
	var args []any
	if s.pg {
		query += " AND next_run <= ?"
		args = append(args, now)
	}
	rows, err := s.query(ctx, query, args...)

	if err != nil {
		return nil, fmt.Errorf("store: list due jobs: %w", err)
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		if j.NextRun.Valid && !j.NextRun.Time.After(now) {
			out = append(out, j)
		}
	}
	return out, rows.Err()
}

// DisableJob 停用任务定义并清空 next_run（cronloop 对无效 spec 的防御路径：
// 必须真正落库停用，仅清 next_run 会因零值时间戳仍命中到期扫描而每 tick 重触发）。
func (s *Store) DisableJob(ctx context.Context, jobID string, updatedAt time.Time) error {
	res, err := s.exec(ctx, `UPDATE jobs SET enabled = ?, next_run = NULL, updated_at = ? WHERE job_id = ?`,
		false, updatedAt.UTC().Round(0), jobID)
	if err != nil {
		return fmt.Errorf("store: disable job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateJobNextRun 推进下次触发时间。
// UpdateJobNextRun 推进下次触发时间；updatedAt 由调用方传入（与 cronloop 的 now
// 同源，避免两处取时的微小偏差，时间确定性测试也可注入）。
func (s *Store) UpdateJobNextRun(ctx context.Context, jobID string, next, updatedAt time.Time) error {
	_, err := s.exec(ctx, `UPDATE jobs SET next_run = ?, updated_at = ? WHERE job_id = ?`,
		next.UTC().Round(0), updatedAt.UTC().Round(0), jobID)
	if err != nil {
		return fmt.Errorf("store: update next_run: %w", err)
	}
	return nil
}

// ClaimCronOccurrence owns one due slot with a conditional update and persists
// its pending outbox in the same transaction. No process-local lock or lease
// can expire between these writes; a failed commit leaves both unchanged.
func (s *Store) ClaimCronOccurrence(ctx context.Context, j Job, next, now time.Time, r Run) (bool, error) {
	if !j.NextRun.Valid {
		return false, fmt.Errorf("cron occurrence has no due slot")
	}
	if j.NextRun.Time.After(now) {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	timePredicate := "next_run=? AND updated_at=?"
	dueToken, updatedToken := any(j.NextRun.Time.Round(0)), any(j.UpdatedAt.Round(0))
	if !s.pg {
		if !j.nextRunToken.Valid || j.updatedAtToken == "" {
			return false, fmt.Errorf("cron claim requires a stored definition snapshot")
		}
		// SQLite legacy timestamps can contain m=+ and named zones. Rebinding the
		// scanned time changes their text, so compare the exact tokens read from DB.
		timePredicate = "CAST(next_run AS TEXT)=? AND CAST(updated_at AS TEXT)=?"
		dueToken, updatedToken = j.nextRunToken.String, j.updatedAtToken
	}
	result, err := tx.ExecContext(ctx, s.pgq(`UPDATE jobs SET next_run=?,updated_at=?
 WHERE job_id=? AND enabled AND trigger_type='cron' AND `+timePredicate+`
 AND action_id=? AND params=? AND cron_spec=? AND dept=? AND timeout_secs=?`),
		next.UTC().Round(0), now.UTC().Round(0), j.JobID, dueToken, updatedToken, j.ActionID, j.Params, j.CronSpec, j.Dept, j.TimeoutSecs)

	if err != nil {
		return false, fmt.Errorf("store: claim cron due: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, s.pgq(`INSERT INTO job_runs
 (task_id,request_id,action,job_id,dept,params,callback_url,status,submitted_by,source_ip,enqueued_at,queued,enqueue_payload)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`), r.TaskID, r.RequestID, r.Action, r.JobID, r.Dept, r.Params, r.CallbackURL, StatusPending, r.SubmittedBy, r.SourceIP, r.EnqueuedAt.Round(0), 0, r.EnqueuePayload)
	if err != nil {
		return false, fmt.Errorf("store: persist cron occurrence: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit cron occurrence: %w", err)
	}
	return true, nil
}

func (s *Store) DisableCronSnapshot(ctx context.Context, j Job, now time.Time) error {
	predicate := "updated_at=?"
	token := any(j.UpdatedAt.Round(0))
	if !s.pg {
		if j.updatedAtToken == "" {
			return fmt.Errorf("invalid cron disable requires stored snapshot")
		}
		predicate = "CAST(updated_at AS TEXT)=?"
		token = j.updatedAtToken
	}
	_, err := s.exec(ctx, `UPDATE jobs SET enabled=?,next_run=NULL,updated_at=? WHERE job_id=? AND enabled AND trigger_type='cron' AND `+predicate+` AND cron_spec=?`, false, now.UTC().Round(0), j.JobID, token, j.CronSpec)
	if err != nil {
		return fmt.Errorf("store: disable invalid cron snapshot: %w", err)
	}
	return nil
}
func normalizedJobTime(t sql.NullTime) sql.NullTime {
	if t.Valid {
		t.Time = t.Time.UTC().Round(0)
	}
	return t
}

var ErrJobSnapshotChanged = errors.New("store: job definition changed before run commit")

// ClaimJobRun rechecks the exact enabled execution snapshot under the same
// local transaction which freezes its pending payload. A no-op UPDATE takes
// the row/write lock in both PG and SQLite; it does not change the definition.
func (s *Store) ClaimJobRun(ctx context.Context, j Job, r Run) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	predicate := "updated_at=?"
	token := any(j.UpdatedAt.Round(0))
	if !s.pg {
		if j.updatedAtToken == "" {
			return false, fmt.Errorf("job submission requires stored definition snapshot")
		}
		predicate = "CAST(updated_at AS TEXT)=?"
		token = j.updatedAtToken
	}
	result, err := tx.ExecContext(ctx, s.pgq(`UPDATE jobs SET job_id=job_id WHERE job_id=? AND enabled AND `+predicate+`
 AND action_id=? AND params=? AND trigger_type=? AND cron_spec=? AND dept=? AND timeout_secs=?`), j.JobID, token, j.ActionID, j.Params, j.TriggerType, j.CronSpec, j.Dept, j.TimeoutSecs)
	if err != nil {
		return false, fmt.Errorf("store: recheck job snapshot: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, ErrJobSnapshotChanged
	}
	result, err = tx.ExecContext(ctx, s.pgq(`INSERT INTO job_runs
 (task_id,request_id,action,job_id,dept,params,callback_url,status,submitted_by,source_ip,enqueued_at,queued,enqueue_payload)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO NOTHING`), r.TaskID, r.RequestID, r.Action, r.JobID, r.Dept, r.Params, r.CallbackURL, StatusPending, r.SubmittedBy, r.SourceIP, r.EnqueuedAt.Round(0), 0, r.EnqueuePayload)
	if err != nil {
		return false, fmt.Errorf("store: persist job run: %w", err)
	}
	n, err = result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit job run: %w", err)
	}
	return n == 1, nil
}
