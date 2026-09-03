package cronloop

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

type recSubmitter struct {
	submitted []task.Payload
	fail      bool
}

func (r *recSubmitter) Submit(_ context.Context, p task.Payload) (bool, error, error) {
	if r.fail {
		return false, nil, context.DeadlineExceeded
	}
	r.submitted = append(r.submitted, p)
	return true, nil, nil
}

func TestFireDueAdvancesNextRun(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	past := time.Now().Add(-2 * time.Minute)
	st.CreateJob(context.Background(), store.Job{
		JobID: "j1", ActionID: "audit_archive", TriggerType: store.TriggerCron,
		CallbackURL: "http://zhu.internal/internal/jobs/audit_archive",
		CronSpec: "0 3 * * *", Params: `{"retain_days":90}`, Dept: "audit",
		Enabled: true, CreatedAt: past, UpdatedAt: past,
		NextRun: sql.NullTime{Valid: true, Time: past}, // 已到期
	})

	sub := &recSubmitter{}
	loop := &Loop{Store: st, Submit: sub, Logger: slog.Default(),
		NewTaskID: func() string { return "cron-tk-1" }, Now: func() time.Time { return time.Now() }}
	loop.fireDue(context.Background())

	if len(sub.submitted) != 1 {
		t.Fatalf("want 1 submission, got %d", len(sub.submitted))
	}
	p := sub.submitted[0]
	if p.JobID != "j1" || p.Action != "audit_archive" || p.TaskID != "cron-tk-1" {
		t.Fatalf("payload: %+v", p)
	}
	if p.SubmittedBy != "" || p.RequestID != "" {
		t.Fatalf("cron 触发不应带用户上下文: %+v", p)
	}
	if string(p.Params) != `{"retain_days":90}` {
		t.Fatalf("params not carried: %s", p.Params)
	}

	// next_run 已推进到未来：再次 fireDue 不应重复触发
	loop2 := &Loop{Store: st, Submit: sub, Logger: slog.Default(),
		NewTaskID: func() string { return "cron-tk-2" }}
	loop2.fireDue(context.Background())
	if len(sub.submitted) != 1 {
		t.Fatalf("advanced next_run should not refire, got %d", len(sub.submitted))
	}

	// 推进后的 next_run 落在下一个 03:00（未来、时分为 3:00）
	j, _ := st.GetJob(context.Background(), "j1")
	if !j.NextRun.Valid || !j.NextRun.Time.After(time.Now()) || j.NextRun.Time.Hour() != 3 || j.NextRun.Time.Minute() != 0 {
		t.Fatalf("next_run not advanced properly: %+v", j.NextRun)
	}
}

func TestSubmitFailureDoesNotAdvance(t *testing.T) {
	st, _ := store.Open(filepath.Join(t.TempDir(), "t.db"))
	defer st.Close()

	past := time.Now().Add(-time.Minute)
	st.CreateJob(context.Background(), store.Job{
		JobID: "j2", ActionID: "a", TriggerType: store.TriggerCron, CronSpec: "* * * * *",
		CallbackURL: "http://x", Enabled: true, CreatedAt: past, UpdatedAt: past,
		NextRun: sql.NullTime{Valid: true, Time: past},
	})
	sub := &recSubmitter{fail: true}
	loop := &Loop{Store: st, Submit: sub, Logger: slog.Default(), NewTaskID: func() string { return "t" }}
	loop.fireDue(context.Background())

	j, _ := st.GetJob(context.Background(), "j2")
	if !j.NextRun.Valid || j.NextRun.Time.After(time.Now()) {
		t.Fatal("submit 失败时不应推进 next_run（需下个 tick 重试）")
	}
}

func TestNextRunDisabled(t *testing.T) {
	if _, ok := NextRun("* * * * *", false, time.Now()); ok {
		t.Fatal("disabled job should have no next_run")
	}
	if _, ok := NextRun("0 3 * * *", true, time.Now()); !ok {
		t.Fatal("enabled cron job should have next_run")
	}
}
