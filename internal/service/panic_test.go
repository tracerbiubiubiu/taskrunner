package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// A real child process is necessary: recovering in the test goroutine would not
// show whether a panic in the production background goroutine kills the process.
func TestBackgroundPanicSurvivesNextTick(t *testing.T) {
	for _, component := range []string{"cron", "reclaim"} {
		t.Run(component, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBackgroundPanicChild$", "-test.v")
			cmd.Env = append(os.Environ(), "TASKRUNNER_PANIC_CHILD="+component)
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), "AUDIT_ALIVE_NEXT_TICK") {
				t.Fatalf("background %s must survive and execute next tick: %v\n%s", component, err, out)
			}
		})
	}
}

type panicSubmitter struct {
	calls int
	next  chan struct{}
}

func (p *panicSubmitter) Submit(context.Context, task.Payload) (bool, error, error) {
	p.calls++
	if p.calls == 1 {
		panic("audit-cron-injected")
	}
	if p.calls == 2 {
		close(p.next)
	}
	return true, nil, nil
}

type panicEnqueuer struct {
	calls int
	next  chan struct{}
}

func (p *panicEnqueuer) Enqueue(*asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error) {
	p.calls++
	if p.calls == 1 {
		panic("audit-reclaim-injected")
	}
	if p.calls == 2 {
		close(p.next)
	}
	return &asynq.TaskInfo{}, nil
}

func TestBackgroundPanicChild(t *testing.T) {
	component := os.Getenv("TASKRUNNER_PANIC_CHILD")
	if component == "" {
		t.Skip("subprocess helper")
	}
	st, err := repository.Open(filepath.Join(t.TempDir(), "panic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var log bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&log, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	next, stopped := make(chan struct{}), make(chan struct{})
	past := time.Now().Add(-time.Hour)
	var run func(context.Context)
	switch component {
	case "cron":
		err = st.CreateJob(ctx, repository.Job{JobID: "panic", ActionID: "audit", TriggerType: repository.TriggerCron,
			CronSpec: "* * * * *", Params: `{}`, Enabled: true, CreatedAt: past, UpdatedAt: past,
			NextRun: sql.NullTime{Valid: true, Time: past}})
		if err != nil {
			t.Fatal(err)
		}
		l := &Loop{Store: st, Submit: &panicSubmitter{next: next}, Logger: logger, Tick: 5 * time.Millisecond}
		run = l.Run
	case "reclaim":
		p, _ := json.Marshal(task.Payload{TaskID: "panic", Action: "audit", CallbackURL: "http://callback.invalid"})
		err = st.InsertPending(ctx, repository.Run{TaskID: "panic", Action: "audit", EnqueuedAt: past, EnqueuePayload: string(p)})
		if err != nil {
			t.Fatal(err)
		}
		l := &ReclaimLoop{Store: st, Client: &panicEnqueuer{next: next}, Inspector: newReclaimInspector(), Logger: logger,
			Tick: 5 * time.Millisecond, StaleAfter: time.Millisecond}
		run = l.Run
	default:
		t.Fatalf("unknown component %q", component)
	}
	go func() { defer close(stopped); run(ctx) }()
	select {
	case <-next:
	case <-ctx.Done():
		t.Fatal("next tick did not run")
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop")
	}
	if !strings.Contains(log.String(), "panic") || !strings.Contains(log.String(), "stack") || !strings.Contains(log.String(), component) {
		t.Fatalf("panic needs component and stack evidence: %s", log.String())
	}
	t.Log("AUDIT_ALIVE_NEXT_TICK")
}
