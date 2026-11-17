package app

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
)

type markerLoop struct{ path string }

func (m markerLoop) Run(ctx context.Context) {
	_ = os.WriteFile(m.path, []byte("background started"), 0600)
	<-ctx.Done()
	_ = os.WriteFile(m.path+".stopped", []byte("background stopped"), 0600)
}
func TestAppSingletonChild(t *testing.T) {
	dbpath := os.Getenv("TR_INSTANCE_CHILD_DB")
	if dbpath == "" {
		return
	}
	var st *repository.Store
	var err error
	if dsn := os.Getenv("TR_INSTANCE_CHILD_PG"); dsn != "" {
		st, err = repository.OpenPG(dsn)
	} else {
		st, err = repository.Open(dbpath)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var cfg config.Config
	cfg.Server.Addr = "127.0.0.1:0"
	cfg.Queue = os.Getenv("TR_INSTANCE_CHILD_QUEUE")
	cfg.ShutdownTimeout = time.Second
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := gin.New()
	worker := asynq.NewServer(asynq.RedisClientOpt{Addr: os.Getenv("TASKRUNNER_TEST_REDIS_ADDR")}, asynq.Config{Concurrency: 1, Queues: map[string]int{cfg.Queue: 1}, ShutdownTimeout: time.Second})
	a := NewApp(&cfg, logger, r, worker, markerLoop{os.Getenv("TR_INSTANCE_CHILD_MARK")}, markerLoop{os.Getenv("TR_INSTANCE_CHILD_MARK")}, st, nil)
	if err := a.Run(); err != nil {
		t.Fatal(err)
	}
}
func TestSecondAppRejectedBeforeBackgroundStarts(t *testing.T) {
	if os.Getenv("TASKRUNNER_TEST_REDIS_ADDR") == "" {
		t.Skip("requires dedicated Redis")
	}
	dir := t.TempDir()
	dbpath := filepath.Join(dir, "one.db")
	firstmark := filepath.Join(dir, "first")
	secondmark := filepath.Join(dir, "second")
	child := func(ctx context.Context, marker string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAppSingletonChild$", "-test.v")
		cmd.Env = append(os.Environ(), "TR_INSTANCE_CHILD_DB="+dbpath, "TR_INSTANCE_CHILD_MARK="+marker, "TR_INSTANCE_CHILD_QUEUE=instance-"+uuid.NewString())
		return cmd
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := child(ctx, firstmark)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); first.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(firstmark); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first app did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	secondCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	out, err := child(secondCtx, secondmark).CombinedOutput()
	if secondCtx.Err() != nil || err == nil || !strings.Contains(string(out), "instance already running") {
		t.Fatalf("second app must fail before serving: err=%v timeout=%v output=%s", err, secondCtx.Err(), out)
	}
	if _, err := os.Stat(secondmark); !os.IsNotExist(err) {
		t.Fatalf("rejected app started background work: %v", err)
	}
	// A killed owner releases the OS lock; the same fact file can restart.
	cancel()
	_ = first.Wait()
	restartCtx, killRestart := context.WithCancel(context.Background())
	defer killRestart()
	restartMark := filepath.Join(dir, "restart")
	restart := child(restartCtx, restartMark)
	if err := restart.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { killRestart(); restart.Wait() }()
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(restartMark); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("same-file restart did not reacquire instance")
		}
		time.Sleep(10 * time.Millisecond)
	}

}

type failedOwnership struct{}

func (failedOwnership) Check(context.Context) error {
	return errors.New("injected instance ownership loss")
}
func TestLostInstanceCancelsLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	done := watchInstance(ctx, failedOwnership{}, time.Millisecond, cancel)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("lost ownership did not cancel lifecycle")
	}
	<-done
	if !strings.Contains(context.Cause(ctx).Error(), "ownership loss") {
		t.Fatal(context.Cause(ctx))
	}
}

func TestPGInstanceOwnershipLossStopsProcess(t *testing.T) {
	dsn := os.Getenv("TASKRUNNER_TEST_PG_DSN")
	if dsn == "" || os.Getenv("TASKRUNNER_TEST_REDIS_ADDR") == "" {
		t.Skip("requires dedicated PG/Redis")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "app_instance_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(`CREATE DATABASE "` + name + `"`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(`DROP DATABASE "` + name + `"`); err != nil {
			t.Error(err)
		}
	}()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dir := t.TempDir()
	marker := filepath.Join(dir, "pg-owner")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAppSingletonChild$", "-test.v")
	cmd.Env = append(os.Environ(), "TR_INSTANCE_CHILD_DB=pg", "TR_INSTANCE_CHILD_PG="+u.String(), "TR_INSTANCE_CHILD_MARK="+marker, "TR_INSTANCE_CHILD_QUEUE=instance-loss-"+uuid.NewString())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owner process cleanup timed out")
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PG owner did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var killed bool
	if err := admin.QueryRow(`SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=$1) LIMIT 1`, name).Scan(&killed); err != nil || !killed {
		t.Fatalf("terminate own instance session: %v %v", killed, err)
	}
	select {
	case err := <-done:
		done <- err
		if err == nil {
			t.Fatal("lost instance must report failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lock loss did not stop serving process")
	}
	if _, err := os.Stat(marker + ".stopped"); err != nil {
		t.Fatalf("lost instance left background running: %v", err)
	}
}
