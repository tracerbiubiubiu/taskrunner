package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
)

type drainWorker struct {
	stopped     chan struct{}
	releaseStop chan struct{}
	once        sync.Once
}

func (w *drainWorker) Start(asynq.Handler) error { return nil }
func (w *drainWorker) Stop() {
	w.once.Do(func() { close(w.stopped) })
	if w.releaseStop != nil {
		<-w.releaseStop
	}
}
func (w *drainWorker) Shutdown() { w.Stop() }

type cancelLoop struct{}

func (cancelLoop) Run(ctx context.Context) { <-ctx.Done() }

func TestLostInstanceStopsWorkerAdmissionBeforeHTTPDrain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbpath := filepath.Join(t.TempDir(), "owned.db")
	lockpath := dbpath + ".instance.lock"
	store, err := repository.Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	engine := gin.New()
	engine.GET("/readyz", func(c *gin.Context) { c.Status(http.StatusOK) })
	engine.GET("/hold", func(c *gin.Context) { close(held); <-release; c.Status(http.StatusNoContent) })
	var cfg config.Config
	cfg.Server.Addr = addr
	cfg.ShutdownTimeout = 5 * time.Second
	worker := &drainWorker{stopped: make(chan struct{}), releaseStop: make(chan struct{})}
	var workerReleaseOnce sync.Once
	unblockWorker := func() { workerReleaseOnce.Do(func() { close(worker.releaseStop) }) }
	a := NewApp(&cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), engine, nil, cancelLoop{}, cancelLoop{}, store, nil)
	a.asynqSrv = worker
	done := make(chan error, 1)
	go func() { done <- a.Run() }()
	defer func() {
		unblockWorker()
		unblock()
		_ = os.Remove(lockpath) // Force only this fixture's watcher to stop on an early assertion failure.
		select {
		case err := <-done:
			if err == nil {
				t.Error("lost ownership must report failure")
			}
		case <-time.After(8 * time.Second):
			t.Error("owned app did not finish draining")
		}
	}()
	client := &http.Client{Timeout: 10 * time.Second}
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		deadline := time.Now().Add(3 * time.Second)
		for {
			resp, err := client.Get("http://" + addr + "/hold")
			if err == nil {
				resp.Body.Close()
				return
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case <-held:
	case <-time.After(4 * time.Second):
		t.Fatal("owned HTTP request did not enter")
	}
	// Replacing only this test's lock inode causes the real ownership watcher
	// to cancel lifecycle while the accepted HTTP request is still draining.
	if err := os.Rename(lockpath, lockpath+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockpath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-worker.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("lost ownership left worker admission open behind an in-flight HTTP request")
	}
	// Hold Stop itself to make the gap before HTTP Shutdown observable. Ownership
	// cancellation must already reject new requests and report unready here.
	resp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("lost owner still accepted HTTP during worker stop: %d", resp.StatusCode)
	}
	var body struct {
		Code int `json:"code"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.Code != 10008 {
		t.Fatalf("lost-owner HTTP failure must keep the service-unavailable contract: code=%d err=%v", body.Code, err)
	}
	unblockWorker()
	select {
	case <-requestDone:
		t.Fatal("HTTP fixture was not held through worker admission check")
	default:
	}
}
