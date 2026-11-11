package notify

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slog writes asynchronously from the notifier goroutine; use a protected sink.
type safePanicLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safePanicLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *safePanicLog) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

type panicTransport struct {
	calls       atomic.Int32
	first, next chan struct{}
}

func (p *panicTransport) RoundTrip(*http.Request) (*http.Response, error) {
	if p.calls.Add(1) == 1 {
		close(p.first)
		panic("audit-notifier-injected")
	}
	close(p.next)
	return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
}

func TestNotifierPanicSurvivesNextDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNotifierPanicChild$", "-test.v")
	cmd.Env = append(os.Environ(), "TASKRUNNER_NOTIFY_PANIC_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "AUDIT_ALIVE_NEXT_DELIVERY") {
		t.Fatalf("notifier panic must not kill process: %v\n%s", err, out)
	}
}

func TestNotifierPanicChild(t *testing.T) {
	if os.Getenv("TASKRUNNER_NOTIFY_PANIC_CHILD") == "" {
		t.Skip("subprocess helper")
	}
	var logs safePanicLog
	n := New(Config{TargetURL: "http://notify.invalid", AK: "audit", SK: []byte("audit-secret")}, slog.New(slog.NewJSONHandler(&logs, nil)))
	tr := &panicTransport{first: make(chan struct{}), next: make(chan struct{})}
	n.client.Transport = tr
	n.NotifyDeadLetter(Payload{TaskID: "panic-first"})
	select {
	case <-tr.first:
	case <-time.After(time.Second):
		t.Fatal("first notification absent")
	}
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(logs.String(), "panic") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !strings.Contains(logs.String(), "panic") || !strings.Contains(logs.String(), "stack") {
		t.Fatalf("panic missing diagnostics: %s", logs.String())
	}
	n.NotifyDeadLetter(Payload{TaskID: "second"})
	select {
	case <-tr.next:
	case <-time.After(time.Second):
		t.Fatal("next notification absent")
	}
	t.Log("AUDIT_ALIVE_NEXT_DELIVERY")
}
