package callback

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

func payload(url string) task.Payload {
	return task.Payload{
		TaskID: "t1", RequestID: "r1", Action: "audit_archive", CallbackURL: url,
		Params: []byte(`{"retain_days":90}`), SubmittedBy: "10086",
	}
}

func TestSuccess(t *testing.T) {
	var gotBody, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		r.Body.Read(b)
		gotBody, gotCT = string(b), r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(time.Second)
	if err := c.Do(context.Background(), payload(srv.URL)); err != nil {
		t.Fatalf("want success, got %v", err)
	}
	want := `{"task_id":"t1","request_id":"r1","action":"audit_archive","params":{"retain_days":90},"actor":"10086"}`
	if gotBody != want {
		t.Fatalf("callback body mismatch:\n got %s\nwant %s", gotBody, want)
	}
	if gotCT != "application/json" {
		t.Fatalf("content-type: %s", gotCT)
	}
}

func Test4xxNonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad params", http.StatusBadRequest)
	}))
	defer srv.Close()

	err := New(time.Second).Do(context.Background(), payload(srv.URL))
	if !errors.Is(err, ErrNonRetryable) {
		t.Fatalf("want ErrNonRetryable, got %v", err)
	}
}

func Test5xxRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := New(time.Second).Do(context.Background(), payload(srv.URL))
	if err == nil || errors.Is(err, ErrNonRetryable) {
		t.Fatalf("want retryable error, got %v", err)
	}
}

func TestTimeoutRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := New(50 * time.Millisecond).Do(context.Background(), payload(srv.URL))
	if err == nil || errors.Is(err, ErrNonRetryable) {
		t.Fatalf("want timeout (retryable) error, got %v", err)
	}
}

func TestPerTaskTimeoutOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := payload(srv.URL)
	p.TimeoutSecs = 1 // 默认 50ms 太短，按任务覆盖为 1s 后应成功
	if err := New(50 * time.Millisecond).Do(context.Background(), p); err != nil {
		t.Fatalf("override timeout should succeed, got %v", err)
	}
}
