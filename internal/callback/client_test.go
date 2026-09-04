package callback

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"

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

	c := New(Config{Timeout: time.Second})
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

	err := New(Config{Timeout: time.Second}).Do(context.Background(), payload(srv.URL))
	if !errors.Is(err, ErrNonRetryable) {
		t.Fatalf("want ErrNonRetryable, got %v", err)
	}
}

func Test5xxRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := New(Config{Timeout: time.Second}).Do(context.Background(), payload(srv.URL))
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

	err := New(Config{Timeout: 50 * time.Millisecond}).Do(context.Background(), payload(srv.URL))
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
	if err := New(Config{Timeout: 50 * time.Millisecond}).Do(context.Background(), p); err != nil {
		t.Fatalf("override timeout should succeed, got %v", err)
	}
}

// TestC9SignedCallback C9：回调请求以自身 SK 做 AK/SK 签名（覆盖 body/X-Request-ID/
// X-Operator）——zhuzhao /internal 端点验签不通过即 401，签名正确才放行。
func TestC9SignedCallback(t *testing.T) {
	verifier := &aksk.Verifier{Keys: map[string][]byte{"taskrunner": []byte("sk-self")}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := verifier.Verify(r, body); err != nil {
			w.WriteHeader(401)
			w.Write([]byte(err.Error()))
			return
		}
		if r.Header.Get("X-Request-ID") != "req-cb-9" {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := New(Config{Timeout: time.Second, AK: "taskrunner", SK: []byte("sk-self")})
	if err := c.Do(context.Background(), task.Payload{
		TaskID: "t9", RequestID: "req-cb-9", Action: "a", CallbackURL: srv.URL,
		SubmittedBy: "10086",
	}); err != nil {
		t.Fatalf("signed callback must pass verifier: %v", err)
	}

	// 错误 SK：验签拒绝 → 401 → 可重试错误
	bad := New(Config{Timeout: time.Second, AK: "taskrunner", SK: []byte("sk-wrong")})
	if err := bad.Do(context.Background(), task.Payload{
		TaskID: "t9b", Action: "a", CallbackURL: srv.URL,
	}); err == nil {
		t.Fatal("bad sk must be rejected (401 → retryable error)")
	}
}
