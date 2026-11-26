package callback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestRedirectDoesNotSatisfyCallback(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Error("callback must stay at the configured endpoint")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer target.Close()
			configured := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL, status)
			}))
			defer configured.Close()

			err := New(Config{}).Do(context.Background(), payload(configured.URL))
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", status)) {
				t.Fatalf("configured endpoint did not complete the callback: %v", err)
			}
		})
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
	// 错误消息落 job_runs.error：须带分类、HTTP 状态码与动作，运维扫列即可定位
	if got := err.Error(); !strings.Contains(got, "不可重试") || !strings.Contains(got, "HTTP 400") ||
		!strings.Contains(got, "audit_archive") {
		t.Fatalf("4xx error message lacks category/status/action: %q", got)
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
	if got := err.Error(); !strings.Contains(got, "可重试") || !strings.Contains(got, "HTTP 500") {
		t.Fatalf("5xx error message lacks category/status: %q", got)
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
	if got := err.Error(); !strings.Contains(got, "可重试") {
		t.Fatalf("timeout error message should be marked retryable: %q", got)
	}
}

func TestBadCallbackURLNonRetryable(t *testing.T) {
	// callback_url 非法属配置错误：重试无法恢复，须直接终败并指出排查对象
	err := New(Config{Timeout: time.Second}).Do(context.Background(),
		payload("http://[missing-bracket"))
	if !errors.Is(err, ErrNonRetryable) {
		t.Fatalf("want ErrNonRetryable for bad url, got %v", err)
	}
	if got := err.Error(); !strings.Contains(got, "回调地址非法") || !strings.Contains(got, "callback_url") {
		t.Fatalf("bad-url message should point to callback_url: %q", got)
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

func TestTransient4xxRetryable(t *testing.T) {
	for _, status := range []int{408, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer srv.Close()
			err := New(Config{Timeout: time.Second}).Do(context.Background(), payload(srv.URL))
			if err == nil || errors.Is(err, ErrNonRetryable) {
				t.Fatalf("HTTP%d must retry: %v", status, err)
			}
		})
	}
}

func TestCallbackCarriesJobAssociation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["job_id"] != "trusted-job" {
			t.Errorf("missing originating job_id: %v", body)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	p := payload(srv.URL)
	p.JobID = "trusted-job"
	if err := New(Config{Timeout: time.Second}).Do(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

func TestB3CallbackRequiresMatchingReceipt(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        int
		reply         string
		wantSuccess   bool
		wantRejection bool
	}{
		{"valid", 200, `{"code":0,"data":{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}}`, true, false},
		{"202 is pending", 202, `{"code":0,"data":{"task_id":"t1","operation_id":"op-original","state":"in_progress"}}`, false, false},
		{"503 is unresolved", 503, `{}`, false, false},
		{"204 cannot succeed", 204, "", false, false},
		{"unenveloped success", 200, `{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}`, false, false},
		{"legacy success has no receipt", 200, `{"code":0,"data":{"task_id":"t1","status":"succeeded"}}`, false, false},
		{"code missing", 200, `{"data":{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}}`, false, false},
		{"code null", 200, `{"code":null,"data":{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}}`, false, false},
		{"code not zero", 200, `{"code":1,"data":{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}}`, false, false},
		{"duplicate code", 200, `{"code":1,"code":0,"data":{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}}`, false, false},
		{"wrong task", 200, `{"code":0,"data":{"task_id":"different","operation_id":"op-original","state":"succeeded","receipt_id":"receipt-original"}}`, false, false},
		{"wrong operation", 200, `{"code":0,"data":{"task_id":"t1","operation_id":"different","state":"succeeded","receipt_id":"receipt-original"}}`, false, false},
		{"unknown state", 200, `{"code":0,"data":{"task_id":"t1","operation_id":"op-original","state":"outcome_unknown","receipt_id":"receipt-original"}}`, false, false},
		{"empty receipt", 200, `{"code":0,"data":{"task_id":"t1","operation_id":"op-original","state":"succeeded","receipt_id":" "}}`, false, false},
		{"bad body", 200, `{"code":0`, false, false},
		{"403 is definitive delivery rejection", 403, `{}`, false, true},
		{"409 is definitive delivery rejection", 409, `{}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.reply)
			}))
			defer srv.Close()
			p := payload(srv.URL)
			p.Action = "ticket.evidence.report.generate"
			p.Params = []byte(`{"operation_id":"op-original"}`)
			err := New(Config{}).Do(context.Background(), p)
			if (err == nil) != tc.wantSuccess || errors.Is(err, ErrNonRetryable) != tc.wantRejection {
				t.Fatalf("success=%v rejection=%v, got %v", tc.wantSuccess, tc.wantRejection, err)
			}
		})
	}
}
