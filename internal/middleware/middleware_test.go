package middleware

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
)

func testRouter(h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(h)
	r.GET("/ping", func(c *gin.Context) { c.String(200, "pong") })
	return r
}

// ---- RequestID ----

func TestRequestIDGeneratedAndEchoed(t *testing.T) {
	r := testRouter(RequestID())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/ping", nil))
	rid := w.Header().Get("X-Request-ID")
	if !strings.HasPrefix(rid, "req-") || len(rid) != len("req-")+32 {
		t.Fatalf("generated rid format: %q", rid)
	}

	// 带头回显同值
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "/ping", nil)
	req2.Header.Set("X-Request-ID", "my-rid")
	r.ServeHTTP(w2, req2)
	if got := w2.Header().Get("X-Request-ID"); got != "my-rid" {
		t.Fatalf("echo: %q", got)
	}
}

// ---- AccessLog ----

func TestAccessLogFieldsAndProbeSkip(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RequestID(), AccessLog(logger))
	r.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
	r.GET("/ping", func(c *gin.Context) { c.String(200, "pong") })

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
	if strings.Contains(buf.String(), `"msg":"access"`) {
		t.Fatal("healthz should be skipped in access log")
	}

	req := httptest.NewRequest("GET", "/ping?x=1", nil)
	req.Header.Set("X-Operator", "10086")
	r.ServeHTTP(httptest.NewRecorder(), req)

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line: %v (%s)", err, buf.String())
	}
	for _, k := range []string{"method", "path", "status", "cost_ms", "request_id", "operator", "caller", "ip"} {
		if _, ok := line[k]; !ok {
			t.Fatalf("missing field %q in %v", k, line)
		}
	}
	if line["operator"] != "10086" || line["path"] != "/ping" {
		t.Fatalf("values: %v", line)
	}
}

// ---- AKSKAuth ----

const testSK = "sk-test"

func signedRequest(t *testing.T, method, url string, body []byte, sk []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	aksk.Sign(req, body, aksk.SignOptions{AK: "zhuzhao", SK: sk,
		RequestID: "rid-1", Operator: "10086"})
	req.Header.Set("Content-Type", "application/json")
	return req
}

func akskRouter(caller *string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(AKSKAuth(map[string][]byte{"zhuzhao": []byte(testSK)}))
	r.POST("/echo", func(c *gin.Context) {
		s := c.GetString("caller")
		if caller != nil {
			*caller = s
		}
		c.String(200, s)
	})
	return r
}

func TestAKSKAuth(t *testing.T) {
	r := akskRouter(nil)

	// 未签名 → 401
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("POST", "/echo", strings.NewReader(`{}`)))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned want 401, got %d", w.Code)
	}

	// 错 SK → 401
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, signedRequest(t, "POST", "http://t/echo", []byte(`{}`), []byte("wrong")))
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("wrong sk want 401, got %d", w2.Code)
	}

	// 有效签名 → 200 + caller 归因
	var caller string
	r2 := akskRouter(&caller)
	w3 := httptest.NewRecorder()
	r2.ServeHTTP(w3, signedRequest(t, "POST", "http://t/echo", []byte(`{"a":1}`), []byte(testSK)))
	if w3.Code != http.StatusOK || caller != "zhuzhao" {
		t.Fatalf("valid sign: code=%d caller=%q", w3.Code, caller)
	}
}

func TestAKSKAuthBodyLimit(t *testing.T) {
	r := akskRouter(nil)
	big := strings.Repeat("x", 8<<20+1) // 8MB+1：超过预鉴权读体上限
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/echo", strings.NewReader(big))
	r.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body want 413, got %d", w.Code)
	}
}

func TestCredentialOf(t *testing.T) {
	req := httptest.NewRequest("POST", "/", nil)
	req.Header.Set("Authorization", `HMAC Credential=zhuzhao,Ts=123,Sig=abc`)
	if got := credentialOf(req); got != "zhuzhao" {
		t.Fatalf("credentialOf: %q", got)
	}
	req2 := httptest.NewRequest("POST", "/", nil)
	if got := credentialOf(req2); got != "" {
		t.Fatalf("no header: %q", got)
	}
}
