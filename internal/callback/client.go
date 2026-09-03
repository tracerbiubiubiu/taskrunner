// Package callback 执行对 zhuzhao 内网端点的 HTTP 回调（设计文档 §4 契约）：
// 2xx = 受理成功；4xx = 不可重试失败；5xx / 超时 / 网络错误 = 可重试。
package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// ErrNonRetryable 4xx 类失败：worker 应跳过重试直接判失败。
var ErrNonRetryable = errors.New("callback: non-retryable failure")

// Client 回调执行器。DefaultTimeout 为默认超时，载荷 TimeoutSecs 可按任务覆盖。
type Client struct {
	DefaultTimeout time.Duration
	HTTP           *http.Client
}

func New(defaultTimeout time.Duration) *Client {
	return &Client{
		DefaultTimeout: defaultTimeout,
		HTTP:           &http.Client{}, // 超时走 per-request context，便于按任务覆盖
	}
}

// body 回调请求体（契约见 zhuzhao-integration.md §2.1）。
type body struct {
	TaskID    string    `json:"task_id"`
	RequestID string    `json:"request_id"`
	Action    string    `json:"action"`
	Params    paramJSON `json:"params,omitempty"`
	Actor     string    `json:"actor,omitempty"`  // 原始提交人工号，回传供 zhuzhao 审计串联
	SourceIP  string    `json:"source_ip,omitempty"`
}

// paramJSON 保持 params 原样透传（不二次编码）。
type paramJSON json.RawMessage

func (p paramJSON) MarshalJSON() ([]byte, error) {
	if len(p) == 0 {
		return []byte("null"), nil
	}
	return p, nil
}

// Do 执行一次回调。nil = 成功；ErrNonRetryable = 4xx；其他 error = 可重试。
func (c *Client) Do(ctx context.Context, p task.Payload) error {
	timeout := c.DefaultTimeout
	if p.TimeoutSecs > 0 {
		timeout = time.Duration(p.TimeoutSecs) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	b, err := json.Marshal(body{
		TaskID:    p.TaskID,
		RequestID: p.RequestID,
		Action:    p.Action,
		Params:    paramJSON(p.Params),
		Actor:     p.SubmittedBy,
		SourceIP:  p.SourceIP,
	})
	if err != nil {
		return fmt.Errorf("callback: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.CallbackURL, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("callback: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("callback: do: %w", err) // 含 context 超时，可重试
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// 业务级失败经响应体状态字段表达：字段约定随 M3 定（zhuzhao-integration.md §2.1），当前 2xx 即成功
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return fmt.Errorf("%w: status=%d action=%s body=%q", ErrNonRetryable, resp.StatusCode, p.Action, readSnippet(resp.Body))
	default:
		return fmt.Errorf("callback: retryable: status=%d action=%s body=%q", resp.StatusCode, p.Action, readSnippet(resp.Body))
	}
}

// readSnippet 读取错误响应片段（截断），仅供日志定位。
func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 1<<10))
	return string(b)
}
