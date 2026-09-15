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

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"

	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// ErrNonRetryable 不可重试失败：4xx（对端明确拒绝，重试不会改变结果）
// 或重试无法恢复的本地构造错误（如 callback_url 非法）。worker 跳过重试直接判终败。
// 错误文本会落 job_runs.error 并经查询接口回显，故用中文写明分类与定位要素。
var ErrNonRetryable = errors.New("回调失败（不可重试）")

// Config 回调客户端参数。
type Config struct {
	Timeout time.Duration // 默认回调超时（载荷 TimeoutSecs 可按任务覆盖）
	AK      string        // 本服务签名身份（zhuzhao /internal 验签用；空 = 不签名）
	SK      []byte
}

// Client 回调执行器（C9：以 taskrunner 自身 SK 做 AK/SK 签名 + X-Request-ID 头透传）。
type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{cfg: cfg, http: &http.Client{}} // 超时走 per-request context
}

// body 回调请求体（契约见 zhuzhao-integration.md §2.1）。
type body struct {
	TaskID    string    `json:"task_id"`
	RequestID string    `json:"request_id"`
	Action    string    `json:"action"`
	Params    paramJSON `json:"params,omitempty"`
	Actor     string    `json:"actor,omitempty"` // 原始提交人工号，回传供 zhuzhao 审计串联
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

// Do 执行一次回调。nil = 成功；包装 ErrNonRetryable = 不可重试（4xx / 本地构造错误）；
// 其他 error = 可重试（5xx / 网络错误 / 超时）。
func (c *Client) Do(ctx context.Context, p task.Payload) error {
	timeout := c.cfg.Timeout
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
		// 入参已在提交侧校验，正常不可达；一旦发生属载荷本身缺陷，重试无法恢复
		return fmt.Errorf("%w：请求体序列化失败（请检查 params）: %v", ErrNonRetryable, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.CallbackURL, bytes.NewReader(b))
	if err != nil {
		// callback_url 非法：配置错误，重试不会改变结果——直接终败并指明排查对象
		return fmt.Errorf("%w：回调地址非法（请检查 callback_url=%s）: %v", ErrNonRetryable, p.CallbackURL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	// C9（基线 §9）：回调请求以自身 SK 签名（覆盖 body/X-Request-ID/X-Operator），
	// zhuzhao /internal 端点验签——「回调鉴权不做」的原拍板被 AK/SK 基线修订覆盖。
	// X-Request-ID 透传：zhuzhao 入站中间件复用同一 rid，回调链路与 job_runs 不再断链。
	if c.cfg.AK != "" && len(c.cfg.SK) > 0 {
		aksk.Sign(req, b, aksk.SignOptions{AK: c.cfg.AK, SK: c.cfg.SK,
			RequestID: p.RequestID, Operator: p.SubmittedBy})
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// 网络错误 / 连接重置 / context 超时：对端可能恢复，Asynq 退避重试
		return fmt.Errorf("回调失败（可重试，网络错误或超时），动作 %s，地址 %s: %w", p.Action, p.CallbackURL, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// 2xx = 执行完全成功（拍板 2026-09-03：无响应体状态字段——zhuzhao handler 业务失败
		// 直接映射 4xx/5xx，本判定即最终行为）；body 不再解析，排障走 zhuzhao 侧日志。
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 4xx = 对端明确拒绝（动作不存在/参数错误/鉴权失败）：重试不改变结果，终败
		return fmt.Errorf("%w：对端返回 HTTP %d（客户端错误），动作 %s，响应片段: %q",
			ErrNonRetryable, resp.StatusCode, p.Action, readSnippet(resp.Body))
	default:
		// 5xx 及其他 = 对端暂时异常：退避重试
		return fmt.Errorf("回调失败（可重试）：对端返回 HTTP %d（服务端错误），动作 %s，响应片段: %q",
			resp.StatusCode, p.Action, readSnippet(resp.Body))
	}
}

// readSnippet 读取错误响应片段（截断），仅供日志定位。
func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 1<<10))
	return string(b)
}
