// Package notify 终败通知（P4-2 死信告警链路——E-⑥ 终败通知端点形态）：
// 死信产生时向 zhuzhao /internal/notify/dead-letter 投递（AK/SK 签名，与回调
// client 同套 utils aksk），由其按通知配置分发 webhook。
//
// 语义：尽力而为、异步非阻塞——通知失败只记日志，绝不影响死信落库与 worker
// 返回（告警链路不得成为新的故障源）；未配置（target 空）= 关闭。
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
)

// Config 通知出站参数（env TASKRUNNER_NOTIFY_*）
type Config struct {
	TargetURL string // zhuzhao /internal/notify/dead-letter；空 = 功能关闭
	AK        string
	SK        []byte
}

// Payload 死信事件载荷（zhuzhao DeadLetterNotifyPayload 契约）
type Payload struct {
	TaskID    string `json:"task_id"`
	RequestID string `json:"request_id,omitempty"`
	Action    string `json:"action"`
	JobID     string `json:"job_id,omitempty"`
	Dept      string `json:"dept,omitempty"`
	Error     string `json:"error,omitempty"`
	Attempts  int    `json:"attempts,omitempty"`
	FailedAt  string `json:"failed_at,omitempty"`
}

// Notifier 死信通知器（nil TargetURL 时为空操作——worker 零分支接入）
type Notifier struct {
	cfg    Config
	logger *slog.Logger
	client *http.Client
}

func New(cfg Config, logger *slog.Logger) *Notifier {
	return &Notifier{cfg: cfg, logger: logger, client: &http.Client{Timeout: 5 * time.Second}}
}

// Enabled 功能开关（app 装配处决定是否注入 worker）
func (n *Notifier) Enabled() bool { return n != nil && n.cfg.TargetURL != "" }

// NotifyDeadLetter 异步投递（调用方=worker 死信落库路径——不等待不阻塞）。
// payload 闭包拷贝防并发修改；ctx 脱离 worker 取消链（死信窗口 asynq 会 cancel）。
func (n *Notifier) NotifyDeadLetter(p Payload) {
	if !n.Enabled() {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 8*time.Second)
		defer cancel()
		body, err := json.Marshal(p)
		if err != nil {
			n.logger.Error("dead-letter notify: marshal", "err", err)
			return
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.TargetURL, bytes.NewReader(body))
		if err != nil {
			n.logger.Error("dead-letter notify: build request", "err", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if n.cfg.AK != "" && len(n.cfg.SK) > 0 {
			aksk.Sign(req, body, aksk.SignOptions{AK: n.cfg.AK, SK: n.cfg.SK})
		}
		resp, err := n.client.Do(req)
		if err != nil {
			n.logger.Error("dead-letter notify: post", "err", err, "task_id", p.TaskID)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			n.logger.Error("dead-letter notify: rejected", "status", resp.StatusCode, "task_id", p.TaskID)
			return
		}
		n.logger.Info("dead-letter notify: delivered", "task_id", p.TaskID)
	}()
}
