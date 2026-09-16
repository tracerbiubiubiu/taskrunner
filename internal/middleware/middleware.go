// middleware 中间件（request_id 回显 / 统一访问日志）。服务间验签统一走
// utils aksk.GinMiddleware + response.AKSKFail()（2026-09-16 服务间验签统一批——
// 原自研 AKSKAuth 退役：读体上限/失败文案/caller·operator 归因收编由 utils 承接，
// 挂载见 handler/server.go /v1 组）。
package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
)

// RequestID 读入站 X-Request-ID（taskrunner→zhuzhao 回调 / zhuzhao client 均携带），
// 缺省自生成并回显响应头；全链路关联键（job_runs.request_id / 访问日志 / zhuzhao 跨查）。
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader("X-Request-ID")
		if rid == "" {
			rid = "req-" + randomHex(16)
		}
		c.Set("request_id", rid)
		c.Header("X-Request-ID", rid)
		c.Next()
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// AccessLog 统一访问日志出口（C1）：每请求一行——method/path/status/耗时/
// request_id/caller/operator/ip。operator 与 caller 由 utils aksk.GinMiddleware
// 验签通过后写入 gin context（归因收编 2026-09-16 统一批）；未过验签的路径
// （探针已 skip）operator 兜底 "system"（§9 口径）。
// 将来启用脱敏只改本函数一处（预留钩子，ADR-003 审计落点机制同款）。
func AccessLog(logger *slog.Logger) gin.HandlerFunc {
	skip := map[string]bool{"/healthz": true, "/readyz": true}
	return func(c *gin.Context) {
		if skip[c.Request.URL.Path] {
			c.Next()
			return
		}
		start := time.Now()
		c.Next()
		q := c.Request.URL.RawQuery
		if len(q) > 4096 {
			q = q[:4096]
		}
		operator := c.GetString(aksk.ContextKeyOperator)
		if operator == "" {
			// ctx 值来自验签后的 GinMiddleware（可信归因）；无验签的挂载场景
			// 回退读头，最终兜底 "system"（§9 口径）
			if operator = c.GetHeader("X-Operator"); operator == "" {
				operator = "system"
			}
		}
		logger.Info("access",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"query", q,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", c.GetString("request_id"),
			"operator", operator,
			"caller", c.GetString(aksk.ContextKeyCaller),
			"ip", c.ClientIP(),
		)
	}
}
