// middleware 中间件（基线 §9 对齐 activelist 协议：request_id 回显 / 访问日志 / AK-SK 验签）。
package middleware

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
	"github.com/tracerbiubiubiu/zhuzhao-utils/response"
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
// request_id/operator（X-Operator，缺失兜底 "system"，对齐 activelist 契约）。
// 将来启用脱敏只改本函数一处（预留钩子，ADR-003 审计落点机制同款）。
func AccessLog(logger *slog.Logger) gin.HandlerFunc {
	skip := map[string]bool{"/healthz": true, "/readyz": true}
	return func(c *gin.Context) {
		if skip[c.Request.URL.Path] {
			c.Next()
			return
		}
		start := time.Now()
		// operator 提前读取并入 ctx（归因口径 2026-09-11）：handler 据此兜底业务
		// 归因字段（如 jobs.created_by）——X-Operator 入签名覆盖不可伪造，缺失
		// 兜底 "system"（对齐 activelist COALESCE 惯例）
		operator := c.GetHeader("X-Operator")
		if operator == "" {
			operator = "system"
		}
		c.Set("operator", operator)
		c.Next()
		q := c.Request.URL.RawQuery
		if len(q) > 4096 {
			q = q[:4096]
		}
		logger.Info("access",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"query", q,
			"status", c.Writer.Status(),
			"cost_ms", time.Since(start).Milliseconds(),
			"request_id", c.GetString("request_id"),
			"operator", operator,
			"caller", c.GetString("caller"),
			"ip", c.ClientIP(),
		)
	}
}

// AKSKAuth 服务间 AK/SK HMAC 验签（C2，替换静态 Bearer——2026-09-03 基线修订拍板）。
// keys = 预期调用方密钥环（AK→SK）；Credential 即 caller id（多调用方时代的归属轴）。
// 请求体被读取参与验签后以内存副本还原，handler 的 BindJSON 不受影响。
func AKSKAuth(keys map[string][]byte) gin.HandlerFunc {
	verifier := &aksk.Verifier{Keys: keys}
	return func(c *gin.Context) {
		// 预鉴权读体上限 8MB（b11ef0a④——commit 声称已修实际未落地，本次补交付；
		// 对齐 utils aksk.GinMiddleware 默认）：未认证请求不得以超大 body 占用内存
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8<<20)
		var body []byte
		var err error
		if c.Request.Body != nil {
			if body, err = io.ReadAll(c.Request.Body); err != nil {
				var mbe *http.MaxBytesError
				// 区分两种状态：超限 = 调用方可自行修正（缩小 body）；读取失败 = 连接层问题
				if errors.As(err, &mbe) {
					response.Fail(c, http.StatusRequestEntityTooLarge, 10001, "请求体超过 8MB 上限，请缩小后重试")
				} else {
					response.BadRequest(c, "请求体读取失败，请检查连接后重试")
				}
				c.Abort()
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewBuffer(body))
		}
		if err := verifier.Verify(c.Request, body); err != nil {
			response.Unauthorized(c, authErrMsg(err))
			c.Abort()
			return
		}
		if ak := credentialOf(c.Request); ak != "" {
			c.Set("caller", ak)
		}
		c.Next()
	}
}

// authErrMsg 把 aksk 验签错误映射为中文可读消息：调用方按失败原因即可自查
// （漏带头 / 头格式错 / AK 未开通 / 时钟偏移 / SK 不匹配），不必对照英文库错误排查。
// 注意安全边界：不回显任何签名串或密钥信息。
func authErrMsg(err error) string {
	switch {
	case errors.Is(err, aksk.ErrMissingHeader):
		return "缺少 Authorization 认证头，请使用 AK/SK 签名后重试"
	case errors.Is(err, aksk.ErrBadHeader):
		return "Authorization 头格式错误，应为：HMAC Credential=<AK>,Ts=<RFC3339时间>,Sig=<签名>"
	case errors.Is(err, aksk.ErrUnknownCredential):
		return "访问凭证（AK）未登记或已停用，请联系管理员开通"
	case errors.Is(err, aksk.ErrExpired):
		return "请求时间戳超出允许时间窗口，请校准本机时钟后重试"
	case errors.Is(err, aksk.ErrBadSignature):
		return "签名校验失败，请检查 SK、请求体及 X-Request-ID/X-Operator 头是否与签名一致"
	default:
		return "鉴权失败，请检查 AK/SK 签名后重试"
	}
}

// credentialOf 从已验签请求的 Authorization 头取 Credential（AK；仅作归因展示）。
func credentialOf(req *http.Request) string {
	h := req.Header.Get("Authorization")
	rest, ok := strings.CutPrefix(h, "HMAC ")
	if !ok {
		return ""
	}
	for _, part := range strings.Split(rest, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(part), "="); ok && k == "Credential" {
			return v
		}
	}
	return ""
}
