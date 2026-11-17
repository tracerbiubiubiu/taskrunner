package service

import (
	"bytes"
	"context"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
	"strings"
	"unicode/utf8"
)

type callerKey struct{}
type callerIdentity struct{ caller, operator string }

// WithCaller is only set after successful service authentication. It carries
// attribution, never business authorization. Body actor fields are untrusted.
func WithCaller(ctx context.Context, caller, operator string) context.Context {
	return context.WithValue(ctx, callerKey{}, callerIdentity{caller: caller, operator: operator})
}
func (s *TaskService) SetTrustedOperatorCallers(callers []string) {
	s.trustedOperators = map[string]bool{}
	for _, caller := range callers {
		s.trustedOperators[caller] = true
	}
}
func (s *TaskService) attribution(ctx context.Context) (actor, owner string) {
	identity, _ := ctx.Value(callerKey{}).(callerIdentity)
	if identity.caller == "" {
		return "service:taskrunner", "taskrunner"
	}
	operator := strings.TrimSpace(identity.operator)
	if s.trustedOperators[identity.caller] && operator != "" {
		return operator, identity.caller
	}
	return "service:" + identity.caller, identity.caller
}

func validateParameters(params []byte) error {
	if len(params) > 64<<10 {
		return invalid("params 超过上限（64KB）")
	}
	if bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		return invalid("params 不可为 null")
	}
	if _, err := task.EqualJSON(params, params); err != nil {
		return invalid("params 非法: %s", err)
	}
	return nil
}

func validateTaskID(id string) error {
	if id == "" || !utf8.ValidString(id) || utf8.RuneCountInString(id) > 64 {
		return invalid("task_id 必须为1–64个Unicode字符")
	}
	return nil
}
