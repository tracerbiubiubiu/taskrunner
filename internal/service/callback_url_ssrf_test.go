package service

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// W0b（十七批 SSRF 根治·taskrunner 三口）：callback_url 一律服务端定——
// 用户传入非空即 400；空值时使用配置的回调目标（TASKRUNNER_CALLBACK_TARGET_URL，
// 生态内唯一合法消费者=zhuzhao /internal/jobs/callback）。四口收口后
// 出站 URL 永不出自请求体/jobs 旧行（存量列读侧覆盖）。

type ssrfCapSubmitter struct {
	captured task.Payload
}

func (f *ssrfCapSubmitter) Submit(ctx context.Context, p task.Payload) (bool, error, error) {
	f.captured = p
	return true, nil, nil
}

func newSSRFSvc(t *testing.T) (*TaskService, *ssrfCapSubmitter) {
	t.Helper()
	st, err := repository.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	cap := &ssrfCapSubmitter{}
	svc := NewTaskService(st, cap, &fakeInspector{}, "jobs", slog.Default(), "http://zhuzhao:33333/internal/jobs/callback")
	return svc, cap
}

// 口 1：POST /v1/tasks——带 URL 拒收；空 URL 用配置目标。
func TestSSRF_SubmitCallbackURLServerSideOnly(t *testing.T) {
	svc, cap := newSSRFSvc(t)
	ctx := context.Background()

	_, err := svc.Submit(ctx, SubmitInput{Action: "a", CallbackURL: "http://evil.example.com/hook"})
	if err == nil || !strings.Contains(err.Error(), "callback_url") {
		t.Fatalf("user-supplied callback_url must be rejected, got %v", err)
	}

	out, err := svc.Submit(ctx, SubmitInput{Action: "a"})
	if err != nil {
		t.Fatalf("submit with empty url should use configured target: %v", err)
	}
	if !out.Accepted {
		t.Fatal("expected accepted")
	}
	if cap.captured.CallbackURL != "http://zhuzhao:33333/internal/jobs/callback" {
		t.Fatalf("payload must carry configured target, got %q", cap.captured.CallbackURL)
	}

	// 配置缺失 + 请求空 → 明确配置错误（fail-fast，不静默无回调）
	svc2, _ := newSSRFSvc(t)
	svc2.callbackTarget = ""
	_, err = svc2.Submit(ctx, SubmitInput{Action: "a"})
	if err == nil || !strings.Contains(err.Error(), "未配置") {
		t.Fatalf("missing config must fail fast, got %v", err)
	}
}

// 口 2：POST /v1/jobs——建定义同规则；jobs 列写配置值。
func TestSSRF_CreateJobCallbackURLServerSideOnly(t *testing.T) {
	svc, _ := newSSRFSvc(t)
	ctx := context.Background()

	_, err := svc.CreateJob(ctx, JobInput{
		ActionID: "audit_archive", TriggerType: "cron", CronSpec: "* * * * *",
		CallbackURL: "http://evil.example.com/hook",
	})
	if err == nil || !strings.Contains(err.Error(), "callback_url") {
		t.Fatalf("create job with user url must be rejected, got %v", err)
	}

	jv, err := svc.CreateJob(ctx, JobInput{
		ActionID: "audit_archive", TriggerType: "cron", CronSpec: "* * * * *",
	})
	if err != nil {
		t.Fatalf("create job should use configured target: %v", err)
	}
	if jv.CallbackURL != "http://zhuzhao:33333/internal/jobs/callback" {
		t.Fatalf("job row must store configured target, got %q", jv.CallbackURL)
	}
}

// 口 3：PATCH /v1/jobs/update——显式拒改 callback_url。
func TestSSRF_UpdateJobRejectsCallbackURL(t *testing.T) {
	svc, _ := newSSRFSvc(t)
	ctx := context.Background()
	jv, err := svc.CreateJob(ctx, JobInput{ActionID: "a2", TriggerType: "manual"})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	evil := "http://evil.example.com/hook"
	_, err = svc.UpdateJob(ctx, jv.JobID, JobPatch{CallbackURL: &evil})
	if err == nil || !strings.Contains(err.Error(), "callback_url") {
		t.Fatalf("patch callback_url must be rejected, got %v", err)
	}
}
