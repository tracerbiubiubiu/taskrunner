package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// env-only 成功路径 + 日志轮转默认值（锁住 §6「MaxAge 必须显式 / 默认 90 天」承诺，
// 防止“文档承诺、实现落空”复发——该问题曾真实发生）。
func TestLoadEnvOnlyAndRotationDefaults(t *testing.T) {
	t.Setenv("TASKRUNNER_CALLER_ZHUZHAO_SK", "sk-z")
	t.Setenv("TASKRUNNER_SELF_SK", "sk-t")
	t.Setenv("TASKRUNNER_QUEUE", "jobs")

	cfg, err := Load("") // 无 yaml：env-only 模式
	if err != nil {
		t.Fatalf("env-only load: %v", err)
	}
	if cfg.Security.Callers["zhuzhao"] != "sk-z" || cfg.Security.SelfSK != "sk-t" {
		t.Fatalf("security: %+v", cfg.Security)
	}
	if cfg.Log.MaxAgeDays != 90 || cfg.Log.MaxSizeMB != 100 || cfg.Log.MaxBackups != 0 {
		t.Fatalf("rotation defaults: %+v", cfg.Log)
	}
	if cfg.DB.Driver != "sqlite" || cfg.DB.Path == "" {
		t.Fatalf("db defaults: %+v", cfg.DB)
	}
}

func writeYaml(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadFailClosed(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "空密钥环",
			yaml:    "queue: jobs\n",
			wantErr: "callers 为空",
		},
		{
			name:    "密钥环含空 SK",
			yaml:    "queue: jobs\nsecurity:\n  callers:\n    zhuzhao: \"\"\n",
			wantErr: "空 SK",
		},
		{
			name:    "self_sk 缺失（回调签名身份缺失）",
			yaml:    "queue: jobs\nsecurity:\n  callers:\n    zhuzhao: sk-z\n",
			wantErr: "self_ak/self_sk 缺失",
		},
		{
			name:    "空 queue",
			yaml:    "queue: \"\"\nsecurity:\n  callers:\n    zhuzhao: sk-z\n",
			env:     map[string]string{"TASKRUNNER_SELF_SK": "sk-t"},
			wantErr: "queue 不能为空",
		},
		{
			name:    "pg 缺 dbname",
			yaml:    "queue: jobs\ndb:\n  driver: pg\n  host: h\nsecurity:\n  callers:\n    zhuzhao: sk-z\n",
			env:     map[string]string{"TASKRUNNER_SELF_SK": "sk-t"},
			wantErr: "db.dbname",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			// 隔离宿主干扰：相关键一律显式 t.Setenv（自动随子测试恢复；
			// viper 为全局单例，顺序执行下各子测试互不污染的前提是键全覆盖）
			t.Setenv("TASKRUNNER_SELF_AK", "taskrunner")
			// 注意：不要在此无条件 Setenv QUEUE/DB_DRIVER 等——会覆盖 yaml 注入的
			// 缺陷值（如空 queue、driver=pg），导致 fail-closed 断言被环境抹掉
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			_, err := Load(writeYaml(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want err containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
