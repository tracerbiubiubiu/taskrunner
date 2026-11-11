package config

import (
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

// Keep env-only, frozen yaml and Docker image on the allocated service port;
// explicit deployment overrides still take precedence over the baseline.
func TestLoadHTTPPortAlignment(t *testing.T) {
	for _, tc := range []struct{ name, path, env, want string }{
		{"env_only_default", "", "", ":18080"},
		{"frozen_yaml", filepath.Join("..", "..", "configs", "config.yaml"), "", ":18080"},
		{"explicit_env_over_yaml", filepath.Join("..", "..", "configs", "config.yaml"), ":19291", ":19291"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv("TASKRUNNER_HTTP_ADDR", tc.env)
			t.Setenv("TASKRUNNER_CALLER_ZHUZHAO_SK", "audit-port")
			t.Setenv("TASKRUNNER_SELF_SK", "audit-port")
			t.Setenv("TASKRUNNER_QUEUE", "jobs")
			cfg, err := Load(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Server.Addr != tc.want {
				t.Fatalf("allocated HTTP port mismatch: got=%q want=%q", cfg.Server.Addr, tc.want)
			}
		})
	}
}
