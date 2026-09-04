// Package config 配置加载（C6：yaml + `${VAR}` 环境变量展开，对齐 zhuzhao 模式；
// 原全 env 的 TASKRUNNER_* 变量继续生效——BindEnv 显式绑定，无 yaml 也可纯 env 运行）。
package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server struct {
		Addr string `mapstructure:"addr"` // :8080
	}
	Redis struct {
		Addr     string `mapstructure:"addr"`
		Password string `mapstructure:"password"`
		DB       int    `mapstructure:"db"`
	}
	DB struct {
		Path string `mapstructure:"path"` // SQLite（B3：迁移 PG 前的既有实现）
	}
	Queue           string        `mapstructure:"queue"`
	Concurrency     int           `mapstructure:"concurrency"`
	MaxRetry        int           `mapstructure:"max_retry"`
	CallbackTimeout time.Duration `mapstructure:"callback_timeout"`
	CronTick        time.Duration `mapstructure:"cron_tick"`
	Log             struct {
		Level string `mapstructure:"level"`
		Dir   string `mapstructure:"dir"`
	}
	// Security AK/SK（基线 §9：服务间 HMAC 签名）。Callers = 验签密钥环
	// （预期调用方 AK→SK，当前唯一调用方 zhuzhao）；Self = 本服务签名身份
	//（回调 zhuzhao 时使用，AK 须与 zhuzhao internal_jobs.taskrunner_ak 一致）。
	Security struct {
		Callers map[string]string `mapstructure:"callers"`
		SelfAK  string            `mapstructure:"self_ak"`
		SelfSK  string            `mapstructure:"self_sk"`
	}
}

// Load 从 path 读 yaml；env 覆盖（容器部署敏感值注入）。
func Load(path string) (*Config, error) {
	if path != "" {
		viper.SetConfigFile(path)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}
	viper.AutomaticEnv()

	// 全 env 兼容（与原 config 等价，零 yaml 也可跑）
	bind := func(key, env, def string) {
		viper.BindEnv(key, env)
		if viper.GetString(key) == "" {
			viper.SetDefault(key, def)
		}
	}
	bind("server.addr", "TASKRUNNER_HTTP_ADDR", ":8080")
	bind("redis.addr", "TASKRUNNER_REDIS_ADDR", "127.0.0.1:6379")
	bind("redis.password", "TASKRUNNER_REDIS_PASSWORD", "")
	viper.BindEnv("redis.db", "TASKRUNNER_REDIS_DB")
	viper.SetDefault("redis.db", 0)
	bind("db.path", "TASKRUNNER_DB_PATH", "data/taskrunner.db")
	bind("queue", "TASKRUNNER_QUEUE", "jobs")
	viper.BindEnv("concurrency", "TASKRUNNER_CONCURRENCY")
	viper.SetDefault("concurrency", 10)
	viper.BindEnv("max_retry", "TASKRUNNER_MAX_RETRY")
	viper.SetDefault("max_retry", 5)
	bind("callback_timeout", "TASKRUNNER_CALLBACK_TIMEOUT", "30s")
	bind("cron_tick", "TASKRUNNER_CRON_TICK", "30s")
	bind("log.level", "TASKRUNNER_LOG_LEVEL", "info")
	bind("log.dir", "TASKRUNNER_LOG_DIR", "logs")
	bind("security.self_ak", "TASKRUNNER_SELF_AK", "taskrunner")
	bind("security.self_sk", "TASKRUNNER_SELF_SK", "")
	viper.BindEnv("security.callers.zhuzhao", "TASKRUNNER_CALLER_ZHUZHAO_SK")

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.Security.Callers == nil {
		cfg.Security.Callers = map[string]string{}
	}
	if sk := viper.GetString("security.callers.zhuzhao"); sk != "" {
		cfg.Security.Callers["zhuzhao"] = sk
	}
	// fail-closed：验签密钥环为空 = API 完全裸奔——拒绝启动（对齐 zhuzhao internal_jobs 拍板）
	if len(cfg.Security.Callers) == 0 {
		return nil, fmt.Errorf("security.callers 为空（env TASKRUNNER_CALLER_ZHUZHAO_SK 注入 zhuzhao 的 SK）——API 不允许无验签启动")
	}
	return &cfg, nil
}
