// Package config 配置加载（C6：yaml + `${VAR}` 环境变量展开，对齐 zhuzhao 模式；
// 原全 env 的 TASKRUNNER_* 变量继续生效——BindEnv 显式绑定，无 yaml 也可纯 env 运行）。
package config

import (
	"fmt"
	"os"
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
		Driver   string `mapstructure:"driver"` // sqlite | pg（C7；默认 sqlite，开发/单测零依赖）
		Path     string `mapstructure:"path"`   // sqlite 文件路径
		Host     string `mapstructure:"host"`
		Port     int    `mapstructure:"port"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
		DBName   string `mapstructure:"dbname"`
		SSLMode  string `mapstructure:"sslmode"`
	}
	Queue           string        `mapstructure:"queue"`
	Concurrency     int           `mapstructure:"concurrency"`
	MaxRetry        int           `mapstructure:"max_retry"`
	CallbackTimeout time.Duration `mapstructure:"callback_timeout"`
	CronTick        time.Duration `mapstructure:"cron_tick"`
	Log             struct {
		Level      string `mapstructure:"level"`
		Dir        string `mapstructure:"dir"`
		MaxSizeMB  int    `mapstructure:"max_size_mb"`
		MaxBackups int    `mapstructure:"max_backups"`
		MaxAgeDays int    `mapstructure:"max_age_days"`
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
// 文件不存在 → env-only 模式（镜像可只注入 TASKRUNNER_* 环境变量运行）。
func Load(path string) (*Config, error) {
	if path != "" {
		if _, statErr := os.Stat(path); statErr == nil {
			viper.SetConfigFile(path)
			if err := viper.ReadInConfig(); err != nil {
				return nil, fmt.Errorf("read config: %w", err)
			}
		} else if !os.IsNotExist(statErr) {
			return nil, fmt.Errorf("stat config: %w", statErr)
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
	bind("db.driver", "TASKRUNNER_DB_DRIVER", "sqlite")
	bind("db.path", "TASKRUNNER_DB_PATH", "data/taskrunner.db")
	bind("db.host", "TASKRUNNER_DB_HOST", "127.0.0.1")
	viper.BindEnv("db.port", "TASKRUNNER_DB_PORT")
	viper.SetDefault("db.port", 5432)
	bind("db.user", "TASKRUNNER_DB_USER", "")
	viper.BindEnv("db.password", "TASKRUNNER_DB_PASSWORD")
	bind("db.dbname", "TASKRUNNER_DB_NAME", "")
	bind("db.sslmode", "TASKRUNNER_DB_SSLMODE", "disable")
	bind("queue", "TASKRUNNER_QUEUE", "jobs")
	viper.BindEnv("concurrency", "TASKRUNNER_CONCURRENCY")
	viper.SetDefault("concurrency", 10)
	viper.BindEnv("max_retry", "TASKRUNNER_MAX_RETRY")
	viper.SetDefault("max_retry", 5)
	bind("callback_timeout", "TASKRUNNER_CALLBACK_TIMEOUT", "30s")
	bind("cron_tick", "TASKRUNNER_CRON_TICK", "30s")
	bind("log.level", "TASKRUNNER_LOG_LEVEL", "info")
	bind("log.dir", "TASKRUNNER_LOG_DIR", "logs")
	// 日志轮转（§6：MaxAge 必须显式——lumberjack 零值不限天数，文件含个人信息）
	viper.BindEnv("log.max_size_mb", "TASKRUNNER_LOG_MAX_SIZE_MB")
	viper.SetDefault("log.max_size_mb", 100)
	viper.BindEnv("log.max_backups", "TASKRUNNER_LOG_MAX_BACKUPS")
	viper.SetDefault("log.max_backups", 0)   // 不限份数，由天数控
	viper.SetDefault("log.max_age_days", 90) // 显式默认 90 天（M4 定 job_runs 保留期时可一并调整）
	viper.BindEnv("log.max_age_days", "TASKRUNNER_LOG_MAX_AGE_DAYS")
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
	// fail-closed：密钥环存在空 SK 条目同样裸奔（HMAC 空密钥可伪造）——拒绝启动
	for ak, sk := range cfg.Security.Callers {
		if sk == "" {
			return nil, fmt.Errorf("security.callers[%q] 为空 SK——空密钥等于无鉴权，拒绝启动", ak)
		}
	}
	// fail-closed：验签密钥环为空 = API 完全裸奔——拒绝启动（对齐 zhuzhao internal_jobs 拍板）
	if len(cfg.Security.Callers) == 0 {
		return nil, fmt.Errorf("security.callers 为空（env TASKRUNNER_CALLER_ZHUZHAO_SK 注入 zhuzhao 的 SK）——API 不允许无验签启动")
	}
	// fail-closed：driver=pg 但库名缺失 → DSN 无法定位库，拒绝启动
	if cfg.DB.Driver == "pg" && cfg.DB.DBName == "" {
		return nil, fmt.Errorf("db.driver=pg 需配置 db.dbname（env TASKRUNNER_DB_NAME）")
	}
	// fail-closed：C9 回调签名身份缺失 = 每次回调被 zhuzhao /internal 验签 401 → 无限重试
	if cfg.Security.SelfAK == "" || cfg.Security.SelfSK == "" {
		return nil, fmt.Errorf("security.self_ak/self_sk 缺失（env TASKRUNNER_SELF_AK/SELF_SK）——回调无签名将被 zhuzhao 拒绝")
	}
	// 显式空串会压过 SetDefault，导致 worker 监听 "" 队列——拒绝
	if cfg.Queue == "" {
		return nil, fmt.Errorf("queue 不能为空")
	}
	return &cfg, nil
}
