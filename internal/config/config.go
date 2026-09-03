// Package config 从环境变量加载 taskrunner 配置（Docker 形态：配置一律走 env）。
// 零值兜底：未设置时取安全默认值。
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	RedisAddr       string // Asynq 队列（独立 Redis）
	RedisPassword   string
	RedisDB         int
	HTTPAddr        string        // API server + /healthz 监听地址
	DBPath          string        // job_runs SQLite 文件
	LogDir          string        // 应用日志目录（utils logger）
	LogLevel        string        // debug|info|warn|error
	CallbackTimeout time.Duration // 回调默认超时，载荷可按任务覆盖
	Concurrency     int           // worker 并发
	MaxRetry        int           // 回调失败默认重试上限
	Queue           string        // Asynq 队列名
}

func Load() Config {
	return Config{
		RedisAddr:       envStr("TASKRUNNER_REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:   envStr("TASKRUNNER_REDIS_PASSWORD", ""),
		RedisDB:         envInt("TASKRUNNER_REDIS_DB", 0),
		HTTPAddr:        envStr("TASKRUNNER_HTTP_ADDR", ":8080"),
		DBPath:          envStr("TASKRUNNER_DB_PATH", "data/taskrunner.db"),
		LogDir:          envStr("TASKRUNNER_LOG_DIR", "logs"),
		LogLevel:        envStr("TASKRUNNER_LOG_LEVEL", "info"),
		CallbackTimeout: envDur("TASKRUNNER_CALLBACK_TIMEOUT", 30*time.Second),
		Concurrency:     envInt("TASKRUNNER_CONCURRENCY", 10),
		MaxRetry:        envInt("TASKRUNNER_MAX_RETRY", 5),
		Queue:           envStr("TASKRUNNER_QUEUE", "jobs"),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
