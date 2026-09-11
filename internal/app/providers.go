// Package app 应用装配（providers——Wire 依赖的构造函数；wire.go 为注入描述）。
package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	goredis "github.com/redis/go-redis/v9"

	utilslogger "github.com/tracerbiubiubiu/zhuzhao-utils/logger"
	"github.com/tracerbiubiubiu/zhuzhao-utils/postgres"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/handler"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/service"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// provideLogger 应用日志（utils logger：slog + lumberjack 轮转，JSON Lines 稳定字段）。
func provideLogger(cfg *config.Config) *slog.Logger {
	return utilslogger.New(utilslogger.Config{
		Level: cfg.Log.Level, Dir: cfg.Log.Dir,
		MaxSize: cfg.Log.MaxSizeMB, MaxBackups: cfg.Log.MaxBackups, MaxAge: cfg.Log.MaxAgeDays,
	})
}

// provideStore 仓储（委托 OpenStore——serve 与 CLI enqueue 共用同一开库逻辑）。
func provideStore(cfg *config.Config) (*repository.Store, func(), error) {
	return OpenStore(cfg)
}

// OpenStore 按配置双驱动开库（C7：sqlite 开发/单测零依赖；pg 走 utils postgres 构造 DSN，
// database/sql + pgx stdlib 接口无感切换）。导出供 cmd CLI 复用，避免开库逻辑分叉。
func OpenStore(cfg *config.Config) (*repository.Store, func(), error) {
	var st *repository.Store
	var err error
	if cfg.DB.Driver == repository.DriverPG {
		pc := postgres.Config{
			Host: cfg.DB.Host, Port: cfg.DB.Port, User: cfg.DB.User, Password: cfg.DB.Password,
			DBName: cfg.DB.DBName, SSLMode: cfg.DB.SSLMode, ApplicationName: "taskrunner",
		}
		pc.ApplyDefaults()
		st, err = repository.OpenPG(pc.DSN())
	} else {
		st, err = repository.Open(cfg.DB.Path)
	}
	if err != nil {
		return nil, nil, err
	}
	return st, func() { st.Close() }, nil
}

// provideRedisOpt Asynq Redis 连接（队列归属 taskrunner，独立命名空间/库）。
func provideRedisOpt(cfg *config.Config) asynq.RedisClientOpt {
	return asynq.RedisClientOpt{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB}
}

// provideSubmitter 统一受理路径（submit.go：A1 终身幂等）。cleanup 关闭底层 asynq client。
func provideSubmitter(cfg *config.Config, st *repository.Store, opt asynq.RedisClientOpt) (*service.Service, func(), error) {
	cli := asynq.NewClient(opt)
	return &service.Service{
		Store: st, Client: cli, Queue: cfg.Queue, MaxRetry: cfg.MaxRetry,
	}, func() { cli.Close() }, nil
}

// provideInspector Asynq 检视（实时态/取消/重试/死信）。cleanup 关闭连接。
func provideInspector(opt asynq.RedisClientOpt) (*asynq.Inspector, func(), error) {
	ins := asynq.NewInspector(opt)
	return ins, func() { ins.Close() }, nil
}

// provideTaskService 任务域服务。
func provideTaskService(cfg *config.Config, st *repository.Store,
	sub *service.Service, ins *asynq.Inspector, logger *slog.Logger) *service.TaskService {
	return service.NewTaskService(st, sub, ins, cfg.Queue, logger)
}

// provideCron cron 定时触发（分钟级 tick 扫 DB，设计 §10 定案）。
func provideCron(cfg *config.Config, st *repository.Store, sub *service.Service, logger *slog.Logger) *service.Loop {
	return &service.Loop{Store: st, Submit: sub, Logger: logger, Tick: cfg.CronTick}
}

// provideAsynqServer worker 进程（并发/队列/退避重试）。
func provideAsynqServer(cfg *config.Config, opt asynq.RedisClientOpt, logger *slog.Logger) (*asynq.Server, func(), error) {
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second // C5：默认 30s；更长的在途任务被 requeue 重投（at-least-once）
	}
	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency:     cfg.Concurrency,
		ShutdownTimeout: shutdownTimeout,
		Queues:          map[string]int{cfg.Queue: 1},
		RetryDelayFunc:  asynq.DefaultRetryDelayFunc,
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, t *asynq.Task, err error) {
			var p task.Payload
			fields := []any{"type", t.Type(), "err", err}
			if jerr := json.Unmarshal(t.Payload(), &p); jerr == nil {
				fields = append(fields, "request_id", p.RequestID, "task_id", p.TaskID, "action", p.Action)
			}
			logger.Error("task ended with error", fields...)
		}),
	})
	return srv, func() { srv.Stop() }, nil
}

// provideCallback 回调客户端（C9：以 taskrunner 自身 SK 做 AK/SK 签名 + rid 透传）。
func provideCallback(cfg *config.Config) *callback.Client {
	return callback.New(callback.Config{
		Timeout: cfg.CallbackTimeout, AK: cfg.Security.SelfAK, SK: []byte(cfg.Security.SelfSK),
	})
}

// provideKeys API 验签密钥环（C2：预期调用方 AK→SK，当前唯一调用方 zhuzhao）。
func provideKeys(cfg *config.Config) map[string][]byte {
	keys := make(map[string][]byte, len(cfg.Security.Callers))
	for ak, sk := range cfg.Security.Callers {
		keys[ak] = []byte(sk)
	}
	return keys
}

// provideReadyz 依赖就绪探针（C4：检 Redis ping + SQLite 可查询）。
func provideReadyz(cfg *config.Config, st *repository.Store) (func() error, func(), error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB,
	})
	ready := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			return err
		}
		return st.Ping()
	}
	return ready, func() { rdb.Close() }, nil
}

// provideEngine HTTP 路由引擎。
func provideEngine(tasks *service.TaskService, keys map[string][]byte,
	ready func() error, logger *slog.Logger) *gin.Engine {
	return handler.New(handler.Deps{Tasks: tasks, Ready: ready, Keys: keys, Logger: logger})
}
