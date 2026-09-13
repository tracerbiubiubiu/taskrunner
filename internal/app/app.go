// Package app 应用装配与生命周期（Wire DI——微服务结构对齐 zhuzhao，2026-09-04）。
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/worker"
)

// App 应用实例：HTTP（/healthz /readyz /v1）+ Asynq worker + cron loop 同进程（§5）。
type App struct {
	cfg      *config.Config
	logger   *slog.Logger
	engine   *gin.Engine
	server   *http.Server
	asynqSrv *asynq.Server
	cron     cronRunner
	store    *repository.Store
	cb       *callback.Client
}

// cronRunner cron 循环最小接口（*service.Loop 满足；解耦 app↔service 具体类型）。
type cronRunner interface{ Run(ctx context.Context) }

func NewApp(cfg *config.Config, logger *slog.Logger, engine *gin.Engine,
	asynqSrv *asynq.Server, cron cronRunner, store *repository.Store, cb *callback.Client) *App {
	return &App{cfg: cfg, logger: logger, engine: engine, asynqSrv: asynqSrv,
		cron: cron, store: store, cb: cb}
}

// Run 启动并阻塞至退出信号；优雅停止（HTTP drain → worker 收尾）。
func (a *App) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// F-8 四超时对齐 zhuzhao：防慢速连接耗尽资源（内网暴露面亦不裸奔）
	a.server = &http.Server{
		Addr:              a.cfg.Server.Addr,
		Handler:           a.engine,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		a.logger.Info("taskrunner serving", "addr", a.cfg.Server.Addr, "queue", a.cfg.Queue)
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	go a.cron.Run(ctx)
	if err := a.asynqSrv.Start(worker.NewMux(worker.Deps{
		Store: a.store, Callback: a.cb, Logger: a.logger,
	})); err != nil {
		return fmt.Errorf("worker start: %w", err)
	}

	select {
	case err := <-serverErr:
		a.logger.Error("server failed", "err", err)
		return err
	case <-ctx.Done():
		a.logger.Info("shutting down")
	}

	// 排空对称（批次8）：HTTP 排空与 asynq 收尾共用 shutdown_timeout 上限——
	// 总停机 ≈ 2×shutdown_timeout，编排侧 terminationGracePeriod 须 ≥ 该值；
	// 排空超时不再静默（此前 `_ =` 吞掉，在途请求被无痕丢弃——HTTP 侧无
	// requeue 兜底，与任务侧 at-least-once 不同）
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	if err := a.server.Shutdown(shutdownCtx); err != nil {
		a.logger.Warn("http drain timeout/skipped, in-flight requests dropped", "err", err)
	}
	a.asynqSrv.Shutdown() // 等在跑任务收尾
	return nil
}
