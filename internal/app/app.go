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

	a.server = &http.Server{Addr: a.cfg.Server.Addr, Handler: a.engine}

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = a.server.Shutdown(shutdownCtx)
	a.asynqSrv.Shutdown() // 等在跑任务收尾
	return nil
}
