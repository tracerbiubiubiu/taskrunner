// Package app 应用装配与生命周期（Wire DI——微服务结构对齐 zhuzhao，2026-09-04）。
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/tracerbiubiubiu/zhuzhao-utils/errcode"
	"github.com/tracerbiubiubiu/zhuzhao-utils/response"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/notify"
	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/worker"
)

// App 应用实例：HTTP（/healthz /readyz /v1）+ Asynq worker + cron loop + reclaim 扫描器同进程（§5）。
type App struct {
	cfg      *config.Config
	logger   *slog.Logger
	engine   *gin.Engine
	server   *http.Server
	asynqSrv taskWorker
	cron     cronRunner
	reclaim  cronRunner // ADR-004：outbox 补偿扫描器（与 cron 同款最小接口解耦）
	store    *repository.Store
	cb       *callback.Client
	notifier *notify.Notifier
}

// cronRunner cron 循环最小接口（*service.Loop / *service.ReclaimLoop 满足；解耦 app↔service 具体类型）。
type cronRunner interface{ Run(ctx context.Context) }

// taskWorker keeps lifecycle admission and draining observable independently.
type taskWorker interface {
	Start(asynq.Handler) error
	Stop()
	Shutdown()
}

func NewApp(cfg *config.Config, logger *slog.Logger, engine *gin.Engine,
	asynqSrv *asynq.Server, cron, reclaim cronRunner, store *repository.Store, cb *callback.Client) *App {
	// P4-2 死信告警：target 空 = nil（worker 侧 nil 安全跳过）。
	// 半配置 fail-closed 校验在 config.Load（NewApp 无 error 返回位）
	var notifier *notify.Notifier
	if cfg.NotifyTargetURL != "" {
		notifier = notify.New(notify.Config{
			TargetURL: cfg.NotifyTargetURL, AK: cfg.NotifyAK, SK: []byte(cfg.NotifySK)}, logger)
	}
	return &App{cfg: cfg, logger: logger, engine: engine, asynqSrv: asynqSrv,
		cron: cron, reclaim: reclaim, store: store, cb: cb, notifier: notifier}
}

// Run 启动并阻塞至退出信号；停止拉取新任务后排空HTTP及既有worker。
func (a *App) Run() error {
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancelCause(signalCtx)
	defer cancel(nil)
	lease, err := a.store.AcquireInstance(ctx)
	if err != nil {
		return fmt.Errorf("acquire taskrunner instance: %w", err)
	}
	defer lease.Close()
	watcher := watchInstance(ctx, lease, 500*time.Millisecond, cancel)
	defer func() { cancel(nil); <-watcher }()

	// F-8 四超时对齐 zhuzhao：防慢速连接耗尽资源（内网暴露面亦不裸奔）
	a.server = &http.Server{
		Addr: a.cfg.Server.Addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if ctx.Err() != nil {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(response.Response{Code: errcode.ErrServiceUnavailable.Code, Message: errcode.ErrServiceUnavailable.Message})
				return
			}
			a.engine.ServeHTTP(w, r)
		}),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	if err := a.asynqSrv.Start(worker.NewMux(worker.Deps{
		Notifier: a.notifier, Store: a.store, Callback: a.cb, Logger: a.logger,
	})); err != nil {
		return fmt.Errorf("worker start: %w", err)
	}
	serverErr := make(chan error, 1)
	go func() {
		a.logger.Info("taskrunner serving", "addr", a.cfg.Server.Addr, "queue", a.cfg.Queue)
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	var background sync.WaitGroup
	background.Add(2)
	go func() { defer background.Done(); a.cron.Run(ctx) }()
	go func() { defer background.Done(); a.reclaim.Run(ctx) }()
	var runErr error
	select {
	case runErr = <-serverErr:
		a.logger.Error("server failed", "err", runErr)
	case <-ctx.Done():
		if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
			runErr = cause
		}
		a.logger.Info("shutting down", "cause", runErr)
	}
	cancel(runErr)
	// Ownership loss must close worker admission before an accepted HTTP request
	// can hold up draining. Shutdown still joins the already-running tasks below.
	a.asynqSrv.Stop()

	// 排空对称（批次8）：HTTP 排空与 asynq 收尾共用 shutdown_timeout 上限——
	// 总停机 ≈ 2×shutdown_timeout，编排侧 terminationGracePeriod 须 ≥ 该值；
	// 排空超时不再静默（此前 `_ =` 吞掉，在途请求被无痕丢弃——HTTP 侧无
	// requeue 兜底，与任务侧 at-least-once 不同）
	// 三轮审计（2026-09-30）：≤0（容器 env-only 且漏配时）→ 兜底 30s（asynq 侧同款）
	httpDrain := a.cfg.ShutdownTimeout
	if httpDrain <= 0 {
		httpDrain = 30 * time.Second
	}
	shutdownCtx, drainCancel := context.WithTimeout(context.Background(), httpDrain)
	defer drainCancel()
	if err := a.server.Shutdown(shutdownCtx); err != nil {
		a.logger.Warn("http drain timeout/skipped, in-flight requests dropped", "err", err)
	}
	a.asynqSrv.Shutdown() // 等在跑任务收尾
	background.Wait()
	return runErr
}

// Check the dedicated PG session (or the exact local lock inode) throughout
// serving. Lost or unverifiable ownership cancels HTTP/background lifecycle.
type instanceChecker interface{ Check(context.Context) error }

func watchInstance(ctx context.Context, lease instanceChecker, tick time.Duration, cancel context.CancelCauseFunc) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTicker(tick)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				checkCtx, stop := context.WithTimeout(ctx, 2*time.Second)
				err := lease.Check(checkCtx)
				stop()
				if err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return done
}
