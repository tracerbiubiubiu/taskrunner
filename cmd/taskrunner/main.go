// taskrunner 入口：单进程 = HTTP API（/healthz + /v1）+ Asynq worker + cronloop（分钟级 tick 触发 cron 定义）。
//
// 用法：
//
//	taskrunner serve                     # 常驻服务（需 TASKRUNNER_API_TOKEN）
//	taskrunner enqueue --action A ...    # CLI 入队（调试用；正式入口是 POST /v1/tasks）
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	utilslogger "github.com/tracerbiubiubiu/zhuzhao-utils/logger"

	"github.com/tracerbiubiubiu/taskrunner/internal/api"
	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/cronloop"
	"github.com/tracerbiubiubiu/taskrunner/internal/enqueue"
	"github.com/tracerbiubiubiu/taskrunner/internal/store"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
	"github.com/tracerbiubiubiu/taskrunner/internal/worker"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		serve(config.Load())
		return
	}
	switch args[0] {
	case "serve":
		serve(config.Load())
	case "enqueue":
		if err := enqueueCmd(config.Load(), args[1:]); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "用法: taskrunner [serve|enqueue]\n")
		os.Exit(2)
	}
}

func serve(cfg config.Config) {
	logger := utilslogger.New(utilslogger.Config{
		Level: cfg.LogLevel,
		Dir:   cfg.LogDir,
	})
	if cfg.APIToken == "" {
		logger.Error("TASKRUNNER_API_TOKEN 未设置：/v1 面向内网但绝不能无鉴权启动，拒绝启动")
		os.Exit(1)
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		logger.Error("open store failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB}
	client := asynq.NewClient(redisOpt)
	defer client.Close()
	submitter := &submitterAdapter{&enqueue.Service{Store: st, Client: client, Queue: cfg.Queue, MaxRetry: cfg.MaxRetry}}
	inspector := asynq.NewInspector(redisOpt)
	defer inspector.Close()

	// HTTP：/healthz + /v1 全量 API（gin + utils errcode/response）
	engine := api.New(api.Deps{
		Store: st, Submitter: submitter, Inspector: inspector,
		Queue: cfg.Queue, Token: cfg.APIToken, Logger: logger,
	})
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: engine}

	// worker
	srv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: cfg.Concurrency,
		Queues:      map[string]int{cfg.Queue: 1},
		RetryDelayFunc: asynq.DefaultRetryDelayFunc,
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, t *asynq.Task, err error) {
			logger.Error("task ended with error", slogTask(t, err)...)
		}),
	})

	// cronloop：分钟级 tick 触发 cron 定义（动态 cron 定稿方案）
	loop := &cronloop.Loop{Store: st, Submit: submitter, Logger: logger, Tick: cfg.CronTick}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go loop.Run(ctx)
	if err := srv.Start(worker.NewMux(worker.Deps{
		Store:    st,
		Callback: callback.New(cfg.CallbackTimeout),
		Logger:   logger,
	})); err != nil {
		logger.Error("worker start failed", "err", err)
		os.Exit(1)
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server exited", "err", err)
			stop()
		}
	}()
	logger.Info("taskrunner serving",
		"http_addr", cfg.HTTPAddr, "redis", cfg.RedisAddr, "db", cfg.DBPath, "queue", cfg.Queue)

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpServer.Shutdown(shutdownCtx)
	srv.Shutdown() // 等在跑任务收尾
}

func enqueueCmd(cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	action := fs.String("action", "", "预置动作 id（必填）")
	callbackURL := fs.String("callback-url", "", "回调 URL（必填，zhuzhao 内网端点）")
	params := fs.String("params", "{}", "业务参数 JSON")
	requestID := fs.String("request-id", "", "关联 zhuzhao 的 request_id")
	taskID := fs.String("task-id", "", "任务 id，缺省自动生成；重复提交即幂等去重")
	submittedBy := fs.String("submitted-by", "", "提交人工号（审计归因）")
	sourceIP := fs.String("source-ip", "", "原始调用 IP（审计归因）")
	timeoutSecs := fs.Int("timeout-secs", 0, "回调超时秒数，0 = 用默认值")
	fs.Parse(args)

	if *action == "" || *callbackURL == "" {
		return errors.New("--action 与 --callback-url 必填")
	}
	if !json.Valid([]byte(*params)) {
		return errors.New("--params 不是合法 JSON")
	}
	if *taskID == "" {
		*taskID = uuid.NewString()
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer client.Close()

	accepted, warning, err := (&enqueue.Service{Store: st, Client: client, Queue: cfg.Queue, MaxRetry: cfg.MaxRetry}).
		Submit(context.Background(), task.Payload{
			TaskID: *taskID, RequestID: *requestID, Action: *action, CallbackURL: *callbackURL,
			Params: json.RawMessage(*params), SubmittedBy: *submittedBy, SourceIP: *sourceIP,
			TimeoutSecs: *timeoutSecs,
		})
	if err != nil {
		return err
	}
	if warning != nil {
		fmt.Fprintf(os.Stderr, "警告: 入队成功但 job_runs 落库失败（不影响执行，仅损失记录）: %v\n", warning)
	}
	if !accepted {
		fmt.Printf("task_id=%s 已存在（幂等受理）\n", *taskID)
		return nil
	}
	fmt.Printf("task_id=%s 已受理（受理 ≠ 执行成功，结果经查询接口获取）\n", *taskID)
	return nil
}

// submitterAdapter 让 *enqueue.Service 满足 api/cronloop 的 Submitter 接口。
type submitterAdapter struct{ *enqueue.Service }

// slogTask 给 ErrorHandler 提取稳定字段的日志属性。
func slogTask(t *asynq.Task, err error) []any {
	var p task.Payload
	if jerr := json.Unmarshal(t.Payload(), &p); jerr == nil {
		return []any{"request_id", p.RequestID, "task_id", p.TaskID, "action", p.Action, "type", t.Type(), "err", err}
	}
	return []any{"type", t.Type(), "err", err}
}
