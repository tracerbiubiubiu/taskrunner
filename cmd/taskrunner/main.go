// taskrunner 入口：单进程 = API server（M1 仅 /healthz）+ Asynq worker + Scheduler（空载，M2 接入 job 定义）。
//
// 用法：
//
//	taskrunner serve                     # 常驻服务
//	taskrunner enqueue --action A ...    # M1 的任务入队入口（M2 由 HTTP API 接管）
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	utilslogger "github.com/tracerbiubiubiu/zhuzhao-utils/logger"

	"github.com/tracerbiubiubiu/taskrunner/internal/callback"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
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
		if err := enqueue(config.Load(), args[1:]); err != nil {
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
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		logger.Error("open store failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB}

	// HTTP：M1 仅健康检查；M2 挂全部 v1 API，M4 挂 /monitor（asynqmon）
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	// worker
	srv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: cfg.Concurrency,
		Queues:      map[string]int{cfg.Queue: 1},
		RetryDelayFunc: asynq.DefaultRetryDelayFunc,
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, t *asynq.Task, err error) {
			logger.Error("task ended with error", slogTask(t, err)...)
		}),
	})

	// scheduler：空载接线（无周期任务）；M2 由 job 定义动态注册
	scheduler := asynq.NewScheduler(redisOpt, &asynq.SchedulerOpts{Logger: asynqLogger{logger}})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := scheduler.Run(); err != nil {
			logger.Error("scheduler exited", "err", err)
		}
	}()
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server exited", "err", err)
			stop()
		}
	}()

	if err := srv.Start(worker.NewMux(worker.Deps{
		Store:    st,
		Callback: callback.New(cfg.CallbackTimeout),
		Logger:   logger,
	})); err != nil {
		logger.Error("worker start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("taskrunner serving",
		"http_addr", cfg.HTTPAddr, "redis", cfg.RedisAddr, "db", cfg.DBPath, "queue", cfg.Queue)

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutdownCtx)
	scheduler.Shutdown()
	srv.Shutdown() // 等在跑任务收尾
}

func enqueue(cfg config.Config, args []string) error {
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

	p := task.Payload{
		TaskID:      *taskID,
		RequestID:   *requestID,
		Action:      *action,
		CallbackURL: *callbackURL,
		Params:      json.RawMessage(*params),
		SubmittedBy: *submittedBy,
		SourceIP:    *sourceIP,
		TimeoutSecs: *timeoutSecs,
	}

	// 先入队再落库：入队失败不留孤儿 pending 行；落库失败仅损失 job_runs（worker 侧已容忍缺行）
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer client.Close()
	payload, _ := json.Marshal(p)
	if _, err := client.Enqueue(
		asynq.NewTask(task.TypeCallback, payload),
		asynq.TaskID(p.TaskID),
		asynq.MaxRetry(cfg.MaxRetry),
		asynq.Queue(cfg.Queue),
	); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			fmt.Printf("task_id=%s 已存在（幂等受理）\n", p.TaskID)
			return nil
		}
		return err
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.InsertPending(context.Background(), store.Run{
		TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, CallbackURL: p.CallbackURL,
		SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP, EnqueuedAt: time.Now(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "警告: 入队成功但 job_runs 落库失败（不影响执行，仅损失记录）: %v\n", err)
	}
	fmt.Printf("task_id=%s 已受理（受理 ≠ 执行成功，结果经查询接口获取）\n", p.TaskID)
	return nil
}

// slogTask 给 ErrorHandler 提取稳定字段的日志属性。
func slogTask(t *asynq.Task, err error) []any {
	var p task.Payload
	if jerr := json.Unmarshal(t.Payload(), &p); jerr == nil {
		return []any{"request_id", p.RequestID, "task_id", p.TaskID, "action", p.Action, "type", t.Type(), "err", err}
	}
	return []any{"type", t.Type(), "err", err}
}

// asynqLogger 把 asynq 内部日志桥接到 slog（printf 风格参数；Debug 静默防刷屏）。
type asynqLogger struct{ l *slog.Logger }

func (a asynqLogger) Debug(args ...interface{}) {}
func (a asynqLogger) Info(args ...interface{})  { a.l.Info(fmt.Sprint(args...)) }
func (a asynqLogger) Warn(args ...interface{})  { a.l.Warn(fmt.Sprint(args...)) }
func (a asynqLogger) Error(args ...interface{}) { a.l.Error(fmt.Sprint(args...)) }
func (a asynqLogger) Fatal(args ...interface{}) {
	a.l.Error(fmt.Sprint(args...))
	os.Exit(1)
}
