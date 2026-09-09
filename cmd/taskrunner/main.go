// taskrunner 入口（微服务结构对齐 zhuzhao，2026-09-04：薄入口——装配走 internal/app Wire）。
//
// 用法：
//
//	taskrunner serve [config.yaml]   # 常驻服务（默认读 configs/config.yaml；纯 env 亦可）
//	taskrunner enqueue ...           # CLI 入队（调试用；正式入口是 POST /v1/tasks）
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/app"
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
	"github.com/tracerbiubiubiu/taskrunner/internal/service"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		serve("configs/config.yaml")
		return
	}
	switch args[0] {
	case "serve":
		path := "configs/config.yaml"
		if len(args) > 1 {
			path = args[1]
		}
		serve(path)
	case "enqueue":
		if err := enqueueCmd(args[1:]); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "用法: taskrunner [serve [config.yaml]|enqueue]\n")
		os.Exit(2)
	}
}

func serve(configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	a, cleanup, err := app.InitializeApp(cfg)
	if err != nil {
		log.Fatalf("init: %v", err)
	}
	defer cleanup()
	if err := a.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// enqueueCmd CLI 直入队（调试路径，手工装配不走 Wire）。
func enqueueCmd(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	cfgPath := fs.String("config", "configs/config.yaml", "配置文件")
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
		return fmt.Errorf("--action 与 --callback-url 必填")
	}
	if !json.Valid([]byte(*params)) {
		return fmt.Errorf("--params 不是合法 JSON")
	}
	if *taskID == "" {
		*taskID = uuid.NewString()
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	st, cleanup, err := app.OpenStore(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
	defer client.Close()

	sub := &service.Service{Store: st, Client: client, Queue: cfg.Queue, MaxRetry: cfg.MaxRetry}
	accepted, warning, err := sub.Submit(context.Background(), task.Payload{
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
