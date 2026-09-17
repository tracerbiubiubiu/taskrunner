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
	"path/filepath"

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
	// R5：启动期可写探测 fail-closed——非 root 镜像 + 宿主 bind mount 时，目录
	// 属主不受镜像 chown 控制；lumberjack/sqlite 写失败均为静默（仅 stdout，
	// 而 stdout 正是排障通道），必须在起服务前暴露
	probeDir := func(dir, what string) {
		if dir == "" {
			return
		}
		// 先补齐目录：首启时目录可能尚不存在——缺失 ≠ 不可写，直接探测会把
		// 首次部署误判为 fail（MkdirAll 失败才是真不可写）
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("%s 目录无法创建（%s）：%v", what, dir, err)
		}
		probe := filepath.Join(dir, ".write-probe")
		f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatalf("%s 目录不可写（%s）：%v——检查挂载目录属主（容器用户 uid 100）或改用可写目录", what, dir, err)
		}
		f.Close()
		_ = os.Remove(probe)
	}
	probeDir(cfg.Log.Dir, "log")
	if cfg.DB.Driver != "pg" {
		probeDir(filepath.Dir(cfg.DB.Path), "sqlite 数据")
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
		return fmt.Errorf("--params 不是合法 JSON，请检查后重试")
	}
	// 与 HTTP 入口同口径：CLI 原先超界会被入队层静默截断为 86400（用户意图被悄悄改写），显式报错
	if *timeoutSecs < 0 || *timeoutSecs > 86400 {
		return fmt.Errorf("--timeout-secs 取值须为 0（用默认 30 秒）或 1–86400 之间的整数秒")
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
	ins := asynq.NewInspector(asynq.RedisClientOpt{
		Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
	defer ins.Close()

	// 与 serve 路径同参（ADR-003：Inspector 供 A3 补偿撤销，Retention 入队留观）
	sub := &service.Service{Store: st, Client: client, Inspector: ins, Queue: cfg.Queue,
		MaxRetry: cfg.MaxRetry, Retention: cfg.Reclaim.Retention}
	accepted, warning, err := sub.Submit(context.Background(), task.Payload{
		TaskID: *taskID, RequestID: *requestID, Action: *action, CallbackURL: *callbackURL,
		Params: json.RawMessage(*params), SubmittedBy: *submittedBy, SourceIP: *sourceIP,
		TimeoutSecs: *timeoutSecs,
	})
	if err != nil {
		return err
	}
	if warning != nil {
		fmt.Fprintf(os.Stderr, "警告: job_runs 已落库但入队失败（reclaim 扫描器将重试投递，结果经查询接口获取）: %v\n", warning)
	}
	if !accepted {
		fmt.Printf("task_id=%s 已存在（幂等受理）\n", *taskID)
		return nil
	}
	fmt.Printf("task_id=%s 已受理（受理 ≠ 执行成功，结果经查询接口获取）\n", *taskID)
	return nil
}
