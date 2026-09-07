// Package task 定义 Asynq 任务载荷。载荷即跨进程契约，字段命名保持稳定
// （同时是应用日志 / 未来 ES 采集的索引字段，见设计文档 §6）。
package task

import "encoding/json"

// TypeCallback 回调型任务的 Asynq 类型名。
const TypeCallback = "taskrunner:callback"

// Payload 一次任务的全部调度信息。
type Payload struct {
	TaskID      string          `json:"task_id"` // 调用方生成，全局唯一，幂等键
	RequestID   string          `json:"request_id"`
	Action      string          `json:"action"`           // 预置动作 id（zhuzhao 注册表键）
	JobID       string          `json:"job_id,omitempty"` // 经由哪个任务定义触发
	Dept        string          `json:"dept,omitempty"`   // 归属标签快照（C11：job 触发取定义、一次性提交由 zhuzhao 携带；入 job_runs.dept）
	CallbackURL string          `json:"callback_url"`
	Params      json.RawMessage `json:"params,omitempty"`
	SubmittedBy string          `json:"submitted_by,omitempty"` // 工号，审计归因（§4 透传）
	SourceIP    string          `json:"source_ip,omitempty"`    // 原始调用 IP，审计归因
	TimeoutSecs int             `json:"timeout_secs,omitempty"` // 覆盖默认回调超时
}
