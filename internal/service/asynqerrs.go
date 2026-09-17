// asynq 错误分类 helper（ADR-003 F60/F64/F65）：CancelTask 降级、A3/扫描器补偿撤销、
// 第三域探针计数共用同一分类，避免各处字符串匹配漂移。
// asynq v0.26.0 对 DeleteTask 的 active 态与 Lua 脏态 error_reply 无导出 sentinel，
// 只能匹配错误文本——原文钉死如下，升级 asynq 时必须回归（ADR-003 实施计划 6）。
package service

import (
	"errors"
	"strings"

	"github.com/hibiken/asynq"
)

// asynqDeleteActiveErrText v0.26.0 internal/rdb.deleteTaskCmd 对 active 态的原文
// （FailedPrecondition 包装，无导出 sentinel）。
const asynqDeleteActiveErrText = "cannot delete task in active state. use CancelProcessing instead."

// asynqDirtyStateErrText deleteTaskCmd 的 redis.error_reply 原文前缀：任务键存在但与
// 所属 list/zset 不一致的脏态（"task is not found in list: ..." / "task is not found in zset: ..."）。
// 注意与 ErrTaskNotFound（"task not found"）区分——脏态意味着任务键仍在 Redis，不可视为消亡。
const asynqDirtyStateErrText = "task is not found in "

// isDeleteActiveErr DeleteTask 命中 active 态（检查后瞬间被 worker 取走）。
func isDeleteActiveErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), asynqDeleteActiveErrText)
}

// isRedisDirtyStateErr DeleteTask 遇 Lua 脏态——任务键仍可能执行，第三域须留域重探。
func isRedisDirtyStateErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), asynqDirtyStateErrText)
}

// isKeyGoneErr 键已消亡确认（F64）：任务不存在，或队列名已不在 asynq:queues 注册集
// （asynq 自身 API 删队列要求空队或先删全部任务键，FLUSHDB 同清注册集——队列不在 ⇒ 任务必不在；
// 唯一例外是运维手工 SREM 注册集，属口径外病态操作）。消亡确认用于：补偿撤销容忍、
// 第三域出域、取消路径容忍。
func isKeyGoneErr(err error) bool {
	return errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound)
}
