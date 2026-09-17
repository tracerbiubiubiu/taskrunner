package service

import (
	"errors"
	"testing"

	"github.com/hibiken/asynq"
)

// asynq 错误分类 helper 钉住测试（F60/F64/F65）：分类面是取消/补偿/探针三路共用的
// 判据，asynq 升级时若原文变化在此率先暴露。
func TestAsynqErrClassification(t *testing.T) {
	activeErr := errors.New("asynq: INTERNAL_ERROR: cannot delete task in active state. use CancelProcessing instead.")
	dirtyList := errors.New("task is not found in list: asynq:{jobs}:pending")
	dirtyZset := errors.New("task is not found in zset: asynq:{jobs}:retry")

	if !isDeleteActiveErr(activeErr) {
		t.Fatal("active 态错误必须被识别")
	}
	if isDeleteActiveErr(errors.New("something else")) || isDeleteActiveErr(nil) {
		t.Fatal("误报")
	}
	if !isRedisDirtyStateErr(dirtyList) || !isRedisDirtyStateErr(dirtyZset) {
		t.Fatal("Lua 脏态 error_reply 必须被识别")
	}
	// 与 ErrTaskNotFound（"task not found"）严格区分——消亡确认与脏态语义相反
	if isRedisDirtyStateErr(asynq.ErrTaskNotFound) {
		t.Fatal("ErrTaskNotFound 不得归类为脏态")
	}
	if isRedisDirtyStateErr(errors.New("random")) || isRedisDirtyStateErr(nil) {
		t.Fatal("误报")
	}
	for _, e := range []error{asynq.ErrTaskNotFound, asynq.ErrQueueNotFound} {
		if !isKeyGoneErr(e) {
			t.Fatalf("%v 必须归为键消亡", e)
		}
	}
	if isKeyGoneErr(activeErr) || isKeyGoneErr(dirtyList) || isKeyGoneErr(nil) {
		t.Fatal("误报：active/脏态/nil 不是键消亡")
	}
}
