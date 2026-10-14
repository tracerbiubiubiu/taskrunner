// 契约审计 B3 护栏（2026-10-14）：Payload 与 zhuzhao DeadLetterNotifyPayload
// 成对恒出——零值字段（RequestID/JobID/Attempts）序列化后键不得消失。
package notify

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPayload_AlwaysEmittedKeys(t *testing.T) {
	raw, err := json.Marshal(Payload{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"task_id":`, `"request_id":`, `"action":`, `"job_id":`, `"attempts":0`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("恒出键缺失 %s：%s", key, raw)
		}
	}
}
