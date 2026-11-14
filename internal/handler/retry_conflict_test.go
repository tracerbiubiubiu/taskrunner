package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/zhuzhao-utils/errcode"
)

func TestRetryBrokerConflictHTTP(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   int
	}{
		{"pending", errors.New("asynq: FAILED_PRECONDITION: task is already in pending state"), http.StatusConflict, errcode.ErrConflict.Code},
		{"wrapped_active", fmt.Errorf("inspector: %w", errors.New("asynq: FAILED_PRECONDITION: task is already running")), http.StatusConflict, errcode.ErrConflict.Code},
		{"redis_unavailable", errors.New("dial tcp: synthetic private connection refused"), http.StatusInternalServerError, errcode.ErrInternal.Code},
		{"unknown_precondition", errors.New("asynq: FAILED_PRECONDITION: unknown synthetic invariant"), http.StatusInternalServerError, errcode.ErrInternal.Code},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.insp.runErr = tc.err
			ctx := context.Background()
			id := "retry-conflict"
			if err := f.st.InsertPending(ctx, repository.Run{TaskID: id, Action: "synthetic", EnqueuedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if err := f.st.MarkRunning(ctx, id, 1, time.Now(), "fixture-delivery"); err != nil {
				t.Fatal(err)
			}
			if err := f.st.Finish(ctx, id, repository.StatusFailed, "synthetic 500", 1, time.Now(), time.Now(), "fixture-delivery"); err != nil {
				t.Fatal(err)
			}
			before, err := f.st.GetByTaskID(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			w := f.do(t, http.MethodPost, "/v1/tasks/retry", map[string]any{"task_id": id})
			if w.Code != tc.status {
				t.Errorf("HTTP status=%d, want %d: %s", w.Code, tc.status, w.Body.String())
			}
			var body struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Code != tc.code || body.Message == "" {
				t.Errorf("response code/message=%+v, want code %d and safe explanation", body, tc.code)
			}
			if strings.Contains(w.Body.String(), "asynq:") || strings.Contains(w.Body.String(), "synthetic private") || strings.Contains(w.Body.String(), "unknown synthetic") {
				t.Errorf("response leaked infrastructure detail: %s", w.Body.String())
			}
			after, err := f.st.GetByTaskID(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Errorf("rejected HTTP retry changed record: before=%+v after=%+v", before, after)
			}
		})
	}
}
