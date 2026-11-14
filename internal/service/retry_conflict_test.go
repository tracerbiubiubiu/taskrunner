package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
)

// A failed DB row can outlive the broker's retry -> pending/active transition.
// Reject that intervention as a conflict without erasing its execution record.
func TestRetryBrokerStateConflict(t *testing.T) {
	pending := errors.New("asynq: FAILED_PRECONDITION: task is already in pending state")
	active := errors.New("asynq: FAILED_PRECONDITION: task is already running")
	for _, tc := range []struct {
		name     string
		err      error
		conflict bool
	}{
		{"pending", pending, true},
		{"active", active, true},
		{"wrapped_pending", fmt.Errorf("retry inspector: %w", pending), true},
		{"wrapped_active", fmt.Errorf("retry inspector: %w", active), true},
		{"redis_unavailable", errors.New("dial tcp: connection refused"), false},
		{"unknown_precondition", errors.New("asynq: FAILED_PRECONDITION: unknown queue invariant"), false},
		{"lookalike_message", errors.New("redis connection failed: task is already running"), false},
		{"different_error_code", errors.New("asynq: UNKNOWN: task is already running"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, status := range []string{repository.StatusFailed, repository.StatusDead} {
				t.Run(status, func(t *testing.T) {
					s := newSvc(t, &fakeInspector{runErr: tc.err})
					row(t, s, "retry-race", status)
					ctx := context.Background()
					before, err := s.repo.GetByTaskID(ctx, "retry-race")
					if err != nil {
						t.Fatal(err)
					}
					err = s.RetryTask(ctx, "retry-race")
					if isConflict(err) != tc.conflict {
						t.Errorf("conflict=%v, want %v; error=%v", isConflict(err), tc.conflict, err)
					}
					if !tc.conflict && !errors.Is(err, tc.err) {
						t.Errorf("unrecognized infrastructure error changed: got %v, want %v", err, tc.err)
					}
					after, readErr := s.repo.GetByTaskID(ctx, "retry-race")
					if readErr != nil {
						t.Fatal(readErr)
					}
					if !reflect.DeepEqual(before, after) {
						t.Errorf("rejected retry changed execution record: before=%+v after=%+v", before, after)
					}
				})
			}
		})
	}
}
