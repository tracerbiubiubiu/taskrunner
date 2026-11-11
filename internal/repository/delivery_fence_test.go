package repository

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestDeliveryFence_Storage(t *testing.T) {
	drivers := []string{DriverSQLite}
	if os.Getenv("TASKRUNNER_TEST_PG_DSN") != "" {
		drivers = append(drivers, DriverPG)
	}
	for _, driver := range drivers {
		t.Run(driver, func(t *testing.T) {
			var s *Store
			if driver == DriverPG {
				s = openTestStorePG(t)
			} else {
				s = openTestStore(t)
			}
			ctx := context.Background()
			now := time.Now()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(s.InsertPending(ctx, Run{TaskID: "fence", Action: "synthetic", EnqueuedAt: now}))
			must(s.MarkRunning(ctx, "fence", 1, now, "old"))
			must(s.MarkRunning(ctx, "fence", 1, now.Add(time.Second), "new"))
			if err := s.Finish(ctx, "fence", StatusSucceeded, "stale", 1, now, now.Add(2*time.Second), "old"); !errors.Is(err, ErrDeliverySuperseded) {
				t.Fatalf("same-attempt old delivery not fenced: %v", err)
			}
			r, err := s.GetByTaskID(ctx, "fence")
			must(err)
			if r.Status != StatusRunning {
				t.Fatalf("old completion changed status: %s", r.Status)
			}
			must(s.MarkRunning(ctx, "fence", 2, now.Add(2*time.Second), "retry2"))
			if err := s.MarkRunning(ctx, "fence", 1, now, "late-attempt1"); !errors.Is(err, ErrDeliverySuperseded) {
				t.Fatalf("old retry overwrote newer delivery claim: %v", err)
			}
			must(s.Finish(ctx, "fence", StatusDead, "current", 2, now, now.Add(3*time.Second), "retry2"))
			reset, err := s.ResetPendingIfTerminal(ctx, "fence")
			must(err)
			if !reset {
				t.Fatal("manual retry not reset")
			}
			// Manual retry intentionally restarts asynq retry count, but never reuses the
			// old execution identity even when count=1 recurs.
			must(s.MarkRunning(ctx, "fence", 1, now.Add(4*time.Second), "manual-retry"))
			if err := s.Finish(ctx, "fence", StatusFailed, "old", 1, now, now, "old"); !errors.Is(err, ErrDeliverySuperseded) {
				t.Fatalf("manual retry old completion not fenced: %v", err)
			}
			must(s.Finish(ctx, "fence", StatusSucceeded, "", 1, now, now, "manual-retry"))
			if err := s.MarkRunning(ctx, "fence", 3, now, "late"); !errors.Is(err, ErrTerminalRejected) {
				t.Fatalf("terminal resurrection: %v", err)
			}
			if err := s.MarkRunning(ctx, "fence", 1, now, ""); err == nil {
				t.Fatal("empty identity admitted")
			}
			if err := s.Finish(ctx, "fence", StatusSucceeded, "", 1, now, now, ""); err == nil {
				t.Fatal("empty completion identity admitted")
			}
			// Exercise the startup migration against an actual old shape with an
			// existing row, then repeat it to prove additive/idempotent upgrade.
			_, err = s.exec(ctx, `ALTER TABLE job_runs DROP COLUMN execution_id`)
			must(err)
			must(migrateColumns(s.db, driver))
			must(migrateColumns(s.db, driver))
			var oldID string
			must(s.queryRow(ctx, `SELECT execution_id FROM job_runs WHERE task_id=?`, "fence").Scan(&oldID))
			if oldID != "" {
				t.Fatal("legacy row was rewritten during additive migration")
			}
			r, err = s.GetByTaskID(ctx, "fence")
			must(err)
			if r.Status != StatusSucceeded {
				t.Fatal("legacy terminal state changed during migration")
			}
			must(s.InsertPending(ctx, Run{TaskID: "after-upgrade", Action: "synthetic", EnqueuedAt: now}))
			must(s.MarkRunning(ctx, "after-upgrade", 1, now, "upgraded-delivery"))
			must(s.Finish(ctx, "after-upgrade", StatusSucceeded, "", 1, now, now, "upgraded-delivery"))
		})
	}
}
