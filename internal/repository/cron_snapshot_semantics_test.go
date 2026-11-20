package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// Each PG invocation owns one random schema; never use the legacy helper that
// drops tables in the supplied DSN's default schema.
func cronSnapshotStorePG(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TASKRUNNER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN not set: real PostgreSQL not run")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid dedicated PostgreSQL configuration")
	}
	config.ConnectTimeout = 5 * time.Second
	admin := stdlib.OpenDB(*config)
	t.Cleanup(func() { admin.Close() })
	schema := "cron_snapshot_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("clean own schema: %v", err)
		}
	})
	config.RuntimeParams["search_path"] = schema
	registered := stdlib.RegisterConnConfig(config)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(registered) })
	s, err := OpenPG(registered)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCronExistingSnapshotSemantics(t *testing.T) {
	for _, driver := range []string{"sqlite", "pg"} {
		t.Run(driver, func(t *testing.T) {
			open := openTestStore
			if driver == "pg" {
				open = cronSnapshotStorePG
			}
			testCronExistingSnapshotSemantics(t, open)
		})
	}
}

func TestPGLegacyColumnMigrationWithSiblingSchema(t *testing.T) {
	// Both schemas are visible to the database role. Only the target schema is on
	// the store's search_path, so a matching column in its sibling cannot satisfy
	// the target store's startup check.
	sibling := cronSnapshotStorePG(t)
	target := cronSnapshotStorePG(t)
	ctx := context.Background()
	if err := target.InsertPending(ctx, Run{TaskID: "legacy", Action: "synthetic", EnqueuedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.exec(ctx, `ALTER TABLE job_runs DROP COLUMN execution_id`); err != nil {
		t.Fatal(err)
	}
	if err := sibling.InsertPending(ctx, Run{TaskID: "sibling", Action: "synthetic", EnqueuedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := sibling.MarkRunning(ctx, "sibling", 1, time.Now(), "sibling-delivery"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := migrateColumns(target.db, DriverPG); err != nil {
			t.Fatal(err)
		}
	}
	legacy, err := target.GetByTaskID(ctx, "legacy")
	if err != nil || legacy.Status != StatusPending || legacy.ExecutionID != "" {
		t.Fatalf("target legacy record not available after upgrade: %+v, %v", legacy, err)
	}
	other, err := sibling.GetByTaskID(ctx, "sibling")
	if err != nil || other.ExecutionID != "sibling-delivery" {
		t.Fatalf("sibling record changed: %+v, %v", other, err)
	}
}

// A byte comparison must fail the positive cases. Removing the snapshot guard
// must fail the negative cases, including byte-identical corrupt snapshots.
func testCronExistingSnapshotSemantics(t *testing.T, open func(*testing.T) *Store) {
	const params = `{"n":9007199254740993,"a":[1000,0,0.01]}`
	const equivalent = ` { "a" : [1e3,-0,1e-2], "n":9007199254740993.0 } `
	cases := []struct {
		name   string
		change func(*Run, *Run)
		accept bool
	}{
		{"identical", func(*Run, *Run) {}, true},
		{"nonzero_timeout", func(old, current *Run) {
			old.EnqueuePayload = strings.TrimSuffix(old.EnqueuePayload, "}") + `,"timeout_secs":30}`
			current.EnqueuePayload = old.EnqueuePayload
		}, true},
		{"nonzero_metadata", func(old, current *Run) {
			var p task.Payload
			if err := json.Unmarshal([]byte(old.EnqueuePayload), &p); err != nil {
				t.Fatal(err)
			}
			p.RequestID, p.Dept, p.CallbackURL, p.SubmittedBy, p.SourceIP = "request", "dept", "https://example.invalid/callback", "actor", "192.0.2.1"
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			old.RequestID, old.Dept, old.CallbackURL, old.SubmittedBy, old.SourceIP = p.RequestID, p.Dept, p.CallbackURL, p.SubmittedBy, p.SourceIP
			old.EnqueuePayload = string(raw)
			*current = *old
		}, true},
		{"params_depth_256", func(old, current *Run) {
			old.Params = strings.Repeat("[", 256) + "0" + strings.Repeat("]", 256)
			current.Params = old.Params
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, params, old.Params, 1)
			current.EnqueuePayload = old.EnqueuePayload
		}, true},
		{"params_depth_257", func(old, current *Run) {
			old.Params = strings.Repeat("[", 257) + "0" + strings.Repeat("]", 257)
			current.Params = old.Params
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, params, old.Params, 1)
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"ledger_json_semantics", func(old, _ *Run) { old.Params = equivalent }, true},
		{"payload_json_semantics", func(old, _ *Run) {
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, params, equivalent, 1)
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, `"task_id":"cron:semantic","request_id":""`, `"request_id":"", "task_id":"cron:semantic"`, 1)
		}, true},
		{"legacy_optional_defaults", func(old, _ *Run) {
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, `"request_id":"",`, "", 1)
			old.EnqueuePayload = strings.TrimSuffix(old.EnqueuePayload, "}") + `,"dept":"","submitted_by":"","source_ip":"","timeout_secs":0}`
		}, true},
		{"absent_params_default", func(old, current *Run) {
			old.Params, current.Params = "", "{}"
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, `,"params":`+params, "", 1)
			current.EnqueuePayload = strings.Replace(current.EnqueuePayload, params, "{}", 1)
		}, true},
		{"precise_unequal_ledger_number", func(old, _ *Run) { old.Params = strings.Replace(params, "9007199254740993", "9007199254740992", 1) }, false},
		{"precise_unequal_payload_number", func(old, _ *Run) {
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, "9007199254740993", "9007199254740992", 1)
		}, false},
		{"self_consistent_unequal_params", func(old, _ *Run) {
			old.Params = strings.Replace(params, "9007199254740993", "9007199254740992", 1)
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, params, old.Params, 1)
		}, false},
		{"payload_timeout_changed", func(old, _ *Run) {
			old.EnqueuePayload = strings.TrimSuffix(old.EnqueuePayload, "}") + `,"timeout_secs":1}`
		}, false},
		{"foreign_payload_identity", func(old, current *Run) {
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, `"task_id":"cron:semantic"`, `"task_id":"foreign"`, 1)
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"unknown_payload_field", func(old, current *Run) {
			old.EnqueuePayload = strings.TrimSuffix(old.EnqueuePayload, "}") + `,"future":1}`
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"case_alias_collision", func(old, current *Run) {
			old.EnqueuePayload = strings.TrimSuffix(old.EnqueuePayload, "}") + `,"ACTION":"action"}`
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"null_params_not_empty_object", func(old, current *Run) {
			old.Params, current.Params = "null", "{}"
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, params, "null", 1)
			current.EnqueuePayload = strings.Replace(current.EnqueuePayload, params, "{}", 1)
		}, false},
		{"missing_payload", func(old, current *Run) { old.EnqueuePayload, current.EnqueuePayload = "", "" }, false},
		{"null_payload", func(old, current *Run) { old.EnqueuePayload, current.EnqueuePayload = "null", "null" }, false},
		{"empty_payload", func(old, current *Run) { old.EnqueuePayload, current.EnqueuePayload = "{}", "{}" }, false},
		{"missing_occurrence_job", func(old, current *Run) {
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, `,"job_id":"semantic-job"`, "", 1)
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"duplicate_payload_field", func(old, current *Run) {
			old.EnqueuePayload = strings.TrimSuffix(old.EnqueuePayload, "}") + `,"action":"action"}`
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"duplicate_nested_params", func(old, current *Run) {
			old.Params, current.Params = `{"n":1,"n":1}`, `{"n":1,"n":1}`
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, params, old.Params, 1)
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
		{"malformed_payload", func(old, current *Run) { old.EnqueuePayload, current.EnqueuePayload = "{", "{" }, false},
		{"trailing_payload", func(old, current *Run) { old.EnqueuePayload += " {}"; current.EnqueuePayload = old.EnqueuePayload }, false},
		{"wrong_payload_type", func(old, current *Run) { old.EnqueuePayload, current.EnqueuePayload = "[]", "[]" }, false},
		{"ledger_payload_disagree", func(old, current *Run) {
			old.EnqueuePayload = strings.Replace(old.EnqueuePayload, `"action":"action"`, `"action":"other"`, 1)
			current.EnqueuePayload = old.EnqueuePayload
		}, false},
	}
	for _, field := range []string{"RequestID", "Action", "JobID", "Dept", "CallbackURL", "SubmittedBy", "SourceIP"} {
		cases = append(cases, struct {
			name   string
			change func(*Run, *Run)
			accept bool
		}{
			name: "ledger_" + field, change: func(old, _ *Run) { reflect.ValueOf(old).Elem().FieldByName(field).SetString("other") },
		})
		cases = append(cases, struct {
			name   string
			change func(*Run, *Run)
			accept bool
		}{
			name: "self_consistent_different_" + field, change: func(old, _ *Run) {
				var p task.Payload
				if err := json.Unmarshal([]byte(old.EnqueuePayload), &p); err != nil {
					t.Fatal(err)
				}
				reflect.ValueOf(&p).Elem().FieldByName(field).SetString("other")
				reflect.ValueOf(old).Elem().FieldByName(field).SetString("other")
				raw, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				old.EnqueuePayload = string(raw)
			},
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := open(t)
			ctx := context.Background()
			now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
			job := Job{JobID: "semantic-job", ActionID: "action", Params: params, TriggerType: TriggerCron, CronSpec: "* * * * *", Enabled: true,
				NextRun: sql.NullTime{Valid: true, Time: now.Add(-time.Minute)}, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
			if err := st.CreateJob(ctx, job); err != nil {
				t.Fatal(err)
			}
			storedJob, err := st.GetJob(ctx, job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			p := task.Payload{TaskID: "cron:semantic", Action: "action", JobID: job.JobID, Params: []byte(params)}
			payload, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			old := Run{TaskID: p.TaskID, Action: p.Action, JobID: p.JobID, Params: params, EnqueuePayload: string(payload), EnqueuedAt: now.Add(-time.Hour)}
			current := old
			current.EnqueuedAt = now
			tc.change(&old, &current)
			if err := st.InsertPending(ctx, old); err != nil {
				t.Fatal(err)
			}
			before, err := st.GetByTaskID(ctx, old.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := st.ClaimCronOccurrence(ctx, *storedJob, now.Add(time.Minute), now, current)
			if claimed || (err == nil) != tc.accept {
				t.Fatalf("existing snapshot accept=%v: claimed=%v err=%v", tc.accept, claimed, err)
			}
			afterJob, err := st.GetJob(ctx, job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			wantNext, wantUpdated := storedJob.NextRun.Time, storedJob.UpdatedAt
			if tc.accept {
				wantNext, wantUpdated = now.Add(time.Minute), now
			}
			if !afterJob.NextRun.Time.Equal(wantNext) || !afterJob.UpdatedAt.Equal(wantUpdated) {
				t.Fatalf("schedule transaction: got next=%v updated=%v want next=%v updated=%v", afterJob.NextRun, afterJob.UpdatedAt, wantNext, wantUpdated)
			}
			after, err := st.GetByTaskID(ctx, old.TaskID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("stored occurrence changed: before=%+v after=%+v err=%v", before, after, err)
			}
			_, total, err := st.ListRuns(ctx, RunFilter{})
			if err != nil || total != 1 {
				t.Fatalf("must preserve one occurrence: count=%d err=%v", total, err)
			}
		})
	}
}
