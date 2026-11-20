package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// Unlike the repository's legacy PG fixture, this helper never drops shared
// tables. Every invocation owns a fresh schema, and every pooled connection has
// that schema as its entire search_path before OpenPG creates any tables.
func openReclaimSnapshotPG(t *testing.T) *repository.Store {
	t.Helper()
	dsn := os.Getenv("TASKRUNNER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TASKRUNNER_TEST_PG_DSN 未设置，跳过真实 PostgreSQL reclaim 快照回归")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse TASKRUNNER_TEST_PG_DSN: invalid dedicated PostgreSQL configuration")
	}
	config.ConnectTimeout = 5 * time.Second
	admin := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close schema owner connection: %v", err)
		}
	})
	schema := "reclaim_snapshot_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create isolated reclaim schema: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop owned reclaim schema: %v", err)
		}
	})
	scopedConfig := config.Copy()
	scopedConfig.RuntimeParams["search_path"] = schema
	registered := stdlib.RegisterConnConfig(scopedConfig)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(registered) })
	store, err := repository.OpenPG(registered)
	if err != nil {
		t.Fatalf("open isolated PostgreSQL store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close isolated PostgreSQL store: %v", err)
		}
	})
	return store
}

func reclaimSnapshotPGFixture(id string) (repository.Run, task.Payload) {
	p := task.Payload{
		TaskID: id, RequestID: "request-" + id, Action: "synthetic:reclaim",
		JobID: "job-" + id, Dept: "synthetic-dept", CallbackURL: "https://callback.example.invalid/reclaim",
		Params: json.RawMessage(`{"key":"value"}`), SubmittedBy: "synthetic-actor",
		SourceIP: "192.0.2.1", TimeoutSecs: 30,
	}
	r := repository.Run{
		TaskID: p.TaskID, RequestID: p.RequestID, Action: p.Action, JobID: p.JobID,
		Dept: p.Dept, CallbackURL: p.CallbackURL, Params: string(p.Params),
		SubmittedBy: p.SubmittedBy, SourceIP: p.SourceIP,
		EnqueuedAt: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC),
	}
	return r, p
}

func insertReclaimSnapshotPG(t *testing.T, store *repository.Store, row repository.Run, p task.Payload) {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal synthetic snapshot: %v", err)
	}
	row.EnqueuePayload = string(raw)
	if err := store.InsertPending(context.Background(), row); err != nil {
		t.Fatalf("insert synthetic reclaim row: %v", err)
	}
}

func newReclaimSnapshotPGLoop(store *repository.Store) (*ReclaimLoop, *fakeEnqueuer, *capHandler) {
	fake, capture := &fakeEnqueuer{}, &capHandler{}
	loop := &ReclaimLoop{
		Store: store, Client: fake, Logger: slog.New(capture), BatchSize: 1,
		StaleAfter: time.Minute, Queue: "synthetic-reclaim", MaxRetry: 3,
		Now: func() time.Time { return time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC) },
	}
	return loop, fake, capture
}

// Removing any ledger comparison must expose an actual wrong delivery or a
// hidden queued=1 row. PostgreSQL must load every comparison column, preserve
// rejected rows, and let the cursor advance to a later valid row at batch size 1.
func TestReclaimSnapshotLedgerMismatchRealPostgres(t *testing.T) {
	for _, field := range []string{
		"task_id", "request_id", "action", "job_id", "dept", "callback_url", "submitted_by", "source_ip", "params",
	} {
		t.Run(field, func(t *testing.T) {
			store := openReclaimSnapshotPG(t)
			ctx := context.Background()
			bad, snapshot := reclaimSnapshotPGFixture("bad-" + field)
			switch field {
			case "task_id":
				snapshot.TaskID = "wrong-snapshot-task"
			case "request_id":
				snapshot.RequestID = "wrong-snapshot-request"
			case "action":
				snapshot.Action = "synthetic:wrong-action"
			case "job_id":
				snapshot.JobID = "wrong-snapshot-job"
			case "dept":
				snapshot.Dept = "wrong-snapshot-dept"
			case "callback_url":
				snapshot.CallbackURL = "https://callback.example.invalid/wrong-target"
			case "submitted_by":
				snapshot.SubmittedBy = "wrong-snapshot-actor"
			case "source_ip":
				snapshot.SourceIP = "192.0.2.2"
			case "params":
				snapshot.Params = json.RawMessage(`{"key":"wrong-value"}`)
			}
			insertReclaimSnapshotPG(t, store, bad, snapshot)
			good, goodSnapshot := reclaimSnapshotPGFixture("good-" + field)
			insertReclaimSnapshotPG(t, store, good, goodSnapshot)
			before, err := store.GetByTaskID(ctx, bad.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			loop, fake, capture := newReclaimSnapshotPGLoop(store)
			loop.ReclaimStalePending(ctx)
			if fake.count() != 0 {
				t.Fatalf("mismatched %s snapshot was delivered", field)
			}
			loop.ReclaimStalePending(ctx)
			if fake.count() != 1 || fake.enqueues[0].TaskID != good.TaskID {
				t.Fatalf("later valid row must advance after the rejected row; deliveries=%+v", fake.enqueues)
			}
			// Complete the finite sweep, then revisit the isolated row twice.
			for tick := 0; tick < 4; tick++ {
				loop.ReclaimStalePending(ctx)
			}
			if fake.count() != 1 {
				t.Fatalf("isolated row was delivered on a later sweep: %+v", fake.enqueues)
			}
			after, err := store.GetByTaskID(ctx, bad.TaskID)
			if err != nil || after.Status != repository.StatusPending || after.Queued {
				t.Fatalf("bad row must remain queryable pending+queued=0: row=%+v err=%v", after, err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected row was rewritten: before=%+v after=%+v", before, after)
			}
			goodAfter, err := store.GetByTaskID(ctx, good.TaskID)
			if err != nil || !goodAfter.Queued || goodAfter.Status != repository.StatusPending {
				t.Fatalf("good row must be marked delivered: row=%+v err=%v", goodAfter, err)
			}
			lastID, err := store.LatestRunID(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := store.ListUnqueued(ctx, loop.Now().Add(-loop.StaleAfter), 0, lastID, 10)
			if err != nil || len(pending) != 1 || pending[0].TaskID != bad.TaskID {
				t.Fatalf("bad row must remain in reclaim discovery domain: rows=%+v err=%v", pending, err)
			}
			originalSnapshot, err := json.Marshal(snapshot)
			if err != nil || pending[0].EnqueuePayload != string(originalSnapshot) {
				t.Fatalf("rejected snapshot must remain unchanged: row=%+v err=%v", pending[0], err)
			}
			visible, total, err := store.ListRuns(ctx, repository.RunFilter{RequestID: bad.RequestID, Status: repository.StatusPending})
			if err != nil || total != 1 || len(visible) != 1 || visible[0].TaskID != bad.TaskID || visible[0].Dept != bad.Dept {
				t.Fatalf("bad row must remain visible through run queries: rows=%+v total=%d err=%v", visible, total, err)
			}
			if capture.count(slog.LevelWarn, "snapshot/ledger mismatch") != 2 || capture.count(slog.LevelError, "snapshot/ledger mismatch persistently") != 1 {
				t.Fatal("isolated row must warn twice and remain visible through persistent-error escalation")
			}
		})
	}
}

// A textual JSON comparison would incorrectly quarantine these equivalent
// params. In particular, the large integer must survive PG text round trips
// without a float64 conversion losing its last digit.
func TestReclaimSnapshotEquivalentJSONRealPostgres(t *testing.T) {
	for _, tc := range []struct {
		name, ledger, snapshot string
	}{
		{"key_order_and_whitespace", `{ "b": [2, {"c":true}], "a": 1 }`, `{"a":1,"b":[2,{"c":true}]}`},
		{"numeric_notation", `{"n":1000.0}`, `{"n":1e3}`},
		{"large_integer", `{"n":9007199254740993}`, `{"n":9007199254740993.0}`},
		{"missing_params", `{}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openReclaimSnapshotPG(t)
			row, snapshot := reclaimSnapshotPGFixture("equivalent-" + tc.name)
			row.Params, snapshot.Params = tc.ledger, json.RawMessage(tc.snapshot)
			insertReclaimSnapshotPG(t, store, row, snapshot)
			loop, fake, capture := newReclaimSnapshotPGLoop(store)
			ctx := context.Background()
			loop.ReclaimStalePending(ctx)
			if fake.count() != 1 || fake.enqueues[0].TaskID != row.TaskID {
				t.Fatalf("semantically equivalent params must be delivered: %+v", fake.enqueues)
			}
			after, err := store.GetByTaskID(ctx, row.TaskID)
			if err != nil || !after.Queued || after.Status != repository.StatusPending || after.Params != tc.ledger {
				t.Fatalf("compatible row must retain ledger params and be marked queued: row=%+v err=%v", after, err)
			}
			if capture.count(slog.LevelWarn, "snapshot/ledger mismatch") != 0 || capture.count(slog.LevelError, "snapshot/ledger mismatch") != 0 {
				t.Fatal("equivalent JSON must not produce a mismatch alert")
			}
		})
	}
}
