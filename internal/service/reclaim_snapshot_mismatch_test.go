package service

// P2-9（F2a 存量任务一致性，2026-11-20 复核续批）：可解析但与 job_runs 权威列语义
// 不符的存量快照，第一轮重投前必须有界识别——隔离（保留行、不投递、不置位、零写）、
// 聚合可见报错（Warn 1–2 次、连续 ≥3 升级 Error）、合法后续行不阻断、
// 不自动覆盖任何一侧（人工修复后才恢复）。
//
// 兼容口径：空 enqueue_payload 沿既有判空路径（前 ADR-004 存量行不进比较）；
// 快照缺键反序列化为零值，与空权威列相等（各代 writer 单源同写 payload+列，
// 分歧只能来自落库后篡改/恢复，即「不明历史」）。
import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// mismatchRow 落一行 pending+queued=0，其 enqueue_payload 与权威列按 field 给出的
// 字段语义不符（模拟存量快照被篡改/备份恢复后的「不明历史」）。
func mismatchRow(t *testing.T, s *repository.Store, id, field, colVal, payloadVal string) {
	t.Helper()
	p := task.Payload{TaskID: id, RequestID: "req-" + id, Action: "a", JobID: "j-" + id,
		Dept: "d-" + id, CallbackURL: "http://x/" + id, Params: []byte(`{"k":"v"}`),
		SubmittedBy: "u-" + id, SourceIP: "10.0.0.1", TimeoutSecs: 30}
	r := repository.Run{TaskID: id, RequestID: p.RequestID, Action: "a", JobID: "j-" + id,
		Dept: "d-" + id, Params: `{"k":"v"}`, CallbackURL: "http://x/" + id,
		SubmittedBy: "u-" + id, SourceIP: "10.0.0.1",
		EnqueuedAt: reclaimBase, EnqueuePayload: ""}
	switch field {
	case "request_id":
		p.RequestID, r.RequestID = payloadVal, colVal
	case "action":
		p.Action, r.Action = payloadVal, colVal
	case "job_id":
		p.JobID, r.JobID = payloadVal, colVal
	case "dept":
		p.Dept, r.Dept = payloadVal, colVal
	case "callback_url":
		p.CallbackURL, r.CallbackURL = payloadVal, colVal
	case "submitted_by":
		p.SubmittedBy, r.SubmittedBy = payloadVal, colVal
	case "source_ip":
		p.SourceIP, r.SourceIP = payloadVal, colVal
	case "params":
		p.Params, r.Params = []byte(payloadVal), colVal
	default:
		t.Fatalf("task_id mismatch owns the row key itself; use a dedicated row fixture")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal %s: %v", id, err)
	}
	r.EnqueuePayload = string(raw)
	if err := s.InsertPending(context.Background(), r); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

// 快照与权威列语义不符：绝不重投、行保持 pending+queued=0 留在域内、
// 聚合 Warn 首现、连续 ≥3 升级 Error。
func TestReclaimSnapshotLedgerMismatchIsolated(t *testing.T) {
	for _, field := range []string{"action", "job_id", "dept", "callback_url", "submitted_by", "source_ip", "params"} {
		t.Run(field, func(t *testing.T) {
			l, st, fake, _, cap := newReclaim(t)
			ctx := context.Background()
			colVal, payloadVal := "ledger-"+field, "snapshot-"+field
			if field == "params" {
				colVal, payloadVal = `{"k":"ledger"}`, `{"k":"snapshot"}`
			}
			mismatchRow(t, st, "mm-"+field, field, colVal, payloadVal)

			l.ReclaimStalePending(ctx)
			l.ReclaimStalePending(ctx)
			if fake.count() != 0 {
				t.Fatalf("mismatched snapshot must never be re-enqueued, got %d", fake.count())
			}
			if got := cap.count(slog.LevelWarn, "snapshot/ledger mismatch"); got != 2 {
				t.Fatalf("want 2 aggregated Warn ticks, got %d", got)
			}
			if got := cap.count(slog.LevelError, "snapshot/ledger mismatch persistently"); got != 0 {
				t.Fatalf("premature Error escalation, got %d", got)
			}
			l.ReclaimStalePending(ctx)
			if got := cap.count(slog.LevelError, "snapshot/ledger mismatch persistently"); got != 1 {
				t.Fatalf("want 1 aggregated escalation Error, got %d", got)
			}
			if fake.count() != 0 {
				t.Fatalf("escalated row must still not be delivered, got %d", fake.count())
			}
			r, err := st.GetByTaskID(ctx, "mm-"+field)
			if err != nil || r.Status != repository.StatusPending || r.Queued {
				t.Fatalf("row must stay pending+queued=0 untouched: %+v %v", r, err)
			}
		})
	}
}

// 不符行不占死批位/不阻断合法后续行：坏行占首个批位，好行下一轮照常投递。
func TestReclaimMismatchedSnapshotDoesNotStarveLaterRows(t *testing.T) {
	l, st, fake, _, _ := newReclaim(t)
	l.BatchSize = 1
	ctx := context.Background()
	mismatchRow(t, st, "bad-mm", "action", "ledger-action", "snapshot-action")
	seedOld(t, st, "good", `{"task_id":"good"}`, 0)
	l.ReclaimStalePending(ctx)
	if fake.count() != 0 {
		t.Fatal("mismatched snapshot must not be enqueued")
	}
	l.ReclaimStalePending(ctx)
	queued, err := st.GetQueued(ctx, "good")
	if err != nil || !queued || fake.count() != 1 {
		t.Fatalf("later valid row must still be projected, queued=%v enqueues=%d err=%v", queued, fake.count(), err)
	}
}

// 兼容旧快照：缺 job_id/dept/request_id/submitted_by/source_ip 键、params 键缺省，
// 与空/{} 权威列语义相等——不得误隔离，照常重投。
func TestReclaimLegacyShapedSnapshotStillProjects(t *testing.T) {
	l, st, fake, _, cap := newReclaim(t)
	ctx := context.Background()
	// pre-C11 形态：payload 只有 task_id/action/callback_url/params；列迁移默认值空。
	raw := `{"task_id":"legacy","action":"a","callback_url":"http://x","params":{"k":"v"}}`
	if err := st.InsertPending(ctx, repository.Run{
		TaskID: "legacy", Action: "a", Params: `{"k":"v"}`, CallbackURL: "http://x",
		EnqueuedAt: reclaimBase, EnqueuePayload: raw,
	}); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	// params 文本形态差异（空白）语义相等。
	if err := st.InsertPending(ctx, repository.Run{
		TaskID: "fmt", Action: "a", Params: `{ "k" : "v" }`, CallbackURL: "http://y",
		EnqueuedAt: reclaimBase.Add(time.Minute), EnqueuePayload: `{"task_id":"fmt","action":"a","callback_url":"http://y","params":{"k":"v"}}`,
	}); err != nil {
		t.Fatalf("seed fmt: %v", err)
	}
	// 列缺省 {} 与快照缺 params 键相等。
	if err := st.InsertPending(ctx, repository.Run{
		TaskID: "empty-params", Action: "a", Params: "{}", CallbackURL: "http://z",
		EnqueuedAt: reclaimBase.Add(2 * time.Minute), EnqueuePayload: `{"task_id":"empty-params","action":"a","callback_url":"http://z"}`,
	}); err != nil {
		t.Fatalf("seed empty-params: %v", err)
	}

	l.ReclaimStalePending(ctx)
	if fake.count() != 3 {
		t.Fatalf("compatible legacy snapshots must project, got %d enqueues", fake.count())
	}
	if got := cap.count(slog.LevelWarn, "snapshot/ledger mismatch"); got != 0 {
		t.Fatalf("compatible snapshots must not be isolated, got %d mismatch Warns", got)
	}
}

// 权威列不是可安全比对的 JSON（重复键），不得猜语义——按不符隔离。
func TestReclaimUnsafeLedgerParamsIsolated(t *testing.T) {
	l, st, fake, _, _ := newReclaim(t)
	ctx := context.Background()
	if err := st.InsertPending(ctx, repository.Run{
		TaskID: "unsafe", Action: "a", Params: `{"k":1,"k":2}`, CallbackURL: "http://x",
		EnqueuedAt: reclaimBase, EnqueuePayload: `{"task_id":"unsafe","action":"a","callback_url":"http://x","params":{"k":1}}`,
	}); err != nil {
		t.Fatalf("seed unsafe: %v", err)
	}
	l.ReclaimStalePending(ctx)
	if fake.count() != 0 {
		t.Fatalf("unsafe ledger params must not be re-enqueued, got %d", fake.count())
	}
}

// 集合成员行快照失配：不得经「只补标记」分支静默收敛（收敛会以不信任的快照
// 宣告「已投递」并把分歧移出可见域）；出集、留域、升级报错。
func TestReclaimMismatchedMemberDoesNotConverge(t *testing.T) {
	l, st, fake, _, cap := newReclaim(t)
	ctx := context.Background()
	mismatchRow(t, st, "mm-member", "action", "ledger-action", "snapshot-action")
	l.defaults()
	l.unmarked["mm-member"] = time.Now()

	l.ReclaimStalePending(ctx)
	if fake.count() != 0 {
		t.Fatal("mismatched member must skip mark-only retry")
	}
	if _, ok := l.unmarked["mm-member"]; ok {
		t.Fatal("mismatched member must leave the mark-only set")
	}
	if q, _ := st.GetQueued(ctx, "mm-member"); q {
		t.Fatal("mismatched member row must not be marked queued=1")
	}
	if got := cap.count(slog.LevelWarn, "snapshot/ledger mismatch"); got != 1 {
		t.Fatalf("mismatch must stay visible in domain 1, got %d Warns", got)
	}
}

// 并发回归（8 轮）：每轮补投与并发取消竞争。失配行在任一轮都不得被投递；
// 合法行恰投一次；被取消行走撤销补偿而非重投。
func TestReclaimMismatchSnapshotSurvivesCancelRace(t *testing.T) {
	for round := 1; round <= 8; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			l, st, fake, ins, _ := newReclaim(t)
			ctx := context.Background()
			mismatchRow(t, st, "mm-race", "params", `{"other":1}`, `{"k":"v"}`)

			type target struct {
				id     string
				cancel bool // 入队瞬间被并发取消
			}
			targets := []target{{id: "g-stay"}, {id: "g-cancel", cancel: true}}
			for _, tg := range targets {
				seedOld(t, st, tg.id, `{"task_id":"`+tg.id+`"}`, 0)
			}
			var mu sync.Mutex
			fake.hook = func(p task.Payload) {
				mu.Lock()
				defer mu.Unlock()
				for _, tg := range targets {
					// 并发取消：行在投递瞬间离开 pending（API 取消竞态窗口）
					if p.TaskID == tg.id && tg.cancel {
						_, _ = st.MarkCanceledIfPending(ctx, &repository.Run{TaskID: tg.id}, "canceled via API", time.Now(), false)
					}
				}
			}

			l.ReclaimStalePending(ctx)

			mu.Lock()
			defer mu.Unlock()
			for _, tg := range targets {
				delivered := 0
				for _, p := range fake.enqueues {
					if p.TaskID == tg.id {
						delivered++
					}
				}
				// at-least-once：被取消行仍会首投一次（随后撤销补偿），不得因
				// 扫描轮转产生第二次投递；合法行恰投一次。
				if delivered != 1 {
					t.Fatalf("row %s must project exactly once per sweep, got %d", tg.id, delivered)
				}
			}
			for _, p := range fake.enqueues {
				if p.TaskID == "mm-race" {
					t.Fatal("mismatched snapshot must never be delivered under cancel races")
				}
			}
			if r, err := st.GetByTaskID(ctx, "mm-race"); err != nil || r.Status != repository.StatusPending || r.Queued {
				t.Fatalf("mismatched row must stay untouched: %+v %v", r, err)
			}
			compensated := false
			for _, id := range ins.deleted {
				if id == "g-cancel" {
					compensated = true
				}
			}
			if !compensated {
				t.Fatalf("canceled delivery must be compensated, deleted=%v", ins.deleted)
			}
		})
	}
}

// 失配报错必须点名不符字段；人工修复快照（改回一致）后恢复重投，
// TTL 回收保证旧计数不把修复后的行误升级。
func TestReclaimMismatchLogNamesFieldsAndRecoversAfterRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "repair.db")
	st, err := repository.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	rawDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer rawDB.Close()

	fake := &fakeEnqueuer{}
	cap := &capHandler{}
	l := &ReclaimLoop{Store: st, Client: fake, Logger: slog.New(cap), Inspector: newReclaimInspector(),
		Tick: time.Second, StaleAfter: time.Minute, Retention: time.Hour,
		BatchSize: 100, Queue: "jobs", MaxRetry: 3}
	ctx := context.Background()
	mismatchRow(t, st, "mm-fields", "action", "ledger-action", "snapshot-action")

	l.ReclaimStalePending(ctx)
	named := false
	for _, r := range cap.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, "snapshot/ledger mismatch") {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "mismatched_fields" && strings.Contains(a.Value.String(), "action") {
					named = true
				}
				return true
			})
		}
	}
	if !named {
		t.Fatal("mismatch log must name the mismatched fields for manual repair")
	}

	// 人工修复：快照改回与权威列一致（仅人工处置路径，无自动改写）。
	row, err := st.GetByTaskID(ctx, "mm-fields")
	if err != nil {
		t.Fatal(err)
	}
	fixed, _ := json.Marshal(task.Payload{TaskID: "mm-fields", RequestID: row.RequestID, Action: row.Action,
		JobID: row.JobID, Dept: row.Dept, CallbackURL: row.CallbackURL, Params: []byte(row.Params),
		SubmittedBy: row.SubmittedBy, SourceIP: row.SourceIP})
	if _, err := rawDB.ExecContext(ctx,
		`UPDATE job_runs SET enqueue_payload = ? WHERE task_id = ?`, string(fixed), "mm-fields"); err != nil {
		t.Fatalf("manual repair fixture: %v", err)
	}
	l.Now = func() time.Time { return time.Now().Add(failStatTTL + time.Minute) }
	l.ReclaimStalePending(ctx)
	if fake.count() != 1 {
		t.Fatalf("repaired row must project again after TTL reset, got %d", fake.count())
	}
	if got := cap.count(slog.LevelError, "snapshot/ledger mismatch persistently"); got != 0 {
		t.Fatalf("repair must stop escalation, got %d Errors", got)
	}
}

// task_id 失配整行独立构造（行键与快照键不一致是失配本体）：重投会以异键执行、
// 失配行自身永不推进——最危险形态钉死为零投递。
func TestReclaimTaskIDMismatchNeverRedeliversForeignKey(t *testing.T) {
	l, st, fake, _, _ := newReclaim(t)
	ctx := context.Background()
	// 行键 mm-row；快照 task_id=other-row（指向另一身份的历史残留）。
	raw, _ := json.Marshal(task.Payload{TaskID: "other-row", Action: "a", CallbackURL: "http://x", Params: []byte(`{}`)})
	if err := st.InsertPending(ctx, repository.Run{
		TaskID: "mm-row", Action: "a", Params: "{}", CallbackURL: "http://x",
		EnqueuedAt: reclaimBase, EnqueuePayload: string(raw),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	l.ReclaimStalePending(ctx)
	if fake.count() != 0 {
		t.Fatal("foreign-key snapshot must not be re-enqueued under the row sweep")
	}
	for _, p := range fake.enqueues {
		if p.TaskID == "other-row" {
			t.Fatal("delivery must not execute against a foreign task identity")
		}
	}
	if q, err := st.GetQueued(ctx, "mm-row"); err != nil || q {
		t.Fatalf("mismatched row must not converge via any branch: queued=%v err=%v", q, err)
	}
}
