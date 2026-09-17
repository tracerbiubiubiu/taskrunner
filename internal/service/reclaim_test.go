package service

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tracerbiubiubiu/taskrunner/internal/repository"
	"github.com/tracerbiubiubiu/taskrunner/internal/task"
)

// ---- 测试基建 ----

// capHandler 捕获日志记录（断言 Error/Warn 打点与 F62 聚合）。
type capHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

func (h *capHandler) count(level slog.Level, msgContains string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, msgContains) {
			n++
		}
	}
	return n
}

// reclaimInspector 第二/三域假件：states 缺省 = ErrTaskNotFound（absent）；
// delErr 按 task_id 可编程。
type reclaimInspector struct {
	mu      sync.Mutex
	states  map[string]asynq.TaskState
	getErrs map[string]error
	delErr  map[string]error
	gotInfo []string
	deleted []string
}

func newReclaimInspector() *reclaimInspector {
	return &reclaimInspector{states: map[string]asynq.TaskState{}, getErrs: map[string]error{}, delErr: map[string]error{}}
}

func (f *reclaimInspector) GetTaskInfo(_, id string) (*asynq.TaskInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotInfo = append(f.gotInfo, id)
	if e, ok := f.getErrs[id]; ok {
		return nil, e
	}
	st, ok := f.states[id]
	if !ok {
		return nil, asynq.ErrTaskNotFound
	}
	return &asynq.TaskInfo{State: st}, nil
}

func (f *reclaimInspector) DeleteTask(_, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, id)
	return f.delErr[id]
}

func (f *reclaimInspector) RunTask(string, string) error { return nil }

func (f *reclaimInspector) ListArchivedTasks(string, ...asynq.ListOption) ([]*asynq.TaskInfo, error) {
	return nil, nil
}

func newReclaim(t *testing.T) (*ReclaimLoop, *repository.Store, *fakeEnqueuer, *reclaimInspector, *capHandler) {
	t.Helper()
	st, err := repository.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	fake := &fakeEnqueuer{}
	ins := newReclaimInspector()
	cap := &capHandler{}
	l := &ReclaimLoop{
		Store: st, Client: fake, Inspector: ins, Logger: slog.New(cap),
		Tick: time.Second, StaleAfter: time.Minute, Retention: time.Hour,
		BatchSize: 100, Queue: "jobs", MaxRetry: 3,
	}
	return l, st, fake, ins, cap
}

var reclaimBase = time.Now().Add(-time.Hour) // 宽限外

func seedOld(t *testing.T, s *repository.Store, id, payload string, offset time.Duration) {
	t.Helper()
	if err := s.InsertPending(context.Background(), repository.Run{
		TaskID: id, Action: "a", CallbackURL: "http://x",
		EnqueuedAt: reclaimBase.Add(offset), EnqueuePayload: payload,
	}); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

// ---- 第一轮 ----

// 宽限外才扫：宽限内的 queued=0 行不重投。
func TestReclaimGracePeriod(t *testing.T) {
	l, st, fake, _, _ := newReclaim(t)
	seedOld(t, st, "fresh", `{"task_id":"fresh"}`, 2*time.Hour) // enqueued_at = now-1h+2h = 未来 → 宽限内
	l.ReclaimStalePending(context.Background())
	if fake.count() != 0 {
		t.Fatalf("grace-period row must not be requeued, got %d enqueues", fake.count())
	}
}

// 成功重投 + 冲突收敛（F32）：冲突是崩溃恢复主路径，条件标记收敛、不计失败计数。
func TestReclaimConflictBranchConverges(t *testing.T) {
	l, st, fake, _, cap := newReclaim(t)
	fake.err = asynq.ErrTaskIDConflict // 模拟「入队成功标记失败后重启」——任务已在 Redis
	seedOld(t, st, "cf", `{"task_id":"cf"}`, 0)
	l.ReclaimStalePending(context.Background())
	if q, _ := st.GetQueued(context.Background(), "cf"); !q {
		t.Fatal("conflict branch must converge the row (queued=1)")
	}
	// 第一轮失败计数不叠加（冲突不计计数）；行置位后进入第二轮域，
	// 假件查不到该任务会打 absent Error——属二轮职责，与一轮计数无关
	if cap.count(slog.LevelError, "persistently failing") != 0 {
		t.Fatal("conflict must not count as failure (no domain-1 Error)")
	}
	// 下轮行已离开扫描域，不再重投
	before := fake.count()
	l.ReclaimStalePending(context.Background())
	if fake.count() != before {
		t.Fatal("converged row must leave the domain")
	}
}

// F3/F33：集合成员只重试标记、跳过重投；成功后出集。
func TestReclaimUnmarkedMemberMarkOnly(t *testing.T) {
	l, st, fake, _, _ := newReclaim(t)
	seedOld(t, st, "m1", `{"task_id":"m1"}`, 0)
	l.defaults() // 仅初始化进程内状态（不跑扫描——避免首轮把行置位）
	l.unmarked["m1"] = time.Now()

	l.ReclaimStalePending(context.Background())
	if fake.count() != 0 {
		t.Fatal("member must skip re-enqueue (mark-only retry)")
	}
	if _, ok := l.unmarked["m1"]; ok {
		t.Fatal("member must leave the set after successful mark")
	}
	if q, _ := st.GetQueued(context.Background(), "m1"); !q {
		t.Fatal("member row must be marked queued=1")
	}
}

// F34：集合成员 0 行（list 后、mark 前行已离开 pending，最典型并发取消）→
// DeleteTask 补偿 + 出集（否则成员永久驻留、集合无界增长）。
// 时序构造：同批先行行 a1 的 Enqueue hook 在成员 m2 被标记前将其取消。
func TestReclaimUnmarkedMemberZeroRowsCompensates(t *testing.T) {
	l, st, fake, ins, _ := newReclaim(t)
	ctx := context.Background()
	seedOld(t, st, "a1", `{"task_id":"a1"}`, 0) // 先插入 → id 小 → 同批先处理
	seedOld(t, st, "m2", `{"task_id":"m2"}`, 0)
	l.defaults()
	l.unmarked["m2"] = time.Now()
	fake.hook = func(task.Payload) { // a1 入队时取消 m2（此刻 m2 尚未被本轮标记）
		_, _ = st.MarkCanceledIfPending(ctx, "m2", "canceled via API", time.Now())
	}

	l.ReclaimStalePending(ctx)
	if len(ins.deleted) == 0 || ins.deleted[0] != "m2" {
		t.Fatalf("member 0 rows must compensate via DeleteTask first, got %+v", ins.deleted)
	}
	if _, ok := l.unmarked["m2"]; ok {
		t.Fatal("member must leave the set after 0-row compensation")
	}
	// 行保持 canceled（第一轮不得复活）；其 queued=1 来自同 tick 第三域的
	// 消亡确认出域（MarkCleanupDone）——非第一轮标记，属合法路径
	if r, _ := st.GetByTaskID(ctx, "m2"); r.Status != repository.StatusCanceled {
		t.Fatalf("canceled row must stay canceled, got %s", r.Status)
	}
}

// 非冲突失败计数（F62）：同一行 1–2 次 → 聚合 Warn；≥3 → 聚合 Error（每 tick 单条）。
func TestReclaimFailureCountingAggregated(t *testing.T) {
	l, st, fake, _, cap := newReclaim(t)
	fake.err = errors.New("dial tcp: connection refused")
	seedOld(t, st, "f1", `{"task_id":"f1"}`, 0)
	seedOld(t, st, "f2", `{"task_id":"f2"}`, 0)

	l.ReclaimStalePending(context.Background()) // 各 1 次 → Warn 聚合 1 条（rows=2）
	l.ReclaimStalePending(context.Background()) // 各 2 次 → Warn
	if got := cap.count(slog.LevelWarn, "enqueue failed (will retry next tick)"); got != 2 {
		t.Fatalf("want 2 aggregated Warn ticks, got %d", got)
	}
	if got := cap.count(slog.LevelError, "persistently failing"); got != 0 {
		t.Fatalf("premature Error escalation, got %d", got)
	}

	l.ReclaimStalePending(context.Background()) // 各 3 次 → Error 聚合 1 条
	if got := cap.count(slog.LevelError, "persistently failing"); got != 1 {
		t.Fatalf("want 1 aggregated Error, got %d", got)
	}
}

// 判空/反序列化失败防御（F7/F66）：跳过按 tick 聚合 Warn；连续 ≥3 升级聚合 Error
// （评审六·3：防永久逐行刷屏，给出人工修复信号）。
func TestReclaimEmptyOrCorruptPayloadSkipped(t *testing.T) {
	l, st, fake, _, cap := newReclaim(t)
	seedOld(t, st, "empty", "", 0)
	seedOld(t, st, "corrupt", "{not-json", 0)
	l.ReclaimStalePending(context.Background())
	if fake.count() != 0 {
		t.Fatal("empty/corrupt snapshot must not be re-enqueued")
	}
	if got := cap.count(slog.LevelWarn, "empty/corrupt, skipped"); got != 1 {
		t.Fatalf("want 1 aggregated Warn tick, got %d", got)
	}
	if got := cap.count(slog.LevelError, "empty/corrupt persistently"); got != 0 {
		t.Fatalf("premature Error escalation, got %d", got)
	}
	l.ReclaimStalePending(context.Background())
	l.ReclaimStalePending(context.Background()) // 第 3 次 → 升级聚合 Error
	if got := cap.count(slog.LevelError, "empty/corrupt persistently"); got != 1 {
		t.Fatalf("want 1 aggregated escalation Error, got %d", got)
	}
}

// 计数条目 TTL 回收（评审六·1）：行经非成功路径离开域（此处 = 被 worker 取走转 running）
// 后，失败计数不再永久驻留。
func TestReclaimFailStatTTLPruned(t *testing.T) {
	l, st, fake, _, _ := newReclaim(t)
	fake.err = errors.New("dial tcp: connection refused")
	seedOld(t, st, "p1", `{"task_id":"p1"}`, 0)
	l.ReclaimStalePending(context.Background()) // 失败 1 次，计数在册
	if len(l.enqueueFailCnt) != 1 {
		t.Fatalf("counter should exist, got %+v", l.enqueueFailCnt)
	}
	// 行离开第一轮域（worker 取走 → running）
	if err := st.MarkRunning(context.Background(), "p1", 1, time.Now()); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	// 时钟快进超过 TTL：条目被回收（行已不在域内，无需人为再触发）
	ttl := failStatTTL + time.Minute
	l.Now = func() time.Time { return time.Now().Add(ttl) }
	l.ReclaimStalePending(context.Background())
	if len(l.enqueueFailCnt) != 0 {
		t.Fatalf("stale counter must be pruned, got %+v", l.enqueueFailCnt)
	}
}

// 集合孤儿 TTL 回收（评审七·1）：成员行经其它路径离开 pending（worker 取走 running /
// 取消 canceled）后不再被成员路径检视，条目按 TTL 回收、不永久驻留。
func TestReclaimUnmarkedOrphanPruned(t *testing.T) {
	l, _, _, _, _ := newReclaim(t)
	l.defaults()
	l.unmarked["orph"] = time.Now().Add(-failStatTTL - time.Minute) // 孤儿：超过 TTL 未被检视
	l.unmarked["live"] = time.Now().Add(-time.Minute)               // 新鲜条目（行仍待处理）

	l.ReclaimStalePending(context.Background())
	if _, ok := l.unmarked["orph"]; ok {
		t.Fatal("orphan entry must be pruned after TTL")
	}
	if _, ok := l.unmarked["live"]; !ok {
		t.Fatal("fresh entry must survive prune")
	}
}

// ---- 第二轮（F20/F36/F64）----

func seedStuck(t *testing.T, s *repository.Store, id string, offset time.Duration) {
	t.Helper()
	seedOld(t, s, id, "", offset)
	if _, err := s.MarkQueued(context.Background(), id); err != nil {
		t.Fatalf("mark queued %s: %v", id, err)
	}
}

func TestReclaimStuckStateDispatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   asynq.TaskState
		wantErr bool
	}{
		{"absent → Error", asynq.TaskState(0), true}, // states 缺省 = ErrTaskNotFound
		{"completed → Error", asynq.TaskStateCompleted, true},
		{"archived → Error", asynq.TaskStateArchived, true},
		{"scheduled → 在途跳过", asynq.TaskStateScheduled, false},
		{"active → 在途跳过", asynq.TaskStateActive, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, st, fake, ins, cap := newReclaim(t)
			seedStuck(t, st, "s1", 0)
			if tc.state != 0 {
				ins.states["s1"] = tc.state
			}
			l.ReclaimStalePending(context.Background())
			if got := cap.count(slog.LevelError, ""); got != boolToInt(tc.wantErr) {
				t.Fatalf("Error records = %d, want %v", got, tc.wantErr)
			}
			if fake.count() != 0 {
				t.Fatal("domain-2 must never re-enqueue")
			}
		})
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// F36 游标轮转：>batch 在途行占批位时，异常行 eventual 被检视。
func TestReclaimStuckCursorRotation(t *testing.T) {
	l, st, _, ins, cap := newReclaim(t)
	l.BatchSize = 2
	// t1/t2 在途（scheduled），t3 异常（absent）——enqueued_at 递增保证游标序
	seedStuck(t, st, "t1", 0)
	seedStuck(t, st, "t2", time.Minute)
	seedStuck(t, st, "t3", 2*time.Minute)
	ins.states["t1"] = asynq.TaskStateScheduled
	ins.states["t2"] = asynq.TaskStateScheduled

	l.ReclaimStalePending(context.Background()) // 检视 t1,t2（批满，游标推进）
	if len(ins.gotInfo) != 2 {
		t.Fatalf("batch bound: got %+v", ins.gotInfo)
	}
	l.ReclaimStalePending(context.Background()) // 游标轮转到 t3（absent → Error）
	if got := cap.count(slog.LevelError, ""); got != 1 {
		t.Fatalf("t3 must be alerted after rotation, Error=%d", got)
	}
	if len(ins.gotInfo) != 3 {
		t.Fatalf("inspect sequence: %+v", ins.gotInfo)
	}
}

// ---- 第三域（F45/F48/F49/F50/F56/F64/F65）----

func seedCanceled(t *testing.T, s *repository.Store, id string, offset time.Duration) {
	t.Helper()
	seedOld(t, s, id, "", offset)
	ok, err := s.MarkCanceledIfPending(context.Background(), id, "canceled via API", time.Now())
	if !ok || err != nil {
		t.Fatalf("cancel %s: %v %v", id, ok, err)
	}
}

func TestReclaimCancelCleanupProbes(t *testing.T) {
	l, st, fake, ins, cap := newReclaim(t)
	seedCanceled(t, st, "c-ok", 0)    // DeleteTask 成功 → 出域
	seedCanceled(t, st, "c-gone", 0)  // ErrTaskNotFound → 出域
	seedCanceled(t, st, "c-qgone", 0) // F64：ErrQueueNotFound（队列注册集被清）→ 出域
	seedCanceled(t, st, "c-act", 0)   // active → 不出域
	seedCanceled(t, st, "c-dirty", 0) // Lua 脏态 → 不出域 + 聚合 Warn（F65 分类面）
	ins.delErr["c-act"] = errors.New("asynq: INTERNAL_ERROR: cannot delete task in active state. use CancelProcessing instead.")
	ins.delErr["c-dirty"] = errors.New("task is not found in list: asynq:{jobs}:pending")

	// 宽限内的取消行不探（F45 判据）
	seedOld(t, st, "c-fresh", "", 2*time.Hour)
	if ok, _ := st.MarkCanceledIfPending(context.Background(), "c-fresh", "x", time.Now()); !ok {
		t.Fatal("cancel c-fresh")
	}

	l.ReclaimStalePending(context.Background())

	for _, id := range []string{"c-ok", "c-gone", "c-qgone"} {
		if q, err := st.GetQueued(context.Background(), id); err != nil || !q {
			t.Fatalf("%s must leave domain (queued=1): %v %v", id, q, err)
		}
	}
	if q, _ := st.GetQueued(context.Background(), "c-act"); q {
		t.Fatal("active probe failure must keep row in domain")
	}
	if q, _ := st.GetQueued(context.Background(), "c-dirty"); q {
		t.Fatal("dirty-state probe failure must keep row in domain")
	}
	if got := cap.count(slog.LevelWarn, "dirty task state"); got != 1 {
		t.Fatalf("dirty-state Warn must be aggregated per tick, got %d", got)
	}
	if fake.count() != 0 {
		t.Fatal("domain-3 must never re-enqueue")
	}
	// 宽限内不探
	for _, id := range ins.deleted {
		if id == "c-fresh" {
			t.Fatal("grace-period canceled row must not be probed")
		}
	}
}

// F65：探针持续失败 ≥3 → 聚合 Error；F66：队头失败行不阻塞后续行（游标轮转）。
func TestReclaimCancelProbeFailureVisibilityAndRotation(t *testing.T) {
	l, st, _, ins, cap := newReclaim(t)
	l.BatchSize = 2
	// x1 队头持续失败（脏态/瞬时错误），x2/x3 正常出域——游标轮转保证 x2/x3 不被饿死
	seedCanceled(t, st, "x1", 0)
	seedCanceled(t, st, "x2", time.Minute)
	seedCanceled(t, st, "x3", 2*time.Minute)
	ins.delErr["x1"] = errors.New("redis timeout")

	l.ReclaimStalePending(context.Background()) // x1 fail(1), x2 出域 → 批满
	l.ReclaimStalePending(context.Background()) // x3 出域 → 不足批，回卷
	l.ReclaimStalePending(context.Background()) // x1 fail(2)
	l.ReclaimStalePending(context.Background()) // x1 fail(3) → Error

	if got := cap.count(slog.LevelError, "cancel-cleanup probe persistently failing"); got != 1 {
		t.Fatalf("want 1 aggregated probe Error, got %d", got)
	}
	if q, _ := st.GetQueued(context.Background(), "x1"); q {
		t.Fatal("failing probe must keep row in domain")
	}
	for _, id := range []string{"x2", "x3"} {
		if q, _ := st.GetQueued(context.Background(), id); !q {
			t.Fatalf("%s must leave domain despite stuck head", id)
		}
	}
}

// F64：消亡容忍集不分 sentinel 类型——TaskNotFound 与 QueueNotFound 等价出域。
func TestReclaimKeyGoneTolerance(t *testing.T) {
	l, st, _, ins, _ := newReclaim(t)
	seedCanceled(t, st, "k1", 0)
	ins.delErr["k1"] = asynq.ErrQueueNotFound
	l.ReclaimStalePending(context.Background())
	if q, err := st.GetQueued(context.Background(), "k1"); err != nil || !q {
		t.Fatalf("ErrQueueNotFound must be treated as key-gone: %v %v", q, err)
	}
}

// 编译期守住 task 包引用（payload 反序列化路径）。
var _ = task.Payload{}
