#!/usr/bin/env bash
# ADR-003 E2E 端到端验证（可独立复核）——真实进程 + 真 Redis + AK/SK 签名 HTTP。
#
# 用法：scripts/e2e/e2e.sh   （可选 E2E_INCLUDE_CRON=1 追加 cron 到点场景，+≤70s）
# 依赖：docker（起专用 Redis，不碰已有实例）、go、python3、curl
# 端口：6397(serve HTTP) / 6398(回调桩) / 6399(Redis)——须空闲
# 场景：①API 全链路 ②幂等重提 ③4xx 终败 ④5xx→死信→RetryTask 重放
#       ⑤reclaim 宕机修复 ⑥F58 降级取消+第三域收尾 ⑦F32 崩溃恢复冲突收敛
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
WORK=$(mktemp -d /tmp/taskrunner-e2e.XXXXXX)
RNAME=taskrunner-e2e-redis
export E2E_WORK="$WORK"
PASS=0; FAIL=0
SERVE_PID=""; STUB_PID=""

cleanup() {
  [ -n "$SERVE_PID" ] && kill "$SERVE_PID" 2>/dev/null || true
  [ -n "$STUB_PID" ] && kill "$STUB_PID" 2>/dev/null || true
  docker rm -f "$RNAME" >/dev/null 2>&1 || true
  echo "—— 工作目录留存：$WORK"
}
trap cleanup EXIT

ok()   { echo "✓ $1"; PASS=$((PASS+1)); }
fail() { echo "✗ $1"; FAIL=$((FAIL+1)); }
assert_eq() { [ "$2" = "$3" ] && ok "$1" || { fail "$1（期望 [$3] 实得 [$2]）"; }; }
wait_status() { # id want timeout_s：轮询直至 status 匹配
  local id=$1 want=$2 t=${3:-15} st=""
  for ((i = 0; i < t; i++)); do
    st=$("$WORK"/client get "$id" | sed 's/^[0-9]* //' |
      python3 -c "import json,sys; print(json.load(sys.stdin)['data']['status'])" 2>/dev/null || echo "?")
    [ "$st" = "$want" ] && return 0
    sleep 1
  done
  return 1
}
json_data() { sed 's/^[0-9]* //' | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print($1)"; }
db() { python3 -c "
import sqlite3
c = sqlite3.connect('$WORK/e2e.db')
r = c.execute(\"$1\").fetchone()
print(r[0] if r else '<none>')
"; }
redis_up()   { docker start "$RNAME" >/dev/null; sleep 1; }
redis_down() { docker stop "$RNAME" >/dev/null; sleep 1; }

echo "—— 构建 + 环境"
go build -o "$WORK/taskrunner" "$ROOT/cmd/taskrunner" 
(cd "$ROOT" && go build -o "$WORK/cbstub" ./scripts/e2e/cbstub && go build -o "$WORK/client" ./scripts/e2e/client)
docker rm -f "$RNAME" >/dev/null 2>&1 || true
docker run -d --name "$RNAME" -p 6399:6399 redis:7-alpine redis-server --port 6399 >/dev/null
sleep 2
"$WORK/cbstub" & STUB_PID=$!
export TASKRUNNER_HTTP_ADDR=":6397" TASKRUNNER_REDIS_ADDR="127.0.0.1:6399" \
  TASKRUNNER_DB_PATH="$WORK/e2e.db" TASKRUNNER_RECLAIM_TICK="1s" \
  TASKRUNNER_RECLAIM_STALE_AFTER="2s" TASKRUNNER_RECLAIM_BATCH="100" \
  TASKRUNNER_RECLAIM_RETENTION="30s" TASKRUNNER_CALLER_ZHUZHAO_SK="sk-e2e-zhuzhao" \
  TASKRUNNER_SELF_AK="taskrunner" TASKRUNNER_SELF_SK="sk-e2e-self" \
  TASKRUNNER_LOG_DIR="$WORK/logs" TASKRUNNER_CRON_TICK="5s" TASKRUNNER_MAX_RETRY="1" \
  TASKRUNNER_QUEUE="jobs"
"$WORK/taskrunner" serve > "$WORK/serve.log" 2>&1 & SERVE_PID=$!
for i in $(seq 1 15); do curl -sf "http://127.0.0.1:6397/readyz" >/dev/null && break; sleep 1; done
curl -sf "http://127.0.0.1:6397/readyz" >/dev/null || { echo "serve 未就绪"; exit 1; }
C="$WORK/client"

echo "—— ① API 全链路"
$C submit e2e-1 /cb >/dev/null
wait_status e2e-1 succeeded 15 && ok "①提交→执行→succeeded" || fail "①提交→执行"
live=$($C get e2e-1 | json_data "d['live_state']")
assert_eq "①live_state=completed（Retention 留观）" "$live" "completed"

echo "—— ② 幂等重提"
out=$($C submit e2e-1 /cb)
assert_eq "②重复提交 accepted=false" "$(echo "$out" | json_data "d['accepted']")" "False"

echo "—— ③ 4xx 非重试终败"
$C submit e2e-2 /cb404 >/dev/null
wait_status e2e-2 failed 10 && ok "③404 → failed 终态" || fail "③404 → failed"
att=$($C get e2e-2 | json_data "d['attempts']")
assert_eq "③attempts=1（SkipRetry）" "$att" "1"

echo "—— ④ 5xx → 死信 → RetryTask 重放"
$C submit e2e-3 /cb500 >/dev/null
wait_status e2e-3 dead 40 && ok "④重试耗尽 → dead" || fail "④重试耗尽 → dead"
dead=$($C dead | json_data "','.join(x['task_id'] for x in d['list'])")
echo "$dead" | grep -q e2e-3 && ok "④死信列表在列" || fail "④死信列表缺 e2e-3"
$C retry e2e-3 >/dev/null
wait_status e2e-3 succeeded 15 && ok "④RetryTask 重放成功" || fail "④RetryTask 重放"

echo "—— ⑤ reclaim 宕机修复"
redis_down
$C submit e2e-4 /cb >/dev/null
queued=$(db "SELECT queued FROM job_runs WHERE task_id='e2e-4'")
assert_eq "⑤宕机提交 queued=0" "$queued" "0"
redis_up
wait_status e2e-4 succeeded 20 && ok "⑤reclaim 自动修复执行" || fail "⑤reclaim 自动修复"
grep -q "re-enqueued.*e2e-4\|e2e-4.*re-enqueued" "$WORK"/logs/*.log && ok "⑤re-enqueued 日志在案" || fail "⑤缺 re-enqueued 日志"

echo "—— ⑥ F58 降级取消 + 第三域收尾"
redis_down
$C submit e2e-5 /cb >/dev/null
out=$($C cancel e2e-5)
assert_eq "⑥Redis 宕机取消 200（降级）" "$(echo "$out" | json_data "d['status']")" "canceled"
redis_up
sleep 5 # > stale_after+tick：第三域探针确认键消亡后置位
assert_eq "⑥第三域出域 status=canceled" "$(db "SELECT status FROM job_runs WHERE task_id='e2e-5'")" "canceled"
assert_eq "⑥第三域出域 queued=1" "$(db "SELECT queued FROM job_runs WHERE task_id='e2e-5'")" "1"
cb=$(grep -c e2e-5 "$WORK/callbacks.log" 2>/dev/null || true)
assert_eq "⑥已取消任务零回调" "$cb" "0"

echo "—— ⑦ F32 崩溃恢复冲突收敛"
$C submit e2e-6 /cb >/dev/null
wait_status e2e-6 succeeded 15
before=$(grep -c e2e-crash "$WORK/callbacks.log" 2>/dev/null || true)
python3 -c "
import sqlite3
c = sqlite3.connect('$WORK/e2e.db')
c.execute(\"UPDATE job_runs SET task_id='e2e-crash' WHERE task_id='e2e-6'\")
c.execute(\"UPDATE job_runs SET status='pending', queued=0, attempts=0, started_at=NULL, finished_at=NULL, error='' WHERE task_id='e2e-crash'\")
c.commit()
"
sleep 5 # > stale_after+tick：第一轮重投撞冲突（留观键占住 TaskID）→ 条件标记收敛
assert_eq "⑦冲突分支收敛 status=pending" "$(db "SELECT status FROM job_runs WHERE task_id='e2e-crash'")" "pending"
assert_eq "⑦冲突分支收敛 queued=1" "$(db "SELECT queued FROM job_runs WHERE task_id='e2e-crash'")" "1"
after=$(grep -c e2e-crash "$WORK/callbacks.log" 2>/dev/null || true)
assert_eq "⑦零重执行" "$after" "$before"
db "DELETE FROM job_runs WHERE task_id='e2e-crash'" >/dev/null # 清理手术行，免第二轮告警

if [ "${E2E_INCLUDE_CRON:-0}" = "1" ]; then
  echo "—— ⑧ cron 到点自动触发（可选，≤70s）"
  $C createjob /cb >/dev/null
  slept=0
  while [ $slept -lt 75 ]; do
    fired=$(db "SELECT count(*) FROM job_runs WHERE job_id != ''" | tr -d '[]')
    [ "$fired" -ge 1 ] && break
    sleep 5; slept=$((slept+5))
  done
  fired=$(db "SELECT count(*) FROM job_runs WHERE job_id != ''" | tr -d '[]')
  assert_eq "⑧cron 定义自动触发" "$fired" "1"
fi

echo "—— 结果：$PASS 过 / $FAIL 败"
[ "$FAIL" = "0" ]
