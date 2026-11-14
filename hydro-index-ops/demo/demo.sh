#!/usr/bin/env bash
# =============================================================================
# 水文索引运维 · 端到端演示脚本
#
# 覆盖：
#   场景 1  长事务导致 CIC 构建等待 → 释放后构建成功 → 执行计划走新索引
#   场景 2  构建中取消 → 等待会话结束 + 残件删净 → CANCELLED → 可受理新批
#           （取消后的迟到完成不会留索引/报成功）
#   场景 3  清理失败自动重试（HYDRO_CLEANUP_FAIL_FIRST 注入）
#   场景 4  API 重启恢复：停遗留构建、清残件；成功索引与人工索引保留
#
# 用法：
#   ./demo/demo.sh up        # 构建并启动 compose（首次）
#   ./demo/demo.sh all       # 依次跑场景 1~4（默认）
#   ./demo/demo.sh 1|2|3|4   # 只跑单个场景
#
# 依赖：docker compose、curl、jq（缺失 jq 时脚本自动降级为 python3 -m json.tool）
# =============================================================================
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
IDX="idx_readings_station_observed"
PSQL=(docker exec -i hydro-db psql -U hydro -d hydro_warehouse -tA)

if command -v jq >/dev/null 2>&1; then
  jqr() { jq -r "$1"; }
else
  # 无 jq 时的降级：覆盖本脚本实际用到的表达式形态
  jqr() {
    local expr="$1"
    python3 - "$expr" <<'PY'
import sys, json
expr = sys.argv[1]
d = json.load(sys.stdin)
if expr == ".[0] | \"read query ok, latest id=\\(.id)\"":
    print(f"read query ok, latest id={d[0]['id']}")
elif expr == ".[] | \"id=\\(.id) state=\\(.state) phase=\\(.phase)\"":
    for r in d:
        print(f"id={r['id']} state={r['state']} phase={r['phase']}")
else:
    key = expr.lstrip(".")
    print(d[key])
PY
  }
fi

c() { printf '\n\033[1;36m── %s\033[0m\n' "$*"; }
ok() { printf '\033[1;32m✓ %s\033[0m\n' "$*"; }
die() { printf '\033[1;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

req() { # method path [data]
  local m="$1" p="$2" d="${3:-}"
  if [[ -n "$d" ]]; then
    curl -fsS -X "$m" "$BASE$p" -H 'Content-Type: application/json' -d "$d"
  else
    curl -fsS -X "$m" "$BASE$p"
  fi
}

wait_health() {
  c "等待 API 健康检查通过"
  for i in $(seq 1 60); do
    if curl -fsS "$BASE/health" >/dev/null 2>&1; then ok "API 就绪"; return; fi
    sleep 2
  done
  die "API 60 轮内未就绪"
}

idx_exists() {
  local n
  n=$("${PSQL[@]}" -c "SELECT count(*) FROM pg_indexes WHERE indexname='$IDX'")
  [[ "$n" == "1" ]]
}
idx_valid() {
  local v
  v=$("${PSQL[@]}" -c "
    SELECT indisvalid FROM pg_index ix JOIN pg_class c ON c.oid=ix.indexrelid
    WHERE c.relname='$IDX'")
  [[ "$v" == "t" ]]
}
drop_managed_idx() {
  "${PSQL[@]}" -c "DROP INDEX CONCURRENTLY IF EXISTS $IDX" >/dev/null
}
show_batch() {
  req GET "/index-batches/$1" | python3 -m json.tool
}
# 等到批次进入指定状态之一，超时（秒）退出
wait_state() {
  local bid="$1" timeout="${2:-120}"; shift 2
  local deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    local st
    st=$(req GET "/index-batches/$bid" | jqr '.state')
    for want in "$@"; do [[ "$st" == "$want" ]] && { ok "批次 $bid 进入 $st"; return; }; done
    sleep 1
  done
  die "批次 $bid 等待状态 $* 超时"
}
show_pg_activity() {
  "${PSQL[@]}" -c "
    SELECT pid, application_name, state, wait_event_type, wait_event,
           left(query, 60) AS query
    FROM pg_stat_activity
    WHERE application_name LIKE 'hydro-%' AND pid <> pg_backend_pid()
    ORDER BY application_name" || true
}

up() {
  c "构建并启动 docker compose（db + api）"
  docker compose up -d --build
  wait_health
  req GET /health | python3 -m json.tool
  ok "Swagger 文档: $BASE/docs"
}

# --------------------------------------------------------------------------- #
scenario1() {
  c "场景 1：长事务阻塞 → 构建等待 → 释放后成功，期间 readings 读写照常"
  drop_managed_idx
  ok "已清理旧的管理索引"

  c "1.1 开启持有 readings 快照的长事务（90s）"
  local ltx pid
  ltx=$(req POST "/demo/long-tx?seconds=90")
  pid=$(echo "$ltx" | jqr '.pid')
  ok "长事务 pid=$pid"

  c "1.2 受理构建批次（此刻 CIC 必须等待旧快照结束）"
  local bid
  bid=$(req POST /index-batches '{"note":"scenario-1 normal build"}' | jqr '.id')
  ok "批次 id=$bid"

  c "1.3 连续观察构建阶段（应看到 initializing/building 且 wait=Lock.* 等待事件）"
  for i in 1 2 3; do
    req GET "/index-batches/$bid" | jqr '"state=\(.state) phase=\(.phase)"'
    show_pg_activity
    sleep 2
  done

  c "1.4 构建等待期间写入+查询 readings（普通读写不受影响）"
  req POST /readings '{"station_id":1,"observed_at":"2026-11-14T08:00:00Z","water_level":22.315}' \
    | jqr '"inserted reading id=\(.id)"'
  req GET "/readings?station_id=1&limit=3" | jqr '.[0] | "read query ok, latest id=\(.id)"'

  c "1.5 结束长事务，CIC 解除等待并完成"
  req DELETE "/demo/long-tx/$pid" >/dev/null
  wait_state "$bid" 120 SUCCEEDED
  show_batch "$bid"
  idx_valid || die "索引应存在且 valid"
  ok "索引 $IDX 已在线（CREATE INDEX CONCURRENTLY 全程不阻塞读写）"

  c "1.6 执行计划确认走复合索引"
  req GET "/readings/explain?station_id=7" | python3 -m json.tool
}

# --------------------------------------------------------------------------- #
scenario2() {
  c "场景 2：构建中取消 → 会话结束+残件删净 → CANCELLED，随后可受理新批"
  drop_managed_idx

  local ltx pid
  ltx=$(req POST "/demo/long-tx?seconds=300")
  pid=$(echo "$ltx" | jqr '.pid')
  ok "长事务 pid=$pid（让构建停在等待阶段，便于观察取消）"

  local bid
  bid=$(req POST /index-batches '{"note":"scenario-2 cancel"}' | jqr '.id')
  sleep 3
  req GET "/index-batches/$bid" | jqr '"取消前: state=\(.state) phase=\(.phase)"'

  c "2.1 请求取消（状态转 CANCELLING，已向构建会话发 pg_cancel_backend）"
  req POST "/index-batches/$bid/cancel" | jqr '"cancel 响应: state=\(.state) cancel_requested=\(.cancel_requested)"'

  c "2.2 此时再受理必须被拒（只受理一批，清理未结束）"
  if req POST /index-batches '{"note":"should be rejected"}' 2>/dev/null; then
    die "清理未完成时不应受理新批"
  else
    ok "新批次被 409 拒绝"
  fi

  c "2.3 释放长事务 → 构建会话结束 → CLEANING 删残件 → CANCELLED"
  req DELETE "/demo/long-tx/$pid" >/dev/null
  wait_state "$bid" 120 CANCELLED
  show_batch "$bid"
  if idx_exists; then die "取消后不得残留索引（含迟到成功的索引）"; fi
  ok "残件已删净，人工索引仍在:"
  "${PSQL[@]}" -c "SELECT indexname FROM pg_indexes WHERE tablename='readings' ORDER BY 1"

  c "2.4 终态后可受理新批（再演示一次受理后立即取消的快速路径）"
  local bid2
  bid2=$(req POST /index-batches '{"note":"scenario-2 second batch"}' | jqr '.id')
  req POST "/index-batches/$bid2/cancel" >/dev/null
  wait_state "$bid2" 60 CANCELLED
  ok "批次 $bid2 已 CANCELLED，批次历史:"
  req GET "/index-batches?limit=5" | jqr '.[] | "id=\(.id) state=\(.state) phase=\(.phase)"'
}

# --------------------------------------------------------------------------- #
scenario3() {
  c "场景 3：清理失败保留 CLEANING 并自动重试（注入前 2 次 DROP 失败）"
  c "3.1 用 compose override 以 HYDRO_CLEANUP_FAIL_FIRST=2 重建 api"
  cat > docker-compose.override.yml <<'YAML'
services:
  api:
    environment:
      HYDRO_CLEANUP_FAIL_FIRST: "2"
YAML
  docker compose up -d --no-deps api
  wait_health

  drop_managed_idx
  # 受理后立即取消：批次从未进入构建（无会话/残件），但仍必须走完整清理流程，
  # 便于稳定观察 CLEANING 的失败重试而不受长事务时序影响
  local bid
  bid=$(req POST /index-batches '{"note":"scenario-3 retry"}' | jqr '.id')
  req POST "/index-batches/$bid/cancel" >/dev/null

  c "3.2 观察 cleanup_attempts 增长、状态保持 CLEANING（期间 API 与读写照常）"
  for i in $(seq 1 20); do
    req GET "/index-batches/$bid" \
      | jqr '"state=\(.state) attempts=\(.cleanup_attempts) error=\(.error)"'
    req POST /readings '{"station_id":2,"observed_at":"2026-11-14T09:00:00Z","water_level":18.02}' \
      | jqr '"等待重试期间写入成功 id=\(.id)"'
    local st; st=$(req GET "/index-batches/$bid" | jqr '.state')
    [[ "$st" == "CANCELLED" ]] && break
    sleep 3
  done
  wait_state "$bid" 120 CANCELLED
  show_batch "$bid"

  c "3.3 移除 override 并恢复默认配置（HYDRO_CLEANUP_FAIL_FIRST=0）"
  rm -f docker-compose.override.yml
  docker compose up -d --no-deps api
  wait_health
  ok "自动重试成功，残件删净；api 已恢复默认配置"
}

# --------------------------------------------------------------------------- #
scenario4() {
  c "场景 4：重启恢复——停遗留构建、清残件；成功索引与人工索引保留"
  drop_managed_idx

  c "4.1 长事务 + 新批次，让构建停在等待阶段后直接重启 api"
  local ltx pid
  ltx=$(req POST "/demo/long-tx?seconds=300")
  pid=$(echo "$ltx" | jqr '.pid')
  local bid
  bid=$(req POST /index-batches '{"note":"scenario-4 restart recovery"}' | jqr '.id')
  sleep 4
  req GET "/index-batches/$bid" | jqr '"重启前: state=\(.state) build_pid=\(.build_pid)"'

  docker compose restart api
  req DELETE "/demo/long-tx/$pid" >/dev/null || true
  wait_health

  c "4.2 恢复日志与批次终态（BUILDING 中重启 → 终止遗留会话 → 无残件 → FAILED）"
  docker compose logs api --tail 30 | grep -E 'recovery|terminate' || true
  wait_state "$bid" 120 FAILED
  show_batch "$bid"
  if idx_exists; then die "恢复后不应残留无效索引"; fi
  ok "遗留构建会话已终止、残件清净，现在可以受理新批"

  c "4.3 成功索引与人工索引在重启后均保留"
  drop_managed_idx
  local bid2
  ltx=$(req POST "/demo/long-tx?seconds=60"); pid=$(echo "$ltx" | jqr '.pid')
  bid2=$(req POST /index-batches '{"note":"scenario-4 success then restart"}' | jqr '.id')
  req DELETE "/demo/long-tx/$pid" >/dev/null
  wait_state "$bid2" 120 SUCCEEDED

  docker compose restart api
  wait_health
  req GET /health | python3 -m json.tool
  idx_valid || die "成功索引必须在重启后保留"
  "${PSQL[@]}" -c "SELECT indexname FROM pg_indexes WHERE tablename='readings' ORDER BY 1"
  ok "管理索引 valid、人工索引 idx_readings_ingested_manual 完好"
}

main() {
  local what="${1:-all}"
  case "$what" in
    up) up ;;
    1) wait_health; scenario1 ;;
    2) wait_health; scenario2 ;;
    3) wait_health; scenario3 ;;
    4) wait_health; scenario4 ;;
    all)
      up
      scenario1
      scenario2
      scenario3
      scenario4
      c "全部场景演示完成"
      ;;
    *) die "用法: $0 [up|all|1|2|3|4]" ;;
  esac
}
main "$@"
