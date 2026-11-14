# 水文数仓 · 水位明细索引运维与站日小时汇总服务

在持续接收观测的 PostgreSQL `readings` 水位明细表上，以 **`CREATE INDEX CONCURRENTLY`**
构建普通复合索引 `(station_id, observed_at)`，通过 FastAPI + psycopg3 提供
**单批次受理、阶段查询、取消、失败/取消清理、重启恢复** 的完整运维闭环；
并提供**按站日的逐小时水位汇总**：一致快照计算、整日原子发布、多实例并发刷新仲裁。

## 为什么这样设计

- `CREATE INDEX CONCURRENTLY`（CIC）不持有阻止源表写入的长锁，构建期间
  `readings` 的普通读写照常；代价是失败后会留下 `indisvalid=false` 的索引残件，
  必须显式 `DROP INDEX CONCURRENTLY`，且 CIC 不能跑在事务块里。
- 因此系统用**持久化状态机 + 独立 autocommit 构建会话**驱动批次，保证：
  取消/失败后必须等构建会话结束、残件删净才进入终态，之后才接收新批；
  清理失败保持待处理状态自动重试；重启后先恢复再受理。

## 目录结构

```
hydro-index-ops/
├── docker-compose.yml          # db(postgres:16) + api ×2（同一镜像/同一库）
├── Dockerfile
├── requirements.txt
├── app/
│   ├── main.py                 # FastAPI 路由（Swagger: /docs）
│   ├── worker.py               # 批次状态机：构建/取消/清理/恢复
│   ├── station_day.py          # 站日小时汇总：一致快照、整日原子发布、CAS 仲裁
│   ├── sql_helpers.py          # 索引状态、CIC 进度、会话终止、批次 CRUD
│   ├── db.py                   # psycopg 异步连接池 + 批次表/站日表 DDL
│   ├── models.py               # Pydantic 模型
│   └── config.py               # 环境变量配置
├── sql/
│   ├── 01_init_readings.sql    # 水位样表 + 10 万行样例数据 + 人工索引
│   ├── 02_long_tx.sql          # 长事务样例（制造 CIC 构建等待）
│   └── 03_observe.sql          # 排障观察 SQL 速查
├── demo/
│   ├── demo.sh                 # 四个端到端演示场景
│   └── reset_index.sql
├── tests/
│   ├── integration_test.py     # 索引批次状态机端到端测试（42 断言）
│   └── station_day_test.py     # 站日汇总端到端测试（49 断言，含双实例并发）
└── docs/state-machine.md       # 状态机、汇总一致性与并发保证说明
```

## 快速启动

```bash
cd hydro-index-ops
docker compose up -d --build
# 等待 healthcheck 通过后：
open http://localhost:8000/docs          # Swagger UI（api 实例 1）
open http://localhost:8001/docs          # Swagger UI（api 实例 2，同一数据库）
curl -s http://localhost:8000/health | python3 -m json.tool
```

首次启动会自动建表并写入 10 个测站 × 1 万条/站 = 10 万行水位样例数据，
并创建一个**人工索引** `idx_readings_ingested_manual`，用于验证清理/恢复流程
绝不会误删非系统创建的索引。`api` 与 `api2` 是两个独立的 API 进程，共享同一
PostgreSQL：用于验证并发刷新同一站日时的快照仲裁（索引批次状态机仍由咨询锁
保证只有一个实例驱动，另一个只提供读/写 API）。

## API 一览（Swagger 中有完整 schema 与示例）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/health` | 健康检查、管理索引状态、是否可受理新批 |
| POST | `/index-batches` | 受理一个构建批次（活动批次存在或索引已 valid 时 409） |
| GET  | `/index-batches` | 批次历史（状态全部持久化在 `index_build_batches` 表） |
| GET  | `/index-batches/{id}` | 查状态/构建阶段（pg_stat_progress_create_index）/错误 |
| POST | `/index-batches/{id}/cancel` | 请求取消，终态前不允许再受理 |
| POST | `/readings` | 写入水位观测（构建/等待/清理期间始终可用） |
| GET  | `/readings?station_id=&start=&end=` | 按站号+时间查询 |
| GET  | `/readings/explain?station_id=` | EXPLAIN 验证复合索引是否生效 |
| POST | `/demo/long-tx?seconds=` | 开长事务制造 CIC 构建等待 |
| DELETE | `/demo/long-tx/{pid}` | 结束演示长事务（只认 `hydro-demo-longtx-*` 会话） |
| POST | `/station-days/{station_id}/{date}/refresh` | 刷新站日 24 小时汇总（一致快照、整日原子发布） |
| GET  | `/station-days/{station_id}/{date}` | 查询站日 24 小时汇总（只读最近一次完整发布） |

## 站日逐小时汇总

水文数仓分析员需要按站日查看**逐小时最高水位和有效观测数**，以找出缺测时段。

```bash
# 刷新站 7 的 2027-04-01（UTC 日期）
curl -s -X POST http://localhost:8000/station-days/7/2027-04-01/refresh | python3 -m json.tool
# 查询（永远返回固定 24 项）
curl -s "http://localhost:8000/station-days/7/2027-04-01" | python3 -m json.tool
```

```json
{
  "station_id": 7, "date": "2027-04-01", "timezone": "UTC",
  "revision": 1, "published": true, "refreshed_at": "...",
  "snapshot": "853:853:", "applied": true,
  "hours": [
    {"hour": 0,  "good_count": 0, "max_water_level": null},
    {"hour": 1,  "good_count": 2, "max_water_level": 11.9},
    ...  /* 固定 24 项，hour 0..23 */
    {"hour": 23, "good_count": 1, "max_water_level": 12.0}
  ]
}
```

- **汇总口径**：按 `observed_at` 的 UTC 小时分桶，只统计 `quality='GOOD'` 的
  readings；`good_count` 为有效观测数，`max_water_level` 为最高水位。
  缺测小时固定为 `good_count=0`、`max_water_level=null`。
- **一致快照**：刷新在只读 `REPEATABLE READ` 事务里取 `pg_current_snapshot()`
  并在同一快照下聚合。刷新期间水位写入照常，新提交的观测不会混入本次结果；
  **迟到观测**（`observed_at` 早于当前时间但提交晚）会在下次刷新时进入其
  `observed_at` 所属小时，而不是按入库时间归桶。
- **整日原子发布**：24 个小时行与修订号（`revision`）在同一事务内发布；查询
  只按现行修订号取小时行，因此永远看到某一修订的**完整 24 小时**。刷新失败或
  进程被杀时事务整体回滚，查询仍只看到先前完整的 24 小时及其修订号。
- **多实例并发刷新**：发布时对 `(station_id, observed_date)` 修订行做 CAS，
  条件为 `hydro_snapshot_not_older(新快照, 现行快照)`。两个实例并发刷新同一站日时，
  较旧快照的刷新不会覆盖较新结果、也不会抬升修订号（返回 `applied=false`
  与现行已发布内容）。服务重启后查询结果同样保持不变（全部落库）。

## 四个演示场景

```bash
chmod +x demo/demo.sh
./demo/demo.sh all       # 启动并依次跑场景 1~4
# 或单独执行：./demo/demo.sh 1 | 2 | 3 | 4
```

1. **构建等待 → 成功**：先开持有 `readings` 快照的长事务，再受理批次；
   观察 CIC 停在等待阶段（`wait=Lock...`），期间读写照常；结束长事务后
   构建完成，EXPLAIN 显示走 `idx_readings_station_observed`。
2. **构建中取消**：等待阶段发起 cancel，状态经 `CANCELLING → CLEANING → CANCELLED`；
   期间受理新批返回 409；残件删净后才能受理新批；取消后的迟到完成同样删索引、
   绝不报成功。
3. **清理失败自动重试**：以 `HYDRO_CLEANUP_FAIL_FIRST=2` 注入前两次 DROP 失败
   （compose override 注入），观察批次停留在 `CLEANING`、`cleanup_attempts`
   递增、错误信息可查，源表读写与状态查询全程正常，重试成功后自动进入终态。
4. **重启恢复**：构建等待中 `docker compose restart api`；恢复在后台执行、
   不阻塞 API 就绪，先终止遗留构建会话（按 pid + application_name 双重校验，
   不断其他会话）、清净残件再进入终态；另演示孤儿构建迟到成功时索引被采纳
   保留，以及成功索引与人工索引在重启后均不受影响。

手动排障见 `sql/03_observe.sql`：

```bash
docker exec -it hydro-db psql -U hydro -d hydro_warehouse
```

## 关键安全边界

- **只管理一个固定名称的索引** `idx_readings_station_observed`，所有
  DROP 都带该名称且 `IF EXISTS`，不触碰任何人工索引。
- **只终止自有构建会话**：`pg_cancel_backend` / `pg_terminate_backend`
  均带 `application_name LIKE 'hydro-cic-builder-%'` 双重身份校验。
- 批次表上有部分唯一索引，数据库层面兜底“同一时刻只有一个活动批次”。
- worker 用 PostgreSQL 咨询锁保证多副本部署时只有一个实例驱动状态机；
  锁由专用长连接持有并在同一会话释放。
- 状态全部落库（`index_build_batches` / `station_day_refresh` /
  `station_day_hourly`），进程重启不丢状态。
- 站日汇总查询只读“现行修订号”的小时行；发布事务与查询隔离，任何时刻都
  不会暴露非完整 24 小时的中间态。

## 配置项（api 环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HYDRO_PG_DSN` | 指向 compose 内 db | PostgreSQL 连接串 |
| `HYDRO_WORK_POLL_INTERVAL` | `1.0` | 调度/阶段刷新间隔（秒） |
| `HYDRO_CLEANUP_RETRY_INTERVAL` | `3.0` | 清理失败重试间隔（秒） |
| `HYDRO_CLEANUP_FAIL_FIRST` | `0` | 演示用：前 N 次清理故意失败，生产必须为 0 |
| `HYDRO_STATION_DAY_PUBLISH_DELAY` | `0` | 测试/演示用：站日刷新取快照后延迟发布秒数（复现旧快照晚到），生产必须为 0 |
| `HYDRO_STATION_DAY_FAIL_BEFORE_COMMIT` | `false` | 测试/演示用：站日发布事务提交前注入失败（验证整日回滚），生产必须为 false |
| `HYDRO_LOG_LEVEL` | `INFO` | 日志级别 |

## 停止与清理

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 连同样例数据卷一并删除
docker exec -i hydro-db psql -U hydro -d hydro_warehouse < demo/reset_index.sql
```

## 集成测试（真实 PostgreSQL，非模拟）

`tests/integration_test.py` 启动一个**真实 PG 实例 + 真实 uvicorn 子进程**，
覆盖 42 项断言：正常构建与执行计划、长事务等待、取消与残件清理、清理失败
自动重试、SIGKILL 崩溃后的后台恢复（含孤儿构建迟到成功的采纳）、成功/人工
索引保留、批次持久化等。

`tests/station_day_test.py` 额外启动**两个真实的 uvicorn 实例**（共享同一 PG），
覆盖 49 项断言：快照新旧比较语义、固定 24 小时与缺测小时、GOOD 过滤与 UTC
小时归桶、刷新期间写入不混入快照、迟到观测下次刷新进入原小时、双实例并发
刷新时旧快照不覆盖新结果、发布失败整日回滚、重启后查询不变、OpenAPI/Swagger 可用。

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt httpx
# 准备一个可用的 PostgreSQL，并执行 sql/01_init_readings.sql，然后：
HYDRO_PG_DSN=postgresql://user:pass@host:5432/hydro_warehouse \
  python tests/integration_test.py
HYDRO_PG_DSN=postgresql://user:pass@host:5432/hydro_warehouse \
  python tests/station_day_test.py
```

两个测试默认连接 `127.0.0.1:55432/hydro_warehouse`（见文件顶部常量，可按环境改），
并分别占用 8011 / 8021~8023 端口。

