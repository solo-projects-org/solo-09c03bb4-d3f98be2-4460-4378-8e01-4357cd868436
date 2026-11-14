# 批次状态机与关键保证

## 状态集合

| 状态 | 含义 | 是否允许受理新批 |
|---|---|---|
| `PENDING` | 已受理，等待 worker 调度构建 | 否 |
| `BUILDING` | CIC 构建会话进行中，`build_pid` 记录后端 pid | 否 |
| `CANCELLING` | 已请求取消，等待构建会话结束 | 否 |
| `CLEANING` | 失败/取消后循环 DROP 残件；DROP 失败会停留在此并重试 | 否 |
| `SUCCEEDED` | 终态：索引 valid 在线（重启后也保留） | 是* |
| `FAILED` | 终态：构建失败且残件已删净（错误信息保留在 `error`） | 是* |
| `CANCELLED` | 终态：取消完成且残件已删净 | 是* |

\* 受理接口还会检查管理索引是否已存在且 valid，已存在则返回 409，
避免对同一索引重复发起 CIC。

## 状态转移

```
                POST /index-batches
                       │
                       ▼
                    PENDING ──cancel(构建前)──┐
                       │                      │
                  worker 调度                 │
                       ▼                      ▼
                    BUILDING ──cancel──► CANCELLING
                  │       │                  │
        构建成功  │       │ 构建失败/会话消失 │
                  ▼       ▼                  ▼
              SUCCEEDED  └──────────────► CLEANING ◄──── 清理失败
                                            │  ▲            (保留状态,
                                  DROP 成功  │  └─ 自动重试  cleanup_attempts+1)
                                            ▼
                              ┌── cancel_requested=true ──► CANCELLED
                              └── false ──────────────────► FAILED
```

## 关键保证如何落地

### 1. 构建期间源表普通读写照常

- CIC 在**独立连接、autocommit 模式**下执行（CIC 不能在事务块中），
  不与 API 的连接池事务互相阻塞。
- CIC 不拿 `SHARE` 以上的长锁，只需要与旧快照/写事务同步；等待期间
  新写入照常提交（演示场景 1 验证）。
- 清理用的 `DROP INDEX CONCURRENTLY` 同理，也是独立 autocommit 会话。

### 2. 取消/失败必须等会话结束、残件删净

- `cancel` 只置 `cancel_requested` 并先发一次 `pg_cancel_backend`；
  worker 的 `_terminate_and_wait` 轮询 `pg_stat_activity`，先优雅取消、
  10 秒后升级 `pg_terminate_backend`，直到 pid 消失。
- 清理循环开头兜底检查 `build_pid` 是否还活着，活着就继续等，
  绝不在构建会话未结束时执行 DROP（同对象并发 CIC/DROP 会报错）。
- 只有 `index_status()` 返回 `None`（pg_index 中查不到该名索引）
  才允许写终态；DROP 不报错但索引仍在时继续重试。

### 3. 取消后的迟到完成不能留索引或报成功

构建语句返回后先读 `cancel_requested`：

- 为 true → 即使 `indisvalid=true`（迟到成功）也走清理，终态 `CANCELLED`；
- 为 false 且 valid → 才记 `SUCCEEDED`。

### 4. 清理失败自动重试，等待期间服务照常

- DROP 抛错时只更新 `error` 与 `cleanup_attempts`，状态保持 `CLEANING`，
  sleep `HYDRO_CLEANUP_RETRY_INTERVAL` 后重试，无重试次数上限。
- 该循环只占用 worker 协程；API 连接池、批次查询、`readings` 读写
  走独立连接，完全不受影响。

### 5. 重启恢复（后台执行，不阻塞启动；恢复完前不接新批）

被长事务挡住的孤儿构建，光终止就要等待较长时间，因此恢复**不在 lifespan
里同步执行**，而是 worker 拿到咨询锁后作为后台任务 `_guarded_recovery` 运行：

- API 进程启动后健康检查、状态查询、源表读写立即可用；
- 恢复期间 `_recovery_done` 未置位，主调度循环空转，不与恢复并发操作同一批次；
- 恢复未完成前旧批次仍处活动态，受理接口因“只受理一批”返回 409，语义上
  仍然是“先恢复、后受理”。

恢复步骤：

1. 查活动批次（部分唯一索引保证最多一条）；
2. 对 `build_pid` 执行带身份校验的 `pg_cancel_backend`，超时升级
   `pg_terminate_backend`，并轮询等待会话消失；
3. 查索引状态：
   - `cancel_requested=true` → 删索引（含迟到成功）→ `CANCELLED`；
   - 索引 valid 且未取消 → **保留索引**，批次记 `SUCCEEDED`；
   - 索引 invalid → DROP 残件 → `FAILED`；
   - 无索引且原本是 `PENDING` → 保留 PENDING，正常调度构建；
   - 无索引且为其他中间态 → `FAILED`（构建会话蒸发）。
4. 若 DROP 被仍在运行的长事务阻塞，批次停在 `CLEANING` 自动重试，
   长事务结束后自动进入终态。
5. 恢复任务任何异常都被捕获并记录，不会杀死 worker；随后由主调度的
   孤儿回收路径 `_reap_orphan_building` 兜底。
   受理接口会因“只受理一批”拒绝新请求。

### 6. 不中止其他会话、不删人工索引

- 终止函数的 SQL 固定带
  `WHERE pid = %s AND application_name LIKE 'hydro-cic-builder-%'`；
  演示长事务的终止接口也限定 `hydro-demo-longtx-*`。
- 索引识别只按 `pg_class.relname = 'idx_readings_station_observed'`，
  DROP 语句写死该名称，人工索引（如 `idx_readings_ingested_manual`）
  在任何路径下都不会被匹配。

## 并发与多实例

- worker 启动时用专用长连接 `pg_try_advisory_lock(982451653)` 抢锁，
  拿不到锁的实例只提供 API、不驱动状态机；锁在同一会话释放。
- `index_build_batches` 上的部分唯一索引
  `WHERE state IN (活动态)` 在数据库层兜底单活动批次，
  受理接口同时捕获 `UniqueViolation` 返回 409。
- API 受理路径另有进程内 `asyncio.Lock` 串行化检查与插入。

## 观察点

- 构建阶段：`pg_stat_progress_create_index.phase`
  （`initializing` / `waiting for writers before build` /
  `building index` / `waiting for writers before validation` /
  `index validation: ...` / `waiting for old snapshots` 等），
  未进入进度视图时回退显示 `pg_stat_activity.wait_event`。
- 批次表：`SELECT * FROM index_build_batches ORDER BY id;`
- 阻塞链与索引有效性：见 `sql/03_observe.sql`。
