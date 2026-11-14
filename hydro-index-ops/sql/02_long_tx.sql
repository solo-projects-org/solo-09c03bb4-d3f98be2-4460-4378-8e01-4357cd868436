-- ============================================================================
-- 长事务样例：模拟一个持有 readings 旧快照的长事务（如报表/ETL 作业）。
-- CREATE INDEX CONCURRENTLY 在“等待最早写事务结束”阶段会被此类事务阻塞，
-- 从而可以演示「构建等待」阶段；期间 readings 的普通读写完全不受影响。
--
-- 用法（容器内）：
--   docker exec -i hydro-db psql -U hydro -d hydro_warehouse -f - < sql/02_long_tx.sql
-- 或用 API：  POST /demo/long-tx?seconds=60
-- ============================================================================

BEGIN;

-- 行级快照在第一条查询后固定；readings 上的后续写入会产生新的事务号，
-- CIC 构建会停在 waiting for writers before build / waiting for old snapshots。
SELECT count(*) AS snapshot_rows, now() AS tx_start
FROM readings;

-- 模拟长事务持有快照
SELECT pg_sleep(60);

-- 查看本事务持有的快照（另开会话执行）：
--   SELECT pid, state, xact_start, wait_event_type, query
--   FROM pg_stat_activity WHERE backend_type = 'client backend' AND xact_start IS NOT NULL;

COMMIT;
