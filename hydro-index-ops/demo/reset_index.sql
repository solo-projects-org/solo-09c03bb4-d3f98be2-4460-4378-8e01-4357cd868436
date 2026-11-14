-- 演示间重置：仅删除本系统管理的索引（人工索引 idx_readings_ingested_manual 不受影响）
DROP INDEX CONCURRENTLY IF EXISTS idx_readings_station_observed;
