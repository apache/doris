-- ============================================================================
-- 06_view_fail_test.sql
-- 目标：复现 Mv-on-(Mv-over-View) 匹配不上
--
-- 结构：
--   VIEW: v_fact2
--   Layer0: mv_layer0_on_view        = MV over VIEW
--   Layer1: mv_layer1_on_mv          = MV over Layer0 (MV-on-MV)
--   对照:  mv_direct_on_view         = 直接 MV over VIEW
--
-- 预期：
--   - mv_direct_on_view 可能 chose
--   - mv_layer1_on_mv   fail: The graph logic between query and view is not consistent
-- ============================================================================

DROP DATABASE IF EXISTS test_view_fail;
CREATE DATABASE test_view_fail;
USE test_view_fail;

-- 基表：事实表（原始列里有 sku_flag）
CREATE TABLE t_fact2 (
    item_sku_id VARCHAR(200),
    amount DOUBLE,
    flag INT,
    sku_flag STRING,
    dt DATE NOT NULL
)
DUPLICATE KEY(item_sku_id)
AUTO PARTITION BY RANGE(date_trunc(dt, 'day'))()
DISTRIBUTED BY HASH(item_sku_id) BUCKETS 1
PROPERTIES('replication_allocation' = 'tag.location.default: 1');

-- 基表：维表（有 sku_type）
CREATE TABLE t_dim2 (
    item_sku_id VARCHAR(200),
    bu_id STRING,
    sku_type STRING,
    dt DATE NOT NULL
)
DUPLICATE KEY(item_sku_id)
AUTO PARTITION BY RANGE(date_trunc(dt, 'day'))()
DISTRIBUTED BY HASH(item_sku_id) BUCKETS 1
PROPERTIES('replication_allocation' = 'tag.location.default: 1');

INSERT INTO t_fact2 VALUES
('1', 100, 1, '1', '2025-12-01'),
('2', 200, 1, '2', '2025-12-01');

INSERT INTO t_dim2 VALUES
('1', 'BU_A', '1', '2026-01-10'),
('2', 'BU_B', '2', '2026-01-10');

-- VIEW：把 sku_flag 映射成 sku_type，并过滤 flag=1
DROP VIEW IF EXISTS v_fact2;
CREATE VIEW v_fact2 AS
SELECT
    item_sku_id,
    amount,
    dt,
    IF(sku_flag IN ('1', '4', '5'), '1', '2') AS sku_type
FROM t_fact2
WHERE flag = 1;

-- 对照 MV：直接基于 VIEW 建 JOIN MV
DROP MATERIALIZED VIEW IF EXISTS mv_direct_on_view;
CREATE MATERIALIZED VIEW mv_direct_on_view
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY HASH(item_sku_id) BUCKETS 1
PROPERTIES('replication_allocation' = 'tag.location.default: 1')
AS
SELECT v.dt, v.item_sku_id, d.bu_id, v.amount
FROM v_fact2 v
LEFT JOIN t_dim2 d
    ON v.item_sku_id = d.item_sku_id
    AND v.sku_type = d.sku_type
    AND d.dt = '2026-01-10';

-- Layer0：MV over VIEW（这是你要的 mv-over-view）
DROP MATERIALIZED VIEW IF EXISTS mv_layer0_on_view;
CREATE MATERIALIZED VIEW mv_layer0_on_view
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY HASH(item_sku_id) BUCKETS 1
PROPERTIES('replication_allocation' = 'tag.location.default: 1')
AS
SELECT
    v.dt,
    v.item_sku_id,
    v.amount,
    v.sku_type
FROM v_fact2 v;

-- Layer1：MV-on-(MV-over-VIEW)
DROP MATERIALIZED VIEW IF EXISTS mv_layer1_on_mv;
CREATE MATERIALIZED VIEW mv_layer1_on_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY HASH(item_sku_id) BUCKETS 1
PROPERTIES('replication_allocation' = 'tag.location.default: 1')
AS
SELECT m.dt, m.item_sku_id, d.bu_id, m.amount
FROM mv_layer0_on_view m
LEFT JOIN t_dim2 d
    ON m.item_sku_id = d.item_sku_id
    AND m.sku_type = d.sku_type
    AND d.dt = '2026-01-10';

SET enable_materialized_view_rewrite = true;
SET enable_materialized_view_nest_rewrite = true;

-- 复现查询：通过 VIEW 访问（与业务写法一致）
EXPLAIN 
SELECT v.dt, v.item_sku_id, d.bu_id, v.amount
FROM v_fact2 v
LEFT JOIN t_dim2 d
    ON v.item_sku_id = d.item_sku_id
    AND v.sku_type = d.sku_type
    AND d.dt = '2026-01-10';