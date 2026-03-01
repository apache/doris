-- 测试物化视图残差谓词补偿场景
-- 由于 Doris 物化视图定义不支持 OR，我们创建简单 MV，用带 OR 的查询测试残差匹配

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS test_mv_residual_db;
USE test_mv_residual_db;

-- 2. 创建测试表
DROP TABLE IF EXISTS test_residual_t1;
CREATE TABLE IF NOT EXISTS test_residual_t1 (
    id bigint,
    score bigint,
    name varchar(100)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id, score) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 3. 插入测试数据
INSERT INTO test_residual_t1 VALUES
    (5, 100, 'a'),    -- id=5
    (5, 200, 'b'),    -- id=5
    (10, 1, 'c'),     -- score=1
    (20, 1, 'd'),     -- score=1
    (15, 50, 'e'),    -- 其他
    (3, 2, 'f');      -- 其他

-- 4. 创建简单的物化视图（只包含 id = 5 的条件）
DROP MATERIALIZED VIEW IF EXISTS test_mv_residual_db.mv_test;
CREATE MATERIALIZED VIEW test_mv_residual_db.mv_test
AS
SELECT id, score, name
FROM test_mv_residual_db.test_residual_t1
WHERE id = 5;

-- 注意：由于 MV 定义不支持 OR，我们无法在 MV 中定义 OR 条件
-- 但可以测试以下场景：
-- 1. 查询使用 OR 条件，看是否能匹配到 MV 的部分条件
-- 2. 查询使用更复杂的残差谓词，测试残差补偿逻辑

-- 5. 刷新物化视图（如果需要手动刷新）
-- REFRESH MATERIALIZED VIEW test_mv_residual_db.mv_test;

-- 6. 测试查询1：查询完全匹配 MV（id = 5）
-- 期望：应该直接使用 MV，无需补偿
EXPLAIN
SELECT id, score, name
FROM test_mv_residual_db.test_residual_t1
WHERE id = 5;

-- 7. 测试查询2：查询使用 OR 条件，包含 MV 的条件（id = 5 OR score = 1）
-- 期望：如果 MV 残差匹配逻辑实现，可能会尝试使用 MV 并补偿 score = 1 的部分
EXPLAIN
SELECT id, score, name
FROM test_mv_residual_db.test_residual_t1
WHERE id = 5 OR score = 1;

-- 8. 测试查询3：查询比 MV 更严格（id = 5 AND score = 100）
-- 期望：应该使用 MV 并补偿 score = 100 条件
EXPLAIN
SELECT id, score, name
FROM test_mv_residual_db.test_residual_t1
WHERE id = 5 AND score = 100;

SELECT id, score, name
FROM test_mv_residual_db.test_residual_t1
WHERE id = 5 AND score = 100;

-- 9. 查看物化视图信息
SHOW MATERIALIZED VIEWS FROM test_mv_residual_db;

-- 10. 清理（取消注释以执行）
-- DROP MATERIALIZED VIEW IF EXISTS test_mv_residual_db.mv_test;
-- DROP TABLE IF EXISTS test_mv_residual_db.test_residual_t1;
-- DROP DATABASE IF EXISTS test_mv_residual_db;
