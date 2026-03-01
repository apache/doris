-- 测试异步物化视图 OR 谓词覆盖场景
-- 使用异步物化视图（BUILD DEFERRED REFRESH AUTO）可能支持 OR 条件

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS test_mv_or_db;
USE test_mv_or_db;

-- 2. 创建测试表（不带分区）
DROP TABLE IF EXISTS test_mv_or_t1;
CREATE TABLE IF NOT EXISTS test_mv_or_t1 (
    id bigint,
    score bigint,
    ds bigint
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id, score) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 3. 插入测试数据（ds 使用数值类型）
INSERT INTO test_mv_or_t1 VALUES
    (5, 100, 202530),   -- 匹配 ds = 202530
    (5, 200, 202530),   -- 匹配 ds = 202530
    (10, 1, 202531),    -- 匹配 ds >= 202531
    (20, 1, 202532),    -- 匹配 ds >= 202531
    (15, 50, 202529),   -- 不匹配
    (3, 2, 202528);     -- 不匹配

-- 4. 创建异步物化视图（使用 OR 条件，类似你提供的示例）
DROP MATERIALIZED VIEW IF EXISTS test_mv_or_db.mv_or_test;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv_or_db.mv_or_test
BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 1 MINUTE
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
    'enable_nondeterministic_function' = 'true',
    'replication_num' = '1'
)
AS
SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE ds >= 202531 OR id = 5;

-- 5. 手动触发首次构建（如果需要）
-- REFRESH MATERIALIZED VIEW test_mv_or_db.mv_or_test;

-- 6. 测试查询1：查询只有 OR 的一个分支（ds = 202530）
-- 期望：应该使用 MV，并在补偿中添加 NOT(ds >= 202531) 来收紧结果
EXPLAIN
SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE id = 5

-- 7. 执行查询验证结果
SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE ds = 202530
ORDER BY id, score, ds;

-- 8. 测试查询2：查询匹配 MV 的另一个分支（ds >= 202531）
EXPLAIN
SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE ds >= 202531
ORDER BY id, score, ds;

SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE ds >= 202531
ORDER BY id, score, ds;

-- 9. 测试查询3：查询匹配整个 OR（ds >= 202531 OR ds = 202530）
EXPLAIN
SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE ds >= 202531 OR ds = 202530
ORDER BY id, score, ds;

SELECT id, score, ds
FROM test_mv_or_db.test_mv_or_t1
WHERE ds >= 202531 OR ds = 202530
ORDER BY id, score, ds;

-- 10. 查看物化视图信息
SHOW MATERIALIZED VIEWS FROM test_mv_or_db;

-- 11. 查看物化视图任务状态
SHOW MTMV JOB FROM test_mv_or_db;

-- 12. 清理（取消注释以执行）
-- DROP MATERIALIZED VIEW IF EXISTS test_mv_or_db.mv_or_test;
-- DROP TABLE IF EXISTS test_mv_or_db.test_mv_or_t1;
-- DROP DATABASE IF EXISTS test_mv_or_db;
