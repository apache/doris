-- 测试物化视图 OR 谓词覆盖场景（替代方案）
-- 注意：如果 Doris 不支持在 MV 定义中使用 OR，使用此简化版本测试残差补偿基本逻辑

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS test_mv_or_db;
USE test_mv_or_db;

-- 2. 创建测试表
DROP TABLE IF EXISTS test_mv_or_t1;
CREATE TABLE IF NOT EXISTS test_mv_or_t1 (
    id bigint,
    score bigint
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id, score) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 3. 插入测试数据
INSERT INTO test_mv_or_t1 VALUES
    (5, 100),
    (5, 200),
    (10, 1),
    (20, 1),
    (15, 50),
    (3, 2);

-- 4. 尝试创建物化视图（如果支持 OR）
-- 如果报错，说明当前版本可能不支持 OR，需要检查 Doris 版本或使用 UNION 方案
DROP MATERIALIZED VIEW IF EXISTS test_mv_or_db.mv_or_test;

-- 尝试1：直接使用 OR（如果支持）
-- CREATE MATERIALIZED VIEW test_mv_or_db.mv_or_test
-- AS
-- SELECT id, score
-- FROM test_mv_or_db.test_mv_or_t1
-- WHERE id = 5 OR score = 1;

-- 尝试2：使用 UNION（如果 OR 不支持）
CREATE MATERIALIZED VIEW test_mv_or_db.mv_or_test
AS
SELECT id, score
FROM (
    SELECT id, score FROM test_mv_or_db.test_mv_or_t1 WHERE id = 5
    UNION
    SELECT id, score FROM test_mv_or_db.test_mv_or_t1 WHERE score = 1
) t;

-- 5. 刷新物化视图（如果需要）
-- REFRESH MATERIALIZED VIEW test_mv_or_db.mv_or_test;

-- 6. 测试查询：查询只有 id = 5（应该使用 MV 并通过补偿过滤）
EXPLAIN
SELECT id, score
FROM test_mv_or_db.test_mv_or_t1
WHERE id = 5;

-- 7. 执行查询验证结果
SELECT id, score
FROM test_mv_or_db.test_mv_or_t1
WHERE id = 5
ORDER BY id, score;

-- 8. 测试查询：查询 score = 1
EXPLAIN
SELECT id, score
FROM test_mv_or_db.test_mv_or_t1
WHERE score = 1;

SELECT id, score
FROM test_mv_or_db.test_mv_or_t1
WHERE score = 1
ORDER BY id, score;

-- 9. 查看物化视图信息
SHOW MATERIALIZED VIEWS FROM test_mv_or_db;

-- 10. 清理（取消注释以执行）
-- DROP MATERIALIZED VIEW IF EXISTS test_mv_or_db.mv_or_test;
-- DROP TABLE IF EXISTS test_mv_or_db.test_mv_or_t1;
-- DROP DATABASE IF EXISTS test_mv_or_db;
