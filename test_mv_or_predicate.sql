-- 测试物化视图 OR 谓词覆盖场景
-- 场景：MV 有 OR 谓词，查询为其中一个分支，应通过 MV 残差覆盖并生成 NOT 补偿

-- 1. 创建数据库（如不存在）
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
    (5, 100),      -- 匹配 id=5
    (5, 200),      -- 匹配 id=5
    (10, 1),       -- 匹配 score=1
    (20, 1),       -- 匹配 score=1
    (15, 50),      -- 不匹配任何条件
    (3, 2);        -- 不匹配任何条件

-- 4. 创建物化视图（OR 谓词）
-- 先检查并删除已存在的物化视图
DROP MATERIALIZED VIEW IF EXISTS test_mv_or_db.mv_or_test;

-- 注意：Doris 可能不支持在物化视图定义中直接使用 OR 条件
-- 如果报错 "Duplicate column name"，可能需要使用 UNION 替代 OR
-- 方案1：尝试使用 UNION 模拟 OR
CREATE MATERIALIZED VIEW test_mv_or_db.mv_or_test
AS
SELECT id, score
FROM test_mv_or_db.test_mv_or_t1
WHERE id = 5
UNION
SELECT id, score
FROM test_mv_or_db.test_mv_or_t1
WHERE score = 1;

-- 如果 UNION 方案也不行，可以尝试：
-- 方案2：创建无 OR 的物化视图用于测试残差逻辑
-- CREATE MATERIALIZED VIEW test_mv_or_db.mv_or_test
-- AS
-- SELECT id, score
-- FROM test_mv_or_db.test_mv_or_t1
-- WHERE id = 5;

-- 5. 等待物化视图构建完成（实际可能需要手动触发刷新）
-- REFRESH MATERIALIZED VIEW mv_or_test;

-- 6. 测试查询：查询只有 OR 的一个分支（id = 5）
-- 期望：应该使用 MV，并在补偿中添加 NOT(score = 1) 来收紧结果
EXPLAIN
SELECT id, score
FROM test_mv_or_t1
WHERE id = 5;

-- 7. 验证查询结果（应该只返回 id=5 的行，不含 score=1 的行）
SELECT id, score
FROM test_mv_or_t1
WHERE id = 5
ORDER BY id, score;

-- 8. 测试查询：查询匹配 MV 的另一个分支（score = 1）
EXPLAIN
SELECT id, score
FROM test_mv_or_t1
WHERE score = 1;

SELECT id, score
FROM test_mv_or_t1
WHERE score = 1
ORDER BY id, score;

-- 9. 测试查询：查询匹配整个 OR（id = 5 OR score = 1）
EXPLAIN
SELECT id, score
FROM test_mv_or_t1
WHERE id = 5 OR score = 1;

SELECT id, score
FROM test_mv_or_t1
WHERE id = 5 OR score = 1
ORDER BY id, score;

-- 10. 清理
-- DROP MATERIALIZED VIEW IF EXISTS mv_or_test;
-- DROP TABLE IF EXISTS test_mv_or_t1;
-- DROP DATABASE IF EXISTS test_mv_or_db;
