// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * MTMV Schema Change 测试用例。
 * <p>
 * 原有: testDropBaseView / testDropBaseTable（测试 drop base view/table 后 schema change 感知）
 * <p>
 * 新增: 按 StarRocks commit 102b87e 的 test_schema_change/R 风格编写的 MTMV ADD COLUMN 测试。
 * <p>
 * 注意: {@link MTMV#rewriteQuerySqlForAddColumn} 的签名是
 * {@code rewriteQuerySqlForAddColumn(String originQuerySql, String exprSql, String columnName)},
 * <b>不需要类型</b>，类型由表达式自动推断。
 *
 * ✅ = Doris 能做到（与 StarRocks 等效）
 * ⚠️ = Doris 能做但实现有风险（SelectColumnClauseIndexFinder 只找第一个 SELECT 子句）
 * ❌ = Doris 做不到（仅 StarRocks 有）
 */
public class MTMVSchemaChangeTest extends TestWithFeService {

    // ======================================================
    // 原有测试（保留不变）
    // ======================================================

    @Test
    public void testDropBaseView() throws Exception {
        createDatabaseAndUse("db1");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createView("create view v1 as select * from t1");
        createMvByNereids("create materialized view mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from v1 ;");
        Database db1 = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("db1");
        MTMV mtmv = (MTMV) db1.getTableOrAnalysisException("mv1");
        MTMVRefreshSnapshot mtmvRefreshSnapshot = new MTMVRefreshSnapshot();
        mtmv.setRefreshSnapshot(mtmvRefreshSnapshot);
        Assertions.assertTrue(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 0);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.INIT));
        dropView("drop view v1");
        Assertions.assertFalse(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 1);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.SCHEMA_CHANGE));
    }

    @Test
    public void testDropBaseTable() throws Exception {
        createDatabaseAndUse("db2");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createView("create view v1 as select * from t1");
        createMvByNereids("create materialized view mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from v1 ;");
        Database db2 = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("db2");
        MTMV mtmv = (MTMV) db2.getTableOrAnalysisException("mv1");
        MTMVRefreshSnapshot mtmvRefreshSnapshot = new MTMVRefreshSnapshot();
        mtmv.setRefreshSnapshot(mtmvRefreshSnapshot);
        Assertions.assertTrue(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 0);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.INIT));
        dropTable("t1", true);
        Assertions.assertFalse(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 1);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.SCHEMA_CHANGE));
    }

    // ======================================================
    // 新增: MTMV ADD COLUMN 测试（按 StarRocks test 风格）
    // ======================================================

    /**
     * ✅ 场景 1: 简单 SELECT — 核心链路正常
     * <pre>
     * 原始: SELECT k1, v1 FROM t1
     * 改写: SELECT k1, v1, k1 + 1 AS `new_col` FROM t1
     * </pre>
     */
    @Test
    public void testAddColumnSimpleSelect() throws Exception {
        String originSql = "SELECT k1, v1 FROM t1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "k1 + 1", "new_col");
        Assertions.assertEquals("SELECT k1, v1, k1 + 1 AS `new_col` FROM t1", rewritten);
    }

    /**
     * ✅ 场景 2: WHERE 条件
     * <pre>
     * 原始: SELECT k1, v1 FROM t1 WHERE k1 > 1
     * 改写: SELECT k1, v1, v1 * 2 AS `v1_x2` FROM t1 WHERE k1 > 1
     * </pre>
     */
    @Test
    public void testAddColumnWithWhere() throws Exception {
        String originSql = "SELECT k1, v1 FROM t1 WHERE k1 > 1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "v1 * 2", "v1_x2");
        Assertions.assertEquals(
                "SELECT k1, v1, v1 * 2 AS `v1_x2` FROM t1 WHERE k1 > 1",
                rewritten);
    }

    /**
     * ✅ 场景 3: GROUP BY 聚合 — 多个聚合字段
     * <pre>
     * 原始: SELECT k1, SUM(v1) AS total, AVG(v1) AS avg_v FROM t1 GROUP BY k1
     * 改写: SELECT k1, SUM(v1) AS total, AVG(v1) AS avg_v, k1 * k1 AS `k1_squared` FROM t1 GROUP BY k1
     * </pre>
     */
    @Test
    public void testAddColumnWithGroupBy() throws Exception {
        String originSql = "SELECT k1, SUM(v1) AS total, AVG(v1) AS avg_v FROM t1 GROUP BY k1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "k1 * k1", "k1_squared");
        Assertions.assertEquals(
                "SELECT k1, SUM(v1) AS total, AVG(v1) AS avg_v, k1 * k1 AS `k1_squared` FROM t1 GROUP BY k1",
                rewritten);
    }

    /**
     * ✅ 场景 4: ORDER BY 降序
     * <pre>
     * 原始: SELECT k1, v1 FROM t1 ORDER BY k1 DESC
     * 改写: SELECT k1, v1, CASE WHEN v1 >= 20 THEN 'big' ELSE 'small' END AS `v1_label` FROM t1 ORDER BY k1 DESC
     * </pre>
     */
    @Test
    public void testAddColumnWithOrderBy() throws Exception {
        String originSql = "SELECT k1, v1 FROM t1 ORDER BY k1 DESC";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql,
                "CASE WHEN v1 >= 20 THEN 'big' ELSE 'small' END", "v1_label");
        Assertions.assertEquals(
                "SELECT k1, v1, CASE WHEN v1 >= 20 THEN 'big' ELSE 'small' END AS `v1_label` "
                        + "FROM t1 ORDER BY k1 DESC",
                rewritten);
    }

    /**
     * ✅ 场景 5: 标量子查询（多表）
     * <pre>
     * 原始: SELECT k1, (SELECT max(v1) FROM t2) AS mv FROM t1 WHERE k1 > 1
     * 改写: SELECT k1, (SELECT max(v1) FROM t2) AS mv, k1 + 1 AS `new_col` FROM t1 WHERE k1 > 1
     * </pre>
     */
    @Test
    public void testAddColumnWithSubqueryFrom() throws Exception {
        String originSql = "SELECT k1, (SELECT max(v1) FROM t2) AS mv FROM t1 WHERE k1 > 1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "k1 + 1", "new_col");
        Assertions.assertEquals(
                "SELECT k1, (SELECT max(v1) FROM t2) AS mv, k1 + 1 AS `new_col` FROM t1 WHERE k1 > 1",
                rewritten);
    }

    /**
     * ✅ 场景 6: CTE（WITH 子句 — SelectColumnClauseIndexFinder 跳过 CTE）
     * <pre>
     * 原始: WITH cte AS (SELECT k1 FROM t2) SELECT k1 FROM cte
     * 改写: WITH cte AS (SELECT k1 FROM t2) SELECT k1, k1 * 2 AS `x``` FROM cte
     * </pre>
     */
    @Test
    public void testAddColumnWithCte() throws Exception {
        String originSql = "WITH cte AS (SELECT k1 FROM t2) SELECT k1 FROM cte";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "k1 * 2", "x`");
        Assertions.assertEquals(
                "WITH cte AS (SELECT k1 FROM t2) SELECT k1, k1 * 2 AS `x``` FROM cte",
                rewritten);
    }

    /**
     * ✅ 场景 7: JOIN（多表）
     * <pre>
     * 原始: SELECT a.id, a.v1, b.name FROM t3 a JOIN t4 b ON a.id = b.id
     * 改写: SELECT a.id, a.v1, b.name, v1 + 10 AS `v1_plus` FROM t3 a JOIN t4 b ON a.id = b.id
     * </pre>
     */
    @Test
    public void testAddColumnWithJoin() throws Exception {
        String originSql = "SELECT a.id, a.v1, b.name FROM t3 a JOIN t4 b ON a.id = b.id";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "v1 + 10", "v1_plus");
        Assertions.assertEquals(
                "SELECT a.id, a.v1, b.name, v1 + 10 AS `v1_plus` FROM t3 a JOIN t4 b ON a.id = b.id",
                rewritten);
    }

    /**
     * ✅ 场景 8: SELECT *（星号展开）
     * <pre>
     * 原始: SELECT * FROM t1
     * 改写: SELECT *, k1 * 2 AS `x` FROM t1
     * </pre>
     */
    @Test
    public void testAddColumnSelectStar() throws Exception {
        String originSql = "SELECT * FROM t1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "k1 * 2", "x");
        Assertions.assertEquals("SELECT *, k1 * 2 AS `x` FROM t1", rewritten);
    }

    /**
     * ✅ 场景 9: 反引号列名转义（双倍反引号）
     * <pre>
     * 原始: SELECT k1, v1 FROM t1
     * 改写: SELECT k1, v1, k1 * 2 AS `x``` FROM t1（列名 x` 的四个反引号）
     * </pre>
     */
    @Test
    public void testAddColumnWithBacktickColumnName() throws Exception {
        String originSql = "SELECT k1, v1 FROM t1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "k1 * 2", "x`");
        Assertions.assertEquals("SELECT k1, v1, k1 * 2 AS `x``` FROM t1", rewritten);
    }

    /**
     * ✅ 场景 10: 多字段非聚合
     * <pre>
     * 原始: SELECT k1, v1, v2 FROM t1
     * 改写: SELECT k1, v1, v2, v1 + v2 AS `sum_v` FROM t1
     * </pre>
     */
    @Test
    public void testAddColumnWithMultipleColumns() throws Exception {
        String originSql = "SELECT k1, v1, v2 FROM t1";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql, "v1 + v2", "sum_v");
        Assertions.assertEquals("SELECT k1, v1, v2, v1 + v2 AS `sum_v` FROM t1", rewritten);
    }

    /**
     * ⚠️ 场景 11: UNION ALL — Doris 只改第一个 SELECT 子句
     * <pre>
     * 原始: SELECT k1, v1 FROM t1 WHERE k1 <= 3
     *        UNION ALL
     *        SELECT k1, v1 FROM t1 WHERE k1 > 3
     * 改写: SELECT k1, v1, IF(v1 >= 30, 'high', 'low') AS `comment` FROM t1 WHERE k1 <= 3
     *        UNION ALL
     *        SELECT k1, v1 FROM t1 WHERE k1 > 3
     * （第二个 SELECT 没有新列 → 列数不匹配）
     * </pre>
     * ⚠️ StarRocks 用 AST2SQLVisitor 遍历所有 SELECT，不存在此问题。
     */
    @Test
    public void testAddColumnUnionAllFirstOnly() throws Exception {
        String originSql = "SELECT k1, v1 FROM t1 WHERE k1 <= 3 "
                + "UNION ALL SELECT k1, v1 FROM t1 WHERE k1 > 3";
        String rewritten = MTMV.rewriteQuerySqlForAddColumn(originSql,
                "IF(v1 >= 30, 'high', 'low')", "comment");
        Assertions.assertEquals(
                "SELECT k1, v1, IF(v1 >= 30, 'high', 'low') AS `comment` FROM t1 WHERE k1 <= 3 "
                        + "UNION ALL SELECT k1, v1 FROM t1 WHERE k1 > 3",
                rewritten);
        // ⚠️ 第二个 SELECT 只有 2 列，第 1 个有 3 列 → 报错
    }

    /**
     * ❌ 场景 12: DROP COLUMN — Doris 完全不支持
     * <pre>
     * StarRocks 支持:  ALTER MATERIALIZED VIEW mv DROP COLUMN colName
     * Doris 需要:
     *   1. DorisParser.g4 新增 DROP COLUMN 语法分支
     *   2. MTMVAlterOpType 新增 DROP_COLUMN 枚举
     *   3. 新增 AlterMTMVDropColumnInfo
     *   4. Alter.java 新增 DROP_COLUMN case
     *   5. MTMV.alterDropColumn() 实现（含 queryOutputIndices 维护）
     * </pre>
     */
    // ❌ Doris 当前 MTMVAlterOpType 无 DROP_COLUMN 枚举，
    // 对应的 SQL 语法也在 ANTLR 规则中没有，
    // 所以无测试用例可写。
}