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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Util;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import mockit.Mock;
import mockit.MockUp;

public class SelectStmtTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_batch_delete_by_default = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createBaseAllStmtStr = "create table db1.baseall(k1 int, k2 varchar(32)) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        String createPratitionTableStr = "CREATE TABLE db1.partition_table (\n" +
                "datekey int(11) NULL COMMENT \"datekey\",\n" +
                "poi_id bigint(20) NULL COMMENT \"poi_id\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(datekey, poi_id)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(datekey)\n" +
                "(PARTITION p20200727 VALUES [(\"20200726\"), (\"20200727\")),\n" +
                "PARTITION p20200728 VALUES [(\"20200727\"), (\"20200728\")))\n" +
                "DISTRIBUTED BY HASH(poi_id) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"storage_type\" = \"COLUMN\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        String createDatePartitionTableStr = "CREATE TABLE db1.date_partition_table (\n" +
                "  `dt` date NOT NULL COMMENT \"\",\n" +
                "  `poi_id` bigint(20) NULL COMMENT \"poi_id\",\n" +
                "  `uv1` bitmap BITMAP_UNION NOT NULL COMMENT \"\",\n" +
                "  `uv2` bitmap BITMAP_UNION NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(    PARTITION `p201701` VALUES LESS THAN (\"2020-09-08\"),\n" +
                "    PARTITION `p201702` VALUES LESS THAN (\"2020-09-09\"),\n" +
                "    PARTITION `p201703` VALUES LESS THAN (\"2020-09-10\"))\n" +
                "DISTRIBUTED BY HASH(`poi_id`) BUCKETS 20\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        String tbl1 = "CREATE TABLE db1.table1 (\n" +
                "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `citycode` smallint(6) NULL COMMENT \"\",\n" +
                "  `username` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`siteid`, `citycode`, `username`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`siteid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")";
        String tbl2 = "CREATE TABLE db1.table2 (\n" +
                "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `citycode` smallint(6) NULL COMMENT \"\",\n" +
                "  `username` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`siteid`, `citycode`, `username`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`siteid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(createTblStmtStr)
                   .withTable(createBaseAllStmtStr)
                   .withTable(createPratitionTableStr)
                   .withTable(createDatePartitionTableStr)
                   .withTable(tbl1)
                   .withTable(tbl2);
    }

    @Test
    public void testGroupingSets() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String selectStmtStr = "select k1,k2,MAX(k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k2),(k1),(k2),());";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr, ctx);
        String selectStmtStr2 = "select k1,k4,MAX(k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),());";
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("column: `k4` cannot both in select list and aggregate functions when using GROUPING"
                + " SETS/CUBE/ROLLUP, please use union instead.");
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr2, ctx);
        String selectStmtStr3 = "select k1,k4,MAX(k4+k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),());";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr3, ctx);
        String selectStmtStr4 = "select k1,k4+k4,MAX(k4+k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),()"
                + ");";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr4, ctx);
    }

    @Test
    public void testSubqueryInCase() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql1 = "SELECT CASE\n" +
                "        WHEN (\n" +
                "            SELECT COUNT(*) / 2\n" +
                "            FROM db1.tbl1\n" +
                "        ) > k4 THEN (\n" +
                "            SELECT AVG(k4)\n" +
                "            FROM db1.tbl1\n" +
                "        )\n" +
                "        ELSE (\n" +
                "            SELECT SUM(k4)\n" +
                "            FROM db1.tbl1\n" +
                "        )\n" +
                "    END AS kk4\n" +
                "FROM db1.tbl1;";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.toSql().contains("`$a$1`.`$c$1` > `k4` THEN `$a$2`.`$c$2` ELSE `$a$3`.`$c$3`"));

        String sql2 = "select case when k1 in (select k1 from db1.tbl1) then \"true\" else k1 end a from db1.tbl1";
        try {
            SelectStmt stmt2 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql2, ctx);
            stmt2.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Assert.fail("syntax not supported.");
        } catch (AnalysisException e) {
        } catch (Exception e) {
            Assert.fail("must be AnalysisException.");
        }
        try {
            String sql3 = "select case k1 when exists (select 1) then \"empty\" else \"p_test\" end a from db1.tbl1";
            SelectStmt stmt3 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql3, ctx);
            stmt3.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Assert.fail("syntax not supported.");
        } catch (AnalysisException e) {
        } catch (Exception e) {
            Assert.fail("must be AnalysisException.");
        }
        String sql4 = "select case when k1 < (select max(k1) from db1.tbl1) and " +
                "k1 > (select min(k1) from db1.tbl1) then \"empty\" else \"p_test\" end a from db1.tbl1";
        SelectStmt stmt4 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql4, ctx);
        stmt4.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt4.toSql().contains(" (`k1` < `$a$1`.`$c$1`) AND (`k1` > `$a$2`.`$c$2`) "));

        String sql5 = "select case when k1 < (select max(k1) from db1.tbl1) is null " +
                "then \"empty\" else \"p_test\" end a from db1.tbl1";
        SelectStmt stmt5 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql5, ctx);
        stmt5.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt5.toSql().contains(" `k1` < `$a$1`.`$c$1` IS NULL "));
    }

    @Test
    public void testDeduplicateOrs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2,\n" +
                "   db1.tbl1 t3,\n" +
                "   db1.tbl1 t4,\n" +
                "   db1.tbl1 t5,\n" +
                "   db1.tbl1 t6\n" +
                "where\n" +
                "   t2.k1 = t1.k1\n" +
                "   and t1.k2 = t6.k2\n" +
                "   and t6.k4 = 2001\n" +
                "   and(\n" +
                "      (\n" +
                "         t1.k2 = t4.k2\n" +
                "         and t3.k3 = t1.k3\n" +
                "         and t3.k1 = 'D'\n" +
                "         and t4.k3 = '2 yr Degree'\n" +
                "         and t1.k4 between 100.00\n" +
                "         and 150.00\n" +
                "         and t4.k4 = 3\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k2 = t4.k2\n" +
                "         and t3.k3 = t1.k3\n" +
                "         and t3.k1 = 'S'\n" +
                "         and t4.k3 = 'Secondary'\n" +
                "         and t1.k4 between 50.00\n" +
                "         and 100.00\n" +
                "         and t4.k4 = 1\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k2 = t4.k2\n" +
                "         and t3.k3 = t1.k3\n" +
                "         and t3.k1 = 'W'\n" +
                "         and t4.k3 = 'Advanced Degree'\n" +
                "         and t1.k4 between 150.00\n" +
                "         and 200.00\n" +
                "         and t4.k4  = 1\n" +
                "      )\n" +
                "   )\n" +
                "   and(\n" +
                "      (\n" +
                "         t1.k1 = t5.k1\n" +
                "         and t5.k2 = 'United States'\n" +
                "         and t5.k3  in ('CO', 'IL', 'MN')\n" +
                "         and t1.k4 between 100\n" +
                "         and 200\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k1 = t5.k1\n" +
                "         and t5.k2 = 'United States'\n" +
                "         and t5.k3 in ('OH', 'MT', 'NM')\n" +
                "         and t1.k4 between 150\n" +
                "         and 300\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k1 = t5.k1\n" +
                "         and t5.k2 = 'United States'\n" +
                "         and t5.k3 in ('TX', 'MO', 'MI')\n" +
                "         and t1.k4 between 50 and 250\n" +
                "      )\n" +
                "   );";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        String rewritedFragment1 = "(((`t1`.`k2` = `t4`.`k2`) AND (`t3`.`k3` = `t1`.`k3`)) AND ((((((`t3`.`k1` = 'D')" +
                " AND (`t4`.`k3` = '2 yr Degree')) AND ((`t1`.`k4` >= 100.00) AND (`t1`.`k4` <= 150.00))) AND" +
                " (`t4`.`k4` = 3)) OR ((((`t3`.`k1` = 'S') AND (`t4`.`k3` = 'Secondary')) AND ((`t1`.`k4` >= 50.00)" +
                " AND (`t1`.`k4` <= 100.00))) AND (`t4`.`k4` = 1))) OR ((((`t3`.`k1` = 'W') AND " +
                "(`t4`.`k3` = 'Advanced Degree')) AND ((`t1`.`k4` >= 150.00) AND (`t1`.`k4` <= 200.00)))" +
                " AND (`t4`.`k4` = 1))))";
        String rewritedFragment2 = "(((`t1`.`k1` = `t5`.`k1`) AND (`t5`.`k2` = 'United States')) AND" +
                " ((((`t5`.`k3` IN ('CO', 'IL', 'MN')) AND ((`t1`.`k4` >= 100) AND (`t1`.`k4` <= 200)))" +
                " OR ((`t5`.`k3` IN ('OH', 'MT', 'NM')) AND ((`t1`.`k4` >= 150) AND (`t1`.`k4` <= 300))))" +
                " OR ((`t5`.`k3` IN ('TX', 'MO', 'MI')) AND ((`t1`.`k4` >= 50) AND (`t1`.`k4` <= 250)))))";
        Assert.assertTrue(stmt.toSql().contains(rewritedFragment1));
        Assert.assertTrue(stmt.toSql().contains(rewritedFragment2));

        String sql2 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "(\n" +
                "   t1.k1 = t2.k3\n" +
                "   and t2.k2 = 'United States'\n" +
                "   and t2.k3  in ('CO', 'IL', 'MN')\n" +
                "   and t1.k4 between 100\n" +
                "   and 200\n" +
                ")\n" +
                "or (\n" +
                "   t1.k1 = t2.k1\n" +
                "   and t2.k2 = 'United States1'\n" +
                "   and t2.k3 in ('OH', 'MT', 'NM')\n" +
                "   and t1.k4 between 150\n" +
                "   and 300\n" +
                ")\n" +
                "or (\n" +
                "   t1.k1 = t2.k1\n" +
                "   and t2.k2 = 'United States'\n" +
                "   and t2.k3 in ('TX', 'MO', 'MI')\n" +
                "   and t1.k4 between 50 and 250\n" +
                ")";
        SelectStmt stmt2 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql2, ctx);
        stmt2.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        String fragment3 = "(((((`t1`.`k1` = `t2`.`k3`) AND (`t2`.`k2` = 'United States')) AND " +
                "(`t2`.`k3` IN ('CO', 'IL', 'MN'))) AND ((`t1`.`k4` >= 100) AND (`t1`.`k4` <= 200))) OR" +
                " ((((`t1`.`k1` = `t2`.`k1`) AND (`t2`.`k2` = 'United States1')) AND (`t2`.`k3` IN ('OH', 'MT', 'NM')))" +
                " AND ((`t1`.`k4` >= 150) AND (`t1`.`k4` <= 300)))) OR ((((`t1`.`k1` = `t2`.`k1`) AND " +
                "(`t2`.`k2` = 'United States')) AND (`t2`.`k3` IN ('TX', 'MO', 'MI'))) AND ((`t1`.`k4` >= 50)" +
                " AND (`t1`.`k4` <= 250)))";
        Assert.assertTrue(stmt2.toSql().contains(fragment3));

        String sql3 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "   t1.k1 = t2.k3 or t1.k1 = t2.k3 or t1.k1 = t2.k3";
        SelectStmt stmt3 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql3, ctx);
        stmt3.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertFalse(stmt3.toSql().contains("((`t1`.`k1` = `t2`.`k3`) OR (`t1`.`k1` = `t2`.`k3`)) OR" +
                " (`t1`.`k1` = `t2`.`k3`)"));

        String sql4 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "   t1.k1 = t2.k2 or t1.k1 = t2.k3 or t1.k1 = t2.k3";
        SelectStmt stmt4 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql4, ctx);
        stmt4.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt4.toSql().contains("(`t1`.`k1` = `t2`.`k2`) OR (`t1`.`k1` = `t2`.`k3`)"));

        String sql5 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "   t2.k1 is not null or t1.k1 is not null or t1.k1 is not null";
        SelectStmt stmt5 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql5, ctx);
        stmt5.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt5.toSql().contains("(`t2`.`k1` IS NOT NULL) OR (`t1`.`k1` IS NOT NULL)"));
        Assert.assertEquals(2, stmt5.toSql().split(" OR ").length);

        String sql6 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "   t2.k1 is not null or t1.k1 is not null and t1.k1 is not null";
        SelectStmt stmt6 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql6, ctx);
        stmt6.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt6.toSql().contains("(`t2`.`k1` IS NOT NULL) OR (`t1`.`k1` IS NOT NULL)"));
        Assert.assertEquals(2, stmt6.toSql().split(" OR ").length);

        String sql7 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "   t2.k1 is not null or t1.k1 is not null and t1.k2 is not null";
        SelectStmt stmt7 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql7, ctx);
        stmt7.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt7.toSql().contains("(`t2`.`k1` IS NOT NULL) OR ((`t1`.`k1` IS NOT NULL) " +
                "AND (`t1`.`k2` IS NOT NULL))"));

        String sql8 = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "where\n" +
                "   t2.k1 is not null and t1.k1 is not null and t1.k1 is not null";
        SelectStmt stmt8 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql8, ctx);
        stmt8.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt8.toSql().contains("((`t2`.`k1` IS NOT NULL) AND (`t1`.`k1` IS NOT NULL))" +
                " AND (`t1`.`k1` IS NOT NULL)"));

        String sql9 = "select * from db1.tbl1 where (k1='shutdown' and k4<1) or (k1='switchOff' and k4>=1)";
        SelectStmt stmt9 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql9, ctx);
        stmt9.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt9.toSql().contains("((`k1` = 'shutdown') AND (`k4` < 1))" +
                " OR ((`k1` = 'switchOff') AND (`k4` >= 1))"));
    }

    @Test
    public void testForbiddenCorrelatedSubqueryInHavingClause() throws Exception {
        String sql = "SELECT k1 FROM baseall GROUP BY k1 HAVING EXISTS(SELECT k4 FROM tbl1 GROUP BY k4 HAVING SUM"
                + "(baseall.k1) = k4);";
        try {
            dorisAssert.query(sql).explainQuery();
            Assert.fail("The correlated subquery in having clause should be forbidden.");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testGroupByConstantExpression() throws Exception {
        String sql = "SELECT k1 - 4*60*60 FROM baseall GROUP BY k1 - 4*60*60";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testMultrGroupByInCorrelationSubquery() throws Exception {
        String sql = "SELECT * from baseall where k1 > (select min(k1) from tbl1 where baseall.k1 = tbl1.k4 and baseall.k2 = tbl1.k2)";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testOuterJoinNullUnionView() throws Exception{
        String sql = "WITH test_view(k) AS(SELECT NULL AS k UNION ALL SELECT NULL AS k )\n" +
                "SELECT v1.k FROM test_view AS v1 LEFT OUTER JOIN test_view AS v2 ON v1.k=v2.k";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testDataGripSupport() throws Exception {
        String sql = "select schema();";
        dorisAssert.query(sql).explainQuery();
        sql = "select\n" +
                "collation_name,\n" +
                "character_set_name,\n" +
                "is_default collate utf8_general_ci = 'Yes' as is_default\n" +
                "from information_schema.collations";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testRandFunction() throws Exception {
        String sql = "select rand(db1.tbl1.k1) from db1.tbl1;";
        try {
            dorisAssert.query(sql).explainQuery();
            Assert.fail("The param of rand function must be literal");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }
        sql = "select rand(1234) from db1.tbl1;";
        dorisAssert.query(sql).explainQuery();
        sql = "select rand() from db1.tbl1;";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testImplicitConvertSupport() throws Exception {
        String sql1 = "select count(*) from db1.partition_table where datekey='20200730'";
        Assert.assertTrue(dorisAssert
                .query(sql1)
                .explainQuery()
                .contains("`datekey` = 20200730"));
        String sql2 = "select count(*) from db1.partition_table where '20200730'=datekey";
        Assert.assertTrue(dorisAssert
                .query(sql2)
                .explainQuery()
                .contains("`datekey` = 20200730"));
        String sql3= "select count() from db1.date_partition_table where dt=20200908";
        Assert.assertTrue(dorisAssert
                .query(sql3)
                .explainQuery()
                .contains("`dt` = '2020-09-08 00:00:00'"));
        String sql4= "select count() from db1.date_partition_table where dt='2020-09-08'";
        Assert.assertTrue(dorisAssert
                .query(sql4)
                .explainQuery()
                .contains("`dt` = '2020-09-08 00:00:00'"));
    }

    @Test
    public void testDeleteSign() throws Exception {
        String sql1 = "SELECT * FROM db1.table1  LEFT ANTI JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        String explain = dorisAssert.query(sql1).explainQuery();
        Assert.assertTrue(explain
                .contains("PREDICATES: `default_cluster:db1.table1`.`__DORIS_DELETE_SIGN__` = 0"));
        Assert.assertTrue(explain
                .contains("PREDICATES: `default_cluster:db1.table2`.`__DORIS_DELETE_SIGN__` = 0"));
        Assert.assertFalse(explain.contains("other predicates:"));
        String sql2 = "SELECT * FROM db1.table1 JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        explain = dorisAssert.query(sql2).explainQuery();
        Assert.assertTrue(explain
                .contains("PREDICATES: `default_cluster:db1.table1`.`__DORIS_DELETE_SIGN__` = 0"));
        Assert.assertTrue(explain
                .contains("PREDICATES: `default_cluster:db1.table2`.`__DORIS_DELETE_SIGN__` = 0"));
        Assert.assertFalse(explain.contains("other predicates:"));
        String sql3 = "SELECT * FROM db1.table1";
        Assert.assertTrue(dorisAssert.query(sql3).explainQuery()
                .contains("PREDICATES: `default_cluster:db1.table1`.`__DORIS_DELETE_SIGN__` = 0"));
        String sql4 = " SELECT * FROM db1.table1 table2";
        Assert.assertTrue(dorisAssert.query(sql4).explainQuery()
                .contains("PREDICATES: `table2`.`__DORIS_DELETE_SIGN__` = 0"));
        new MockUp<Util>() {
            @Mock
            public boolean showHiddenColumns() {
                return true;
            }
        };
        String sql5 = "SELECT * FROM db1.table1  LEFT ANTI JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        Assert.assertFalse(dorisAssert.query(sql5).explainQuery().contains("`table1`.`__DORIS_DELETE_SIGN__` = 0"));
        String sql6 = "SELECT * FROM db1.table1 JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        Assert.assertFalse(dorisAssert.query(sql6).explainQuery().contains("`table1`.`__DORIS_DELETE_SIGN__` = 0"));
        String sql7 = "SELECT * FROM db1.table1";
        Assert.assertFalse(dorisAssert.query(sql7).explainQuery().contains("`table1`.`__DORIS_DELETE_SIGN__` = 0"));
        String sql8 = " SELECT * FROM db1.table1 table2";
        Assert.assertFalse(dorisAssert.query(sql8).explainQuery().contains("`table2`.`__DORIS_DELETE_SIGN__` = 0"));
    }

    @Test
    public void testSelectHintSetVar() throws Exception {
        String sql = "SELECT sleep(3);";
        Planner planner = dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(VariableMgr.getDefaultSessionVariable().getQueryTimeoutS(),
                planner.getPlannerContext().getQueryOptions().query_timeout);

        sql = "SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);";
        planner = dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(1, planner.getPlannerContext().getQueryOptions().query_timeout);

        sql = "select * from db1.partition_table where datekey=20200726";
        planner = dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(VariableMgr.getDefaultSessionVariable().getMaxExecMemByte(),
                planner.getPlannerContext().getQueryOptions().mem_limit);

        sql = "select /*+ SET_VAR(exec_mem_limit = 8589934592) */ poi_id, count(*) from db1.partition_table " +
                "where datekey=20200726 group by 1";
        planner = dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(8589934592L, planner.getPlannerContext().getQueryOptions().mem_limit);
	sql = "select /*+ SET_VAR(exec_mem_limit = 8589934592, query_timeout = 1) */ 1 + 2;";
	planner = dorisAssert.query(sql).internalExecuteOneAndGetPlan();
	Assert.assertEquals(1, planner.getPlannerContext().getQueryOptions().query_timeout);
	Assert.assertEquals(8589934592L, planner.getPlannerContext().getQueryOptions().mem_limit);
    }

    @Test
    public void testWithWithoutDatabase() throws Exception {
        String sql = "with tmp as (select count(*) from db1.table1) select * from tmp;";
        dorisAssert.withoutUseDatabase();
        dorisAssert.query(sql).explainQuery();

        sql = "with tmp as (select * from db1.table1) " +
                "select a.siteid, b.citycode, a.siteid from (select siteid, citycode from tmp) a " +
                "left join (select siteid, citycode from tmp) b on a.siteid = b.siteid;";
        dorisAssert.withoutUseDatabase();
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testWithInNestedQueryStmt() throws Exception {
        String sql = "select 1 from (with w as (select 1 from db1.table1) select 1 from w) as tt";
        dorisAssert.query(sql).explainQuery();
    }
}
