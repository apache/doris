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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.Util;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SelectStmtTest {
    private static final String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID() + "/";
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
        Config.enable_http_server_v2 = false;
        UtFrameUtils.createDorisCluster(runningDir);
        FeConstants.runningUnitTest = true;
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32),"
                + " k2 varchar(32), k3 varchar(32), k4 int, k5 largeint) "
                + "AGGREGATE KEY(k1, k2,k3,k4,k5) distributed by hash(k1) buckets 3"
                + " properties('replication_num' = '1');";
        String createBaseAllStmtStr = "create table db1.baseall(k1 int, k2 varchar(32)) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        String createPratitionTableStr = "CREATE TABLE db1.partition_table (\n"
                + "datekey int(11) NULL COMMENT \"datekey\",\n"
                + "poi_id bigint(20) NULL COMMENT \"poi_id\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(datekey, poi_id)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(datekey)\n"
                + "(PARTITION p20200727 VALUES [(\"20200726\"), (\"20200727\")),\n"
                + "PARTITION p20200728 VALUES [(\"20200727\"), (\"20200728\")))\n"
                + "DISTRIBUTED BY HASH(poi_id) BUCKETS 2\n"
                + "PROPERTIES (\n"
                + "\"storage_type\" = \"COLUMN\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");";
        String createDatePartitionTableStr = "CREATE TABLE db1.date_partition_table (\n"
                + "  `dt` date NOT NULL COMMENT \"\",\n"
                + "  `poi_id` bigint(20) NULL COMMENT \"poi_id\",\n"
                + "  `uv1` bitmap BITMAP_UNION NOT NULL COMMENT \"\",\n"
                + "  `uv2` bitmap BITMAP_UNION NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(    PARTITION `p201701` VALUES LESS THAN (\"2020-09-08\"),\n"
                + "    PARTITION `p201702` VALUES LESS THAN (\"2020-09-09\"),\n"
                + "    PARTITION `p201703` VALUES LESS THAN (\"2020-09-10\"))\n"
                + "DISTRIBUTED BY HASH(`poi_id`) BUCKETS 20\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ");";
        String tbl1 = "CREATE TABLE db1.table1 (\n"
                + "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n"
                + "  `citycode` smallint(6) NULL COMMENT \"\",\n"
                + "  `username` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n"
                + "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`siteid`, `citycode`, `username`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`siteid`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")";
        String tbl2 = "CREATE TABLE db1.table2 (\n"
                + "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n"
                + "  `citycode` smallint(6) NULL COMMENT \"\",\n"
                + "  `username` varchar(32) NOT NULL DEFAULT \"\" COMMENT \"\",\n"
                + "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`siteid`, `citycode`, `username`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`siteid`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")";
        String tbl3 = "CREATE TABLE db1.table3 (\n"
                + "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n"
                + "  `citycode` smallint(6) NULL COMMENT \"\",\n"
                + "  `username` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n"
                + "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`siteid`, `citycode`, `username`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(createTblStmtStr)
                   .withTable(createBaseAllStmtStr)
                   .withTable(createPratitionTableStr)
                   .withTable(createDatePartitionTableStr)
                   .withTable(tbl1)
                   .withTable(tbl2)
                   .withTable(tbl3);
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

    @Disabled
    public void testSubqueryInCase() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql1 = "SELECT CASE\n"
                + "        WHEN (\n"
                + "            SELECT COUNT(*) / 2\n"
                + "            FROM db1.tbl1\n"
                + "        ) > k4 THEN (\n"
                + "            SELECT AVG(k4)\n"
                + "            FROM db1.tbl1\n"
                + "        )\n"
                + "        ELSE (\n"
                + "            SELECT SUM(k4)\n"
                + "            FROM db1.tbl1\n"
                + "        )\n"
                + "    END AS kk4\n"
                + "FROM db1.tbl1;";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.toSql().contains("`$a$1`.`$c$1` > `k4` THEN `$a$2`.`$c$2` ELSE `$a$3`.`$c$3`"));

        String sql2 = "select case when k1 in (select k1 from db1.tbl1) then \"true\" else k1 end a from db1.tbl1";
        try {
            SelectStmt stmt2 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql2, ctx);
            stmt2.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
            Assert.fail("syntax not supported.");
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.fail("must be AnalysisException.");
        }
        try {
            String sql3 = "select case k1 when exists (select 1) then \"empty\" else \"p_test\" end a from db1.tbl1";
            SelectStmt stmt3 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql3, ctx);
            stmt3.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
            Assert.fail("syntax not supported.");
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.fail("must be AnalysisException.");
        }
        String sql4 = "select case when k1 < (select max(k1) from db1.tbl1) and "
                + "k1 > (select min(k1) from db1.tbl1) then \"empty\" else \"p_test\" end a from db1.tbl1";
        SelectStmt stmt4 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql4, ctx);
        stmt4.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt4.toSql().contains("`k1` < `$a$1`.`$c$1` AND `k1` > `$a$2`.`$c$2`"));

        String sql5 = "select case when k1 < (select max(k1) from db1.tbl1) is null "
                + "then \"empty\" else \"p_test\" end a from db1.tbl1";
        SelectStmt stmt5 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql5, ctx);
        stmt5.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt5.toSql().contains(" `k1` < `$a$1`.`$c$1` IS NULL "));
    }

    @Test
    public void testDeduplicateOrs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2,\n"
                + "   db1.tbl1 t3,\n"
                + "   db1.tbl1 t4,\n"
                + "   db1.tbl1 t5,\n"
                + "   db1.tbl1 t6\n"
                + "where\n"
                + "   t2.k1 = t1.k1\n"
                + "   and t1.k2 = t6.k2\n"
                + "   and t6.k4 = 2001\n"
                + "   and(\n"
                + "      (\n"
                + "         t1.k2 = t4.k2\n"
                + "         and t3.k3 = t1.k3\n"
                + "         and t3.k1 = 'D'\n"
                + "         and t4.k3 = '2 yr Degree'\n"
                + "         and t1.k4 between 100.00\n"
                + "         and 150.00\n"
                + "         and t4.k4 = 3\n"
                + "      )\n"
                + "      or (\n"
                + "         t1.k2 = t4.k2\n"
                + "         and t3.k3 = t1.k3\n"
                + "         and t3.k1 = 'S'\n"
                + "         and t4.k3 = 'Secondary'\n"
                + "         and t1.k4 between 50.00\n"
                + "         and 100.00\n"
                + "         and t4.k4 = 1\n"
                + "      )\n"
                + "      or (\n"
                + "         t1.k2 = t4.k2\n"
                + "         and t3.k3 = t1.k3\n"
                + "         and t3.k1 = 'W'\n"
                + "         and t4.k3 = 'Advanced Degree'\n"
                + "         and t1.k4 between 150.00\n"
                + "         and 200.00\n"
                + "         and t4.k4  = 1\n"
                + "      )\n"
                + "   )\n"
                + "   and(\n"
                + "      (\n"
                + "         t1.k1 = t5.k1\n"
                + "         and t5.k2 = 'United States'\n"
                + "         and t5.k3  in ('CO', 'IL', 'MN')\n"
                + "         and t1.k4 between 100\n"
                + "         and 200\n"
                + "      )\n"
                + "      or (\n"
                + "         t1.k1 = t5.k1\n"
                + "         and t5.k2 = 'United States'\n"
                + "         and t5.k3 in ('OH', 'MT', 'NM')\n"
                + "         and t1.k4 between 150\n"
                + "         and 300\n"
                + "      )\n"
                + "      or (\n"
                + "         t1.k1 = t5.k1\n"
                + "         and t5.k2 = 'United States'\n"
                + "         and t5.k3 in ('TX', 'MO', 'MI')\n"
                + "         and t1.k4 between 50 and 250\n"
                + "      )\n"
                + "   );";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        String commonExpr1 = "`t1`.`k2` = `t4`.`k2`";
        String commonExpr2 = "`t3`.`k3` = `t1`.`k3`";
        String commonExpr3 = "`t1`.`k1` = `t5`.`k1`";
        String commonExpr4 = "t5`.`k2` = 'United States'";
        String betweenExpanded1 = "CAST(CAST(`t1`.`k4` AS DECIMALV3(12, 2)) AS INT) >= 100 AND CAST(CAST(`t1`.`k4` AS DECIMALV3(12, 2)) AS INT) <= 150";
        String betweenExpanded2 = "CAST(CAST(`t1`.`k4` AS DECIMALV3(12, 2)) AS INT) >= 50 AND CAST(CAST(`t1`.`k4` AS DECIMALV3(12, 2)) AS INT) <= 100";
        String betweenExpanded3 = "`t1`.`k4` >= 50 AND `t1`.`k4` <= 250";

        String rewrittenSql = stmt.toSql();
        Assert.assertTrue(rewrittenSql.contains(commonExpr1));
        Assert.assertEquals(rewrittenSql.indexOf(commonExpr1), rewrittenSql.lastIndexOf(commonExpr1));
        Assert.assertTrue(rewrittenSql.contains(commonExpr2));
        Assert.assertEquals(rewrittenSql.indexOf(commonExpr2), rewrittenSql.lastIndexOf(commonExpr2));
        Assert.assertTrue(rewrittenSql.contains(commonExpr3));
        Assert.assertEquals(rewrittenSql.indexOf(commonExpr3), rewrittenSql.lastIndexOf(commonExpr3));
        Assert.assertTrue(rewrittenSql.contains(commonExpr4));
        Assert.assertEquals(rewrittenSql.indexOf(commonExpr4), rewrittenSql.lastIndexOf(commonExpr4));
        Assert.assertTrue(rewrittenSql.contains(betweenExpanded1));
        Assert.assertTrue(rewrittenSql.contains(betweenExpanded2));
        Assert.assertTrue(rewrittenSql.contains(betweenExpanded3));

        String sql2 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "(\n"
                + "   t1.k1 = t2.k3\n"
                + "   and t2.k2 = 'United States'\n"
                + "   and t2.k3  in ('CO', 'IL', 'MN')\n"
                + "   and t1.k4 between 100\n"
                + "   and 200\n"
                + ")\n"
                + "or (\n"
                + "   t1.k1 = t2.k1\n"
                + "   and t2.k2 = 'United States1'\n"
                + "   and t2.k3 in ('OH', 'MT', 'NM')\n"
                + "   and t1.k4 between 150\n"
                + "   and 300\n"
                + ")\n"
                + "or (\n"
                + "   t1.k1 = t2.k1\n"
                + "   and t2.k2 = 'United States'\n"
                + "   and t2.k3 in ('TX', 'MO', 'MI')\n"
                + "   and t1.k4 between 50 and 250\n"
                + ")";
        SelectStmt stmt2 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql2, ctx);
        stmt2.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        String fragment3 =
                "(((`t1`.`k4` >= 50 AND `t1`.`k4` <= 300) AND `t2`.`k2` IN ('United States', 'United States1') "
                        + "AND `t2`.`k3` IN ('CO', 'IL', 'MN', 'OH', 'MT', 'NM', 'TX', 'MO', 'MI')) "
                        + "AND `t1`.`k1` = `t2`.`k3` AND `t2`.`k2` = 'United States' "
                        + "AND `t2`.`k3` IN ('CO', 'IL', 'MN') AND `t1`.`k4` >= 100 AND `t1`.`k4` <= 200 "
                        + "OR "
                        + "`t1`.`k1` = `t2`.`k1` AND `t2`.`k2` = 'United States1' "
                        + "AND `t2`.`k3` IN ('OH', 'MT', 'NM') AND `t1`.`k4` >= 150 AND `t1`.`k4` <= 300 "
                        + "OR "
                        + "`t1`.`k1` = `t2`.`k1` AND `t2`.`k2` = 'United States' "
                        + "AND `t2`.`k3` IN ('TX', 'MO', 'MI') "
                        + "AND `t1`.`k4` >= 50 AND `t1`.`k4` <= 250)";
        Assert.assertTrue(stmt2.toSql().contains(fragment3));

        String sql3 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "   t1.k1 = t2.k3 or t1.k1 = t2.k3 or t1.k1 = t2.k3";
        SelectStmt stmt3 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql3, ctx);
        stmt3.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertFalse(
                stmt3.toSql().contains("`t1`.`k1` = `t2`.`k3` OR `t1`.`k1` = `t2`.`k3` OR" + " `t1`.`k1` = `t2`.`k3`"));

        String sql4 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "   t1.k1 = t2.k2 or t1.k1 = t2.k3 or t1.k1 = t2.k3";
        SelectStmt stmt4 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql4, ctx);
        stmt4.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt4.toSql().contains("`t1`.`k1` = `t2`.`k2` OR `t1`.`k1` = `t2`.`k3`"));

        String sql5 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "   t2.k1 is not null or t1.k1 is not null or t1.k1 is not null";
        SelectStmt stmt5 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql5, ctx);
        stmt5.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt5.toSql().contains("`t2`.`k1` IS NOT NULL OR `t1`.`k1` IS NOT NULL"));
        Assert.assertEquals(2, stmt5.toSql().split(" OR ").length);

        String sql6 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "   t2.k1 is not null or t1.k1 is not null and t1.k1 is not null";
        SelectStmt stmt6 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql6, ctx);
        stmt6.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt6.toSql().contains("`t2`.`k1` IS NOT NULL OR `t1`.`k1` IS NOT NULL"));
        Assert.assertEquals(2, stmt6.toSql().split(" OR ").length);

        String sql7 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "   t2.k1 is not null or t1.k1 is not null and t1.k2 is not null";
        SelectStmt stmt7 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql7, ctx);
        stmt7.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt7.toSql()
                .contains("`t2`.`k1` IS NOT NULL OR `t1`.`k1` IS NOT NULL AND `t1`.`k2` IS NOT NULL"));

        String sql8 = "select\n"
                + "   avg(t1.k4)\n"
                + "from\n"
                + "   db1.tbl1 t1,\n"
                + "   db1.tbl1 t2\n"
                + "where\n"
                + "   t2.k1 is not null and t1.k1 is not null and t1.k1 is not null";
        SelectStmt stmt8 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql8, ctx);
        stmt8.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt8.toSql()
                .contains("`t2`.`k1` IS NOT NULL AND `t1`.`k1` IS NOT NULL AND `t1`.`k1` IS NOT NULL"));

        String sql9 = "select * from db1.tbl1 where (k1='shutdown' and k4<1) or (k1='switchOff' and k4>=1)";
        SelectStmt stmt9 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql9, ctx);
        stmt9.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(
                stmt9.toSql().contains("`k1` = 'shutdown' AND `k4` < 1 OR `k1` = 'switchOff' AND `k4` >= 1"));
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
    public void testOuterJoinNullUnionView() throws Exception {
        String sql = "WITH test_view(k) AS(SELECT NULL AS k UNION ALL SELECT NULL AS k )\n"
                + "SELECT v1.k FROM test_view AS v1 LEFT OUTER JOIN test_view AS v2 ON v1.k=v2.k";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testDataGripSupport() throws Exception {
        String sql = "select schema();";
        dorisAssert.query(sql).explainQuery();
        sql = "select\n"

                + "collation_name,\n"

                + "character_set_name,\n"

                + "is_default collate utf8_general_ci = 'Yes' as is_default\n"
                + "from information_schema.collations";
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
        String sql1 = "select /*+ SET_VAR(enable_nereids_planner=false) */ count(*) from db1.partition_table where datekey='20200730'";
        Assert.assertTrue(dorisAssert
                .query(sql1)
                .explainQuery()
                .contains("`datekey` = 20200730"));
        String sql2 = "select /*+ SET_VAR(enable_nereids_planner=false) */ count(*) from db1.partition_table where '20200730'=datekey";
        Assert.assertTrue(dorisAssert
                .query(sql2)
                .explainQuery()
                .contains("`datekey` = 20200730"));
        String sql3 = "select /*+ SET_VAR(enable_nereids_planner=false) */ count() from db1.date_partition_table where dt=20200908";
        Assert.assertTrue(dorisAssert
                .query(sql3)
                .explainQuery()
                .contains(Config.enable_date_conversion ? "`dt` = '2020-09-08'" : "`dt` = '2020-09-08 00:00:00'"));
        String sql4 = "select /*+ SET_VAR(enable_nereids_planner=false) */ count() from db1.date_partition_table where dt='2020-09-08'";
        Assert.assertTrue(dorisAssert
                .query(sql4)
                .explainQuery()
                .contains(Config.enable_date_conversion ? "`dt` = '2020-09-08'" : "`dt` = '2020-09-08 00:00:00'"));
    }

    @Test
    public void testDeleteSign() throws Exception {
        String sql1 = "SELECT /*+ SET_VAR(enable_nereids_planner=true, ENABLE_FALLBACK_TO_ORIGINAL_PLANNER=false) */ * FROM db1.table1  LEFT ANTI JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        String explain = dorisAssert.query(sql1).explainQuery();
        Assert.assertTrue(explain
                .contains("PREDICATES: __DORIS_DELETE_SIGN__ = 0"));
        Assert.assertFalse(explain.contains("other predicates:"));
        String sql2 = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * FROM db1.table1 JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        explain = dorisAssert.query(sql2).explainQuery();
        Assert.assertTrue(explain
                .contains("PREDICATES: `default_cluster:db1`.`table1`.`__DORIS_DELETE_SIGN__` = 0"));
        Assert.assertTrue(explain
                .contains("PREDICATES: `default_cluster:db1`.`table2`.`__DORIS_DELETE_SIGN__` = 0"));
        Assert.assertFalse(explain.contains("other predicates:"));
        String sql3 = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * FROM db1.table1";
        Assert.assertTrue(dorisAssert.query(sql3).explainQuery()
                .contains("PREDICATES: `default_cluster:db1`.`table1`.`__DORIS_DELETE_SIGN__` = 0"));
        String sql4 = " SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * FROM db1.table1 table2";
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
    public void testSelectHints() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // hint with integer literal parameter
        String sql = "select /*+ common_hint(1) */ 1";
        UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);

        // hint with float literal parameter
        sql = "select /*+ common_hint(1.1) */ 1";
        UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);

        // hint with string literal parameter
        sql = "select /*+ common_hint(\"string\") */ 1";
        UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);

        // hint with key value parameter
        sql = "select /*+ common_hint(k = \"v\") */ 1";
        UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);

        // hint with multi-parameters
        sql = "select /*+ common_hint(1, 1.1, \"string\") */ 1";
        UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);

        // multi-hints
        sql = "select /*+ common_hint(1) another_hint(2) */ 1";
        UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
    }

    @Test
    public void testSelectHintSetVar() throws Exception {
        String sql = "SELECT sleep(3);";
        OriginalPlanner planner = (OriginalPlanner) dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(VariableMgr.getDefaultSessionVariable().getQueryTimeoutS(),
                planner.getPlannerContext().getQueryOptions().query_timeout);

        sql = "SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);";
        planner = (OriginalPlanner) dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(1, planner.getPlannerContext().getQueryOptions().query_timeout);

        sql = "select * from db1.partition_table where datekey=20200726";
        planner = (OriginalPlanner) dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(VariableMgr.getDefaultSessionVariable().getMaxExecMemByte(),
                planner.getPlannerContext().getQueryOptions().mem_limit);

        sql = "select /*+ SET_VAR(exec_mem_limit = 8589934592) */ poi_id, count(*) from db1.partition_table "
                + "where datekey=20200726 group by 1";
        planner = (OriginalPlanner) dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        Assert.assertEquals(8589934592L, planner.getPlannerContext().getQueryOptions().mem_limit);

        int queryTimeOut = dorisAssert.getSessionVariable().getQueryTimeoutS();
        long execMemLimit = dorisAssert.getSessionVariable().getMaxExecMemByte();
        sql = "select /*+ SET_VAR(exec_mem_limit = 8589934592, query_timeout = 1) */ 1 + 2;";
        planner = (OriginalPlanner) dorisAssert.query(sql).internalExecuteOneAndGetPlan();
        // session variable have been changed
        Assert.assertEquals(1, planner.getPlannerContext().getQueryOptions().query_timeout);
        Assert.assertEquals(8589934592L, planner.getPlannerContext().getQueryOptions().mem_limit);
        // session variable change have been reverted
        Assert.assertEquals(queryTimeOut, dorisAssert.getSessionVariable().getQueryTimeoutS());
        Assert.assertEquals(execMemLimit, dorisAssert.getSessionVariable().getMaxExecMemByte());
    }

    @Test
    public void testWithWithoutDatabase() throws Exception {
        String sql = "with tmp as (select count(*) from db1.table1) select * from tmp;";
        dorisAssert.withoutUseDatabase();
        dorisAssert.query(sql).explainQuery();

        sql = "with tmp as (select * from db1.table1) "
                + "select a.siteid, b.citycode, a.siteid from (select siteid, citycode from tmp) a "
                + "left join (select siteid, citycode from tmp) b on a.siteid = b.siteid;";
        dorisAssert.withoutUseDatabase();
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testWithInNestedQueryStmt() throws Exception {
        String sql = "select 1 from (with w as (select 1 from db1.table1) select 1 from w) as tt";
        dorisAssert.query(sql).explainQuery();
    }

    @Test
    public void testGetTableRefs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "SELECT * FROM db1.table1 JOIN db1.table2 ON db1.table1.siteid = db1.table2.siteid;";
        dorisAssert.query(sql).explainQuery();
        QueryStmt stmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        List<TableRef> tblRefs = Lists.newArrayList();
        Set<String> parentViewNameSet = Sets.newHashSet();
        stmt.getTableRefs(new Analyzer(ctx.getEnv(), ctx), tblRefs, parentViewNameSet);

        Assert.assertEquals(2, tblRefs.size());
        Assert.assertEquals("table1", tblRefs.get(0).getName().getTbl());
        Assert.assertEquals("table2", tblRefs.get(1).getName().getTbl());
    }

    @Test
    public void testOutfile() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_outfile_to_local = true;
        String sql
                = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET PROPERTIES (\"schema\"=\"required,byte_array,col0\");";
        dorisAssert.query(sql).explainQuery();
        // if shema not set, gen schema
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET;";
        try {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            Assert.assertEquals(1, stmt.getOutFileClause().getParquetSchemas().size());
            Assert.assertEquals("k1", stmt.getOutFileClause().getParquetSchemas().get(0).schema_column_name);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        // schema can not be empty
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET PROPERTIES (\"schema\"=\"\");";
        try {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Parquet schema property should not be empty"));
        }

        // schema must contains 3 fields
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET PROPERTIES (\"schema\"=\"int32,siteid;\");";
        try {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("must only contains repetition type/column type/column name"));
        }

        // unknown repetition type
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET PROPERTIES (\"schema\"=\"repeat, int32,siteid;\");";
        try {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("unknown repetition type"));
        }

        // only support required type
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET PROPERTIES (\"schema\"=\"repeated,int32,siteid;\");";
        try {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("currently only support required type"));
        }

        // unknown data type
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\" FORMAT AS PARQUET PROPERTIES (\"schema\"=\"required,int128,siteid;\");";
        try {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("data type is not supported"));
        }

        // contains parquet properties
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"file:///root/doris/\""
                + " FORMAT AS PARQUET"
                + " PROPERTIES (\"schema\"=\"required,byte_array,siteid;\","
                + " 'parquet.compression'='snappy');";
        dorisAssert.query(sql).explainQuery();
        // support parquet for broker
        sql = "SELECT k1 FROM db1.tbl1 INTO OUTFILE \"hdfs://test/test_sql_prc_2019_02_19/\" FORMAT AS PARQUET "
                + "PROPERTIES (     \"broker.name\" = \"hdfs_broker\",     "
                + "\"broker.hadoop.security.authentication\" = \"kerberos\",     "
                + "\"broker.kerberos_principal\" = \"test\",     "
                + "\"broker.kerberos_keytab_content\" = \"test\" , "
                + "\"schema\"=\"required,byte_array,siteid;\");";
        dorisAssert.query(sql).explainQuery();

        // do not support large int type
        try {
            sql = "SELECT k5 FROM db1.tbl1 INTO OUTFILE \"hdfs://test/test_sql_prc_2019_02_19/\" FORMAT AS PARQUET "
                    + "PROPERTIES (     \"broker.name\" = \"hdfs_broker\",     "
                    + "\"broker.hadoop.security.authentication\" = \"kerberos\",     "
                    + "\"broker.kerberos_principal\" = \"test\",     "
                    + "\"broker.kerberos_keytab_content\" = \"test\" ,"
                    + " \"schema\"=\"required,int32,siteid;\");";
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().contains("should use byte_array"));
        }

        // do not support large int type, contains function
        try {
            sql = "SELECT sum(k5) FROM db1.tbl1 group by k5 INTO OUTFILE \"hdfs://test/test_sql_prc_2019_02_19/\" "
                    + "FORMAT AS PARQUET PROPERTIES (     \"broker.name\" = \"hdfs_broker\",     "
                    + "\"broker.hadoop.security.authentication\" = \"kerberos\",     "
                    + "\"broker.kerberos_principal\" = \"test\",     "
                    + "\"broker.kerberos_keytab_content\" = \"test\" , "
                    + "\"schema\"=\"required,int32,siteid;\");";
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("should use byte_array"));
        }

        // support cast
        try {
            sql = "SELECT cast(sum(k5) as bigint) FROM db1.tbl1 group by k5"
                    + " INTO OUTFILE \"hdfs://test/test_sql_prc_2019_02_19/\" "
                    + "FORMAT AS PARQUET PROPERTIES (     \"broker.name\" = \"hdfs_broker\",     "
                    + "\"broker.hadoop.security.authentication\" = \"kerberos\",     "
                    + "\"broker.kerberos_principal\" = \"test\",     "
                    + "\"broker.kerberos_keytab_content\" = \"test\" , "
                    + "\"schema\"=\"required,int64,siteid;\");";
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx); // CHECKSTYLE IGNORE THIS LINE
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSystemViewCaseInsensitive() throws Exception {
        String sql1 = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = "
                + "'ech_dw' ORDER BY ROUTINES.ROUTINE_SCHEMA\n";
        // The system view names in information_schema are case-insensitive,
        dorisAssert.query(sql1).explainQuery();

        String sql2 = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = "
                + "'ech_dw' ORDER BY routines.ROUTINE_SCHEMA\n";
        try {
            // Should not refer to one of system views using different cases within the same statement.
            // sql2 is wrong because 'ROUTINES' and 'routines' are used.
            dorisAssert.query(sql2).explainQuery();
            Assert.fail("Refer to one of system views using different cases within the same statement is wrong.");
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testWithUnionToSql() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql1 =
                "select \n"
                + "  t.k1 \n"
                + "from (\n"
                + "  with \n"
                + "    v1 as (select t1.k1 from db1.tbl1 t1),\n"
                + "    v2 as (select t2.k1 from db1.tbl1 t2)\n"
                + "  select v1.k1 as k1 from v1\n"
                + "  union\n"
                + "  select v2.k1 as k1 from v2\n"
                + ") t";
        SelectStmt stmt1 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt1.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertEquals("SELECT `t`.`k1` AS `k1` FROM (WITH v1 AS (SELECT `t1`.`k1` "
                + "FROM `default_cluster:db1`.`tbl1` t1),v2 AS (SELECT `t2`.`k1` FROM `default_cluster:db1`.`tbl1` t2) "
                + "SELECT `v1`.`k1` AS `k1` FROM `v1` UNION SELECT `v2`.`k1` AS `k1` FROM `v2`) t", stmt1.toSql());

        String sql2 =
                "with\n"
                + "    v1 as (select t1.k1 from db1.tbl1 t1),\n"
                + "    v2 as (select t2.k1 from db1.tbl1 t2)\n"
                + "select\n"
                + "  t.k1\n"
                + "from (\n"
                + "  select v1.k1 as k1 from v1\n"
                + "  union\n"
                + "  select v2.k1 as k1 from v2\n"
                + ") t";
        SelectStmt stmt2 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql2, ctx);
        stmt2.rewriteExprs(new Analyzer(ctx.getEnv(), ctx).getExprRewriter());
        Assert.assertTrue(stmt2.toSql().contains("WITH v1 AS (SELECT `t1`.`k1` FROM `default_cluster:db1`.`tbl1` t1),"
                + "v2 AS (SELECT `t2`.`k1` FROM `default_cluster:db1`.`tbl1` t2)"));
    }

    @Test
    public void testSelectOuterJoinSql() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql1 = "select l.citycode, group_concat(distinct r.username) from db1.table1 l "
                + "left join db1.table2 r on l.citycode=r.citycode group by l.citycode";
        SelectStmt stmt1 = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        Assert.assertFalse(stmt1.getAnalyzer().getSlotDesc(new SlotId(2)).getIsNullable());
        Assert.assertFalse(stmt1.getAnalyzer().getSlotDescriptor("r.username").getIsNullable());
        FunctionCallExpr expr = (FunctionCallExpr) stmt1.getSelectList().getItems().get(1).getExpr();
        Assert.assertTrue(expr.getFnParams().isDistinct());
    }

    @Test
    public void testHashBucketSelectTablet() throws Exception {
        String sql1 = "SELECT * FROM db1.table1 TABLET(10031,10032,10033)";
        OriginalPlanner planner = (OriginalPlanner) dorisAssert.query(sql1).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds = ((OlapScanNode) planner.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertTrue(sampleTabletIds.contains(10031L));
        Assert.assertTrue(sampleTabletIds.contains(10032L));
        Assert.assertTrue(sampleTabletIds.contains(10033L));
    }

    @Test
    public void testRandomBucketSelectTablet() throws Exception {
        String sql1 = "SELECT * FROM db1.table3 TABLET(10031,10032,10033)";
        OriginalPlanner planner = (OriginalPlanner) dorisAssert.query(sql1).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds = ((OlapScanNode) planner.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertTrue(sampleTabletIds.contains(10031L));
        Assert.assertTrue(sampleTabletIds.contains(10032L));
        Assert.assertTrue(sampleTabletIds.contains(10033L));
    }

    @Test
    public void testSelectSampleHashBucketTable() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:db1");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("table1");
        long tabletId = 10031L;
        for (Partition partition : tbl.getPartitions()) {
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    tablet.setTabletId(tabletId);
                    tabletId += 1;
                }
            }
        }

        // 1. TABLESAMPLE ROWS
        String sql1 = "SELECT * FROM db1.table1 TABLESAMPLE(10 ROWS)";
        OriginalPlanner planner1 = (OriginalPlanner) dorisAssert.query(sql1).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds1 = ((OlapScanNode) planner1.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds1.size());

        String sql2 = "SELECT * FROM db1.table1 TABLESAMPLE(1000 ROWS)";
        OriginalPlanner planner2 = (OriginalPlanner) dorisAssert.query(sql2).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds2 = ((OlapScanNode) planner2.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds2.size());

        String sql3 = "SELECT * FROM db1.table1 TABLESAMPLE(1001 ROWS)";
        OriginalPlanner planner3 = (OriginalPlanner) dorisAssert.query(sql3).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds3 = ((OlapScanNode) planner3.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(2, sampleTabletIds3.size());

        String sql4 = "SELECT * FROM db1.table1 TABLESAMPLE(9500 ROWS)";
        OriginalPlanner planner4 = (OriginalPlanner) dorisAssert.query(sql4).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds4 = ((OlapScanNode) planner4.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(10, sampleTabletIds4.size());

        String sql5 = "SELECT * FROM db1.table1 TABLESAMPLE(11000 ROWS)";
        OriginalPlanner planner5 = (OriginalPlanner) dorisAssert.query(sql5).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds5 = ((OlapScanNode) planner5.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(0, sampleTabletIds5.size()); // no sample, all tablet

        String sql6 = "SELECT * FROM db1.table1 TABLET(10033) TABLESAMPLE(900 ROWS)";
        OriginalPlanner planner6 = (OriginalPlanner) dorisAssert.query(sql6).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds6 = ((OlapScanNode) planner6.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertTrue(sampleTabletIds6.size() >= 1 && sampleTabletIds6.size() <= 2);
        Assert.assertTrue(sampleTabletIds6.contains(10033L));

        // 2. TABLESAMPLE PERCENT
        String sql7 = "SELECT * FROM db1.table1 TABLESAMPLE(10 PERCENT)";
        OriginalPlanner planner7 = (OriginalPlanner) dorisAssert.query(sql7).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds7 = ((OlapScanNode) planner7.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds7.size());

        String sql8 = "SELECT * FROM db1.table1 TABLESAMPLE(15 PERCENT)";
        OriginalPlanner planner8 = (OriginalPlanner) dorisAssert.query(sql8).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds8 = ((OlapScanNode) planner8.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(2, sampleTabletIds8.size());

        String sql9 = "SELECT * FROM db1.table1 TABLESAMPLE(100 PERCENT)";
        OriginalPlanner planner9 = (OriginalPlanner) dorisAssert.query(sql9).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds9 = ((OlapScanNode) planner9.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(0, sampleTabletIds9.size());

        String sql10 = "SELECT * FROM db1.table1 TABLESAMPLE(110 PERCENT)";
        OriginalPlanner planner10 = (OriginalPlanner) dorisAssert.query(sql10).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds10 = ((OlapScanNode) planner10.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(0, sampleTabletIds10.size());

        String sql11 = "SELECT * FROM db1.table1 TABLET(10033) TABLESAMPLE(5 PERCENT)";
        OriginalPlanner planner11 = (OriginalPlanner) dorisAssert.query(sql11).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds11 = ((OlapScanNode) planner11.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertTrue(sampleTabletIds11.size() >= 1 && sampleTabletIds11.size() <= 2);
        Assert.assertTrue(sampleTabletIds11.contains(10033L));

        // 3. TABLESAMPLE REPEATABLE
        String sql12 = "SELECT * FROM db1.table1 TABLESAMPLE(900 ROWS)";
        OriginalPlanner planner12 = (OriginalPlanner) dorisAssert.query(sql12).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds12 = ((OlapScanNode) planner12.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds12.size());

        String sql13 = "SELECT * FROM db1.table1 TABLESAMPLE(900 ROWS) REPEATABLE 2";
        OriginalPlanner planner13 = (OriginalPlanner) dorisAssert.query(sql13).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds13 = ((OlapScanNode) planner13.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds13.size());
        Assert.assertTrue(sampleTabletIds13.contains(10033L));

        String sql14 = "SELECT * FROM db1.table1 TABLESAMPLE(900 ROWS) REPEATABLE 10";
        OriginalPlanner planner14 = (OriginalPlanner) dorisAssert.query(sql14).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds14 = ((OlapScanNode) planner14.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds14.size());
        Assert.assertTrue(sampleTabletIds14.contains(10031L));

        String sql15 = "SELECT * FROM db1.table1 TABLESAMPLE(900 ROWS) REPEATABLE 0";
        OriginalPlanner planner15 = (OriginalPlanner) dorisAssert.query(sql15).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds15 = ((OlapScanNode) planner15.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds15.size());
        Assert.assertTrue(sampleTabletIds15.contains(10031L));

        // 4. select returns 900 rows of results
        String sql16 = "SELECT * FROM (SELECT * FROM db1.table1 TABLESAMPLE(900 ROWS) REPEATABLE 9999999 limit 900) t";
        OriginalPlanner planner16 = (OriginalPlanner) dorisAssert.query(sql16).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds16 = ((OlapScanNode) planner16.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds16.size());
    }

    @Test
    public void testSelectSampleRandomBucketTable() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:db1");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("table3");
        long tabletId = 10031L;
        for (Partition partition : tbl.getPartitions()) {
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    tablet.setTabletId(tabletId);
                    tabletId += 1;
                }
            }
        }

        // 1. TABLESAMPLE ROWS
        String sql1 = "SELECT * FROM db1.table3 TABLESAMPLE(10 ROWS)";
        OriginalPlanner planner1 = (OriginalPlanner) dorisAssert.query(sql1).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds1 = ((OlapScanNode) planner1.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds1.size());

        String sql2 = "SELECT * FROM db1.table3 TABLESAMPLE(1000 ROWS)";
        OriginalPlanner planner2 = (OriginalPlanner) dorisAssert.query(sql2).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds2 = ((OlapScanNode) planner2.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds2.size());

        String sql3 = "SELECT * FROM db1.table3 TABLESAMPLE(1001 ROWS)";
        OriginalPlanner planner3 = (OriginalPlanner) dorisAssert.query(sql3).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds3 = ((OlapScanNode) planner3.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(2, sampleTabletIds3.size());

        String sql4 = "SELECT * FROM db1.table3 TABLESAMPLE(9500 ROWS)";
        OriginalPlanner planner4 = (OriginalPlanner) dorisAssert.query(sql4).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds4 = ((OlapScanNode) planner4.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(10, sampleTabletIds4.size());

        String sql5 = "SELECT * FROM db1.table3 TABLESAMPLE(11000 ROWS)";
        OriginalPlanner planner5 = (OriginalPlanner) dorisAssert.query(sql5).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds5 = ((OlapScanNode) planner5.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(0, sampleTabletIds5.size()); // no sample, all tablet

        String sql6 = "SELECT * FROM db1.table3 TABLET(10033) TABLESAMPLE(900 ROWS)";
        OriginalPlanner planner6 = (OriginalPlanner) dorisAssert.query(sql6).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds6 = ((OlapScanNode) planner6.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertTrue(sampleTabletIds6.size() >= 1 && sampleTabletIds6.size() <= 2);
        Assert.assertTrue(sampleTabletIds6.contains(10033L));

        // 2. TABLESAMPLE PERCENT
        String sql7 = "SELECT * FROM db1.table3 TABLESAMPLE(10 PERCENT)";
        OriginalPlanner planner7 = (OriginalPlanner) dorisAssert.query(sql7).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds7 = ((OlapScanNode) planner7.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds7.size());

        String sql8 = "SELECT * FROM db1.table3 TABLESAMPLE(15 PERCENT)";
        OriginalPlanner planner8 = (OriginalPlanner) dorisAssert.query(sql8).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds8 = ((OlapScanNode) planner8.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(2, sampleTabletIds8.size());

        String sql9 = "SELECT * FROM db1.table3 TABLESAMPLE(100 PERCENT)";
        OriginalPlanner planner9 = (OriginalPlanner) dorisAssert.query(sql9).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds9 = ((OlapScanNode) planner9.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(0, sampleTabletIds9.size());

        String sql10 = "SELECT * FROM db1.table3 TABLESAMPLE(110 PERCENT)";
        OriginalPlanner planner10 = (OriginalPlanner) dorisAssert.query(sql10).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds10 = ((OlapScanNode) planner10.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(0, sampleTabletIds10.size());

        String sql11 = "SELECT * FROM db1.table3 TABLET(10033) TABLESAMPLE(5 PERCENT)";
        OriginalPlanner planner11 = (OriginalPlanner) dorisAssert.query(sql11).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds11 = ((OlapScanNode) planner11.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertTrue(sampleTabletIds11.size() >= 1 && sampleTabletIds11.size() <= 2);
        Assert.assertTrue(sampleTabletIds11.contains(10033L));

        // 3. TABLESAMPLE REPEATABLE
        String sql12 = "SELECT * FROM db1.table3 TABLESAMPLE(900 ROWS)";
        OriginalPlanner planner12 = (OriginalPlanner) dorisAssert.query(sql12).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds12 = ((OlapScanNode) planner12.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds12.size());

        String sql13 = "SELECT * FROM db1.table3 TABLESAMPLE(900 ROWS) REPEATABLE 2";
        OriginalPlanner planner13 = (OriginalPlanner) dorisAssert.query(sql13).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds13 = ((OlapScanNode) planner13.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds13.size());
        Assert.assertTrue(sampleTabletIds13.contains(10033L));

        String sql14 = "SELECT * FROM db1.table3 TABLESAMPLE(900 ROWS) REPEATABLE 10";
        OriginalPlanner planner14 = (OriginalPlanner) dorisAssert.query(sql14).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds14 = ((OlapScanNode) planner14.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds14.size());
        Assert.assertTrue(sampleTabletIds14.contains(10031L));

        String sql15 = "SELECT * FROM db1.table3 TABLESAMPLE(900 ROWS) REPEATABLE 0";
        OriginalPlanner planner15 = (OriginalPlanner) dorisAssert.query(sql15).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds15 = ((OlapScanNode) planner15.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds15.size());
        Assert.assertTrue(sampleTabletIds15.contains(10031L));

        // 4. select returns 900 rows of results
        String sql16 = "SELECT * FROM (SELECT * FROM db1.table3 TABLESAMPLE(900 ROWS) REPEATABLE 9999999 limit 900) t";
        OriginalPlanner planner16 = (OriginalPlanner) dorisAssert.query(sql16).internalExecuteOneAndGetPlan();
        Set<Long> sampleTabletIds16 = ((OlapScanNode) planner16.getScanNodes().get(0)).getSampleTabletIds();
        Assert.assertEquals(1, sampleTabletIds16.size());
    }


    @Test
    public void testSelectExcept() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "SELECT * EXCEPT (siteid) FROM db1.table1";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Assert.assertFalse(stmt.getColLabels().contains("siteid"));
        Assert.assertEquals(stmt.resultExprs.size(), 3);
    }
}
