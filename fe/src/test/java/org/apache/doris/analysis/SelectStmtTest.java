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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

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
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createBaseAllStmtStr = "create table db1.baseall(k1 int) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(createTblStmtStr).withTable(createBaseAllStmtStr);
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
}
