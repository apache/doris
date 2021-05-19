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

package org.apache.doris.planner;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class PlannerTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(runningDir));
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        ctx = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);

        createTblStmtStr = "create table db1.tbl2(k1 int, k2 int sum) "
                + "AGGREGATE KEY(k1) partition by range(k1) () distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @Test
    public void testSetOperation() throws Exception {
        // union
        String sql1 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   union all\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        StmtExecutor stmtExecutor1 = new StmtExecutor(ctx, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        List<PlanFragment> fragments1 = planner1.getFragments();
        String plan1 = planner1.getExplainString(fragments1, new ExplainOptions(false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "UNION"));
        String sql2 = "explain select * from db1.tbl1 where k1='a' and k4=1\n"
                + "union distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl1 where k1='b' and k4=2)\n"
                + "union distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   select * from db1.tbl1 where k1='b' and k4=4)\n"
                + "union all\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        StmtExecutor stmtExecutor2 = new StmtExecutor(ctx, sql2);
        stmtExecutor2.execute();
        Planner planner2 = stmtExecutor2.planner();
        List<PlanFragment> fragments2 = planner2.getFragments();
        String plan2 = planner2.getExplainString(fragments2, new ExplainOptions(false, false));
        Assert.assertEquals(4, StringUtils.countMatches(plan2, "UNION"));

        // intersect
        String sql3 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   intersect\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        StmtExecutor stmtExecutor3 = new StmtExecutor(ctx, sql3);
        stmtExecutor3.execute();
        Planner planner3 = stmtExecutor3.planner();
        List<PlanFragment> fragments3 = planner3.getFragments();
        String plan3 = planner3.getExplainString(fragments3, new ExplainOptions(false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan3, "INTERSECT"));
        String sql4 = "explain select * from db1.tbl1 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   select * from db1.tbl1 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "intersect\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl1 where k1='b' and k4=4)\n"
                + "intersect\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";

        StmtExecutor stmtExecutor4 = new StmtExecutor(ctx, sql4);
        stmtExecutor4.execute();
        Planner planner4 = stmtExecutor4.planner();
        List<PlanFragment> fragments4 = planner4.getFragments();
        String plan4 = planner4.getExplainString(fragments4, new ExplainOptions(false, false));
        Assert.assertEquals(3, StringUtils.countMatches(plan4, "INTERSECT"));

        // except
        String sql5 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   except\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        StmtExecutor stmtExecutor5 = new StmtExecutor(ctx, sql5);
        stmtExecutor5.execute();
        Planner planner5 = stmtExecutor5.planner();
        List<PlanFragment> fragments5 = planner5.getFragments();
        String plan5 = planner5.getExplainString(fragments5, new ExplainOptions(false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan5, "EXCEPT"));

        String sql6 = "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "except distinct\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        StmtExecutor stmtExecutor6 = new StmtExecutor(ctx, sql6);
        stmtExecutor6.execute();
        Planner planner6 = stmtExecutor6.planner();
        List<PlanFragment> fragments6 = planner6.getFragments();
        String plan6 = planner6.getExplainString(fragments6, new ExplainOptions(false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan6, "EXCEPT"));

        String sql7 = "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except distinct\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "except\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        StmtExecutor stmtExecutor7 = new StmtExecutor(ctx, sql7);
        stmtExecutor7.execute();
        Planner planner7 = stmtExecutor7.planner();
        List<PlanFragment> fragments7 = planner7.getFragments();
        String plan7 = planner7.getExplainString(fragments7, new ExplainOptions(false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan7, "EXCEPT"));

        // mixed
        String sql8 = "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "union\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "intersect\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        StmtExecutor stmtExecutor8 = new StmtExecutor(ctx, sql8);
        stmtExecutor8.execute();
        Planner planner8 = stmtExecutor8.planner();
        List<PlanFragment> fragments8 = planner8.getFragments();
        String plan8 = planner8.getExplainString(fragments8, new ExplainOptions(false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan8, "UNION"));
        Assert.assertEquals(1, StringUtils.countMatches(plan8, "INTERSECT"));
        Assert.assertEquals(1, StringUtils.countMatches(plan8, "EXCEPT"));

        String sql9 = "explain select * from db1.tbl1 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl1 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   except\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl1 where k1='b' and k4=4)\n"
                + "except\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";

        StmtExecutor stmtExecutor9 = new StmtExecutor(ctx, sql9);
        stmtExecutor9.execute();
        Planner planner9 = stmtExecutor9.planner();
        List<PlanFragment> fragments9 = planner9.getFragments();
        String plan9 = planner9.getExplainString(fragments9, new ExplainOptions(false, false));
        Assert.assertEquals(2, StringUtils.countMatches(plan9, "UNION"));
        Assert.assertEquals(3, StringUtils.countMatches(plan9, "INTERSECT"));
        Assert.assertEquals(2, StringUtils.countMatches(plan9, "EXCEPT"));

        String sql10 = "select 499 union select 670 except select 499";
        StmtExecutor stmtExecutor10 = new StmtExecutor(ctx, sql10);
        stmtExecutor10.execute();
        Planner planner10 = stmtExecutor10.planner();
        List<PlanFragment> fragments10 = planner10.getFragments();
        Assert.assertTrue(fragments10.get(0).getPlanRoot().getFragment()
                .getPlanRoot().getChild(0) instanceof AggregationNode);
        Assert.assertTrue(fragments10.get(0).getPlanRoot()
                .getFragment().getPlanRoot().getChild(1) instanceof UnionNode);
    }

    @Test
    public void testPushDown() throws Exception{
        String sql1 =
                "SELECT\n" +
                "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                "    k4\n" +
                "FROM\n" +
                "(\n" +
                "    SELECT\n" +
                "        k1,\n" +
                "        k2,\n" +
                "        k3,\n" +
                "        SUM(k4) AS k4\n" +
                "    FROM  db1.tbl1\n" +
                "    WHERE k1 = 0\n" +
                "        AND k4 = 1\n" +
                "        AND k3 = 'foo'\n" +
                "    GROUP BY \n" +
                "    GROUPING SETS (\n" +
                "        (k1),\n" +
                "        (k1, k2),\n" +
                "        (k1, k3),\n" +
                "        (k1, k2, k3)\n" +
                "    )\n" +
                ") t\n" +
                "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        StmtExecutor stmtExecutor1 = new StmtExecutor(ctx, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        List<PlanFragment> fragments1 = planner1.getFragments();
        Assert.assertEquals("if",
                fragments1.get(0).getPlanRoot().conjuncts.get(0).getChild(0).getFn().functionName());
        Assert.assertEquals(3, fragments1.get(0).getPlanRoot().getChild(0).getChild(0).conjuncts.size());

        String sql2 =
                "SELECT\n" +
                        "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                        "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                        "    k4\n" +
                        "FROM\n" +
                        "(\n" +
                        "    SELECT\n" +
                        "        k1,\n" +
                        "        k2,\n" +
                        "        k3,\n" +
                        "        SUM(k4) AS k4\n" +
                        "    FROM  db1.tbl1\n" +
                        "    WHERE k1 = 0\n" +
                        "        AND k4 = 1\n" +
                        "        AND k3 = 'foo'\n" +
                        "    GROUP BY k1, k2, k3\n" +
                        ") t\n" +
                        "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        StmtExecutor stmtExecutor2 = new StmtExecutor(ctx, sql2);
        stmtExecutor2.execute();
        Planner planner2 = stmtExecutor2.planner();
        List<PlanFragment> fragments2 = planner2.getFragments();
        Assert.assertEquals(4, fragments2.get(0).getPlanRoot().getChild(0).conjuncts.size());

    }

    @Test
    public void testWithStmtSlotIsAllowNull() throws Exception {
        // union
        String sql1 = "with a as (select NULL as user_id ), " +
                "b as ( select '543' as user_id) " +
                "select user_id from a union all select user_id from b";

        StmtExecutor stmtExecutor1 = new StmtExecutor(ctx, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        List<PlanFragment> fragments1 = planner1.getFragments();
        String plan1 = planner1.getExplainString(fragments1, new ExplainOptions(true, false));
        Assert.assertEquals(3, StringUtils.countMatches(plan1, "nullIndicatorBit=0"));
    }

    @Test
    public void testAccessingVisibleColumnWithoutPartition() throws Exception {
        String sql = "select count(k1) from db1.tbl2";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
    }

}
