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

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class PlannerTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(runningDir));
    }

    @Test
    public void testSetOperation() throws Exception {
        // union

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
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
        String plan1 = planner1.getExplainString(fragments1, TExplainLevel.VERBOSE);
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
        String plan2 = planner2.getExplainString(fragments2, TExplainLevel.VERBOSE);
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
        String plan3 = planner3.getExplainString(fragments3, TExplainLevel.VERBOSE);
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
        String plan4 = planner4.getExplainString(fragments4, TExplainLevel.VERBOSE);
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
        String plan5 = planner5.getExplainString(fragments5, TExplainLevel.VERBOSE);
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
        String plan6 = planner6.getExplainString(fragments6, TExplainLevel.VERBOSE);
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
        String plan7 = planner7.getExplainString(fragments7, TExplainLevel.VERBOSE);
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
        String plan8 = planner8.getExplainString(fragments8, TExplainLevel.VERBOSE);
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
        String plan9 = planner9.getExplainString(fragments9, TExplainLevel.VERBOSE);
        Assert.assertEquals(2, StringUtils.countMatches(plan9, "UNION"));
        Assert.assertEquals(3, StringUtils.countMatches(plan9, "INTERSECT"));
        Assert.assertEquals(2, StringUtils.countMatches(plan9, "EXCEPT"));
    }

}
