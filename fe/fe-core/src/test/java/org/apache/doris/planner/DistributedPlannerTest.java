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
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class DistributedPlannerTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnableNereidsPlanner(false);
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        // create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 int, k2 varchar(32), v bigint sum) "
                + "AGGREGATE KEY(k1,k2) distributed by hash(k1) buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
        // create table tbl2
        createTblStmtStr = "create table db1.tbl2(k3 int, k4 varchar(32)) "
                + "DUPLICATE KEY(k3) distributed by hash(k3) buckets 1 properties('replication_num' = '1');";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(runningDir));
    }

    @Test
    public void testAssertFragmentWithDistributedInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment,
                                                       @Injectable PlanNodeId planNodeId,
                                                       @Injectable PlanFragmentId planFragmentId,
                                                       @Injectable PlanNode inputPlanRoot,
                                                       @Injectable TupleId tupleId,
                                                       @Mocked PlannerContext plannerContext) {
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        List<TupleId> tupleIdList = Lists.newArrayList(tupleId);
        Set<TupleId> tupleIdSet = Sets.newHashSet(tupleId);
        Deencapsulation.setField(inputPlanRoot, "tupleIds", tupleIdList);
        Deencapsulation.setField(inputPlanRoot, "tblRefIds", tupleIdList);
        Deencapsulation.setField(inputPlanRoot, "nullableTupleIds", Sets.newHashSet(tupleId));
        Deencapsulation.setField(inputPlanRoot, "conjuncts", Lists.newArrayList());
        new Expectations() {
            {
                inputPlanRoot.getOutputTupleDesc();
                result = null;
                inputFragment.isPartitioned();
                result = true;
                plannerContext.getNextNodeId();
                result = planNodeId;
                plannerContext.getNextFragmentId();
                result = planFragmentId;
                inputFragment.getPlanRoot();
                result = inputPlanRoot;
                inputPlanRoot.getTupleIds();
                result = tupleIdList;
                inputPlanRoot.getTblRefIds();
                result = tupleIdList;
                inputPlanRoot.getNullableTupleIds();
                result = tupleIdSet;
                assertNumRowsNode.getChildren();
                result = inputPlanRoot;
            }
        };

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                assertNumRowsNode, inputFragment);
        Assert.assertFalse(assertFragment.isPartitioned());
        Assert.assertSame(assertNumRowsNode, assertFragment.getPlanRoot());
    }

    @Test
    public void testAssertFragmentWithUnpartitionInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment,
                                                       @Mocked PlannerContext plannerContext) {
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                assertNumRowsNode, inputFragment);
        Assert.assertSame(assertFragment, inputFragment);
        Assert.assertTrue(assertFragment.getPlanRoot() instanceof AssertNumRowsNode);
    }

    @Test
    public void testExplicitlyBroadcastJoin() throws Exception {
        String sql = "explain select * from db1.tbl1 join [BROADCAST] db1.tbl2 on tbl1.k1 = tbl2.k3";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(BROADCAST)"));

        sql = "explain select * from db1.tbl1 join [SHUFFLE] db1.tbl2 on tbl1.k1 = tbl2.k3";
        stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        planner = stmtExecutor.planner();
        plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(PARTITIONED)"));
    }

    @Test
    public void testBroadcastJoinCostThreshold() throws Exception {
        String sql = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 join db1.tbl2 on tbl1.k1 = tbl2.k3";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(BROADCAST)"));

        double originThreshold = ctx.getSessionVariable().autoBroadcastJoinThreshold;
        try {
            ctx.getSessionVariable().autoBroadcastJoinThreshold = -1.0;
            stmtExecutor = new StmtExecutor(ctx, sql);
            stmtExecutor.execute();
            planner = stmtExecutor.planner();
            plan = planner.getExplainString(new ExplainOptions(false, false, false));
            Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN(PARTITIONED)"));
        } finally {
            ctx.getSessionVariable().autoBroadcastJoinThreshold = originThreshold;
        }
    }
}
