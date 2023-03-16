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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class ColocatePlanTest {
    public static final String COLOCATE_ENABLE = "COLOCATE";
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir, 2);
        ctx = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        // create table test_colocate (k1 int ,k2 int, k3 int, k4 int)
        // distributed by hash(k1, k2) buckets 10
        // properties ("replication_num" = "2");
        String createColocateTblStmtStr = "create table db1.test_colocate(k1 int, k2 int, k3 int, k4 int) "
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                + "'colocate_with' = 'group1');";
        CreateTableStmt createColocateTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createColocateTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createColocateTableStmt);
        String createTblStmtStr = "create table db1.test(k1 int, k2 int, k3 int, k4 int)"
                + "partition by range(k1) (partition p1 values less than (\"1\"), partition p2 values less than (\"2\"))"
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2')";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        String createMultiPartitionTableStmt = "create table db1.test_multi_partition(k1 int, k2 int)"
                + "partition by range(k1) (partition p1 values less than(\"1\"), partition p2 values less than (\"2\"))"
                + "distributed by hash(k2) buckets 10 properties ('replication_num' = '2', 'colocate_with' = 'group2')";
        CreateTableStmt createMultiTableStmt = (CreateTableStmt) UtFrameUtils
                .parseAndAnalyzeStmt(createMultiPartitionTableStmt, ctx);
        Env.getCurrentEnv().createTable(createMultiTableStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    // without
    // 1. agg: group by column < distributed columns
    // 2. join: src data has been redistributed
    @Test
    public void sqlDistributedSmallerThanData1() throws Exception {
        String sql = "explain select * from (select k1 from db1.test_colocate group by k1) a , db1.test_colocate b "
                + "where a.k1=b.k1";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(DistributedPlanColocateRule.REDISTRIBUTED_SRC_DATA));
    }

    // without : join column < distributed columns;
    @Test
    public void sqlDistributedSmallerThanData2() throws Exception {
        String sql = "explain select * from (select k1 from db1.test_colocate group by k1, k2) a , db1.test_colocate b "
                + "where a.k1=b.k1";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertTrue(plan1.contains(DistributedPlanColocateRule.INCONSISTENT_DISTRIBUTION_OF_TABLE_AND_QUERY));
    }

    // with:
    // 1. agg columns = distributed columns
    // 2. hash columns = agg output columns = distributed columns
    @Test
    public void sqlAggAndJoinSameAsTableMeta() throws Exception {
        String sql = "explain select * from (select k1, k2 from db1.test_colocate group by k1, k2) a , db1.test_colocate b "
                + "where a.k1=b.k1 and a.k2=b.k2";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
    }

    // with:
    // 1. agg columns > distributed columns
    // 2. hash columns = agg output columns > distributed columns
    @Test
    public void sqlAggAndJoinMoreThanTableMeta() throws Exception {
        String sql = "explain select * from (select k1, k2, k3 from db1.test_colocate group by k1, k2, k3) a , "
                + "db1.test_colocate b where a.k1=b.k1 and a.k2=b.k2 and a.k3=b.k3";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
    }

    // with:
    // 1. agg columns > distributed columns
    // 2. hash columns = distributed columns
    @Test
    public void sqlAggMoreThanTableMeta() throws Exception {
        String sql = "explain select * from (select k1, k2, k3 from db1.test_colocate group by k1, k2, k3) a , "
                + "db1.test_colocate b where a.k1=b.k1 and a.k2=b.k2";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
    }

    // without:
    // 1. agg columns = distributed columns
    // 2. table is not in colocate group
    // 3. more then 1 instances
    // Fixed #6028
    @Test
    public void sqlAggWithNonColocateTable() throws Exception {
        String sql = "explain select k1, k2 from db1.test group by k1, k2";
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertFalse(plan1.contains(COLOCATE_ENABLE));
    }

    // check colocate add scan range
    // Fix #6726
    // 1. colocate agg node
    // 2. scan node with two tablet one instance
    @Test
    public void sqlAggWithColocateTable() throws Exception {
        String sql = "select k1, k2, count(*) from db1.test_multi_partition where k2 = 1 group by k1, k2";
        StmtExecutor executor = UtFrameUtils.getSqlStmtExecutor(ctx, sql);
        Planner planner = executor.planner();
        Coordinator coordinator = Deencapsulation.getField(executor, "coord");
        List<ScanNode> scanNodeList = planner.getScanNodes();
        Assert.assertEquals(scanNodeList.size(), 1);
        Assert.assertTrue(scanNodeList.get(0) instanceof OlapScanNode);
        OlapScanNode olapScanNode = (OlapScanNode) scanNodeList.get(0);
        Assert.assertEquals(olapScanNode.getSelectedPartitionIds().size(), 2);
        long selectedTablet = Deencapsulation.getField(olapScanNode, "selectedTabletsNum");
        Assert.assertEquals(selectedTablet, 2);

        List<QueryStatisticsItem.FragmentInstanceInfo> instanceInfo = coordinator.getFragmentInstanceInfos();
        Assert.assertEquals(instanceInfo.size(), 2);
    }

    @Test
    public void checkColocatePlanFragment() throws Exception {
        String sql = "select a.k1 from db1.test_colocate a, db1.test_colocate b where a.k1=b.k1 and a.k2=b.k2 group by a.k1;";
        StmtExecutor executor = UtFrameUtils.getSqlStmtExecutor(ctx, sql);
        Planner planner = executor.planner();
        Coordinator coordinator = Deencapsulation.getField(executor, "coord");
        boolean isColocateFragment0 = Deencapsulation.invoke(coordinator, "isColocateFragment",
                planner.getFragments().get(1), planner.getFragments().get(1).getPlanRoot());
        Assert.assertFalse(isColocateFragment0);
        boolean isColocateFragment1 = Deencapsulation.invoke(coordinator, "isColocateFragment",
                planner.getFragments().get(2), planner.getFragments().get(2).getPlanRoot());
        Assert.assertTrue(isColocateFragment1);
    }

    // Fix #8778
    @Test
    public void rollupAndMoreThanOneInstanceWithoutColocate() throws Exception {
        String createColocateTblStmtStr = "create table db1.test_colocate_one_backend(k1 int, k2 int, k3 int, k4 int) "
                + "distributed by hash(k1, k2, k3) buckets 10 properties('replication_num' = '1');";
        CreateTableStmt createColocateTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createColocateTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createColocateTableStmt);

        String sql = "select a.k1, a.k2, sum(a.k3) "
                + "from db1.test_colocate_one_backend a join[shuffle] db1.test_colocate_one_backend b on a.k1=b.k1 "
                + "group by rollup(a.k1, a.k2);";
        Deencapsulation.setField(ctx.getSessionVariable(), "parallelExecInstanceNum", 2);
        String plan1 = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertEquals(5, StringUtils.countMatches(plan1, "PLAN FRAGMENT"));

    }


}
