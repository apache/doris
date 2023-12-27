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

import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ColocatePlanTest extends TestWithFeService {
    public static final String COLOCATE_ENABLE = "COLOCATE";
    private static final String GLOBAL_GROUP = "__global__group1";
    private static final String GLOBAL_GROUP2 = "__global__group2";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("db1");
        createTable("create table db1.test_colocate(k1 int, k2 int, k3 int, k4 int) "
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                + "'colocate_with' = 'group1');");
        createTable("create table db1.test(k1 int, k2 int, k3 int, k4 int)"
                + "partition by range(k1) (partition p1 values less than (\"1\"), partition p2 values less than (\"2\"))"
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2')");
        createTable("create table db1.test_multi_partition(k1 int, k2 int)"
                + "partition by range(k1) (partition p1 values less than(\"1\"), partition p2 values less than (\"2\"))"
                + "distributed by hash(k2) buckets 10 properties ('replication_num' = '2', 'colocate_with' = 'group2')");

        // global colocate tables
        createDatabase("db2");
        createTable("create table db1.test_global_colocate1(k1 varchar(10), k2 int, k3 int, k4 int) "
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                + "'colocate_with' = '" + GLOBAL_GROUP + "');");
        createTable("create table db2.test_global_colocate2(k1 varchar(20), k2 int, k3 int) "
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                + "'colocate_with' = '" + GLOBAL_GROUP + "');");
        createTable("create table db2.test_global_colocate3(k1 varchar(20), k2 int, k3 date) "
                + "partition by range(k3) (partition p1 values less than(\"2020-01-01\"), partition p2 values less than (\"2020-02-01\")) "
                + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                + "'colocate_with' = '" + GLOBAL_GROUP + "');");
    }

    @Override
    protected int backendNum() {
        return 2;
    }

    // without
    // 1. agg: group by column < distributed columns
    // 2. join: src data has been redistributed
    @Test
    public void sqlDistributedSmallerThanData1() throws Exception {
        String plan1 = getSQLPlanOrErrorMsg(
                "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from (select k1 from db1.test_colocate group by k1) a , db1.test_colocate b "
                        + "where a.k1=b.k1");
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(DistributedPlanColocateRule.REDISTRIBUTED_SRC_DATA));
    }

    // without : join column < distributed columns;
    @Test
    public void sqlDistributedSmallerThanData2() throws Exception {
        String sql = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from (select k1 from db1.test_colocate group by k1, k2) a , db1.test_colocate b "
                + "where a.k1=b.k1";
        String plan1 = getSQLPlanOrErrorMsg(sql);
        Assert.assertTrue(plan1.contains(DistributedPlanColocateRule.INCONSISTENT_DISTRIBUTION_OF_TABLE_AND_QUERY));
    }

    // with:
    // 1. agg columns = distributed columns
    // 2. hash columns = agg output columns = distributed columns
    @Test
    public void sqlAggAndJoinSameAsTableMeta() throws Exception {
        String sql =
                "explain select * from (select k1, k2 from db1.test_colocate group by k1, k2) a , db1.test_colocate b "
                        + "where a.k1=b.k1 and a.k2=b.k2";
        String plan1 = getSQLPlanOrErrorMsg(sql);
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
        String plan1 = getSQLPlanOrErrorMsg(sql);
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
        String plan1 = getSQLPlanOrErrorMsg(sql);
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
        String plan1 = getSQLPlanOrErrorMsg(sql);
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
        StmtExecutor executor = getSqlStmtExecutor(sql);
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
        connectContext.getSessionVariable().setEnableSharedScan(false);
        String sql
                = "select /*+ SET_VAR(enable_nereids_planner=false) */ a.k1 from db1.test_colocate a, db1.test_colocate b where a.k1=b.k1 and a.k2=b.k2 group by a.k1;";
        StmtExecutor executor = getSqlStmtExecutor(sql);
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
        createTable(createColocateTblStmtStr);
        String sql = "select a.k1, a.k2, sum(a.k3) "
                + "from db1.test_colocate_one_backend a join[shuffle] db1.test_colocate_one_backend b on a.k1=b.k1 "
                + "group by rollup(a.k1, a.k2);";
        Deencapsulation.setField(connectContext.getSessionVariable(), "parallelExecInstanceNum", 2);
        String plan1 = getSQLPlanOrErrorMsg(sql);
        Assert.assertEquals(2, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertEquals(5, StringUtils.countMatches(plan1, "PLAN FRAGMENT"));
    }

    @Test
    public void testGlobalColocateGroup() throws Exception {
        Database db1 = Env.getCurrentEnv().getInternalCatalog().getDbNullable("db1");
        Database db2 = Env.getCurrentEnv().getInternalCatalog().getDbNullable("db2");
        OlapTable tbl1 = (OlapTable) db1.getTableNullable("test_global_colocate1");
        OlapTable tbl2 = (OlapTable) db2.getTableNullable("test_global_colocate2");
        OlapTable tbl3 = (OlapTable) db2.getTableNullable("test_global_colocate3");

        String sql = "explain select * from (select k1, k2 from "
                + "db1.test_global_colocate1 group by k1, k2) a , db2.test_global_colocate2 b "
                + "where a.k1=b.k1 and a.k2=b.k2";
        String plan1 = getSQLPlanOrErrorMsg(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(
                GroupId.getFullGroupName(1000, GLOBAL_GROUP));
        Assert.assertNotNull(groupSchema);
        GroupId groupId = groupSchema.getGroupId();
        List<Long> tableIds = colocateTableIndex.getAllTableIds(groupId);
        Assert.assertEquals(3, tableIds.size());
        Assert.assertTrue(tableIds.contains(tbl1.getId()));
        Assert.assertTrue(tableIds.contains(tbl2.getId()));
        Assert.assertTrue(tableIds.contains(tbl3.getId()));
        Assert.assertEquals(3, groupId.getTblId2DbIdSize());
        Assert.assertEquals(db1.getId(), groupId.getDbIdByTblId(tbl1.getId()));
        Assert.assertEquals(db2.getId(), groupId.getDbIdByTblId(tbl2.getId()));
        Assert.assertEquals(db2.getId(), groupId.getDbIdByTblId(tbl3.getId()));

        sql = "explain select * from (select k1, k2 from "
                + "db1.test_global_colocate1 group by k1, k2) a , db2.test_global_colocate3 b "
                + "where a.k1=b.k1 and a.k2=b.k2";
        plan1 = getSQLPlanOrErrorMsg(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan1, "AGGREGATE"));
        Assert.assertTrue(plan1.contains(COLOCATE_ENABLE));

        String addPartitionStmt
                = "alter table db2.test_global_colocate3 add partition p3 values less than (\"2020-03-01\");";
        alterTableSync(addPartitionStmt);

        try {
            createTable("create table db1.test_global_colocate4(k1 int, k2 int, k3 int, k4 int) "
                    + "distributed by hash(k1, k2) buckets 10 properties('replication_num' = '2',"
                    + "'colocate_with' = '" + GLOBAL_GROUP + "');");
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(
                    e.getMessage().contains("Colocate tables distribution columns must have the same data type"));
            List<Long> tmpTableIds = colocateTableIndex.getAllTableIds(groupId);
            Assert.assertEquals(3, tmpTableIds.size());
            Assert.assertTrue(tmpTableIds.contains(tbl1.getId()));
            Assert.assertTrue(tmpTableIds.contains(tbl2.getId()));
            Assert.assertTrue(tmpTableIds.contains(tbl3.getId()));
            Assert.assertEquals(3, groupId.getTblId2DbIdSize());
            Assert.assertEquals(db1.getId(), groupId.getDbIdByTblId(tbl1.getId()));
            Assert.assertEquals(db2.getId(), groupId.getDbIdByTblId(tbl2.getId()));
            Assert.assertEquals(db2.getId(), groupId.getDbIdByTblId(tbl3.getId()));
        }

        // modify table's colocate group
        String modifyStmt = "alter table db2.test_global_colocate3 set ('colocate_with' = '');";
        alterTableSync(modifyStmt);
        tableIds = colocateTableIndex.getAllTableIds(groupId);
        Assert.assertEquals(2, tableIds.size());
        Assert.assertTrue(tableIds.contains(tbl1.getId()));
        Assert.assertTrue(tableIds.contains(tbl2.getId()));
        Assert.assertEquals(2, groupId.getTblId2DbIdSize());
        Assert.assertEquals(db1.getId(), groupId.getDbIdByTblId(tbl1.getId()));
        Assert.assertEquals(db2.getId(), groupId.getDbIdByTblId(tbl2.getId()));

        // change table's colocate group
        modifyStmt = "alter table db2.test_global_colocate2 set ('colocate_with' = '" + GLOBAL_GROUP2 + "');";
        alterTableSync(modifyStmt);
        tableIds = colocateTableIndex.getAllTableIds(groupId);
        Assert.assertEquals(1, tableIds.size());
        Assert.assertTrue(tableIds.contains(tbl1.getId()));
        Assert.assertEquals(1, groupId.getTblId2DbIdSize());
        Assert.assertEquals(db1.getId(), groupId.getDbIdByTblId(tbl1.getId()));

        GroupId groupId2 = colocateTableIndex.getGroupSchema(
                GroupId.getFullGroupName(1000, GLOBAL_GROUP2)).getGroupId();
        tableIds = colocateTableIndex.getAllTableIds(groupId2);
        Assert.assertEquals(1, tableIds.size());
        Assert.assertTrue(tableIds.contains(tbl2.getId()));
        Assert.assertEquals(1, groupId2.getTblId2DbIdSize());
        Assert.assertEquals(db2.getId(), groupId2.getDbIdByTblId(tbl2.getId()));

        // checkpoint
        // Get currentCatalog first
        Env currentEnv = Env.getCurrentEnv();
        // Save real ckptThreadId
        long ckptThreadId = currentEnv.getCheckpointer().getId();
        try {
            // set checkpointThreadId to current thread id, so that when do checkpoint manually here,
            // the Catalog.isCheckpointThread() will return true.
            Deencapsulation.setField(Env.class, "checkpointThreadId", Thread.currentThread().getId());
            currentEnv.getCheckpointer().doCheckpoint();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            // Restore the ckptThreadId
            Deencapsulation.setField(Env.class, "checkpointThreadId", ckptThreadId);
        }
    }
}
