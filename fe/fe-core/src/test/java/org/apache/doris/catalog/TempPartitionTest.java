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

package org.apache.doris.catalog;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.ShowPartitionsStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.ShowTabletStmt;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TempPartitionTest {

    private static String tempPartitionFile = "./TempPartitionTest";
    private static String tblFile = "./tblFile";
    private static String runningDir = "fe/mocked/TempPartitionTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext ctx;

    @BeforeClass
    public static void setup() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 100;
        UtFrameUtils.createDorisCluster(runningDir);
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
        File file2 = new File(tempPartitionFile);
        file2.delete();
        File file3 = new File(tblFile);
        file3.delete();
    }

    @Before
    public void before() {

    }

    private List<List<String>> checkShowPartitionsResultNum(String tbl, boolean isTemp, int expected) throws Exception {
        String showStr = "show " + (isTemp ? "temporary" : "") + " partitions from " + tbl;
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) UtFrameUtils.parseAndAnalyzeStmt(showStr, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        Assert.assertEquals(expected, rows.size());
        return rows;
    }

    private void alterTable(String sql, boolean expectedException) throws Exception {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
            if (expectedException) {
                Assert.fail("expected exception not thrown");
            }
        } catch (Exception e) {
            if (expectedException) {
                System.out.println("got exception: " + e.getMessage());
            } else {
                throw e;
            }
        }
    }

    private List<List<String>> checkTablet(String tbl, String partitions, boolean isTemp, int expected)
            throws Exception {
        String showStr = "show tablets from " + tbl + (isTemp ? " temporary" : "") + " partition (" + partitions + ");";
        ShowTabletStmt showStmt = (ShowTabletStmt) UtFrameUtils.parseAndAnalyzeStmt(showStr, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        if (expected != -1) {
            Assert.assertEquals(expected, rows.size());
        }
        return rows;
    }

    private long getPartitionIdByTabletId(long tabletId) {
        TabletInvertedIndex index = Env.getCurrentInvertedIndex();
        TabletMeta tabletMeta = index.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            return -1;
        }
        return tabletMeta.getPartitionId();
    }

    private void getPartitionNameToTabletIdMap(String tbl, boolean isTemp,
            Map<String, Long> partNameToTabletId) throws Exception {
        partNameToTabletId.clear();
        String showStr = "show " + (isTemp ? "temporary" : "") + " partitions from " + tbl;
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) UtFrameUtils.parseAndAnalyzeStmt(showStr, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, (ShowStmt) showStmt);
        ShowResultSet showResultSet = executor.execute();
        List<List<String>> rows = showResultSet.getResultRows();
        Map<Long, String> partIdToName = Maps.newHashMap();
        for (List<String> row : rows) {
            partIdToName.put(Long.valueOf(row.get(0)), row.get(1));
        }

        rows = checkTablet(tbl, Joiner.on(",").join(partIdToName.values()), isTemp, -1);
        for (List<String> row : rows) {
            long tabletId = Long.valueOf(row.get(0));
            long partitionId = getPartitionIdByTabletId(tabletId);
            String partName = partIdToName.get(partitionId);
            partNameToTabletId.put(partName, tabletId);
        }
    }

    private void checkTabletExists(Collection<Long> tabletIds, boolean checkExist) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (Long tabletId : tabletIds) {
            if (checkExist) {
                Assert.assertNotNull(invertedIndex.getTabletMeta(tabletId));
            } else {
                Assert.assertNull(invertedIndex.getTabletMeta(tabletId));
            }
        }
    }

    private void checkPartitionExist(OlapTable tbl, String partName, boolean isTemp, boolean checkExist) {
        if (checkExist) {
            Assert.assertNotNull(tbl.getPartition(partName, isTemp));
        } else {
            Assert.assertNull(tbl.getPartition(partName, isTemp));
        }
    }

    @Test
    public void testForSinglePartitionTable() throws Exception {
        // create database db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());
        // create table tbl1
        String createTblStmtStr1 = "create table db1.tbl1(k1 int) distributed by hash(k1)"
                + " buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr1, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        // add temp partition
        String stmtStr = "alter table db1.tbl1 add temporary partition p1 values less than ('10');";
        alterTable(stmtStr, true);

        // drop temp partition
        stmtStr = "alter table db1.tbl1 drop temporary partition tbl1;";
        alterTable(stmtStr, true);

        // show temp partition
        checkShowPartitionsResultNum("db1.tbl1", true, 0);
    }

    @Test
    public void testForMultiPartitionTable() throws Exception {
        // create database db2
        String createDbStmtStr = "create database db2;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // create table tbl2
        String createTblStmtStr1 = "create table db2.tbl2 (k1 int, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + "partition p1 values less than('10'),\n"
                + "partition p2 values less than('20'),\n"
                + "partition p3 values less than('30')\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr1, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        Database db2 =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("db2");
        OlapTable tbl2 = (OlapTable) db2.getTableOrAnalysisException("tbl2");

        testSerializeOlapTable(tbl2);

        Map<String, Long> originPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds);
        Assert.assertEquals(3, originPartitionTabletIds.keySet().size());

        // show temp partition
        checkShowPartitionsResultNum("db2.tbl2", true, 0);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        // add temp partition with duplicate name
        String stmtStr = "alter table db2.tbl2 add temporary partition p1 values less than('10');";
        alterTable(stmtStr, true);

        // add temp partition
        stmtStr = "alter table db2.tbl2 add temporary partition tp1 values less than('10');";
        alterTable(stmtStr, false);

        stmtStr = "alter table db2.tbl2 add temporary partition tp2 values less than('10');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 add temporary partition tp1 values less than('20');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 add temporary partition tp2 values less than('20');";
        alterTable(stmtStr, false);

        stmtStr = "alter table db2.tbl2 add temporary partition tp3 values [('18'), ('30'));";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 add temporary partition tp3 values [('20'), ('30'));";
        alterTable(stmtStr, false);

        Map<String, Long> tempPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds);
        Assert.assertEquals(3, tempPartitionTabletIds.keySet().size());

        System.out.println("partition tablets: " + originPartitionTabletIds);
        System.out.println("temp partition tablets: " + tempPartitionTabletIds);

        testSerializeOlapTable(tbl2);

        // drop non exist temp partition
        stmtStr = "alter table db2.tbl2 drop temporary partition tp4;";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 drop temporary partition if exists tp4;";
        alterTable(stmtStr, false);

        stmtStr = "alter table db2.tbl2 drop temporary partition tp3;";
        alterTable(stmtStr, false);

        Map<String, Long> originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        Map<String, Long> tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds2);
        Assert.assertEquals(2, tempPartitionTabletIds2.keySet().size());
        Assert.assertTrue(!tempPartitionTabletIds2.containsKey("tp3"));

        checkShowPartitionsResultNum("db2.tbl2", true, 2);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        stmtStr = "alter table db2.tbl2 add temporary partition tp3 values less than('30');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", true, 3);

        stmtStr = "alter table db2.tbl2 drop partition p1;";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", true, 3);
        checkShowPartitionsResultNum("db2.tbl2", false, 2);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(2, originPartitionTabletIds2.size());
        Assert.assertTrue(!originPartitionTabletIds2.containsKey("p1"));

        String recoverStr = "recover partition p1 from db2.tbl2;";
        RecoverPartitionStmt recoverStmt = (RecoverPartitionStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverStr, ctx);
        Env.getCurrentEnv().recoverPartition(recoverStmt);
        checkShowPartitionsResultNum("db2.tbl2", true, 3);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds2);
        Assert.assertEquals(3, tempPartitionTabletIds2.keySet().size());

        // Here, we should have 3 partitions p1,p2,p3, and 3 temp partitions tp1,tp2,tp3
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) with temporary partition(tp2, tp3);";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) with temporary partition(tp1, tp2)"
                + " properties('invalid' = 'invalid');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) with temporary partition(tp2, tp3)"
                + " properties('strict_range' = 'false');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db2.tbl2 replace partition(p1, p2) with temporary partition(tp1, tp2) force"
                + " properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", true, 1);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);

        checkTabletExists(tempPartitionTabletIds2.values(), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p3")), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p1"),
                originPartitionTabletIds2.get("p2")), false);

        String truncateStr = "truncate table db2.tbl2 partition (p3);";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db2.tbl2", true, 1);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "p3", false, true);
        checkPartitionExist(tbl2, "tp3", true, true);

        stmtStr = "alter table db2.tbl2 drop partition p3;";
        alterTable(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add partition p31 values less than('25');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add partition p32 values less than('35');";
        alterTable(stmtStr, false);

        // for now, we have 4 partitions: tp1, tp2, p31, p32, 1 temp partition: tp3
        checkShowPartitionsResultNum("db2.tbl2", false, 4);
        checkShowPartitionsResultNum("db2.tbl2", true, 1);

        stmtStr = "alter table db2.tbl2 replace partition(p31) with temporary partition(tp3);";
        alterTable(stmtStr, true);
        stmtStr = "alter table db2.tbl2 replace partition(p31, p32) with temporary partition(tp3);";
        alterTable(stmtStr, true);
        stmtStr = "alter table db2.tbl2 replace partition(p31, p32) with temporary partition(tp3)"
                + " properties('strict_range' = 'false');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "tp3", false, true);

        stmtStr = "alter table db2.tbl2 add temporary partition p1 values less than('10');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add temporary partition p2 values less than('20');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add temporary partition p3 values less than('30');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db2.tbl2 replace partition(tp1, tp2) with temporary partition(p1, p2);";
        alterTable(stmtStr, false);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "tp3", false, true);
        checkPartitionExist(tbl2, "p1", true, false);
        checkPartitionExist(tbl2, "p2", true, false);
        checkPartitionExist(tbl2, "p3", true, true);

        stmtStr = "alter table db2.tbl2 replace partition(tp3) with temporary partition(p3)"
                + " properties('use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        checkPartitionExist(tbl2, "tp1", false, true);
        checkPartitionExist(tbl2, "tp2", false, true);
        checkPartitionExist(tbl2, "p3", false, true);
        checkPartitionExist(tbl2, "p1", true, false);
        checkPartitionExist(tbl2, "p2", true, false);
        checkPartitionExist(tbl2, "p3", true, false);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);

        stmtStr = "alter table db2.tbl2 add temporary partition tp1 values less than('10');"; // name conflict
        alterTable(stmtStr, true);
        stmtStr = "alter table db2.tbl2 rename partition p3 tp3;";
        alterTable(stmtStr, false);
        stmtStr = "alter table db2.tbl2 add temporary partition p1 values less than('10');";
        alterTable(stmtStr, false);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", false, originPartitionTabletIds2);
        Assert.assertEquals(3, originPartitionTabletIds2.size());

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db2.tbl2", true, tempPartitionTabletIds2);
        Assert.assertEquals(1, tempPartitionTabletIds2.keySet().size());

        // for now , we have 3 partitions: tp1, tp2, tp3, 1 temp partition: p1
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db2.tbl2 add rollup r1(k1);";
        alterTable(stmtStr, true);

        truncateStr = "truncate table db2.tbl2";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db2.tbl2", false, 3);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);

        stmtStr = "alter table db2.tbl2 add rollup r1(k2, k1);";
        alterTable(stmtStr, false);

        stmtStr = "alter table db2.tbl2 add temporary partition p2 values less than('20');";
        alterTable(stmtStr, true);

        // wait rollup finish
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getDbId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(5000);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        // waiting table state to normal
        Thread.sleep(500);

        stmtStr = "alter table db2.tbl2 add temporary partition p2 values less than('20');";
        alterTable(stmtStr, false);

        TempPartitions tempPartitions = Deencapsulation.getField(tbl2, "tempPartitions");
        testSerializeTempPartitions(tempPartitions);

        stmtStr = "alter table db2.tbl2 replace partition (tp1, tp2) with temporary partition (p2)"
                + " properties('strict_range' = 'false');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db2.tbl2", false, 2);
        checkShowPartitionsResultNum("db2.tbl2", true, 0);
        checkPartitionExist(tbl2, "p2", false, true);
        checkPartitionExist(tbl2, "tp3", false, true);
        checkPartitionExist(tbl2, "tp1", false, false);
        checkPartitionExist(tbl2, "tp2", false, false);
        checkPartitionExist(tbl2, "p2", true, false);

        checkTablet("db2.tbl2", "p2", false, 2);
        checkTablet("db2.tbl2", "tp3", false, 2);

        // for now, we have 2 partitions: p2, tp3, [min, 20), [20, 30). 0 temp partition.
        stmtStr = "alter table db2.tbl2 add temporary partition tp4 values less than('20') "
                + "('in_memory' = 'false') distributed by hash(k1) buckets 3";
        alterTable(stmtStr, true);
        stmtStr = "alter table db2.tbl2 add temporary partition tp4 values less than('20') "
                + "('in_memory' = 'false', 'replication_num' = '2') distributed by hash(k2) buckets 3";
        alterTable(stmtStr, true);
        stmtStr = "alter table db2.tbl2 add temporary partition tp4 values less than('20') "
                + "('in_memory' = 'false', 'replication_num' = '1') distributed by hash(k2) buckets 3";
        alterTable(stmtStr, false);

        Partition p2 = tbl2.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl2.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(1, p2.getDistributionInfo().getBucketNum());

        stmtStr = "alter table db2.tbl2 replace partition (p2) with temporary partition (tp4)";
        alterTable(stmtStr, false);

        // for now, we have 2 partitions: p2, tp3, [min, 20), [20, 30). 0 temp partition.
        // and p2 bucket is 3, 'in_memory' is false.
        p2 = tbl2.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl2.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(3, p2.getDistributionInfo().getBucketNum());
    }

    @Test
    public void testForStrictRangeCheck() throws Exception {
        // create database db3
        String createDbStmtStr = "create database db3;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // create table tbl3
        String createTblStmtStr1 = "create table db3.tbl3 (k1 int, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + "partition p1 values less than('10'),\n"
                + "partition p2 values less than('20'),\n"
                + "partition p3 values less than('30')\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr1, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        Env.getCurrentInternalCatalog().getDbOrAnalysisException("db3");

        // base range is [min, 10), [10, 20), [20, 30)

        // 1. add temp ranges: [10, 15), [15, 25), [25, 30), and replace the [10, 20), [20, 30)
        String stmtStr = "alter table db3.tbl3 add temporary partition tp1 values [('10'), ('15'))";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 add temporary partition tp2 values [('15'), ('25'))";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 add temporary partition tp3 values [('25'), ('30'))";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (p2, p3) with temporary partition(tp1, tp2, tp3)";
        alterTable(stmtStr, false);

        // now base range is [min, 10), [10, 15), [15, 25), [25, 30) -> p1,tp1,tp2,tp3
        stmtStr = "truncate table db3.tbl3";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        // 2. add temp ranges: [10, 31), and replace the [10, 15), [15, 25), [25, 30)
        stmtStr = "alter table db3.tbl3 add temporary partition tp4 values [('10'), ('31'))";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (tp1, tp2, tp3) with temporary partition(tp4)";
        alterTable(stmtStr, true);
        // drop the tp4, and add temp partition tp4 [10,30) to to replace tp1, tp2, tp3
        stmtStr = "alter table db3.tbl3 drop temporary partition tp4";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 add temporary partition tp4 values [('10'), ('30'))";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (tp1, tp2, tp3) with temporary partition(tp4)";
        alterTable(stmtStr, false);

        // now base range is [min, 10), [10, 30) -> p1,tp4
        stmtStr = "truncate table db3.tbl3";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        // 3. add temp partition tp5 [50, 60) and replace partition tp4
        stmtStr = "alter table db3.tbl3 add temporary partition tp5 values [('50'), ('60'))";
        alterTable(stmtStr, false);
        stmtStr = "alter table db3.tbl3 replace partition (tp4) with temporary partition(tp5)";
        alterTable(stmtStr, true);
        stmtStr = "alter table db3.tbl3 replace partition (tp4) with temporary partition(tp5)"
                + " properties('strict_range' = 'true', 'use_temp_partition_name' = 'true')";
        alterTable(stmtStr, true);
        stmtStr = "alter table db3.tbl3 replace partition (tp4) with temporary partition(tp5)"
                + " properties('strict_range' = 'false', 'use_temp_partition_name' = 'true')";
        alterTable(stmtStr, false);

        // now base range is [min, 10), [50, 60) -> p1,tp5
        checkShowPartitionsResultNum("db3.tbl3", false, 2);
        checkShowPartitionsResultNum("db3.tbl3", true, 0);
    }

    @Test
    public void testForListPartitionTable() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // create database db4
        String createDbStmtStr = "create database db4;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // create table tbl4
        String createTblStmtStr1 = "create table db4.tbl4 (k1 int not null, k2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in ('1', '2', '3'),\n"
                + "partition p2 values in ('4', '5', '6'),\n"
                + "partition p3 values in ('7', '8', '9')\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr1, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        Database db4 =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("db4");
        OlapTable tbl4 = (OlapTable) db4.getTableOrAnalysisException("tbl4");

        testSerializeOlapTable(tbl4);

        Map<String, Long> originPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", false, originPartitionTabletIds);
        Assert.assertEquals(3, originPartitionTabletIds.keySet().size());

        // show temp partition
        checkShowPartitionsResultNum("db4.tbl4", true, 0);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);

        // add temp partition with duplicate name
        String stmtStr = "alter table db4.tbl4 add temporary partition p1 values in ('1', '2', '3');";
        alterTable(stmtStr, true);

        // add temp partition
        stmtStr = "alter table db4.tbl4 add temporary partition tp1 values in ('1', '2', '3');";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 add temporary partition tp2 values in ('1', '2', '3');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 add temporary partition tp1 values in ('4', '5', '6');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 add temporary partition tp2 values in ('4', '5', '6');";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 add temporary partition tp3 values in ('6', '7', '8', '9');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 add temporary partition tp3 values in ('7', '8', '9');";
        alterTable(stmtStr, false);

        Map<String, Long> tempPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", true, tempPartitionTabletIds);
        Assert.assertEquals(3, tempPartitionTabletIds.keySet().size());

        System.out.println("partition tablets: " + originPartitionTabletIds);
        System.out.println("temp partition tablets: " + tempPartitionTabletIds);

        testSerializeOlapTable(tbl4);

        // drop non exist temp partition
        stmtStr = "alter table db4.tbl4 drop temporary partition tp4;";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 drop temporary partition if exists tp4;";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 drop temporary partition tp3;";
        alterTable(stmtStr, false);

        Map<String, Long> originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        Map<String, Long> tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", true, tempPartitionTabletIds2);
        Assert.assertEquals(2, tempPartitionTabletIds2.keySet().size());
        Assert.assertTrue(!tempPartitionTabletIds2.containsKey("tp3"));

        checkShowPartitionsResultNum("db4.tbl4", true, 2);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);

        stmtStr = "alter table db4.tbl4 add temporary partition tp3 values in ('7', '8', '9');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db4.tbl4", true, 3);

        stmtStr = "alter table db4.tbl4 drop partition p1;";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db4.tbl4", true, 3);
        checkShowPartitionsResultNum("db4.tbl4", false, 2);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", false, originPartitionTabletIds2);
        Assert.assertEquals(2, originPartitionTabletIds2.size());
        Assert.assertTrue(!originPartitionTabletIds2.containsKey("p1"));

        String recoverStr = "recover partition p1 from db4.tbl4;";
        RecoverPartitionStmt recoverStmt = (RecoverPartitionStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverStr, ctx);
        Env.getCurrentEnv().recoverPartition(recoverStmt);
        checkShowPartitionsResultNum("db4.tbl4", true, 3);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", true, tempPartitionTabletIds2);
        Assert.assertEquals(3, tempPartitionTabletIds2.keySet().size());

        // Here, we should have 3 partitions p1,p2,p3, and 3 temp partitions tp1,tp2,tp3
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db4.tbl4 replace partition(p1, p2) with temporary partition(tp2, tp3);";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 replace partition(p1, p2) with temporary partition(tp1, tp2)"
                + " properties('invalid' = 'invalid');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 replace partition(p1, p2) with temporary partition(tp2, tp3);";
        alterTable(stmtStr, true);

        stmtStr = "alter table db4.tbl4 replace partition(p1, p2) with temporary partition(tp1, tp2) force"
                + " properties('use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db4.tbl4", true, 1); // tp3
        checkShowPartitionsResultNum("db4.tbl4", false, 3); // tp1, tp2, p3

        checkTabletExists(tempPartitionTabletIds2.values(), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p3")), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p1"),
                originPartitionTabletIds2.get("p2")), false);

        String truncateStr = "truncate table db4.tbl4 partition (p3);";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db4.tbl4", true, 1);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);
        checkPartitionExist(tbl4, "tp1", false, true);
        checkPartitionExist(tbl4, "tp2", false, true);
        checkPartitionExist(tbl4, "p3", false, true);
        checkPartitionExist(tbl4, "tp3", true, true);

        stmtStr = "alter table db4.tbl4 drop partition p3;";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add partition p31 values in ('7', '8');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add partition p32 values in ('9');";
        alterTable(stmtStr, false);

        // for now, we have 4 partitions: tp1, tp2, p31, p32, 1 temp partition: tp3
        checkShowPartitionsResultNum("db4.tbl4", false, 4);
        checkShowPartitionsResultNum("db4.tbl4", true, 1);

        stmtStr = "alter table db4.tbl4 replace partition(p31) with temporary partition(tp3);";
        alterTable(stmtStr, true);
        stmtStr = "alter table db4.tbl4 replace partition(p31, p32) with temporary partition(tp3);";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);
        checkShowPartitionsResultNum("db4.tbl4", true, 0);
        checkPartitionExist(tbl4, "tp1", false, true);
        checkPartitionExist(tbl4, "tp2", false, true);
        checkPartitionExist(tbl4, "tp3", false, true);

        stmtStr = "alter table db4.tbl4 add temporary partition p1 values in ('1', '2', '3');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition p2 values in ('4', '5', '6');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition p3 values in ('7', '8', '9');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 replace partition(tp1, tp2) with temporary partition(p1, p2);";
        alterTable(stmtStr, false);
        checkPartitionExist(tbl4, "tp1", false, true);
        checkPartitionExist(tbl4, "tp2", false, true);
        checkPartitionExist(tbl4, "tp3", false, true);
        checkPartitionExist(tbl4, "p1", true, false);
        checkPartitionExist(tbl4, "p2", true, false);
        checkPartitionExist(tbl4, "p3", true, true);

        stmtStr = "alter table db4.tbl4 replace partition(tp3) with temporary partition(p3)"
                + " properties('use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        checkPartitionExist(tbl4, "tp1", false, true);
        checkPartitionExist(tbl4, "tp2", false, true);
        checkPartitionExist(tbl4, "p3", false, true);
        checkPartitionExist(tbl4, "p1", true, false);
        checkPartitionExist(tbl4, "p2", true, false);
        checkPartitionExist(tbl4, "p3", true, false);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);
        checkShowPartitionsResultNum("db4.tbl4", true, 0);

        stmtStr = "alter table db4.tbl4 add temporary partition tp1 values in ('1', '2', '3');"; // name conflict
        alterTable(stmtStr, true);
        stmtStr = "alter table db4.tbl4 rename partition p3 tp3;";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition p1 values in ('1', '2', '3');";
        alterTable(stmtStr, false);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", false, originPartitionTabletIds2);
        Assert.assertEquals(3, originPartitionTabletIds2.size());

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db4.tbl4", true, tempPartitionTabletIds2);
        Assert.assertEquals(1, tempPartitionTabletIds2.keySet().size());

        // for now , we have 3 partitions: tp1, tp2, tp3, 1 temp partition: p1
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db4.tbl4 add rollup r1(k1);";
        alterTable(stmtStr, true);

        // truncate table will delete temporary partitions
        truncateStr = "truncate table db4.tbl4";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db4.tbl4", false, 3);
        checkShowPartitionsResultNum("db4.tbl4", true, 0);

        stmtStr = "alter table db4.tbl4 add rollup r1(k2,k1);";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 add temporary partition p2 values in ('1', '2', '3', '4', '5', '6');";
        alterTable(stmtStr, true);

        // wait rollup finish
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getDbId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(5000);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        // waiting table state to normal
        Thread.sleep(500);

        stmtStr = "alter table db4.tbl4 add temporary partition p2 values in ('1', '2', '3', '4', '5', '6');";
        alterTable(stmtStr, false);

        TempPartitions tempPartitions = Deencapsulation.getField(tbl4, "tempPartitions");
        testSerializeTempPartitions(tempPartitions);

        stmtStr = "alter table db4.tbl4 replace partition (tp1, tp2) with temporary partition (p2);";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db4.tbl4", false, 2);
        checkShowPartitionsResultNum("db4.tbl4", true, 0);
        checkPartitionExist(tbl4, "p2", false, true);
        checkPartitionExist(tbl4, "tp3", false, true);
        checkPartitionExist(tbl4, "tp1", false, false);
        checkPartitionExist(tbl4, "tp2", false, false);
        checkPartitionExist(tbl4, "p2", true, false);

        checkTablet("db4.tbl4", "p2", false, 2);
        checkTablet("db4.tbl4", "tp3", false, 2);

        // for now, we have 2 partitions: p2, tp3, ('1', '2', '3', '4', '5', '6'), ('7', '8', '9'). 0 temp partition.
        stmtStr = "alter table db4.tbl4 add temporary partition tp4 values in ('1', '2', '3', '4', '5', '6')"
                + " ('in_memory' = 'false') distributed by hash(k1) buckets 3";
        alterTable(stmtStr, true);
        stmtStr = "alter table db4.tbl4 add temporary partition tp4 values in ('1', '2', '3', '4', '5', '6')"
                + " ('in_memory' = 'false', 'replication_num' = '2') distributed by hash(k2) buckets 3";
        alterTable(stmtStr, true);
        stmtStr = "alter table db4.tbl4 add temporary partition tp4 values in ('1', '2', '3', '4', '5', '6')"
                + " ('in_memory' = 'false', 'replication_num' = '1') distributed by hash(k2) buckets 3";
        alterTable(stmtStr, false);

        Partition p2 = tbl4.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl4.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(1, p2.getDistributionInfo().getBucketNum());

        stmtStr = "alter table db4.tbl4 replace partition (p2) with temporary partition (tp4)";
        alterTable(stmtStr, false);

        // for now, we have 2 partitions: p2, tp3, ('1', '2', '3', '4', '5', '6'),
        // ('7', '8', '9'). 0 temp partition. and p2 bucket is 3, 'in_memory' is false.
        p2 = tbl4.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl4.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(3, p2.getDistributionInfo().getBucketNum());

        stmtStr = "alter table db4.tbl4 add temporary partition tp1 values in ('1', '2', '3');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition tp2 values in ('4', '5', '6');";
        alterTable(stmtStr, false);

        checkShowPartitionsResultNum("db4.tbl4", false, 2);
        checkShowPartitionsResultNum("db4.tbl4", true, 2);
        checkPartitionExist(tbl4, "p2", false, true);
        checkPartitionExist(tbl4, "tp3", false, true);
        checkPartitionExist(tbl4, "tp1", true, true);
        checkPartitionExist(tbl4, "tp2", true, true);

        stmtStr = "alter table db4.tbl4 replace partition (p2) with temporary partition (tp1, tp2)";
        alterTable(stmtStr, false);

        checkShowPartitionsResultNum("db4.tbl4", false, 3);
        checkShowPartitionsResultNum("db4.tbl4", true, 0);
        checkPartitionExist(tbl4, "tp1", false, true);
        checkPartitionExist(tbl4, "tp2", false, true);
        checkPartitionExist(tbl4, "tp3", false, true);
        checkPartitionExist(tbl4, "tp1", true, false);
        checkPartitionExist(tbl4, "tp2", true, false);

        // for now, tbl4 has 3 formal partition:
        // tp1 (1, 2, 3)
        // tp2 (4, 5, 6)
        // tp3 (7, 8, 9)
        // Test strict range
        stmtStr = "alter table db4.tbl4 add temporary partition p2 values in ('4', '5')";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition p31 values in ('7', '8')";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 add temporary partition p32 values in ('9')";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 replace partition (tp2) with temporary partition (p2)"
                + " properties('strict_range' = 'true');";
        alterTable(stmtStr, true);
        stmtStr = "alter table db4.tbl4 replace partition (tp2) with temporary partition (p2)"
                + " properties('strict_range' = 'false', 'use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        stmtStr = "alter table db4.tbl4 replace partition (tp3) with temporary partition (p31, p32)"
                + " properties('strict_range' = 'true', 'use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 add temporary partition p4 values in ('1', '2', '3', '4')";
        alterTable(stmtStr, false);

        stmtStr = "alter table db4.tbl4 replace partition (tp1) with temporary partition (p4)"
                + " properties('strict_range' = 'false');";
        alterTable(stmtStr, true);
    }

    @Test
    public void testForMultiListPartitionTable() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // create database db5
        String createDbStmtStr = "create database db5;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // create table tbl5
        String createTblStmtStr1 = "create table db5.tbl5 (k1 int not null, k2 varchar not null)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\",\"beijing\"), (\"1\", \"shanghai\")),\n"
                + "partition p2 values in ((\"2\",\"beijing\"), (\"2\", \"shanghai\")),\n"
                + "partition p3 values in ((\"3\",\"beijing\"), (\"3\", \"shanghai\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr1, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        Database db5 =
                Env.getCurrentInternalCatalog().getDbOrAnalysisException("db5");
        OlapTable tbl5 = (OlapTable) db5.getTableOrAnalysisException("tbl5");

        testSerializeOlapTable(tbl5);

        Map<String, Long> originPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", false, originPartitionTabletIds);
        Assert.assertEquals(3, originPartitionTabletIds.keySet().size());

        // show temp partition
        checkShowPartitionsResultNum("db5.tbl5", true, 0);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);

        // add temp partition with duplicate name
        String stmtStr = "alter table db5.tbl5 add temporary partition p1 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"));";
        alterTable(stmtStr, true);

        // add temp partition
        stmtStr = "alter table db5.tbl5 add temporary partition tp1 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"));";
        alterTable(stmtStr, false);

        stmtStr = "alter table db5.tbl5 add temporary partition tp2 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\");";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 add temporary partition tp1 values in"
                + " ((\"2\",\"beijing\"), (\"2\", \"shanghai\"));";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 add temporary partition tp2 values in"
                + " ((\"2\",\"beijing\"), (\"2\", \"shanghai\"));";
        alterTable(stmtStr, false);

        stmtStr = "alter table db5.tbl5 add temporary partition tp3 values in"
                + " ((\"2\",\"beijing\"), (\"3\",\"beijing\"), (\"3\", \"shanghai\"));";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 add temporary partition tp3 values in"
                + " ((\"3\",\"beijing\"), (\"3\", \"shanghai\"));";
        alterTable(stmtStr, false);

        Map<String, Long> tempPartitionTabletIds = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", true, tempPartitionTabletIds);
        Assert.assertEquals(3, tempPartitionTabletIds.keySet().size());

        System.out.println("partition tablets: " + originPartitionTabletIds);
        System.out.println("temp partition tablets: " + tempPartitionTabletIds);

        testSerializeOlapTable(tbl5);

        // drop non exist temp partition
        stmtStr = "alter table db5.tbl5 drop temporary partition tp4;";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 drop temporary partition if exists tp4;";
        alterTable(stmtStr, false);

        stmtStr = "alter table db5.tbl5 drop temporary partition tp3;";
        alterTable(stmtStr, false);

        Map<String, Long> originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        Map<String, Long> tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", true, tempPartitionTabletIds2);
        Assert.assertEquals(2, tempPartitionTabletIds2.keySet().size());
        Assert.assertTrue(!tempPartitionTabletIds2.containsKey("tp3"));

        checkShowPartitionsResultNum("db5.tbl5", true, 2);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);

        stmtStr = "alter table db5.tbl5 add temporary partition tp3 values in"
                + " ((\"3\",\"beijing\"), (\"3\", \"shanghai\"));";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db5.tbl5", true, 3);

        stmtStr = "alter table db5.tbl5 drop partition p1;";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db5.tbl5", true, 3);
        checkShowPartitionsResultNum("db5.tbl5", false, 2);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", false, originPartitionTabletIds2);
        Assert.assertEquals(2, originPartitionTabletIds2.size());
        Assert.assertTrue(!originPartitionTabletIds2.containsKey("p1"));

        String recoverStr = "recover partition p1 from db5.tbl5;";
        RecoverPartitionStmt recoverStmt = (RecoverPartitionStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverStr, ctx);
        Env.getCurrentEnv().recoverPartition(recoverStmt);
        checkShowPartitionsResultNum("db5.tbl5", true, 3);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", false, originPartitionTabletIds2);
        Assert.assertEquals(originPartitionTabletIds2, originPartitionTabletIds);

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", true, tempPartitionTabletIds2);
        Assert.assertEquals(3, tempPartitionTabletIds2.keySet().size());

        // Here, we should have 3 partitions p1,p2,p3, and 3 temp partitions tp1,tp2,tp3
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db5.tbl5 replace partition(p1, p2) with temporary partition(tp2, tp3);";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 replace partition(p1, p2) with temporary partition(tp1, tp2)"
                + " properties('invalid' = 'invalid');";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 replace partition(p1, p2) with temporary partition(tp2, tp3);";
        alterTable(stmtStr, true);

        stmtStr = "alter table db5.tbl5 replace partition(p1, p2) with temporary partition(tp1, tp2) force"
                + " properties('use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db5.tbl5", true, 1); // tp3
        checkShowPartitionsResultNum("db5.tbl5", false, 3); // tp1, tp2, p3

        checkTabletExists(tempPartitionTabletIds2.values(), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p3")), true);
        checkTabletExists(Lists.newArrayList(originPartitionTabletIds2.get("p1"),
                originPartitionTabletIds2.get("p2")), false);

        String truncateStr = "truncate table db5.tbl5 partition (p3);";
        TruncateTableStmt truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db5.tbl5", true, 1);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);
        checkPartitionExist(tbl5, "tp1", false, true);
        checkPartitionExist(tbl5, "tp2", false, true);
        checkPartitionExist(tbl5, "p3", false, true);
        checkPartitionExist(tbl5, "tp3", true, true);

        stmtStr = "alter table db5.tbl5 drop partition p3;";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 add partition p31 values in ((\"3\",\"beijing\"));";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 add partition p32 values in ((\"3\", \"shanghai\"));";
        alterTable(stmtStr, false);

        // for now, we have 4 partitions: tp1, tp2, p31, p32, 1 temp partition: tp3
        checkShowPartitionsResultNum("db5.tbl5", false, 4);
        checkShowPartitionsResultNum("db5.tbl5", true, 1);

        stmtStr = "alter table db5.tbl5 replace partition(p31) with temporary partition(tp3);";
        alterTable(stmtStr, true);
        stmtStr = "alter table db5.tbl5 replace partition(p31, p32) with temporary partition(tp3);";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);
        checkShowPartitionsResultNum("db5.tbl5", true, 0);
        checkPartitionExist(tbl5, "tp1", false, true);
        checkPartitionExist(tbl5, "tp2", false, true);
        checkPartitionExist(tbl5, "tp3", false, true);

        stmtStr = "alter table db5.tbl5 add temporary partition p1 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"));";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 add temporary partition p2 values in"
                + " ((\"2\",\"beijing\"), (\"2\", \"shanghai\"));";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 add temporary partition p3 values in"
                + " ((\"3\",\"beijing\"), (\"3\", \"shanghai\"));";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 replace partition(tp1, tp2) with temporary partition(p1, p2);";
        alterTable(stmtStr, false);
        checkPartitionExist(tbl5, "tp1", false, true);
        checkPartitionExist(tbl5, "tp2", false, true);
        checkPartitionExist(tbl5, "tp3", false, true);
        checkPartitionExist(tbl5, "p1", true, false);
        checkPartitionExist(tbl5, "p2", true, false);
        checkPartitionExist(tbl5, "p3", true, true);

        stmtStr = "alter table db5.tbl5 replace partition(tp3) with temporary partition(p3)"
                + " properties('use_temp_partition_name' = 'true');";
        alterTable(stmtStr, false);
        checkPartitionExist(tbl5, "tp1", false, true);
        checkPartitionExist(tbl5, "tp2", false, true);
        checkPartitionExist(tbl5, "p3", false, true);
        checkPartitionExist(tbl5, "p1", true, false);
        checkPartitionExist(tbl5, "p2", true, false);
        checkPartitionExist(tbl5, "p3", true, false);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);
        checkShowPartitionsResultNum("db5.tbl5", true, 0);

        stmtStr = "alter table db5.tbl5 add temporary partition tp1 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"));"; // name conflict
        alterTable(stmtStr, true);
        stmtStr = "alter table db5.tbl5 rename partition p3 tp3;";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 add temporary partition p1 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"));";
        alterTable(stmtStr, false);

        originPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", false, originPartitionTabletIds2);
        Assert.assertEquals(3, originPartitionTabletIds2.size());

        tempPartitionTabletIds2 = Maps.newHashMap();
        getPartitionNameToTabletIdMap("db5.tbl5", true, tempPartitionTabletIds2);
        Assert.assertEquals(1, tempPartitionTabletIds2.keySet().size());

        // for now , we have 3 partitions: tp1, tp2, tp3, 1 temp partition: p1
        System.out.println("we have partition tablets: " + originPartitionTabletIds2);
        System.out.println("we have temp partition tablets: " + tempPartitionTabletIds2);

        stmtStr = "alter table db5.tbl5 add rollup r1(k1);";
        alterTable(stmtStr, true);

        // truncate table will delete temporary partitions
        truncateStr = "truncate table db5.tbl5";
        truncateTableStmt = (TruncateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(truncateStr, ctx);
        Env.getCurrentEnv().truncateTable(truncateTableStmt);
        checkShowPartitionsResultNum("db5.tbl5", false, 3);
        checkShowPartitionsResultNum("db5.tbl5", true, 0);

        stmtStr = "alter table db5.tbl5 add rollup r1(k2, k1);";
        alterTable(stmtStr, false);

        stmtStr = "alter table db5.tbl5 add temporary partition p2 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"), (\"2\",\"beijing\"), (\"2\", \"shanghai\"));";
        alterTable(stmtStr, true);

        // wait rollup finish
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getDbId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(5000);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        // waiting table state to normal
        Thread.sleep(500);

        stmtStr = "alter table db5.tbl5 add temporary partition p2 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"), (\"2\",\"beijing\"), (\"2\", \"shanghai\"));";
        alterTable(stmtStr, false);

        TempPartitions tempPartitions = Deencapsulation.getField(tbl5, "tempPartitions");
        testSerializeTempPartitions(tempPartitions);

        stmtStr = "alter table db5.tbl5 replace partition (tp1, tp2) with temporary partition (p2);";
        alterTable(stmtStr, false);
        checkShowPartitionsResultNum("db5.tbl5", false, 2);
        checkShowPartitionsResultNum("db5.tbl5", true, 0);
        checkPartitionExist(tbl5, "p2", false, true);
        checkPartitionExist(tbl5, "tp3", false, true);
        checkPartitionExist(tbl5, "tp1", false, false);
        checkPartitionExist(tbl5, "tp2", false, false);
        checkPartitionExist(tbl5, "p2", true, false);

        checkTablet("db5.tbl5", "p2", false, 2);
        checkTablet("db5.tbl5", "tp3", false, 2);

        // for now, we have 2 partitions: p2, tp3,
        // (("1","beijing"), ("1", "shanghai"), ("2","beijing"), ("2", "shanghai")), ('7', '8', '9').
        // 0 temp partition.
        stmtStr = "alter table db5.tbl5 add temporary partition tp4 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"), (\"2\",\"beijing\"), (\"2\", \"shanghai\"))"
                + " ('in_memory' = 'false') distributed by hash(k1) buckets 3";
        alterTable(stmtStr, true);
        stmtStr = "alter table db5.tbl5 add temporary partition tp4 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"), (\"2\",\"beijing\"), (\"2\", \"shanghai\"))"
                + " ('in_memory' = 'false', 'replication_num' = '2') distributed by hash(k2) buckets 3";
        alterTable(stmtStr, true);
        stmtStr = "alter table db5.tbl5 add temporary partition tp4 values in"
                + " ((\"1\",\"beijing\"), (\"1\", \"shanghai\"), (\"2\",\"beijing\"), (\"2\", \"shanghai\"))"
                + " ('in_memory' = 'false', 'replication_num' = '1') distributed by hash(k2) buckets 3";
        alterTable(stmtStr, false);

        Partition p2 = tbl5.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl5.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(1, p2.getDistributionInfo().getBucketNum());

        stmtStr = "alter table db5.tbl5 replace partition (p2) with temporary partition (tp4)";
        alterTable(stmtStr, false);

        // for now, we have 2 partitions: p2, tp3,
        // (("1","beijing"), ("1", "shanghai"), ("2","beijing"), ("2", "shanghai")), ('7', '8', '9').
        // 0 temp partition. and p2 bucket is 3, 'in_memory' is false.
        p2 = tbl5.getPartition("p2");
        Assert.assertNotNull(p2);
        Assert.assertFalse(tbl5.getPartitionInfo().getIsInMemory(p2.getId()));
        Assert.assertEquals(3, p2.getDistributionInfo().getBucketNum());

        stmtStr = "alter table db5.tbl5 add temporary partition tp1"
                + " values in ((\"1\",\"beijing\"), (\"1\", \"shanghai\"));";
        alterTable(stmtStr, false);
        stmtStr = "alter table db5.tbl5 add temporary partition tp2"
                + " values in ((\"2\",\"beijing\"), (\"2\", \"shanghai\"));";
        alterTable(stmtStr, false);

        checkShowPartitionsResultNum("db5.tbl5", false, 2);
        checkShowPartitionsResultNum("db5.tbl5", true, 2);
        checkPartitionExist(tbl5, "p2", false, true);
        checkPartitionExist(tbl5, "tp3", false, true);
        checkPartitionExist(tbl5, "tp1", true, true);
        checkPartitionExist(tbl5, "tp2", true, true);

        stmtStr = "alter table db5.tbl5 replace partition (p2) with temporary partition (tp1, tp2)";
        alterTable(stmtStr, false);

        checkShowPartitionsResultNum("db5.tbl5", false, 3);
        checkShowPartitionsResultNum("db5.tbl5", true, 0);
        checkPartitionExist(tbl5, "tp1", false, true);
        checkPartitionExist(tbl5, "tp2", false, true);
        checkPartitionExist(tbl5, "tp3", false, true);
        checkPartitionExist(tbl5, "tp1", true, false);
        checkPartitionExist(tbl5, "tp2", true, false);

    }

    private void testSerializeOlapTable(OlapTable tbl) throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(tempPartitionFile);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        tbl.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        OlapTable readTbl = OlapTable.read(in);
        Assert.assertEquals(tbl.getId(), readTbl.getId());
        Assert.assertEquals(tbl.getAllTempPartitions().size(), readTbl.getAllTempPartitions().size());
        file.delete();
    }

    private void testSerializeTempPartitions(TempPartitions tempPartitionsInstance) throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File(tempPartitionFile);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        Text.writeString(out, GsonUtils.GSON.toJson(tempPartitionsInstance));
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        TempPartitions readTempPartition = TempPartitions.read(in);
        List<Partition> partitions = readTempPartition.getAllPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(2, partitions.get(0).getMaterializedIndices(IndexExtState.VISIBLE).size());
    }
}
