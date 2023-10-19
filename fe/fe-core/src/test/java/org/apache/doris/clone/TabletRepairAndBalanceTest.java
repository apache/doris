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

package org.apache.doris.clone;

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.BackendClause;
import org.apache.doris.analysis.CancelAlterSystemStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class TabletRepairAndBalanceTest {
    private static final Logger LOG = LogManager.getLogger(TabletRepairAndBalanceTest.class);

    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/TabletRepairAndBalanceTest/" + UUID.randomUUID() + "/";
    private static ConnectContext connectContext;

    private static Random random = new Random(System.currentTimeMillis());

    private static List<Backend> backends = Lists.newArrayList();

    private static Tag tag1;
    private static Tag tag2;

    static {
        try {
            tag1 = Tag.create(Tag.TYPE_LOCATION, "zone1");
            tag2 = Tag.create(Tag.TYPE_LOCATION, "zone2");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        Config.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        Config.colocate_group_relocate_delay_second = 1;
        // 5 backends:
        // 127.0.0.1
        // 127.0.0.2
        // 127.0.0.3
        // 127.0.0.4
        // 127.0.0.5
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 5);
        connectContext = UtFrameUtils.createDefaultCtx();

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);

        // must set disk info, or the tablet scheduler won't work
        backends = Env.getCurrentSystemInfo().getAllBackends();
        for (Backend be : backends) {
            Map<String, TDisk> backendDisks = Maps.newHashMap();
            TDisk tDisk1 = new TDisk();
            tDisk1.setRootPath("/home/doris.HDD");
            tDisk1.setDiskTotalCapacity(2000000000);
            tDisk1.setDataUsedCapacity(1);
            tDisk1.setUsed(true);
            tDisk1.setDiskAvailableCapacity(tDisk1.disk_total_capacity - tDisk1.data_used_capacity);
            tDisk1.setPathHash(random.nextLong());
            tDisk1.setStorageMedium(TStorageMedium.HDD);
            backendDisks.put(tDisk1.getRootPath(), tDisk1);

            TDisk tDisk2 = new TDisk();
            tDisk2.setRootPath("/home/doris.SSD");
            tDisk2.setDiskTotalCapacity(2000000000);
            tDisk2.setDataUsedCapacity(1);
            tDisk2.setUsed(true);
            tDisk2.setDiskAvailableCapacity(tDisk2.disk_total_capacity - tDisk2.data_used_capacity);
            tDisk2.setPathHash(random.nextLong());
            tDisk2.setStorageMedium(TStorageMedium.SSD);
            backendDisks.put(tDisk2.getRootPath(), tDisk2);

            be.updateDisks(backendDisks);
        }
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDirBase);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
        // must set replicas' path hash, or the tablet scheduler won't work
        RebalancerTestUtil.updateReplicaPathHash();
    }

    private static void dropTable(String sql) throws Exception {
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().dropTable(dropTableStmt);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
    }

    @Test
    public void test() throws Exception {
        Assert.assertEquals(5, backends.size());

        // set tag for all backends. 0-2 to zone1, 4 and 5 to zone2
        for (int i = 0; i < backends.size(); ++i) {
            Backend be = backends.get(i);
            String tag = "zone1";
            if (i > 2) {
                tag = "zone2";
            }
            String stmtStr = "alter system modify backend \"" + be.getHost() + ":" + be.getHeartbeatPort()
                    + "\" set ('tag.location' = '" + tag + "')";
            AlterSystemStmt stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
            DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        }

        // Test set tag without location type, expect throw exception
        Backend be1 = backends.get(0);
        String alterString = "alter system modify backend \"" + be1.getHost() + ":" + be1.getHeartbeatPort()
                + "\" set ('tag.compute' = 'abc')";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, BackendClause.NEED_LOCATION_TAG_MSG,
                () -> UtFrameUtils.parseAndAnalyzeStmt(alterString, connectContext));

        // Test set multi tag for a Backend when Config.enable_multi_tags is false
        Config.enable_multi_tags = false;
        String alterString2 = "alter system modify backend \"" + be1.getHost() + ":" + be1.getHeartbeatPort()
                + "\" set ('tag.location' = 'zone3', 'tag.compution' = 'abc')";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, BackendClause.MUTLI_TAG_DISABLED_MSG,
                () -> UtFrameUtils.parseAndAnalyzeStmt(alterString2, connectContext));

        // Test set multi tag for a Backend when Config.enable_multi_tags is true
        Config.enable_multi_tags = true;
        String stmtStr3 = "alter system modify backend \"" + be1.getHost() + ":" + be1.getHeartbeatPort()
                + "\" set ('tag.location' = 'zone1', 'tag.compute' = 'c1')";
        AlterSystemStmt stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr3, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        Map<String, String> tagMap = be1.getTagMap();
        Assert.assertEquals(2, tagMap.size());
        Assert.assertEquals("zone1", tagMap.get(Tag.TYPE_LOCATION));
        Assert.assertEquals("c1", tagMap.get("compute"));
        Assert.assertEquals(Tag.createNotCheck(Tag.TYPE_LOCATION, "zone1"), be1.getLocationTag());

        Tag zone1 = Tag.create(Tag.TYPE_LOCATION, "zone1");
        Tag zone2 = Tag.create(Tag.TYPE_LOCATION, "zone2");
        Assert.assertEquals(zone1, backends.get(0).getLocationTag());
        Assert.assertEquals(zone1, backends.get(1).getLocationTag());
        Assert.assertEquals(zone1, backends.get(2).getLocationTag());
        Assert.assertEquals(zone2, backends.get(3).getLocationTag());
        Assert.assertEquals(zone2, backends.get(4).getLocationTag());

        // create table
        // 1. no default tag, create will fail
        String createStr = "create table test.tbl1\n" + "(k1 date, k2 int)\n" + "partition by range(k1)\n" + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n" + ")\n" + "distributed by hash(k2) buckets 10;";
        ExceptionChecker.expectThrows(DdlException.class, () -> createTable(createStr));

        // nodes of zone2 not enough, create will fail
        // it will fail because of we check tag location during the analysis process, so we check AnalysisException
        String createStr2 = "create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 10\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 3\"\n"
                + ")";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTable(createStr2));

        // normal, create success
        String createStr3 = "create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 10\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 1\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr3));
        Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl1");

        // alter table's replica allocation failed, tag not enough
        String alterStr = "alter table test.tbl1"
                + " set (\"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 3\");";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> alterTable(alterStr));
        ReplicaAllocation tblReplicaAlloc = tbl.getDefaultReplicaAllocation();
        Assert.assertEquals(3, tblReplicaAlloc.getTotalReplicaNum());
        Assert.assertEquals(Short.valueOf((short) 2), tblReplicaAlloc.getReplicaNumByTag(tag1));
        Assert.assertEquals(Short.valueOf((short) 1), tblReplicaAlloc.getReplicaNumByTag(tag2));

        // alter partition's replica allocation succeed
        String alterStr2 = "alter table test.tbl1 modify partition p1"
                + " set (\"replication_allocation\" = \"tag.location.zone1: 1, tag.location.zone2: 2\");";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr2));
        Partition p1 = tbl.getPartition("p1");
        ReplicaAllocation p1ReplicaAlloc = tbl.getPartitionInfo().getReplicaAllocation(p1.getId());
        Assert.assertEquals(3, p1ReplicaAlloc.getTotalReplicaNum());
        Assert.assertEquals(Short.valueOf((short) 1), p1ReplicaAlloc.getReplicaNumByTag(tag1));
        Assert.assertEquals(Short.valueOf((short) 2), p1ReplicaAlloc.getReplicaNumByTag(tag2));
        ExceptionChecker.expectThrows(UserException.class, () -> tbl.checkReplicaAllocation());

        // check backend get() methods
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        Set<Tag> tags = infoService.getTags();
        Assert.assertEquals(2, tags.size());

        // check tablet and replica number
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        Table<Long, Long, Replica> replicaMetaTable = invertedIndex.getReplicaMetaTable();
        Assert.assertEquals(30, replicaMetaTable.rowKeySet().size());
        Assert.assertEquals(5, replicaMetaTable.columnKeySet().size());

        // wait all replica reallocating to correct backend
        checkTableReplicaAllocation(tbl);
        Assert.assertEquals(90, replicaMetaTable.cellSet().size());

        // for now, tbl has 3 partitions:
        // p1: zone1: 1, zone2: 2
        // p2: zone1: 2, zone2: 1
        // p3: zone1: 2, zone2: 1
        // Backends:
        // [0, 1, 2]: zone1
        // [3, 4]:    zone2

        // change backend 2 to zone2
        // set tag for all backends. 0-2 to zone1, 4 and 5 to zone2
        // and wait all replica reallocating to correct backend
        Backend be = backends.get(2);
        String stmtStr = "alter system modify backend \"" + be.getHost() + ":" + be.getHeartbeatPort()
                + "\" set ('tag.location' = 'zone2')";
        stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        Assert.assertEquals(tag2, be.getLocationTag());
        ExceptionChecker.expectThrows(UserException.class, () -> tbl.checkReplicaAllocation());
        checkTableReplicaAllocation(tbl);
        Assert.assertEquals(90, replicaMetaTable.cellSet().size());

        // For now, Backends:
        // [0, 1]:      zone1
        // [2, 3, 4]:   zone2
        // begin to test colocation table
        String createStr4 = "create table test.col_tbl1\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 10\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 1\",\n"
                + "    \"colocate_with\" = \"g1\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr4));
        String createStr5 = "create table test.col_tbl2\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 10\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 1\",\n"
                + "    \"colocate_with\" = \"g1\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr5));

        OlapTable colTbl1 = (OlapTable) db.getTableNullable("col_tbl1");
        OlapTable colTbl2 = (OlapTable) db.getTableNullable("col_tbl2");
        Assert.assertNotNull(colTbl1);
        Assert.assertNotNull(colTbl2);
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(colTbl1.getId());
        Assert.assertEquals(groupId, colocateTableIndex.getGroup(colTbl2.getId()));
        ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(groupId);
        ReplicaAllocation groupReplicaAlloc = groupSchema.getReplicaAlloc();
        Assert.assertEquals(groupReplicaAlloc, colTbl1.getDefaultReplicaAllocation());
        Assert.assertEquals(groupReplicaAlloc, colTbl2.getDefaultReplicaAllocation());
        checkTableReplicaAllocation(colTbl1);
        checkTableReplicaAllocation(colTbl2);

        // change backend4's tag to zone1:
        // [0, 1, 4]: zone1
        // [2, 3]:    zone2
        be = backends.get(4);
        stmtStr = "alter system modify backend \"" + be.getHost() + ":" + be.getHeartbeatPort()
                + "\" set ('tag.location' = 'zone1')";
        stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        Assert.assertEquals(tag1, be.getLocationTag());
        ExceptionChecker.expectThrows(UserException.class, () -> tbl.checkReplicaAllocation());

        checkTableReplicaAllocation(colTbl1);
        checkTableReplicaAllocation(colTbl2);

        // for now,
        // backends' tag:
        // [0, 1, 4]: zone1
        // [2, 3]:    zone2
        //
        // colocate group(col_tbl1, col_tbl2) replica allocation is: zone1:2, zone2:1
        // tbl1's replica allocation is:
        //      p1: zone1:1, zone2:2
        //      p2,p2: zone1:2, zone2:1

        // change tbl1's default replica allocation to zone1:3, which is allowed
        String alterStr3 = "alter table test.tbl1 set ('default.replication_allocation' = 'tag.location.zone1:3')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr3));

        // change tbl1's p1's replica allocation to zone1:4, which is forbidden
        String alterStr4 = "alter table test.tbl1 modify partition p1"
                + " set ('replication_allocation' = 'tag.location.zone1:4')";
        ExceptionChecker.expectThrows(AnalysisException.class, () -> alterTable(alterStr4));

        // change col_tbl1's default replica allocation to zone2:2, which is forbidden
        String alterStr5 = "alter table test.col_tbl1 set ('default.replication_allocation' = 'tag.location.zone2:2')";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Cannot change replication allocation of colocate table", () -> alterTable(alterStr5));

        // Drop all tables
        String dropStmt1 = "drop table test.tbl1 force";
        String dropStmt2 = "drop table test.col_tbl1 force";
        String dropStmt3 = "drop table test.col_tbl2 force";
        ExceptionChecker.expectThrowsNoException(() -> dropTable(dropStmt1));
        ExceptionChecker.expectThrowsNoException(() -> dropTable(dropStmt2));
        ExceptionChecker.expectThrowsNoException(() -> dropTable(dropStmt3));
        Assert.assertEquals(0, replicaMetaTable.size());

        // set all backends' tag to default
        for (int i = 0; i < backends.size(); ++i) {
            Backend backend = backends.get(i);
            String backendStmt = "alter system modify backend \"" + backend.getHost() + ":" + backend.getHeartbeatPort()
                    + "\" set ('tag.location' = 'default')";
            AlterSystemStmt systemStmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(backendStmt,
                    connectContext);
            DdlExecutor.execute(Env.getCurrentEnv(), systemStmt);
        }
        Assert.assertEquals(Tag.DEFAULT_BACKEND_TAG, backends.get(0).getLocationTag());
        Assert.assertEquals(Tag.DEFAULT_BACKEND_TAG, backends.get(1).getLocationTag());
        Assert.assertEquals(Tag.DEFAULT_BACKEND_TAG, backends.get(2).getLocationTag());
        Assert.assertEquals(Tag.DEFAULT_BACKEND_TAG, backends.get(3).getLocationTag());
        Assert.assertEquals(Tag.DEFAULT_BACKEND_TAG, backends.get(4).getLocationTag());

        // create table tbl2 with "replication_num" property
        String createStmt = "create table test.tbl2\n" + "(k1 date, k2 int)\n" + "partition by range(k1)\n" + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n" + ")\n" + "distributed by hash(k2) buckets 10;";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStmt));
        OlapTable tbl2 = (OlapTable) db.getTableNullable("tbl2");
        ReplicaAllocation defaultAlloc = new ReplicaAllocation((short) 3);
        Assert.assertEquals(defaultAlloc, tbl2.getDefaultReplicaAllocation());
        for (Partition partition : tbl2.getPartitions()) {
            Assert.assertEquals(defaultAlloc, tbl2.getPartitionInfo().getReplicaAllocation(partition.getId()));
        }

        // add new partition to tbl2
        String alterStr6 = "alter table test.tbl2 add partition p4 values less than('2021-09-01')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr6));
        Assert.assertEquals(4, tbl2.getPartitionNames().size());
        PartitionInfo partitionInfo = tbl2.getPartitionInfo();
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION,
                partitionInfo.getReplicaAllocation(tbl2.getPartition("p4").getId()));

        // change tbl2 to a colocate table
        String alterStr7 = "alter table test.tbl2 SET (\"colocate_with\"=\"newg\")";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr7));
        ColocateTableIndex.GroupId groupId1 = colocateTableIndex.getGroup(tbl2.getId());
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION,
                colocateTableIndex.getGroupSchema(groupId1).getReplicaAlloc());

        // test colocate table index persist
        ExceptionChecker.expectThrowsNoException(() -> testColocateTableIndexSerialization(colocateTableIndex));

        // test colocate tablet repair
        String createStr6 = "create table test.col_tbl3\n"
                + "(k1 date, k2 int)\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties\n"
                + "(\n"
                + "    \"replication_num\" = \"3\",\n"
                + "    \"colocate_with\" = \"g3\"\n"
                + ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr6));

        OlapTable tbl3 = db.getOlapTableOrDdlException("col_tbl3");
        RebalancerTestUtil.updateReplicaPathHash();
        // Set one replica's state as DECOMMISSION, see if it can be changed to NORMAL
        Tablet oneTablet = null;
        Replica oneReplica = null;
        for (Partition partition : tbl3.getPartitions()) {
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : mIndex.getTablets()) {
                    oneTablet = tablet;
                    for (Replica replica : tablet.getReplicas()) {
                        oneReplica = replica;
                        oneReplica.setState(Replica.ReplicaState.DECOMMISSION);
                        break;
                    }
                }
            }
        }
        Assert.assertTrue(checkReplicaState(oneReplica));

        // set one replica to bad, see if it can be repaired
        oneReplica.setBad(true);
        Assert.assertTrue(checkReplicaBad(oneTablet, oneReplica));


        //test decommission backend by ids

        String stmtStr4 = "alter system decommission backend \"" + be.getHost() + ":" + be.getHeartbeatPort() + "\"";
        stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr4, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);

        String stmtStr5 = "cancel decommission backend \"" + be.getId() + "\"";
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr5, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), cancelAlterSystemStmt);

        Assert.assertFalse(be.isDecommissioned());

    }

    private static boolean checkReplicaState(Replica replica) throws Exception {
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            if (replica.getState() != Replica.ReplicaState.NORMAL) {
                continue;
            }
            return true;
        }
        return false;
    }

    private static boolean checkReplicaBad(Tablet tablet, Replica replica) throws Exception {
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            if (tablet.getReplicaById(replica.getId()) != null) {
                continue;
            }
            return true;
        }
        return false;
    }

    private void testColocateTableIndexSerialization(ColocateTableIndex colocateTableIndex) throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./ColocateTableIndexPersist");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        colocateTableIndex.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        ColocateTableIndex rColocateTableIndex = new ColocateTableIndex();
        rColocateTableIndex.readFields(dis);

        Assert.assertEquals(1, colocateTableIndex.getAllGroupIds().size());
        Set<ColocateTableIndex.GroupId> allGroupIds = colocateTableIndex.getAllGroupIds();
        for (ColocateTableIndex.GroupId groupId : allGroupIds) {
            Map<Tag, List<List<Long>>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
            for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                List<List<Long>> seq = entry.getValue();
                Assert.assertEquals(10, seq.size());
                for (List<Long> beIds : seq) {
                    Assert.assertEquals(3, beIds.size());
                }
            }
        }

        // 3. delete files
        dis.close();
        file.delete();
    }

    private void checkTableReplicaAllocation(OlapTable tbl) throws InterruptedException {
        int maxLoop = 300;
        while (maxLoop-- > 0) {
            try {
                tbl.checkReplicaAllocation();
                break;
            } catch (UserException | NoSuchElementException e) {
                // Why do we add no such element exception because hash map is not a thread safe struct.
                // In this ut using a big loop to iterate the hash map,
                // it will increase the probability of map to throw NoSuchElementException exception.
                System.out.println(e.getMessage());
            }
            Thread.sleep(1000);
            System.out.println("wait table " + tbl.getId() + " to be stable");
        }
        ExceptionChecker.expectThrowsNoException(() -> tbl.checkReplicaAllocation());
        System.out.println("table " + tbl.getId() + " is stable");
    }
}
