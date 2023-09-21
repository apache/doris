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

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class ResourceTagQueryTest {
    private static final Logger LOG = LogManager.getLogger(ResourceTagQueryTest.class);

    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/ResourceTagQueryTest/" + UUID.randomUUID().toString() + "/";
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
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        Config.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
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
            tDisk1.setDirType("STORAGE");
            backendDisks.put(tDisk1.getRootPath(), tDisk1);

            TDisk tDisk2 = new TDisk();
            tDisk2.setRootPath("/home/doris.SSD");
            tDisk2.setDiskTotalCapacity(2000000000);
            tDisk2.setDataUsedCapacity(1);
            tDisk2.setUsed(true);
            tDisk2.setDiskAvailableCapacity(tDisk2.disk_total_capacity - tDisk2.data_used_capacity);
            tDisk2.setPathHash(random.nextLong());
            tDisk2.setStorageMedium(TStorageMedium.SSD);
            tDisk2.setDirType("STORAGE");
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
        updateReplicaPathHash();
    }

    private static void updateReplicaPathHash() {
        Table<Long, Long, Replica> replicaMetaTable = Env.getCurrentInvertedIndex().getReplicaMetaTable();
        for (Table.Cell<Long, Long, Replica> cell : replicaMetaTable.cellSet()) {
            long beId = cell.getColumnKey();
            Backend be = Env.getCurrentSystemInfo().getBackend(beId);
            if (be == null) {
                continue;
            }
            Replica replica = cell.getValue();
            TabletMeta tabletMeta = Env.getCurrentInvertedIndex().getTabletMeta(cell.getRowKey());
            ImmutableMap<String, DiskInfo> diskMap = be.getDisks();
            for (DiskInfo diskInfo : diskMap.values()) {
                if (diskInfo.getStorageMedium() == tabletMeta.getStorageMedium()) {
                    replica.setPathHash(diskInfo.getPathHash());
                    break;
                }
            }
        }
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
    }

    private static void setProperty(String sql) throws Exception {
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getAuth().updateUserProperty(setUserPropertyStmt);
    }

    @Test
    public void test() throws Exception {

        // create table with default tag
        String createStr = "create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + " partition p1 values less than(\"2021-06-01\"),\n"
                + " partition p2 values less than(\"2021-07-01\"),\n"
                + " partition p3 values less than(\"2021-08-01\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 10;";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr));
        Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl1");

        Set<Tag> userTags = Env.getCurrentEnv().getAuth().getResourceTags(Auth.ROOT_USER);
        Assert.assertEquals(0, userTags.size());

        // set default tag for root
        String setPropStr = "set property for 'root' 'resource_tags.location' = 'default';";
        ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));
        userTags = Env.getCurrentEnv().getAuth().getResourceTags(Auth.ROOT_USER);
        Assert.assertEquals(1, userTags.size());

        // update connection context and query
        connectContext.setResourceTags(userTags);
        String queryStr = "explain select * from test.tbl1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("tablets=30/30"));

        // set zone1 tag for root
        String setPropStr2 = "set property for 'root' 'resource_tags.location' = 'zone1';";
        ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr2));
        userTags = Env.getCurrentEnv().getAuth().getResourceTags(Auth.ROOT_USER);
        Assert.assertEquals(1, userTags.size());
        for (Tag tag : userTags) {
            Assert.assertEquals(tag1, tag);
        }

        // update connection context and query, it will failed because no zone1 backend
        connectContext.setResourceTags(userTags);
        Assert.assertTrue(connectContext.isResourceTagsSet());
        queryStr = "explain select * from test.tbl1";
        String error = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(error.contains("have no queryable replicas"));

        // set [0, 1, 2] backends' tag to zone1, so that at least 1 replica can be queried.
        // set tag for all backends. 0-2 to zone1, 4 and 5 to zone2
        for (int i = 0; i < backends.size(); ++i) {
            Backend be = backends.get(i);
            String tag = "zone1";
            if (i > 2) {
                break;
            }
            String stmtStr = "alter system modify backend \"" + be.getHost() + ":" + be.getHeartbeatPort()
                    + "\" set ('tag.location' = '" + tag + "')";
            AlterSystemStmt stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
            DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        }
        Assert.assertEquals(tag1, backends.get(0).getLocationTag());
        Assert.assertEquals(tag1, backends.get(1).getLocationTag());
        Assert.assertEquals(tag1, backends.get(2).getLocationTag());

        queryStr = "explain select * from test.tbl1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("tablets=30/30"));

        // for now, 3 backends with tag zone1, 2 with tag default, so table is not stable.
        ExceptionChecker.expectThrows(UserException.class, () -> tbl.checkReplicaAllocation());
        // alter table's replication allocation to zone1:2 and default:1
        String alterStr
                = "alter table test.tbl1 modify partition (p1, p2, p3) set ('replication_allocation' = 'tag.location.zone1:2, tag.location.default:1')";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr));
        Map<Tag, Short> expectedAllocMap = Maps.newHashMap();
        expectedAllocMap.put(Tag.DEFAULT_BACKEND_TAG, (short) 1);
        expectedAllocMap.put(tag1, (short) 2);
        for (Partition partition : tbl.getPartitions()) {
            ReplicaAllocation replicaAllocation = tbl.getPartitionInfo().getReplicaAllocation(partition.getId());
            Map<Tag, Short> allocMap = replicaAllocation.getAllocMap();
            Assert.assertEquals(expectedAllocMap, allocMap);
        }
        ReplicaAllocation tblReplicaAllocation = tbl.getDefaultReplicaAllocation();
        Assert.assertNotEquals(expectedAllocMap, tblReplicaAllocation.getAllocMap());
        ExceptionChecker.expectThrowsNoException(() -> checkTableReplicaAllocation(tbl));

        // can still query
        queryStr = "explain select * from test.tbl1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("tablets=30/30"));

        // set user exec mem limit
        String setExecMemLimitStr = "set property for 'root' 'exec_mem_limit' = '1000000';";
        ExceptionChecker.expectThrowsNoException(() -> setProperty(setExecMemLimitStr));
        long execMemLimit = Env.getCurrentEnv().getAuth().getExecMemLimit(Auth.ROOT_USER);
        Assert.assertEquals(1000000, execMemLimit);

        List<List<String>> userProps = Env.getCurrentEnv().getAuth().getUserProperties(Auth.ROOT_USER);
        Assert.assertEquals(10, userProps.size());
    }

    private void checkTableReplicaAllocation(OlapTable tbl) throws InterruptedException {
        int maxLoop = 300;
        while (maxLoop-- > 0) {
            try {
                tbl.checkReplicaAllocation();
                break;
            } catch (UserException e) {
                System.out.println(e.getMessage());
            }
            Thread.sleep(1000);
            System.out.println("wait table " + tbl.getId() + " to be stable");
        }
        ExceptionChecker.expectThrowsNoException(() -> tbl.checkReplicaAllocation());
        System.out.println("table " + tbl.getId() + " is stable");
    }
}
