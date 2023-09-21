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

package org.apache.doris.utframe;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.proc.BackendsProcDir;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/*
 * This demo shows how to run unit test with mocked FE and BE.
 * It will
 *  1. start a mocked FE and a mocked BE.
 *  2. Create a database and a tbl.
 *  3. Make a schema change to tbl.
 *  4. send a query and get query plan
 */
public class DemoMultiBackendsTest {

    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/DemoMultiBackendsTest/" + UUID.randomUUID().toString() + "/";
    private static List<Backend> backends = Lists.newArrayList();
    private static Random random = new Random(System.currentTimeMillis());

    @BeforeClass
    public static void beforeClass() throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;

        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 3);

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

    @Test
    public void testCreateDbAndTable() throws Exception {
        // 1. create connect context
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // 2. create database db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3"
                + " properties('replication_num' = '3',"
                + "'colocate_with' = 'g1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
        // must set replicas' path hash, or the tablet scheduler won't work
        updateReplicaPathHash();

        // 4. get and test the created db and table
        Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:db1");
        Assert.assertNotNull(db);
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl1");
        tbl.readLock();
        try {
            Assert.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assert.assertEquals("Doris", tbl.getEngine());
            Assert.assertEquals(1, tbl.getBaseSchema().size());
        } finally {
            tbl.readUnlock();
        }
        // 5. process a schema change job
        String alterStmtStr = "alter table db1.tbl1 add column k2 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, ctx);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(alterTableStmt);
        // 6. check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        Assert.assertEquals(1, alterJobs.size());
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }

        OlapTable tbl1 = (OlapTable) db.getTableNullable("tbl1");
        tbl1.readLock();
        try {
            Assert.assertEquals(2, tbl1.getBaseSchema().size());
            String baseIndexName = tbl1.getIndexNameById(tbl.getBaseIndexId());
            Assert.assertEquals(baseIndexName, tbl1.getName());
            MaterializedIndexMeta indexMeta = tbl1.getIndexMetaByIndexId(tbl1.getBaseIndexId());
            Assert.assertNotNull(indexMeta);
        } finally {
            tbl1.readUnlock();
        }
        // 7. query
        // TODO: we can not process real query for now. So it has to be a explain query
        String queryStr = "explain select * from db1.tbl1";
        String a = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, queryStr);

        System.out.println(a);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        Assert.assertEquals(2, fragments.size());
        PlanFragment fragment = fragments.get(1);
        Assert.assertTrue(fragment.getPlanRoot() instanceof OlapScanNode);
        Assert.assertEquals(0, fragment.getChildren().size());

        // test show backends;
        BackendsProcDir dir = new BackendsProcDir(Env.getCurrentSystemInfo());
        ProcResult result = dir.fetchResult();
        Assert.assertEquals(BackendsProcDir.TITLE_NAMES.size(), result.getColumnNames().size());
        Assert.assertEquals("{\"location\" : \"default\"}",
                result.getRows().get(0).get(BackendsProcDir.TITLE_NAMES.size() - 6));
        Assert.assertEquals(
                "{\"lastSuccessReportTabletsTime\":\"N/A\",\"lastStreamLoadTime\":-1,\"isQueryDisabled\":false,\"isLoadDisabled\":false}",
                result.getRows().get(0).get(BackendsProcDir.TITLE_NAMES.size() - 3));
        Assert.assertEquals("0", result.getRows().get(0).get(BackendsProcDir.TITLE_NAMES.size() - 2));
        Assert.assertEquals(Tag.VALUE_MIX, result.getRows().get(0).get(BackendsProcDir.TITLE_NAMES.size() - 1));
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
}
