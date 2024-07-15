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

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ModifyBackendTest {

    private static String runningDir = "fe/mocked/ModifyBackendTagTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testModifyBackendTag() throws Exception {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Backend> backends = infoService.getAllBackends();
        Assert.assertEquals(1, backends.size());
        String beHostPort = backends.get(0).getHost() + ":" + backends.get(0).getHeartbeatPort();

        // modify backend tag
        String stmtStr = "alter system modify backend \"" + beHostPort + "\" set ('tag.location' = 'zone1')";
        AlterSystemStmt stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        backends = infoService.getAllBackends();
        Assert.assertEquals(1, backends.size());

        // create table
        String createStr = "create table test.tbl1(\n" + "k1 int\n" + ") distributed by hash(k1)\n"
                + "buckets 3 properties(\n" + "\"replication_num\" = \"1\"\n" + ");";
        CreateTableStmt createStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createStr, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to find enough backend, please check the replication num,replication tag and storage medium and avail capacity of backends "
                        + "or maybe all be on same host.\n"
                        + "Create failed replications:\n"
                        + "replication tag: {\"location\" : \"default\"}, replication num: 1, storage medium: HDD",
                () -> DdlExecutor.execute(Env.getCurrentEnv(), createStmt));

        createStr = "create table test.tbl1(\n" + "k1 int\n" + ") distributed by hash(k1)\n" + "buckets 3 properties(\n"
                + "\"replication_allocation\" = \"tag.location.zone1: 1\"\n" + ");";
        CreateTableStmt createStmt2 = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createStr, connectContext);
        ExceptionChecker.expectThrowsNoException(() -> DdlExecutor.execute(Env.getCurrentEnv(), createStmt2));

        // create dynamic partition tbl
        createStr = "create table test.tbl3(\n"
                + "k1 date, k2 int\n"
                + ") partition by range(k1)()\n"
                + "distributed by hash(k1)\n"
                + "buckets 3 properties(\n" + "    \"dynamic_partition.enable\" = \"true\",\n"
                + "    \"dynamic_partition.time_unit\" = \"DAY\",\n" + "    \"dynamic_partition.start\" = \"-3\",\n"
                + "    \"dynamic_partition.end\" = \"3\",\n" + "    \"dynamic_partition.prefix\" = \"p\",\n"
                + "    \"dynamic_partition.buckets\" = \"1\",\n" + "    \"dynamic_partition.replication_num\" = \"3\"\n"
                + ");";
        CreateTableStmt createStmt3 = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createStr, connectContext);
        //partition create failed, because there is no BE with "default" tag
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "replication num should be less than the number of available backends. replication num is 3, available backend num is 1",
                () -> DdlExecutor.execute(Env.getCurrentEnv(), createStmt3));
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test");

        createStr = "create table test.tbl4(\n" + "k1 date, k2 int\n" + ") partition by range(k1)()\n"
                + "distributed by hash(k1)\n" + "buckets 3 properties(\n"
                + "    \"dynamic_partition.enable\" = \"true\",\n" + "    \"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "    \"dynamic_partition.start\" = \"-3\",\n" + "    \"dynamic_partition.end\" = \"3\",\n"
                + "    \"dynamic_partition.prefix\" = \"p\",\n"
                + "    \"dynamic_partition.buckets\" = \"1\",\n"
                + "    \"dynamic_partition.replication_allocation\" = \"tag.location.zone1:1\"\n"
                + ");";
        CreateTableStmt createStmt4 = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createStr, connectContext);
        ExceptionChecker.expectThrowsNoException(() -> DdlExecutor.execute(Env.getCurrentEnv(), createStmt4));
        OlapTable tbl = (OlapTable) db.getTableNullable("tbl4");
        PartitionInfo partitionInfo = tbl.getPartitionInfo();
        Assert.assertEquals(4, partitionInfo.idToItem.size());
        ReplicaAllocation replicaAlloc = new ReplicaAllocation();
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone1"), (short) 1);
        for (ReplicaAllocation allocation : partitionInfo.idToReplicaAllocation.values()) {
            Assert.assertEquals(replicaAlloc, allocation);
        }

        ReplicaAllocation defaultAlloc = tbl.getDefaultReplicaAllocation();
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, defaultAlloc);
        TableProperty tableProperty = tbl.getTableProperty();
        Map<String, String> tblProperties = tableProperty.getProperties();
        // if replication_num or replication_allocation is not set, it will be set to the default one
        Assert.assertTrue(tblProperties.containsKey("default.replication_allocation"));
        Assert.assertEquals("tag.location.default: 3", tblProperties.get("default.replication_allocation"));

        // modify default replica
        String alterStr = "alter table test.tbl4 set ('default.replication_allocation' = 'tag.location.zone1:1')";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStr, connectContext);
        ExceptionChecker.expectThrowsNoException(() -> DdlExecutor.execute(Env.getCurrentEnv(), alterStmt));
        defaultAlloc = tbl.getDefaultReplicaAllocation();
        ReplicaAllocation expectedAlloc = new ReplicaAllocation();
        expectedAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone1"), (short) 1);
        Assert.assertEquals(expectedAlloc, defaultAlloc);
        tblProperties = tableProperty.getProperties();
        Assert.assertTrue(tblProperties.containsKey("default.replication_allocation"));

        // modify partition replica with wrong zone
        // it will fail because of we check tag location during the analysis process, so we check AnalysisException
        String partName = tbl.getPartitionNames().stream().findFirst().get();
        String wrongAlterStr = "alter table test.tbl4 modify partition " + partName
                + " set ('replication_allocation' = 'tag.location.zonex:1')";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "errCode = 2, detailMessage = "
                        + "errCode = 2, detailMessage = Failed to find enough backend, "
                        + "please check the replication num,replication tag and storage medium and avail capacity of backends "
                        + "or maybe all be on same host.\n"
                        + "Create failed replications:\n"
                        + "replication tag: {\"location\" : \"zonex\"}, replication num: 1, storage medium: null",
                () -> UtFrameUtils.parseAndAnalyzeStmt(wrongAlterStr, connectContext));
        tblProperties = tableProperty.getProperties();
        Assert.assertTrue(tblProperties.containsKey("default.replication_allocation"));

        alterStr = "alter table test.tbl4 modify partition " + partName
                + " set ('replication_allocation' = 'tag.location.zone1:1')";
        AlterTableStmt alterStmt3 = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStr, connectContext);
        ExceptionChecker.expectThrowsNoException(() -> DdlExecutor.execute(Env.getCurrentEnv(), alterStmt3));
        tblProperties = tableProperty.getProperties();
        Assert.assertTrue(tblProperties.containsKey("default.replication_allocation"));
    }

    @Test
    public void testModifyBackendAvailableProperty() throws Exception {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Backend> backends = infoService.getAllBackends();
        String beHostPort = backends.get(0).getHost() + ":" + backends.get(0).getHeartbeatPort();
        // modify backend available property
        String stmtStr = "alter system modify backend \"" + beHostPort + "\" set ('disable_query' = 'true', 'disable_load' = 'true')";
        AlterSystemStmt stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        Backend backend = infoService.getAllBackends().get(0);
        Assert.assertFalse(backend.isQueryAvailable());
        Assert.assertFalse(backend.isLoadAvailable());

        stmtStr = "alter system modify backend \"" + beHostPort + "\" set ('disable_query' = 'false', 'disable_load' = 'false')";
        stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
        DdlExecutor.execute(Env.getCurrentEnv(), stmt);
        Assert.assertTrue(backend.isQueryAvailable());
        Assert.assertTrue(backend.isLoadAvailable());
    }
}
