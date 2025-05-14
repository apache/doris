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

package org.apache.doris.alter;

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BuildIndexClause;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.DropIndexClause;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudEnvFactory;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.utframe.MockedMetaServerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudIndexTest {
    private static String fileName = "./CloudIndexTest";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;

    private static Analyzer analyzer;
    private static Database db;
    private static OlapTable olapTable;
    private static CreateIndexClause createIndexClause;
    private static BuildIndexClause buildIndexClause;
    private static DropIndexClause dropIndexClause;
    private static CancelAlterTableStmt cancelAlterTableStmt;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, UserException {
        FeConstants.runningUnitTest = true;
        // Setup for MetaServiceProxy mock
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {

            @Mock
            public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
                Cloud.BeginTxnResponse.Builder beginTxnResponseBuilder = Cloud.BeginTxnResponse.newBuilder();
                beginTxnResponseBuilder.setTxnId(1000)
                        .setStatus(
                                Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));
                return beginTxnResponseBuilder.build();
            }

            @Mock
            public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                Cloud.TxnInfoPB.Builder txnInfoBuilder = Cloud.TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(CatalogTestUtil.testDbId1);
                txnInfoBuilder.addAllTableIds(Lists.newArrayList(olapTable.getId()));
                txnInfoBuilder.setLabel("test_label");
                txnInfoBuilder.setListenerId(-1);
                Cloud.CommitTxnResponse.Builder commitTxnResponseBuilder = Cloud.CommitTxnResponse.newBuilder();
                commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(txnInfoBuilder.build());
                return commitTxnResponseBuilder.build();
            }

            @Mock
            public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request) {
                Cloud.CheckTxnConflictResponse.Builder checkTxnConflictResponseBuilder =
                        Cloud.CheckTxnConflictResponse.newBuilder();
                checkTxnConflictResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setFinished(true);
                return checkTxnConflictResponseBuilder.build();
            }

            @Mock
            public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) {
                Cloud.GetClusterResponse.Builder getClusterResponseBuilder = Cloud.GetClusterResponse.newBuilder();
                Cloud.ClusterPB.Builder clusterBuilder = Cloud.ClusterPB.newBuilder();
                clusterBuilder.setClusterId("test_id").setClusterName("test_group");

                Cloud.NodeInfoPB.Builder node1 = Cloud.NodeInfoPB.newBuilder();
                node1.setCloudUniqueId("test_cloud")
                        .setName("host1")
                        .setIp("host1")
                        .setHost("host1")
                        .setHeartbeatPort(123)
                        .setEditLogPort(125)
                        .setStatus(Cloud.NodeStatusPB.NODE_STATUS_RUNNING);
                clusterBuilder.addNodes(node1.build());
                getClusterResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .addCluster(clusterBuilder.build());
                return getClusterResponseBuilder.build();
            }

            @Mock
            public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) {
                Cloud.CreateTabletsResponse.Builder responseBuilder = Cloud.CreateTabletsResponse.newBuilder();
                responseBuilder.setStatus(
                        Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));
                return responseBuilder.build();
            }

            @Mock
            public Cloud.FinishTabletJobResponse finishTabletJob(Cloud.FinishTabletJobRequest request) {
                Cloud.FinishTabletJobResponse.Builder responseBuilder = Cloud.FinishTabletJobResponse.newBuilder();
                responseBuilder.setStatus(
                        Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));
                return responseBuilder.build();
            }

            @Mock
            public Cloud.IndexResponse prepareIndex(Cloud.IndexRequest request) {
                Cloud.IndexResponse.Builder builder = Cloud.IndexResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }

            @Mock
            public Cloud.IndexResponse commitIndex(Cloud.IndexRequest request) {
                Cloud.IndexResponse.Builder builder = Cloud.IndexResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }

            @Mock
            public Cloud.IndexResponse dropIndex(Cloud.IndexRequest request) {
                Cloud.IndexResponse.Builder builder = Cloud.IndexResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }

            @Mock
            public Cloud.CheckKVResponse checkKv(Cloud.CheckKVRequest request) {
                Cloud.CheckKVResponse.Builder builder = Cloud.CheckKVResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return builder.build();
            }

            @Mock
            public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request) {
                Cloud.GetCurrentMaxTxnResponse.Builder builder = Cloud.GetCurrentMaxTxnResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setCurrentMaxTxnId(1000);
                return builder.build();
            }
        };

        Config.cloud_unique_id = "test_cloud";
        Config.meta_service_endpoint = MockedMetaServerFactory.METASERVER_DEFAULT_IP + ":" + 20121;

        EnvFactory envFactory = EnvFactory.getInstance();
        masterEnv = envFactory.createEnv(false);
        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        FakeEnv.setSystemInfo(cloudSystemInfo);

        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);

        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(masterEnv);
        ctx.setQualifiedUser("root");
        ctx.setThreadLocalInfo();
        ctx.setCloudCluster("test_group");
        Assert.assertTrue(envFactory instanceof CloudEnvFactory);
        Assert.assertTrue(masterEnv instanceof CloudEnv);
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return masterEnv;
            }

            @Mock
            public EditLog getEditLog() {
                return new EditLog("cloud_index_test");
            }

            @Mock
            public ComputeGroupMgr getComputeGroupMgr() {
                return new ComputeGroupMgr(Env.getCurrentSystemInfo());
            }
        };
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        ((CloudSystemInfoService) Env.getCurrentSystemInfo()).addCloudCluster("test_group", "");
        List<Backend> backends =
                ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getBackendsByClusterName("test_group");
        Assert.assertEquals(1, backends.size());
        Assert.assertEquals("host1", backends.get(0).getHost());
        backends.get(0).setAlive(true);
        ctx.setComputeGroup(masterEnv.getComputeGroupMgr().getAllBackendComputeGroup());
        db = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);
        masterEnv.unprotectCreateDb(db);

        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testCreateAndBuildNgramBfIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        OlapTable table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        table.setDataSortInfo(dataSortInfo);
        String indexName = "ngram_bf_index";
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                org.apache.doris.analysis.IndexDef.IndexType.NGRAM_BF,
                Maps.newHashMap(), "ngram bf index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("ngram_bf_index", table.getIndexes().get(0).getIndexName());

        long jobId = indexChangeJobMap.values().stream().findAny().get().jobId;

        buildIndexClause = new BuildIndexClause(tableName, indexName, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);

        schemaChangeHandler.process(alterClauses, db, table);
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != jobId)
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, jobV2.getJobState());
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());
        Assert.assertEquals(1, jobV2.schemaChangeBatchTask.getTaskNum());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, jobV2.getJobState());
    }

    @Test
    public void testCancelNgramBfBuildIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        OlapTable table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        table.setDataSortInfo(dataSortInfo);
        //table.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);
        String indexName = "ngram_bf_index";
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                org.apache.doris.analysis.IndexDef.IndexType.NGRAM_BF,
                Maps.newHashMap(), "ngram bf index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("ngram_bf_index", table.getIndexes().get(0).getIndexName());

        long jobId = indexChangeJobMap.values().stream().findAny().get().jobId;

        buildIndexClause = new BuildIndexClause(tableName, indexName, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);

        schemaChangeHandler.process(alterClauses, db, table);
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != jobId)
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, jobV2.getJobState());
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());
        Assert.assertEquals(1, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());
        Assert.assertEquals(1, jobV2.schemaChangeBatchTask.getTaskNum());

        cancelAlterTableStmt = new CancelAlterTableStmt(ShowAlterStmt.AlterType.INDEX, tableName);
        cancelAlterTableStmt.analyze(analyzer);
        schemaChangeHandler.cancel(cancelAlterTableStmt);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, jobV2.getJobState());
    }

    @Test
    public void testCreateAndBuildRawInvertedIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        OlapTable table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        table.setDataSortInfo(dataSortInfo);
        String indexName = "raw_inverted_index";
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "raw inverted index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("raw_inverted_index", table.getIndexes().get(0).getIndexName());

        long jobId = indexChangeJobMap.values().stream().findAny().get().jobId;

        buildIndexClause = new BuildIndexClause(tableName, indexName, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);

        schemaChangeHandler.process(alterClauses, db, table);
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != jobId)
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, jobV2.getJobState());
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());
        Assert.assertEquals(1, jobV2.schemaChangeBatchTask.getTaskNum());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, jobV2.getJobState());
    }

    @Test
    public void testCancelRawInvertedBuildIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        OlapTable table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        table.setDataSortInfo(dataSortInfo);
        String indexName = "raw_inverted_index";
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "raw inverted index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("raw_inverted_index", table.getIndexes().get(0).getIndexName());

        long jobId = indexChangeJobMap.values().stream().findAny().get().jobId;

        buildIndexClause = new BuildIndexClause(tableName, indexName, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);

        schemaChangeHandler.process(alterClauses, db, table);
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != jobId)
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, jobV2.getJobState());
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());
        Assert.assertEquals(1, jobV2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());
        Assert.assertEquals(1, jobV2.schemaChangeBatchTask.getTaskNum());
        List<Long> alterJobIdList = new ArrayList<>();
        alterJobIdList.add(jobV2.jobId);
        cancelAlterTableStmt = new CancelAlterTableStmt(ShowAlterStmt.AlterType.INDEX, tableName, alterJobIdList);
        cancelAlterTableStmt.analyze(analyzer);
        schemaChangeHandler.cancel(cancelAlterTableStmt);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, jobV2.getJobState());
    }
}
