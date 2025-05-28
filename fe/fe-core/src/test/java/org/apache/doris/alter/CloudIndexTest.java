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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BuildIndexClause;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.DropIndexClause;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.ShowBuildIndexStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.utframe.MockedMetaServerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudIndexTest {
    private static final Logger LOG = LogManager.getLogger(CloudIndexTest.class);

    private static String fileName = "./CloudIndexTest";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;
    private ConnectContext ctx;

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

        ctx = new ConnectContext();
        ctx.setEnv(masterEnv);
        ctx.setQualifiedUser("root");
        UserIdentity rootUser = new UserIdentity("root", "%");
        rootUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(rootUser);
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

        // Mock access manager to allow root user access
        new MockUp<Env>() {
            @Mock
            public AccessControllerManager getAccessManager() {
                return new AccessControllerManager(masterEnv.getAuth()) {
                    @Override
                    public boolean checkTblPriv(ConnectContext ctx, String ctl, String db, String tbl, PrivPredicate wanted) {
                        return true; // Allow all access for test
                    }

                    @Override
                    public boolean checkCloudPriv(UserIdentity user, String cluster, PrivPredicate wanted, ResourceTypeEnum resourceType) {
                        return true; // Allow all cloud privileges for test
                    }
                };
            }
        };

        new MockUp<Auth>() {
            @Mock
            public String getDefaultCloudCluster(String user) {
                return "test_group"; // Return default cluster for test
            }

            @Mock
            public ComputeGroup getComputeGroup(String user) {
                try {
                    return masterEnv.getComputeGroupMgr().getComputeGroupByName("test_group");
                } catch (Exception e) {
                    return masterEnv.getComputeGroupMgr().getAllBackendComputeGroup();
                }
            }
        };

        // Mock cloud environment permissions
        new MockUp<CloudEnv>() {
            @Mock
            public void checkCloudClusterPriv(String cluster) throws Exception {
                // Always allow for tests
            }
        };

        // Mock ConnectContext to avoid compute group permission check
        new MockUp<ConnectContext>() {
            @Mock
            public String getCloudCluster() {
                return "test_group";
            }

            @Mock
            public UserIdentity getCurrentUserIdentity() {
                UserIdentity rootUser = new UserIdentity("root", "%");
                rootUser.setIsAnalyzed();
                return rootUser;
            }
        };

        analyzer = new Analyzer(masterEnv, ctx);

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

        // Add required properties for NGRAM_BF index
        Map<String, String> properties = Maps.newHashMap();
        properties.put("gram_size", "2");
        properties.put("bf_size", "256");

        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                org.apache.doris.analysis.IndexDef.IndexType.NGRAM_BF,
                properties, "ngram bf index");
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

        // Add required properties for NGRAM_BF index
        Map<String, String> properties = Maps.newHashMap();
        properties.put("gram_size", "2");
        properties.put("bf_size", "256");

        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                org.apache.doris.analysis.IndexDef.IndexType.NGRAM_BF,
                properties, "ngram bf index");
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
    public void testShowBuildInvertedIndex() throws Exception {
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

        Expr where = new BinaryPredicate(
                BinaryPredicate.Operator.EQ, new SlotRef(tableName, "TableName"),
                new StringLiteral(table.getName()));
        ShowBuildIndexStmt buildIndexStmt = new ShowBuildIndexStmt(db.getName(), where, null, null);
        buildIndexStmt.analyze(analyzer);
        ShowExecutor executor = new ShowExecutor(ctx, buildIndexStmt);
        ShowResultSet result = executor.execute();
        LOG.info(result.getResultRows());
        Assert.assertEquals(1, result.getResultRows().size());

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
        List<Long> alterJobIdList = new ArrayList<>();
        alterJobIdList.add(jobV2.jobId);
        cancelAlterTableStmt = new CancelAlterTableStmt(ShowAlterStmt.AlterType.INDEX, tableName, alterJobIdList);
        cancelAlterTableStmt.analyze(analyzer);
        schemaChangeHandler.cancel(cancelAlterTableStmt);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, jobV2.getJobState());
    }

    @Test
    public void testCreateAndBuildMultipleInvertedIndexes() throws Exception {
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
        String indexName1 = "raw_inverted_index1";
        String indexName2 = "raw_inverted_index2";

        IndexDef indexDef1 = new IndexDef(indexName1, false,
                Lists.newArrayList(table.getBaseSchema().get(2).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "first inverted index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef1, false);
        createIndexClause.analyze(analyzer);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        long job1Id = indexChangeJobMap.values().stream().findAny().get().jobId;

        IndexDef indexDef2 = new IndexDef(indexName2, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "second inverted index");
        createIndexClause = new CreateIndexClause(tableName, indexDef2, false);
        createIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(2, table.getIndexes().size());
        long job2Id = indexChangeJobMap.values().stream().filter(job -> job.jobId != job1Id).findAny().get().jobId;

        boolean hasIndex1 = false;
        boolean hasIndex2 = false;
        for (int i = 0; i < table.getIndexes().size(); i++) {
            String name = table.getIndexes().get(i).getIndexName();
            if (name.equals(indexName1)) {
                hasIndex1 = true;
            } else if (name.equals(indexName2)) {
                hasIndex2 = true;
            }
        }
        Assert.assertTrue(hasIndex1);
        Assert.assertTrue(hasIndex2);

        buildIndexClause = new BuildIndexClause(tableName, indexName1, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());
        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> (job.jobId != job1Id && job.jobId != job2Id))
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());
        long job3Id = jobV2.getJobId();

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, jobV2.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, jobV2.getJobState());
        Assert.assertEquals(2, table.getIndexes().size());

        buildIndexClause = new BuildIndexClause(tableName, indexName2, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
        .filter(job -> (job.jobId != job2Id && job.jobId != job3Id && job.jobId != job1Id))
        .findFirst()
        .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, jobV2.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, jobV2.getJobState());

        tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(2, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, jobV2.getJobState());

        Assert.assertEquals(2, table.getIndexes().size());
    }

    @Test
    public void testCancelMultipleInvertedBuildIndex() throws Exception {
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

        // Create first inverted index
        String indexName1 = "raw_inverted_index1";
        IndexDef indexDef1 = new IndexDef(indexName1, false,
                Lists.newArrayList(table.getBaseSchema().get(2).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "first inverted index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef1, false);
        createIndexClause.analyze(analyzer);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        long job1Id = indexChangeJobMap.values().stream().findAny().get().jobId;

        // Create second inverted index
        String indexName2 = "raw_inverted_index2";
        IndexDef indexDef2 = new IndexDef(indexName2, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "second inverted index");
        createIndexClause = new CreateIndexClause(tableName, indexDef2, false);
        createIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(2, table.getIndexes().size());
        long job2Id = indexChangeJobMap.values().stream().filter(job -> job.jobId != job1Id).findAny().get().jobId;

        // Verify both indexes were created
        boolean hasIndex1 = false;
        boolean hasIndex2 = false;
        for (int i = 0; i < table.getIndexes().size(); i++) {
            String name = table.getIndexes().get(i).getIndexName();
            if (name.equals(indexName1)) {
                hasIndex1 = true;
            } else if (name.equals(indexName2)) {
                hasIndex2 = true;
            }
        }
        Assert.assertTrue(hasIndex1);
        Assert.assertTrue(hasIndex2);

        // Start building first index
        buildIndexClause = new BuildIndexClause(tableName, indexName1, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());
        SchemaChangeJobV2 job1V2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> (job.jobId != job1Id && job.jobId != job2Id))
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, job1V2.schemaChangeBatchTask.getTaskNum());
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, job1V2.getJobState());
        Assert.assertEquals(0, job1V2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job1V2.getJobState());
        Assert.assertEquals(1, job1V2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job1V2.getJobState());
        Assert.assertEquals(1, job1V2.schemaChangeBatchTask.getTaskNum());
        List<Long> alterJobIdList = new ArrayList<>();
        alterJobIdList.add(job1V2.jobId);
        cancelAlterTableStmt = new CancelAlterTableStmt(ShowAlterStmt.AlterType.INDEX, tableName, alterJobIdList);
        cancelAlterTableStmt.analyze(analyzer);
        schemaChangeHandler.cancel(cancelAlterTableStmt);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job1V2.getJobState());

        // Start building first index
        buildIndexClause = new BuildIndexClause(tableName, indexName2, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());
        SchemaChangeJobV2 job2V2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> (job.jobId != job1Id && job.jobId != job2Id && job.jobId != job1V2.jobId))
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, job2V2.schemaChangeBatchTask.getTaskNum());
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, job2V2.getJobState());
        Assert.assertEquals(0, job2V2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job2V2.getJobState());
        Assert.assertEquals(1, job2V2.schemaChangeBatchTask.getTaskNum());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job2V2.getJobState());
        Assert.assertEquals(1, job2V2.schemaChangeBatchTask.getTaskNum());
        alterJobIdList.clear();
        alterJobIdList.add(job2V2.jobId);
        cancelAlterTableStmt = new CancelAlterTableStmt(ShowAlterStmt.AlterType.INDEX, tableName, alterJobIdList);
        cancelAlterTableStmt.analyze(analyzer);
        schemaChangeHandler.cancel(cancelAlterTableStmt);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job2V2.getJobState());
    }

    @Test
    public void testCreateTokenizedInvertedIndex() throws Exception {
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

        // Set inverted index file storage format to V2 for cloud mode
        table.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);

        String indexName = "tokenized_inverted_index";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("parser", "english");
        properties.put("support_phrase", "true");
        properties.put("lower_case", "true");

        // Use VARCHAR column v1 (index 2) for string type support
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(2).getName()),
                IndexType.INVERTED,
                properties, "tokenized inverted index with english parser");
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
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());

        // This should be a heavyweight schema change for tokenized index
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

        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("tokenized_inverted_index", table.getIndexes().get(0).getIndexName());

        // Verify that the index has the correct properties
        Assert.assertEquals("english", table.getIndexes().get(0).getProperties().get("parser"));
        Assert.assertEquals("true", table.getIndexes().get(0).getProperties().get("support_phrase"));
        Assert.assertEquals("true", table.getIndexes().get(0).getProperties().get("lower_case"));
    }

    @Test
    public void testCreateMixedIndexes() throws Exception {
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

        // Set inverted index file storage format to V2 for cloud mode
        table.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);

        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();

        // Step 1: Create tokenized inverted index (heavyweight schema change)
        String tokenizedIndexName = "tokenized_index";
        Map<String, String> tokenizedProperties = Maps.newHashMap();
        tokenizedProperties.put("parser", "chinese");
        tokenizedProperties.put("support_phrase", "false");
        tokenizedProperties.put("lower_case", "true");

        IndexDef tokenizedIndexDef = new IndexDef(tokenizedIndexName, false,
                Lists.newArrayList(table.getBaseSchema().get(2).getName()),
                IndexType.INVERTED,
                tokenizedProperties, "tokenized inverted index with chinese parser");
        createIndexClause = new CreateIndexClause(tableName, tokenizedIndexDef, false);
        createIndexClause.analyze(analyzer);
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 tokenizedJobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .findFirst()
                .orElse(null);
        long tokenizedJobId = tokenizedJobV2.getJobId();

        // Execute tokenized index creation (heavyweight schema change)
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, tokenizedJobV2.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, tokenizedJobV2.getJobState());
        Assert.assertEquals(1, tokenizedJobV2.schemaChangeBatchTask.getTaskNum());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, tokenizedJobV2.getJobState());
        Assert.assertEquals(1, table.getIndexes().size());

        // Step 2: Create NGRAM_BF index (lightweight)
        String ngramBfIndexName = "ngram_bf_mixed_index";
        Map<String, String> ngramBfProperties = Maps.newHashMap();
        ngramBfProperties.put("gram_size", "3");
        ngramBfProperties.put("bf_size", "512");

        IndexDef ngramBfIndexDef = new IndexDef(ngramBfIndexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                IndexType.NGRAM_BF,
                ngramBfProperties, "ngram bf mixed index");
        createIndexClause = new CreateIndexClause(tableName, ngramBfIndexDef, false);
        createIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(2, indexChangeJobMap.size());
        Assert.assertEquals(2, table.getIndexes().size());
        long ngramBfJobId = indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != tokenizedJobId)
                .findFirst().get().jobId;

        // Step 3: Create raw inverted index (lightweight)
        String rawInvertedIndexName = "raw_inverted_mixed_index";
        IndexDef rawInvertedIndexDef = new IndexDef(rawInvertedIndexName, false,
                Lists.newArrayList(table.getBaseSchema().get(1).getName()),
                IndexType.INVERTED,
                Maps.newHashMap(), "raw inverted mixed index");
        createIndexClause = new CreateIndexClause(tableName, rawInvertedIndexDef, false);
        createIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(3, indexChangeJobMap.size());
        Assert.assertEquals(3, table.getIndexes().size());
        long rawInvertedJobId = indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != tokenizedJobId && job.jobId != ngramBfJobId)
                .findFirst().get().jobId;

        // Verify all indexes were created with correct names and types
        boolean hasTokenizedIndex = false;
        boolean hasNgramBfIndex = false;
        boolean hasRawInvertedIndex = false;
        for (int i = 0; i < table.getIndexes().size(); i++) {
            String name = table.getIndexes().get(i).getIndexName();
            if (name.equals(tokenizedIndexName)) {
                hasTokenizedIndex = true;
                // Verify tokenized index properties
                Assert.assertEquals("chinese", table.getIndexes().get(i).getProperties().get("parser"));
                Assert.assertEquals("false", table.getIndexes().get(i).getProperties().get("support_phrase"));
                Assert.assertEquals("true", table.getIndexes().get(i).getProperties().get("lower_case"));
            } else if (name.equals(ngramBfIndexName)) {
                hasNgramBfIndex = true;
                // Verify ngram bf index properties
                Assert.assertEquals("3", table.getIndexes().get(i).getProperties().get("gram_size"));
                Assert.assertEquals("512", table.getIndexes().get(i).getProperties().get("bf_size"));
            } else if (name.equals(rawInvertedIndexName)) {
                hasRawInvertedIndex = true;
                // Verify raw inverted index has no special properties
                Assert.assertTrue(table.getIndexes().get(i).getProperties().isEmpty()
                        || !table.getIndexes().get(i).getProperties().containsKey("parser"));
            }
        }
        Assert.assertTrue("Tokenized index should be created", hasTokenizedIndex);
        Assert.assertTrue("NGRAM_BF index should be created", hasNgramBfIndex);
        Assert.assertTrue("Raw inverted index should be created", hasRawInvertedIndex);

        // Step 4: Build NGRAM_BF index
        buildIndexClause = new BuildIndexClause(tableName, ngramBfIndexName, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());
        SchemaChangeJobV2 ngramBfBuildJobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != tokenizedJobId && job.jobId != ngramBfJobId && job.jobId != rawInvertedJobId)
                .findFirst()
                .orElse(null);
        Assert.assertNotNull("NGRAM_BF build job should be created", ngramBfBuildJobV2);
        long ngramBfBuildJobId = ngramBfBuildJobV2.getJobId();

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, ngramBfBuildJobV2.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, ngramBfBuildJobV2.getJobState());
        Assert.assertEquals(1, ngramBfBuildJobV2.schemaChangeBatchTask.getTaskNum());

        tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(2, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, ngramBfBuildJobV2.getJobState());

        // Step 5: Build raw inverted index
        buildIndexClause = new BuildIndexClause(tableName, rawInvertedIndexName, null, false);
        buildIndexClause.analyze(analyzer);
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, table);

        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());
        SchemaChangeJobV2 rawInvertedBuildJobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .filter(job -> job.jobId != tokenizedJobId && job.jobId != ngramBfJobId
                           && job.jobId != rawInvertedJobId && job.jobId != ngramBfBuildJobId)
                .findFirst()
                .orElse(null);
        Assert.assertNotNull("Raw inverted build job should be created", rawInvertedBuildJobV2);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, rawInvertedBuildJobV2.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, rawInvertedBuildJobV2.getJobState());
        Assert.assertEquals(1, rawInvertedBuildJobV2.schemaChangeBatchTask.getTaskNum());

        tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, rawInvertedBuildJobV2.getJobState());

        // Final verification: all 3 indexes should exist and be properly configured
        Assert.assertEquals(3, table.getIndexes().size());
        LOG.info("Successfully created and built mixed indexes: tokenized, NGRAM_BF, and raw inverted");
    }
}
