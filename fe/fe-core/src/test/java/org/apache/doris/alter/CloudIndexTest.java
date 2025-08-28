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
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.ResourceTypeEnum;
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
    private static String clusterName = "test_cluster";
    private static String clusterID = "test_id";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;
    private static EditLog testEditLog;
    private ConnectContext ctx;
    private static OlapTable olapTable;

    private static Analyzer analyzer;
    private static Database db;
    private static CreateIndexClause createIndexClause;
    private static SchemaChangeHandler schemaChangeHandler;

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
                clusterBuilder.setClusterId(clusterID).setClusterName(clusterName);

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
        testEditLog = null; // Will be set by MockUp
        FakeEnv.setEnv(masterEnv);

        ctx = new ConnectContext();
        ctx.setEnv(masterEnv);
        ctx.setQualifiedUser("root");
        UserIdentity rootUser = new UserIdentity("root", "%");
        rootUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(rootUser);
        ctx.setThreadLocalInfo();
        ctx.setCloudCluster(clusterName);
        Assert.assertTrue(envFactory instanceof CloudEnvFactory);
        Assert.assertTrue(masterEnv instanceof CloudEnv);
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return masterEnv;
            }

            @Mock
            public EditLog getEditLog() {
                if (testEditLog == null) {
                    // Create a mock EditLog using a no-op approach
                    testEditLog = new EditLog("test") {
                        // Override to avoid initialization issues
                    };
                }
                return testEditLog;
            }

            @Mock
            public SchemaChangeHandler getSchemaChangeHandler() {
                // Create a new independent SchemaChangeHandler for each call
                return schemaChangeHandler;
            }

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
                return clusterName; // Return default cluster for test
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
                return clusterName;
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
        CloudSystemInfoService systemInfo = (CloudSystemInfoService) Env.getCurrentSystemInfo();
        Backend backend = new Backend(10001L, "host1", 123);
        backend.setAlive(true);
        backend.setBePort(456);
        backend.setHttpPort(789);
        backend.setBrpcPort(321);
        Map<String, String> newTagMap = org.apache.doris.resource.Tag.DEFAULT_BACKEND_TAG.toMap();
        newTagMap.put(org.apache.doris.resource.Tag.CLOUD_CLUSTER_STATUS, "NORMAL");
        newTagMap.put(org.apache.doris.resource.Tag.CLOUD_CLUSTER_NAME, clusterName);
        newTagMap.put(org.apache.doris.resource.Tag.CLOUD_CLUSTER_ID, clusterID);
        newTagMap.put(org.apache.doris.resource.Tag.CLOUD_CLUSTER_PUBLIC_ENDPOINT, "");
        newTagMap.put(org.apache.doris.resource.Tag.CLOUD_CLUSTER_PRIVATE_ENDPOINT, "");
        newTagMap.put(org.apache.doris.resource.Tag.CLOUD_UNIQUE_ID, "test_cloud");
        backend.setTagMap(newTagMap);
        List<Backend> backends = Lists.newArrayList(backend);
        systemInfo.updateCloudClusterMapNoLock(backends, new ArrayList<>());
        db = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);
        masterEnv.unprotectCreateDb(db);

        AgentTaskQueue.clearAllTasks();
        schemaChangeHandler = masterEnv.getSchemaChangeHandler();
    }

    @Test
    public void testCreateNgramBfIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);
        schemaChangeHandler = (SchemaChangeHandler) new Alter().getSchemaChangeHandler();

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        olapTable.setDataSortInfo(dataSortInfo);
        String indexName = "ngram_bf_index";

        // Add required properties for NGRAM_BF index
        Map<String, String> properties = Maps.newHashMap();
        properties.put("gram_size", "2");
        properties.put("bf_size", "256");

        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(3).getName()),
                org.apache.doris.analysis.IndexDef.IndexType.NGRAM_BF,
                properties, "ngram bf index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        ctx.getSessionVariable().setEnableAddIndexForNewData(true);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, olapTable.getIndexes().size());
        Assert.assertEquals("ngram_bf_index", olapTable.getIndexes().get(0).getIndexName());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        long createJobId = indexChangeJobMap.values().stream().findAny().get().jobId;

        // Finish the create index job first
        SchemaChangeJobV2 createJobV2 = (SchemaChangeJobV2) indexChangeJobMap.get(createJobId);
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, createJobV2.getJobState());
    }

    @Test
    public void testNormalCreateNgramBfIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);
        schemaChangeHandler = (SchemaChangeHandler) new Alter().getSchemaChangeHandler();

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        olapTable.setDataSortInfo(dataSortInfo);
        String indexName = "ngram_bf_index";

        // Add required properties for NGRAM_BF index
        Map<String, String> properties = Maps.newHashMap();
        properties.put("gram_size", "2");
        properties.put("bf_size", "256");

        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(3).getName()),
                org.apache.doris.analysis.IndexDef.IndexType.NGRAM_BF,
                properties, "ngram bf index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        // Set session variable to false (default)
        ctx.getSessionVariable().setEnableAddIndexForNewData(false);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        long createJobId = indexChangeJobMap.values().stream().findAny().get().jobId;

        // Finish the create index job first
        SchemaChangeJobV2 createJobV2 = (SchemaChangeJobV2) indexChangeJobMap.get(createJobId);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, createJobV2.getJobState());
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, createJobV2.getJobState());
        Assert.assertEquals(1, createJobV2.schemaChangeBatchTask.getTaskNum());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, createJobV2.getJobState());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        Assert.assertEquals(1, olapTable.getIndexes().size());
        Assert.assertEquals("ngram_bf_index", olapTable.getIndexes().get(0).getIndexName());
    }

    @Test
    public void testCreateInvertedIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);
        schemaChangeHandler = (SchemaChangeHandler) new Alter().getSchemaChangeHandler();

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        olapTable.setDataSortInfo(dataSortInfo);
        String indexName = "raw_inverted_index";
        // Explicitly set parser="none" for raw inverted index
        Map<String, String> properties = Maps.newHashMap();
        properties.put("parser", "none");

        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(3).getName()),
                IndexType.INVERTED,
                properties, "raw inverted index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        ctx.getSessionVariable().setEnableAddIndexForNewData(false);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());

        long createJobId = indexChangeJobMap.values().stream().findAny().get().jobId;
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        // Finish the create index job first
        SchemaChangeJobV2 createJobV2 = (SchemaChangeJobV2) indexChangeJobMap.get(createJobId);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, createJobV2.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, createJobV2.getJobState());
        Assert.assertEquals(1, createJobV2.schemaChangeBatchTask.getTaskNum());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, createJobV2.getJobState());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        Assert.assertEquals(1, olapTable.getIndexes().size());
        Assert.assertEquals("raw_inverted_index", olapTable.getIndexes().get(0).getIndexName());
    }

    @Test
    public void testCreateInvertedIndexWithLightweightMode() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);
        schemaChangeHandler = (SchemaChangeHandler) new Alter().getSchemaChangeHandler();

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        olapTable.setDataSortInfo(dataSortInfo);
        String indexName = "lightweight_raw_inverted_index";
        // Explicitly set parser="none" for raw inverted index
        Map<String, String> properties = Maps.newHashMap();
        properties.put("parser", "none");
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(3).getName()),
                IndexType.INVERTED,
                properties, "lightweight raw inverted index");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        // Test with enable_add_index_for_new_data = true, should use lightweight mode
        ctx.getSessionVariable().setEnableAddIndexForNewData(true);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        // Lightweight mode should not create any schema change jobs
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, olapTable.getIndexes().size());
        Assert.assertEquals("lightweight_raw_inverted_index", olapTable.getIndexes().get(0).getIndexName());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        // Verify the index properties
        Assert.assertEquals("none", olapTable.getIndexes().get(0).getProperties().get("parser"));
    }

    @Test
    public void testCreateTokenizedInvertedIndex() throws Exception {
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);

        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setSystemInfo(cloudSystemInfo);
        schemaChangeHandler = (SchemaChangeHandler) new Alter().getSchemaChangeHandler();

        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CatalogTestUtil.createDupTable(db);
        olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        DataSortInfo dataSortInfo = new DataSortInfo();
        dataSortInfo.setSortType(TSortType.LEXICAL);
        olapTable.setDataSortInfo(dataSortInfo);

        // Set inverted index file storage format to V2 for cloud mode
        olapTable.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);

        String indexName = "tokenized_inverted_index";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("parser", "english");
        properties.put("support_phrase", "true");
        properties.put("lower_case", "true");

        // Use VARCHAR column v1 (index 2) for string type support
        IndexDef indexDef = new IndexDef(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(2).getName()),
                IndexType.INVERTED,
                properties, "tokenized inverted index with english parser");
        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

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

        Assert.assertEquals(1, olapTable.getIndexes().size());
        Assert.assertEquals("tokenized_inverted_index", olapTable.getIndexes().get(0).getIndexName());

        // Verify that the index has the correct properties
        Assert.assertEquals("english", olapTable.getIndexes().get(0).getProperties().get("parser"));
        Assert.assertEquals("true", olapTable.getIndexes().get(0).getProperties().get("support_phrase"));
        Assert.assertEquals("true", olapTable.getIndexes().get(0).getProperties().get("lower_case"));
    }
}
