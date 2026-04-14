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

import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.CancelAlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AlterOp;
import org.apache.doris.nereids.trees.plans.commands.info.BuildIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexChangeJobTest {

    private static String fileName = "./IndexChangeJobTest";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Env masterEnv;
    private static Env slaveEnv;

    private static ConnectContext ctx;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, UserException {
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterEnv = CatalogTestUtil.createTestCatalog();
        slaveEnv = CatalogTestUtil.createTestCatalog();
        masterTransMgr = (GlobalTransactionMgr) masterEnv.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterEnv.getEditLog());
        slaveTransMgr = (GlobalTransactionMgr) slaveEnv.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveEnv.getEditLog());
        // Initialize ConnectContext
        ctx = new ConnectContext();
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);
        mockedConnectContext.when(ConnectContext::get).thenReturn(ctx);
        FeConstants.runningUnitTest = true;
        AgentTaskQueue.clearAllTasks();
    }

    @After
    public void tearDown() {
        if (mockedConnectContext != null) {
            mockedConnectContext.close();
        }
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        if (fakeTransactionIDGenerator != null) {
            fakeTransactionIDGenerator.close();
        }
    }

    @Test
    public void testCreateIndexIndexChange() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        createIndexOp.validate(new ConnectContext());
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(0, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
    }

    @Test
    public void testBuildIndexIndexChange() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableNameInfo = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexClause = new CreateIndexOp(tableNameInfo, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexClause.validate(connectContext);
        alterOps.add(createIndexClause);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        BuildIndexOp buildIndexClause = new BuildIndexOp(tableNameInfo, indexName, null, false);
        buildIndexClause.validate(connectContext);
        alterOps.add(buildIndexClause);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
    }

    @Test
    public void testDropIndexIndexChange() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        DropIndexOp dropIndexOp = new DropIndexOp(indexName, false, tableName, false);
        dropIndexOp.validate(connectContext);
        alterOps.add(dropIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        Assert.assertEquals(olapTable.getIndexes().size(), 0);
    }

    @Test
    // start a build index job, then normally finish it
    public void testBuildIndexIndexChangeNormal() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableNameInfo = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableNameInfo, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        BuildIndexOp buildIndexOp = new BuildIndexOp(tableNameInfo, indexName, null, false);
        buildIndexOp.validate(new ConnectContext());
        alterOps.add(buildIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 0);

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 3);
        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        // finish alter tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.FINISHED, indexChangejob.getJobState());
    }

    @Test
    // start a drop index job, then normally finish it
    public void testDropIndexIndexChangeNormal() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        DropIndexOp dropIndexOp = new DropIndexOp(indexName, false, tableName, false);
        dropIndexOp.validate(connectContext);
        alterOps.add(dropIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 0);

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 3);
        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        // finish alter tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.FINISHED, indexChangejob.getJobState());
    }

    @Test
    public void testCancelBuildIndexIndexChangeNormal() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        BuildIndexOp buildIndexOp = new BuildIndexOp(tableName, indexName, null, false);
        buildIndexOp.validate(connectContext);
        alterOps.add(buildIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 0);

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 3);
        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
    }

    @Test
    public void testBuildIndexIndexChangeWhileTableNotStable() throws  Exception {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Table[testTable1]'s state(SCHEMA_CHANGE) is not NORMAL. Do not allow doing ALTER ops");
        schemaChangeHandler.process(alterOps, db, olapTable);

        olapTable.setState(OlapTableState.NORMAL);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        BuildIndexOp buildIndexOp = new BuildIndexOp(tableName, indexName, null, false);
        buildIndexOp.validate(connectContext);
        alterOps.add(buildIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 0);

        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        Assert.assertEquals(IndexState.NORMAL, baseIndex.getState());
        Assert.assertEquals(PartitionState.NORMAL, testPartition.getState());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica2 = replicas.get(1);

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job, set replica2 to clone
        replica2.setState(Replica.ReplicaState.CLONE);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());

        // rerun waiting txn job, set replica2 to normal
        replica2.setState(Replica.ReplicaState.NORMAL);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 3);

        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        // finish alter tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.FINISHED, indexChangejob.getJobState());
    }

    @Test
    public void testDropIndexIndexChangeWhileTableNotStable() throws  Exception {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Table[testTable1]'s state(SCHEMA_CHANGE) is not NORMAL. Do not allow doing ALTER ops");
        schemaChangeHandler.process(alterOps, db, olapTable);

        olapTable.setState(OlapTableState.NORMAL);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        DropIndexOp dropIndexOp = new DropIndexOp(indexName, false, tableName, false);
        dropIndexOp.validate(connectContext);
        alterOps.add(dropIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 0);

        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        Assert.assertEquals(IndexState.NORMAL, baseIndex.getState());
        Assert.assertEquals(PartitionState.NORMAL, testPartition.getState());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica2 = replicas.get(1);

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job, set replica2 to clone
        replica2.setState(Replica.ReplicaState.CLONE);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());

        // rerun waiting txn job, set replica2 to normal
        replica2.setState(Replica.ReplicaState.NORMAL);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 3);

        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        // finish alter tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.FINISHED, indexChangejob.getJobState());
    }

    @Test
    public void testBuildIndexFailedWithMinFailedNum() throws Exception {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        BuildIndexOp buildIndexOp = new BuildIndexOp(tableName, indexName, null, false);
        buildIndexOp.validate(connectContext);
        alterOps.add(buildIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 0);

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(indexChangejob.invertedIndexBatchTask.getTaskNum(), 3);
        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(3, tasks.size());

        // if one task failed, the job should be failed
        // if task error is not OBTAIN_LOCK_FAILED, the job should be failed after
        // MIN_FAILED_NUM = 3 times
        AgentTask agentTask = tasks.get(0);
        agentTask.setErrorCode(TStatusCode.IO_ERROR);
        Assert.assertEquals(agentTask.getFailedTimes(), 0);
        for (int i = 0; i < IndexChangeJob.MIN_FAILED_NUM; i++) {
            agentTask.failed();
            schemaChangeHandler.runAfterCatalogReady();
            if (i < IndexChangeJob.MIN_FAILED_NUM - 1) {
                Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
            }
        }
        Assert.assertEquals(IndexChangeJob.JobState.CANCELLED, indexChangejob.getJobState());
    }

    @Test
    public void testBuildIndexFailedWithMaxFailedNum() throws Exception {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterOps.clear();
        BuildIndexOp buildIndexOp = new BuildIndexOp(tableName, indexName, null, false);
        buildIndexOp.validate(connectContext);
        alterOps.add(buildIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());

        IndexChangeJob indexChangejob = indexChangeJobMap.values().stream().findAny().get();
        Assert.assertEquals(0, indexChangejob.invertedIndexBatchTask.getTaskNum());

        Assert.assertEquals(IndexChangeJob.JobState.WAITING_TXN, indexChangejob.getJobState());
        // run waiting txn job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
        Assert.assertEquals(3, indexChangejob.invertedIndexBatchTask.getTaskNum());
        // run running job
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(3, tasks.size());

        // if one task failed, the job should be failed
        // if task error is OBTAIN_LOCK_FAILED, the job should be failed after
        // MAX_FAILED_NUM = 10 times
        AgentTask agentTask = tasks.get(0);
        agentTask.setErrorCode(TStatusCode.OBTAIN_LOCK_FAILED);
        Assert.assertEquals(agentTask.getFailedTimes(), 0);
        for (int i = 0; i < IndexChangeJob.MAX_FAILED_NUM; i++) {
            agentTask.failed();
            schemaChangeHandler.runAfterCatalogReady();
            if (i < IndexChangeJob.MAX_FAILED_NUM - 1) {
                Assert.assertEquals(IndexChangeJob.JobState.RUNNING, indexChangejob.getJobState());
            }
        }
        Assert.assertEquals(IndexChangeJob.JobState.CANCELLED, indexChangejob.getJobState());
    }

    @Test
    public void testNgramBfBuildIndex() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        Database db = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);
        masterEnv.unprotectCreateDb(db);
        CatalogTestUtil.createDupTable(db);
        OlapTable table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        String indexName = "ngram_bf_index";
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                "NGRAM_BF",
                Maps.newHashMap(), "ngram bf index");
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        alterOps.add(createIndexOp);

        // Test with enable_add_index_for_new_data = true
        ConnectContext context = ConnectContext.get();
        context.getSessionVariable().setEnableAddIndexForNewData(true);
        schemaChangeHandler.process(alterOps, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("ngram_bf_index", table.getIndexes().get(0).getIndexName());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
                .findFirst()
                .orElse(null);
        Assert.assertEquals(0, jobV2.schemaChangeBatchTask.getTaskNum());
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, jobV2.getJobState());

        // Clean up for next test
        table.setIndexes(Lists.newArrayList());
        indexChangeJobMap.clear();
        AgentTaskQueue.clearAllTasks();

        // Test with enable_add_index_for_new_data = false
        context.getSessionVariable().setEnableAddIndexForNewData(false);
        String indexName2 = "ngram_bf_index2";
        IndexDefinition indexDefinition2 = new IndexDefinition(indexName2, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                "NGRAM_BF",
                Maps.newHashMap(), "ngram bf index2");
        createIndexOp = new CreateIndexOp(tableName, indexDefinition2, false);
        createIndexOp.validate(connectContext);
        ArrayList<AlterOp> alterOps2 = new ArrayList<>();
        alterOps2.add(createIndexOp);
        schemaChangeHandler.process(alterOps2, db, table);
        indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
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

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(1, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, jobV2.getJobState());
        Assert.assertEquals(1, table.getIndexes().size());
        Assert.assertEquals("ngram_bf_index2", table.getIndexes().get(0).getIndexName());
    }

    @Test
    public void testCancelNgramBfBuildIndex() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        Database db = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);
        masterEnv.unprotectCreateDb(db);
        CatalogTestUtil.createDupTable(db);
        OlapTable table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);
        String indexName = "ngram_bf_index";
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(table.getBaseSchema().get(3).getName()),
                "NGRAM_BF",
                Maps.newHashMap(), "ngram bf index");
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                table.getName());
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        alterOps.add(createIndexOp);

        //cancel test can only with enable_add_index_for_new_data = false
        ctx.getSessionVariable().setEnableAddIndexForNewData(false);
        schemaChangeHandler.process(alterOps, db, table);
        Map<Long, AlterJobV2> indexChangeJobMap = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, table.getState());

        SchemaChangeJobV2 jobV2 = (SchemaChangeJobV2) indexChangeJobMap.values().stream()
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

        TableNameInfo tableNameInfo = new TableNameInfo(db.getName(), table.getName());
        CancelAlterTableCommand cancelAlterTableCommand = new CancelAlterTableCommand(
                tableNameInfo,
                CancelAlterTableCommand.AlterType.COLUMN,
                Lists.newArrayList());
        schemaChangeHandler.cancel(cancelAlterTableCommand);

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, jobV2.getJobState());
    }

    @Test
    public void testDropIndexOnPartitionValidateRejectsStarPartition() throws Exception {
        // PARTITIONS (*) should be rejected: partitionNames is null
        PartitionNamesInfo starPartition = new PartitionNamesInfo(true);
        DropIndexOp dropIndexOp = new DropIndexOp("index1", false, null, true, starPartition);
        try {
            dropIndexOp.validate(new ConnectContext());
            Assert.fail("Should throw AnalysisException for PARTITIONS (*)");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("PARTITIONS (*) is not supported"));
        }
    }

    @Test
    public void testDropIndexOnPartitionValidateRejectsTempPartition() throws Exception {
        // TEMPORARY PARTITION should be rejected
        PartitionNamesInfo tempPartition = new PartitionNamesInfo(true, Lists.newArrayList("p1"));
        DropIndexOp dropIndexOp = new DropIndexOp("index1", false, null, true, tempPartition);
        try {
            dropIndexOp.validate(new ConnectContext());
            Assert.fail("Should throw AnalysisException for TEMPORARY PARTITION");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("does not support temporary partitions"));
        }
    }

    @Test
    public void testDropIndexOnPartitionValidateAcceptsNormalPartition() throws Exception {
        // Normal partition spec should pass validate
        PartitionNamesInfo normalPartition = new PartitionNamesInfo(false, Lists.newArrayList("p1", "p2"));
        DropIndexOp dropIndexOp = new DropIndexOp("index1", false, null, true, normalPartition);
        // Should not throw
        dropIndexOp.validate(new ConnectContext());
        Assert.assertTrue(dropIndexOp.hasPartitionSpec());
        Assert.assertEquals(2, dropIndexOp.getPartitionNames().size());
    }

    @Test
    public void testDropIndexOnPartitionRejectsNonPartitionedTable() throws UserException {
        // testTable1 uses SinglePartitionInfo (non-partitioned), so DROP INDEX ON PARTITION should fail
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        String indexName = "index1";
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());
        // First create an inverted index
        IndexDefinition indexDefinition = new IndexDefinition(indexName, false,
                Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()),
                "INVERTED",
                Maps.newHashMap(), "balabala");
        CreateIndexOp createIndexOp = new CreateIndexOp(tableName, indexDefinition, false);
        ConnectContext connectContext = new ConnectContext();
        createIndexOp.validate(connectContext);
        alterOps.add(createIndexOp);
        schemaChangeHandler.process(alterOps, db, olapTable);
        Assert.assertEquals(1, olapTable.getIndexes().size());
        alterOps.clear();

        // Now try DROP INDEX ON PARTITION on this non-partitioned table
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, Lists.newArrayList("p1"));
        DropIndexOp dropIndexOp = new DropIndexOp(indexName, false, tableName, false, partitionNamesInfo);
        dropIndexOp.validate(connectContext);
        alterOps.add(dropIndexOp);
        try {
            schemaChangeHandler.process(alterOps, db, olapTable);
            Assert.fail("Should throw DdlException for non-partitioned table");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("is not partitioned"));
        }
        // Index definition should still exist
        Assert.assertEquals(1, olapTable.getIndexes().size());
    }

    @Test
    public void testDropIndexOnPartitionRejectsNonExistentIndex() throws UserException {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());

        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, Lists.newArrayList("p1"));
        DropIndexOp dropIndexOp = new DropIndexOp("non_existent_index", false, tableName, false, partitionNamesInfo);
        dropIndexOp.validate(new ConnectContext());
        alterOps.add(dropIndexOp);
        try {
            schemaChangeHandler.process(alterOps, db, olapTable);
            Assert.fail("Should throw DdlException for non-existent index");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testDropIndexOnPartitionIfExistsNonExistentIndex() throws UserException {
        // IF EXISTS with non-existent index and partition spec should silently succeed
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        fakeEnv = new FakeEnv();
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterOp> alterOps = new ArrayList<>();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        TableNameInfo tableName = new TableNameInfo(masterEnv.getInternalCatalog().getName(), db.getName(),
                olapTable.getName());

        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, Lists.newArrayList("p1"));
        DropIndexOp dropIndexOp = new DropIndexOp("non_existent_index", true, tableName, false, partitionNamesInfo);
        dropIndexOp.validate(new ConnectContext());
        alterOps.add(dropIndexOp);
        // Should not throw — IF EXISTS silently returns
        schemaChangeHandler.process(alterOps, db, olapTable);
    }
}
