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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BuildIndexClause;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.DropIndexClause;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.TableName;
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
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    private static Analyzer analyzer;
    private static Database db;
    private static OlapTable olapTable;
    private static CreateIndexClause createIndexClause;
    private static BuildIndexClause buildIndexClause;
    private static DropIndexClause dropIndexClause;
    private static CancelAlterTableStmt cancelAlterTableStmt;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, UserException {
        FeConstants.runningUnitTest = true;
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
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);

        // set mow table property
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, "false");
        TableProperty tableProperty = new TableProperty(properties);
        olapTable.setTableProperty(tableProperty);

        TableName tableName = new TableName(masterEnv.getInternalCatalog().getName(), db.getName(), olapTable.getName());
        IndexDef indexDef = new IndexDef("index1", false, Lists.newArrayList(olapTable.getBaseSchema().get(1).getName()), IndexDef.IndexType.INVERTED, Maps.newHashMap(), "balabala");
        createIndexClause = new CreateIndexClause(tableName, indexDef, false);
        createIndexClause.analyze(analyzer);

        buildIndexClause = new BuildIndexClause(tableName, indexDef, false);
        buildIndexClause.analyze(analyzer);

        dropIndexClause = new DropIndexClause("index1", false, tableName, false);
        dropIndexClause.analyze(analyzer);

        cancelAlterTableStmt = new CancelAlterTableStmt(ShowAlterStmt.AlterType.INDEX, tableName);
        cancelAlterTableStmt.analyze(analyzer);

        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testCreateIndexIndexChange() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(0, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
    }

    @Test
    public void testBuildIndexIndexChange() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
    }

    @Test
    public void testDropIndexIndexChange() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(dropIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, IndexChangeJob> indexChangeJobMap = schemaChangeHandler.getIndexChangeJobs();
        Assert.assertEquals(1, indexChangeJobMap.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
        Assert.assertEquals(olapTable.getIndexes().size(), 0);
    }

    @Test
    // start a build index job, then normally finish it
    public void testBuildIndexIndexChangeNormal() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(dropIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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

        // cancel build index job
        schemaChangeHandler.cancel(cancelAlterTableStmt);

        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER_INVERTED_INDEX);
        Assert.assertEquals(0, tasks.size());
        Assert.assertEquals(IndexChangeJob.JobState.CANCELLED, indexChangejob.getJobState());
    }

    @Test
    public void testBuildIndexIndexChangeWhileTableNotStable() throws  Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Table[testTable1]'s state(SCHEMA_CHANGE) is not NORMAL. Do not allow doing ALTER ops");
        schemaChangeHandler.process(alterClauses, db, olapTable);

        olapTable.setState(OlapTableState.NORMAL);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Table[testTable1]'s state(SCHEMA_CHANGE) is not NORMAL. Do not allow doing ALTER ops");
        schemaChangeHandler.process(alterClauses, db, olapTable);

        olapTable.setState(OlapTableState.NORMAL);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(dropIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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
        // if task error is not OBTAIN_LOCK_FAILED, the job should be failed after MIN_FAILED_NUM = 3 times
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
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(createIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Assert.assertEquals(olapTable.getIndexes().size(), 1);
        Assert.assertEquals(olapTable.getIndexes().get(0).getIndexName(), "index1");
        alterClauses.clear();
        alterClauses.add(buildIndexClause);
        schemaChangeHandler.process(alterClauses, db, olapTable);
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
        // if task error is OBTAIN_LOCK_FAILED, the job should be failed after MAX_FAILED_NUM = 10 times
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
}
