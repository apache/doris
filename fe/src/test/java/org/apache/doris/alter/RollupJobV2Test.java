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

import mockit.Mock;
import mockit.MockUp;
import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RollupJobV2Test {
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Catalog masterCatalog;
    private static Catalog slaveCatalog;

    private static String transactionSource = "localfe";
    private static Analyzer analyzer;
    private static AddRollupClause clause;
    private static AddRollupClause clause2;

    private FakeCatalog fakeCatalog;
    private FakeEditLog fakeEditLog;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, AnalysisException {
        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterCatalog = CatalogTestUtil.createTestCatalog();
        slaveCatalog = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_61);
        metaContext.setThreadLocalInfo();
        // masterCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
        // slaveCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
        masterTransMgr = masterCatalog.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterCatalog.getEditLog());

        slaveTransMgr = slaveCatalog.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveCatalog.getEditLog());
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        clause = new AddRollupClause(CatalogTestUtil.testRollupIndex2, Lists.newArrayList("k1", "v"), null,
                CatalogTestUtil.testIndex1, null);
        clause.analyze(analyzer);

        clause2 = new AddRollupClause(CatalogTestUtil.testRollupIndex3, Lists.newArrayList("k1", "v"), null,
                CatalogTestUtil.testIndex1, null);
        clause2.analyze(analyzer);

        FeConstants.runningUnitTest = true;
        AgentTaskQueue.clearAllTasks();

        new MockUp<Catalog>() {
            @Mock
            public Catalog getInstance() {
                return masterCatalog;
            }
            @Mock
            public Catalog getCurrentCatalog() {
                return masterCatalog;
            }
        };
    }

    @Test
    public void testRunRollupJobConcurrentLimit() throws UserException {
        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();
        FakeCatalog.setCatalog(masterCatalog);
        MaterializedViewHandler materializedViewHandler = Catalog.getInstance().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        alterClauses.add(clause2);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        materializedViewHandler.process(alterClauses, db.getClusterName(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();

        materializedViewHandler.runAfterCatalogReady();

        Assert.assertEquals(Config.max_running_rollup_job_num_per_table, materializedViewHandler.getTableRunningJobMap().get(CatalogTestUtil.testTableId1).size());
        Assert.assertEquals(2, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    @Test
    public void testAddSchemaChange() throws UserException {
        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();
        FakeCatalog.setCatalog(masterCatalog);
        MaterializedViewHandler materializedViewHandler = Catalog.getInstance().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        materializedViewHandler.process(alterClauses, db.getClusterName(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();
        FakeCatalog.setCatalog(masterCatalog);
        MaterializedViewHandler materializedViewHandler = Catalog.getInstance().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db.getClusterName(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersionInfo(testPartition.getVisibleVersion(),
                        testPartition.getVisibleVersionHash(), shadowReplica.getDataSize(),
                        shadowReplica.getRowCount());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());

        /*
        Assert.assertEquals(CatalogTestUtil.testIndexId1, rollupJob.getBaseIndexId());
        Assert.assertEquals(CatalogTestUtil.testRollupIndex2, rollupJob.getRollupIndexName());
        MaterializedIndex rollupIndex = rollupJob.getRollupIndex(CatalogTestUtil.testPartitionId1);
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(IndexState.ROLLUP, rollupIndex.getState());
        assertEquals(IndexState.NORMAL, baseIndex.getState());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());
        assertEquals(PartitionState.ROLLUP, testPartition.getState());
        Tablet rollupTablet = rollupIndex.getTablets().get(0);
        List<Replica> replicas = rollupTablet.getReplicas();
        Replica rollupReplica1 = replicas.get(0);
        Replica rollupReplica2 = replicas.get(1);
        Replica rollupReplica3 = replicas.get(2);
        
        assertEquals(-1, rollupReplica1.getVersion());
        assertEquals(-1, rollupReplica2.getVersion());
        assertEquals(-1, rollupReplica3.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica1.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica3.getLastFailedVersion());
        assertEquals(-1, rollupReplica1.getLastSuccessVersion());
        assertEquals(-1, rollupReplica2.getLastSuccessVersion());
        assertEquals(-1, rollupReplica3.getLastSuccessVersion());
        
        // rollup handler run one cycle, agent task is generated and send tasks
        rollupHandler.runOneCycle();
        AgentTask task1 = AgentTaskQueue.getTask(rollupReplica1.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
        AgentTask task2 = AgentTaskQueue.getTask(rollupReplica2.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
        AgentTask task3 = AgentTaskQueue.getTask(rollupReplica3.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
        
        // be report finishe rollup success
        TTabletInfo tTabletInfo = new TTabletInfo(rollupTablet.getId(), CatalogTestUtil.testSchemaHash1,
                CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersionHash, 0, 0);
        rollupHandler.handleFinishedReplica(task1, tTabletInfo, -1);
        rollupHandler.handleFinishedReplica(task2, tTabletInfo, -1);
        rollupHandler.handleFinishedReplica(task3, tTabletInfo, -1);
        
        // rollup hander run one cycle again, the rollup job is finishing
        rollupHandler.runOneCycle();
        Assert.assertEquals(JobState.FINISHING, rollupJob.getState());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica3.getVersion());
        assertEquals(-1, rollupReplica1.getLastFailedVersion());
        assertEquals(-1, rollupReplica2.getLastFailedVersion());
        assertEquals(-1, rollupReplica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, rollupReplica1.getLastSuccessVersion());
        */
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();
        FakeCatalog.setCatalog(masterCatalog);
        MaterializedViewHandler materializedViewHandler = Catalog.getInstance().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db.getClusterName(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(MaterializedIndex.IndexState.NORMAL, baseIndex.getState());
        assertEquals(Partition.PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());

        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica1 = replicas.get(0);
        Replica replica2 = replicas.get(1);
        Replica replica3 = replicas.get(2);

        assertEquals(CatalogTestUtil.testStartVersion, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(-1, replica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.PENDING, rollupJob.getJobState());

        // table is stable, runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersionInfo(testPartition.getVisibleVersion(),
                        testPartition.getVisibleVersionHash(), shadowReplica.getDataSize(),
                        shadowReplica.getRowCount());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());
    }
}
