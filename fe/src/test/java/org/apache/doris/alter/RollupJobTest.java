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

import static org.junit.Assert.assertEquals;

import org.apache.doris.alter.AlterJob.JobState;
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
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import mockit.internal.startup.Startup;

public class RollupJobTest {

    private static FakeEditLog fakeEditLog;
    private static FakeCatalog fakeCatalog;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Catalog masterCatalog;
    private static Catalog slaveCatalog;

    private String transactionSource = "localfe";
    private static Analyzer analyzer;
    private static AddRollupClause clause;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, AnalysisException {
        fakeEditLog = new FakeEditLog();
        fakeCatalog = new FakeCatalog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterCatalog = CatalogTestUtil.createTestCatalog();
        slaveCatalog = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_40);
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
    }

    @Test
    public void testAddRollup() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        rollupHandler.process(alterClauses, db, olapTable, false);
        RollupJob rollupJob = (RollupJob) rollupHandler.getAlterJob(CatalogTestUtil.testTableId1);
        Assert.assertEquals(CatalogTestUtil.testIndexId1, rollupJob.getBaseIndexId());
        Assert.assertEquals(CatalogTestUtil.testRollupIndex2, rollupJob.getRollupIndexName());
    }

    // start a rollup, then finished
    @Test
    public void testRollup1() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        rollupHandler.process(alterClauses, db, olapTable, false);
        RollupJob rollupJob = (RollupJob) rollupHandler.getAlterJob(CatalogTestUtil.testTableId1);
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
    }

    // load some data and one replica has errors
    // start a rollup and then load data
    // load finished and rollup finished
    @Test
    public void testRollup2() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        // load one transaction with backend 2 has errors
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, 
                CatalogTestUtil.testTxnLable1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction, backend 2 has errors
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        // TabletCommitInfo tabletCommitInfo2 = new
        // TabletCommitInfo(CatalogTestUtil.testTabletId1,
        // CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        // transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId, transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(CatalogTestUtil.testReplicaId2);
        masterTransMgr.finishTransaction(transactionId, errorReplicaIds);
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());

        // start a rollup
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        rollupHandler.process(alterClauses, db, olapTable, false);
        RollupJob rollupJob = (RollupJob) rollupHandler.getAlterJob(CatalogTestUtil.testTableId1);
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
        Replica rollupReplica3 = replicas.get(1);
        assertEquals(2, rollupTablet.getReplicas().size());

        assertEquals(-1, rollupReplica1.getVersion());
        assertEquals(-1, rollupReplica3.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica1.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica3.getLastFailedVersion());
        assertEquals(-1, rollupReplica1.getLastSuccessVersion());
        assertEquals(-1, rollupReplica3.getLastSuccessVersion());

        // rollup handler run one cycle, agent task is generated and send tasks
        rollupHandler.runOneCycle();
        AgentTask task1 = AgentTaskQueue.getTask(rollupReplica1.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
        AgentTask task3 = AgentTaskQueue.getTask(rollupReplica3.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());

        // be report finishe rollup success
        TTabletInfo tTabletInfo = new TTabletInfo(rollupTablet.getId(), CatalogTestUtil.testSchemaHash1,
                CatalogTestUtil.testStartVersion + 1, CatalogTestUtil.testPartitionNextVersionHash, 0, 0);
        rollupHandler.handleFinishedReplica(task1, tTabletInfo, -1);
        rollupHandler.handleFinishedReplica(task3, tTabletInfo, -1);

        // rollup hander run one cycle again, the rollup job is finishing
        rollupHandler.runOneCycle();
        Assert.assertEquals(JobState.FINISHING, rollupJob.getState());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica3.getVersion());
        assertEquals(-1, rollupReplica1.getLastFailedVersion());
        assertEquals(-1, rollupReplica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica3.getLastSuccessVersion());
    }

    // start a rollup and then load data
    // but load to rolluping index failed, then rollup is cancelled
    @Test
    public void testRollup3() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        rollupHandler.process(alterClauses, db, olapTable, false);
        RollupJob rollupJob = (RollupJob) rollupHandler.getAlterJob(CatalogTestUtil.testTableId1);
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

        // rollup handler run one cycle, agent task is generated and send tasks
        rollupHandler.runOneCycle();
        AgentTask task1 = AgentTaskQueue.getTask(rollupReplica1.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
        AgentTask task2 = AgentTaskQueue.getTask(rollupReplica2.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());
        AgentTask task3 = AgentTaskQueue.getTask(rollupReplica3.getBackendId(), TTaskType.ROLLUP, rollupTablet.getId());

        // load a transaction, but rollup tablet failed, then the rollup job should be
        // cancelled
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, 
                CatalogTestUtil.testTxnLable1, 
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction, backend 2 has errors
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId, transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(CatalogTestUtil.testReplicaId2);
        masterTransMgr.finishTransaction(transactionId, errorReplicaIds);
        transactionState = fakeEditLog.getTransaction(transactionId);

        // rollup replca's last failed version should change to 13
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica1.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, rollupReplica3.getLastFailedVersion());

        // be report finishe rollup success
        TTabletInfo tTabletInfo = new TTabletInfo(rollupTablet.getId(), CatalogTestUtil.testSchemaHash1,
                CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersionHash, 0, 0);
        rollupHandler.handleFinishedReplica(task1, tTabletInfo, -1);
        rollupHandler.handleFinishedReplica(task2, tTabletInfo, -1);
        rollupHandler.handleFinishedReplica(task3, tTabletInfo, -1);

        // rollup hander run one cycle again, the rollup job is finishing
        rollupHandler.runOneCycle();
        Assert.assertEquals(JobState.CANCELLED, rollupJob.getState());
        assertEquals(1, testPartition.getMaterializedIndices().size());
    }
}
