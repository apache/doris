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
import org.apache.doris.analysis.AddColumnClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.AggregateType;
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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ScalarType;
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

public class SchemaChangeJobTest {

    private static FakeEditLog fakeEditLog;
    private static FakeCatalog fakeCatalog;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Catalog masterCatalog;
    private static Catalog slaveCatalog;

    private String transactionSource = "localfe";
    private static Analyzer analyzer;
    private static ColumnDef newCol = new ColumnDef("add_v", new TypeDef(ScalarType.createType(PrimitiveType.INT)), false, AggregateType.MAX,
            false, new DefaultValue(true, "1"), "");
    private static AddColumnClause addColumnClause = new AddColumnClause(newCol, new ColumnPosition("v"), null, null);

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
        addColumnClause.analyze(analyzer);
    }

    @Test
    public void testAddSchemaChange() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(addColumnClause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        schemaChangeHandler.process(alterClauses, "default", db, olapTable);
        SchemaChangeJob schemaChangeJob = (SchemaChangeJob) schemaChangeHandler
                .getAlterJob(CatalogTestUtil.testTableId1);
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();

        // add a schema change job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(addColumnClause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        schemaChangeHandler.process(alterClauses, "default", db, olapTable);
        SchemaChangeJob schemaChangeJob = (SchemaChangeJob) schemaChangeHandler
                .getAlterJob(CatalogTestUtil.testTableId1);
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(IndexState.SCHEMA_CHANGE, baseIndex.getState());
        assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());
        assertEquals(PartitionState.SCHEMA_CHANGE, testPartition.getState());
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

        // schemachange handler run one cycle, agent task is generated and send tasks
        schemaChangeHandler.runOneCycle();
        AgentTask task1 = AgentTaskQueue.getTask(replica1.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        AgentTask task2 = AgentTaskQueue.getTask(replica2.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        AgentTask task3 = AgentTaskQueue.getTask(replica3.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());

        // be report finishe schema change success, report the new schema hash
        TTabletInfo tTabletInfo = new TTabletInfo(baseTablet.getId(),
                schemaChangeJob.getSchemaHashByIndexId(CatalogTestUtil.testIndexId1), CatalogTestUtil.testStartVersion,
                CatalogTestUtil.testStartVersionHash, 0, 0);
        schemaChangeHandler.handleFinishedReplica(task1, tTabletInfo, -1);
        schemaChangeHandler.handleFinishedReplica(task2, tTabletInfo, -1);
        schemaChangeHandler.handleFinishedReplica(task3, tTabletInfo, -1);

        // schema change hander run one cycle again, the rollup job is finishing
        schemaChangeHandler.runOneCycle();
        Assert.assertEquals(JobState.FINISHING, schemaChangeJob.getState());
        assertEquals(CatalogTestUtil.testStartVersion, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(-1, replica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getLastSuccessVersion());
    }

    // load some data and one replica has errors
    // start a schema change and then load data
    // load finished and schema change finished
    @Test
    public void testSchemaChange2() throws Exception {
        FakeCatalog.setCatalog(masterCatalog);
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
        // load one transaction with backend 2 has errors
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
        // errorReplicaIds.add(CatalogTestUtil.testReplicaId2);
        masterTransMgr.finishTransaction(transactionId, errorReplicaIds);
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());

        // start a schema change
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(addColumnClause);
        Database db = masterCatalog.getDb(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        schemaChangeHandler.process(alterClauses, "default", db, olapTable);
        SchemaChangeJob schemaChangeJob = (SchemaChangeJob) schemaChangeHandler
                .getAlterJob(CatalogTestUtil.testTableId1);
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(IndexState.SCHEMA_CHANGE, baseIndex.getState());
        assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());
        assertEquals(PartitionState.SCHEMA_CHANGE, testPartition.getState());
        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica1 = replicas.get(0);
        Replica replica2 = replicas.get(1);
        Replica replica3 = replicas.get(2);
        assertEquals(3, baseTablet.getReplicas().size());

        assertEquals(ReplicaState.SCHEMA_CHANGE, replica1.getState());
        assertEquals(ReplicaState.SCHEMA_CHANGE, replica2.getState());
        assertEquals(ReplicaState.SCHEMA_CHANGE, replica3.getState());

        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(-1, replica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastSuccessVersion());

        // schemachange handler run one cycle, agent task is generated and send tasks
        schemaChangeHandler.runOneCycle();
        AgentTask task1 = AgentTaskQueue.getTask(replica1.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        AgentTask task2 = AgentTaskQueue.getTask(replica2.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        AgentTask task3 = AgentTaskQueue.getTask(replica3.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());

        // be report finish schema change success, report the new schema hash
        TTabletInfo tTabletInfo = new TTabletInfo(baseTablet.getId(),
                schemaChangeJob.getSchemaHashByIndexId(CatalogTestUtil.testIndexId1), CatalogTestUtil.testStartVersion,
                CatalogTestUtil.testStartVersionHash, 0, 0);
        schemaChangeHandler.handleFinishedReplica(task1, tTabletInfo, -1);
        schemaChangeHandler.handleFinishedReplica(task2, tTabletInfo, -1);
        schemaChangeHandler.handleFinishedReplica(task3, tTabletInfo, -1);

        // rollup hander run one cycle again, the rollup job is finishing
        schemaChangeHandler.runOneCycle();
        Assert.assertEquals(JobState.FINISHING, schemaChangeJob.getState());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastSuccessVersion());
    }
}
