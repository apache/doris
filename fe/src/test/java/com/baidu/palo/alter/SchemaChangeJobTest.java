package com.baidu.palo.alter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.palo.alter.AlterJob.JobState;
import com.baidu.palo.analysis.AccessTestUtil;
import com.baidu.palo.analysis.AddColumnClause;
import com.baidu.palo.analysis.AlterClause;
import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.ColumnPosition;
import com.baidu.palo.catalog.AggregateType;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.CatalogTestUtil;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.FakeCatalog;
import com.baidu.palo.catalog.FakeEditLog;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PrimitiveType;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Partition.PartitionState;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.thrift.TTabletInfo;
import com.baidu.palo.thrift.TTaskType;
import com.baidu.palo.transaction.FakeTransactionIDGenerator;
import com.baidu.palo.transaction.GlobalTransactionMgr;
import com.baidu.palo.transaction.TabletCommitInfo;
import com.baidu.palo.transaction.TransactionState;
import com.baidu.palo.transaction.TransactionStatus;
import com.baidu.palo.transaction.TransactionState.LoadJobSourceType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
    private static Column newCol = new Column("add_v", new ColumnType(PrimitiveType.INT), false, AggregateType.MAX,
            false, "1", "");
    private static AddColumnClause addColumnClause = new AddColumnClause(newCol, new ColumnPosition("v"), null, null);

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, AnalysisException {
        fakeEditLog = new FakeEditLog();
        fakeCatalog = new FakeCatalog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterCatalog = CatalogTestUtil.createTestCatalog();
        slaveCatalog = CatalogTestUtil.createTestCatalog();
        masterCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
        slaveCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
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
                LoadJobSourceType.FRONTEND);
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
        assertEquals(ReplicaState.NORMAL, replica2.getState());
        assertEquals(ReplicaState.SCHEMA_CHANGE, replica3.getState());

        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica2.getLastFailedVersion());
        assertEquals(-1, replica3.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastSuccessVersion());

        // schemachange handler run one cycle, agent task is generated and send tasks
        schemaChangeHandler.runOneCycle();
        AgentTask task1 = AgentTaskQueue.getTask(replica1.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        AgentTask task2 = AgentTaskQueue.getTask(replica2.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        AgentTask task3 = AgentTaskQueue.getTask(replica3.getBackendId(), TTaskType.SCHEMA_CHANGE, baseTablet.getId());
        assertNull(task2);

        // be report finish schema change success, report the new schema hash
        TTabletInfo tTabletInfo = new TTabletInfo(baseTablet.getId(),
                schemaChangeJob.getSchemaHashByIndexId(CatalogTestUtil.testIndexId1), CatalogTestUtil.testStartVersion,
                CatalogTestUtil.testStartVersionHash, 0, 0);
        schemaChangeHandler.handleFinishedReplica(task1, tTabletInfo, -1);
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
