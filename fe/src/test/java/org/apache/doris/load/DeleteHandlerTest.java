package org.apache.doris.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.DeleteJob;
import org.apache.doris.task.DeleteJob.DeleteState;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteHandlerTest {

    private DeleteHandler deleteHandler;

    private static final long BACKEND_ID_1 = 10000L;
    private static final long BACKEND_ID_2 = 10001L;
    private static final long BACKEND_ID_3 = 10002L;
    private static final long REPLICA_ID_1 = 70000L;
    private static final long REPLICA_ID_2 = 70001L;
    private static final long REPLICA_ID_3 = 70002L;
    private static final long TABLET_ID = 60000L;

    @Mocked
    private Catalog catalog;
    @Mocked
    private EditLog editLog;
    @Mocked
    private AgentTaskQueue agentTaskQueue;
    @Mocked
    private AgentTaskExecutor executor;
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;

    private Database db;
    private PaloAuth auth;

    Analyzer analyzer;

    private TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;

        deleteHandler = new DeleteHandler();
        auth = AccessTestUtil.fetchAdminAccess();
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            boolean commitAndPublishTransaction(Database db, long transactionId,
                                                List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
                    throws UserException {
                return true;
            }
            @Mock
            public void abortTransaction(long transactionId, String reason) throws UserException {
                return;
            }
        };

        new Expectations(invertedIndex) {
            {
                invertedIndex.getTabletIdByReplica(anyLong);
                minTimes = 0;
                result = TABLET_ID;
            }
        };

        new Expectations() {
            {
                catalog.getDb(anyString);
                minTimes = 0;
                result = db;

                catalog.getDb(anyLong);
                minTimes = 0;
                result = db;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                catalog.getNextId();
                minTimes = 0;
                result = 10L;
            }
        };

        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                Catalog.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = globalTransactionMgr;

                AgentTaskExecutor.submit((AgentBatchTask) any);
                minTimes = 0;

                AgentTaskQueue.addTask((AgentTask) any);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test(expected = DdlException.class)
    public void testUnQuorumTimeout() throws DdlException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        deleteHandler.process(deleteStmt);
    }

    @Test
    public void testQuorumTimeout() throws DdlException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<DeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long transactionId) {
                TransactionState transactionState =  new TransactionState();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                return transactionState;
            }
        };

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        deleteHandler.process(deleteStmt);

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.QUORUM_FINISHED);
        }
    }

    @Test
    public void testNormal() throws DdlException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(TABLET_ID);
        tabletDeleteInfo.getFinishedReplicas().addAll(finishedReplica);

        new MockUp<DeleteJob>() {
            @Mock
            public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
                return Lists.newArrayList(tabletDeleteInfo);
            }
        };

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long transactionId) {
                TransactionState transactionState =  new TransactionState();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                return transactionState;
            }
        };

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        deleteHandler.process(deleteStmt);

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }
}
