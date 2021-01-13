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
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryStateException;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.load.DeleteJob.DeleteState;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DeleteHandlerTest {

    private DeleteHandler deleteHandler;

    private static final long BACKEND_ID_1 = 10000L;
    private static final long BACKEND_ID_2 = 10001L;
    private static final long BACKEND_ID_3 = 10002L;
    private static final long REPLICA_ID_1 = 70000L;
    private static final long REPLICA_ID_2 = 70001L;
    private static final long REPLICA_ID_3 = 70002L;
    private static final long TABLET_ID = 60000L;
    private static final long PARTITION_ID = 40000L;
    private static final long TBL_ID = 30000L;
    private static final long DB_ID = 20000L;

    @Mocked
    private Catalog catalog;
    @Mocked
    private EditLog editLog;
    @Mocked
    private AgentTaskQueue agentTaskQueue;
    @Mocked
    private AgentTaskExecutor executor;

    private Database db;
    private PaloAuth auth;

    Analyzer analyzer;

    private GlobalTransactionMgr globalTransactionMgr;
    private TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
    private ConnectContext connectContext = new ConnectContext();

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;

        globalTransactionMgr = new GlobalTransactionMgr(catalog);
        globalTransactionMgr.setEditLog(editLog);
        deleteHandler = new DeleteHandler();
        auth = AccessTestUtil.fetchAdminAccess();
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TBL_ID, PARTITION_ID, TBL_ID, 0, null);
        invertedIndex.addTablet(TABLET_ID, tabletMeta);
        invertedIndex.addReplica(TABLET_ID, new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        invertedIndex.addReplica(TABLET_ID, new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        invertedIndex.addReplica(TABLET_ID, new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));

        new MockUp<EditLog>() {
            @Mock
            public void logSaveTransactionId(long transactionId) {
            }
            @Mock
            public void logInsertTransactionState(TransactionState transactionState) {
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

                catalog.getTabletInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());

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
    public void testUnQuorumTimeout() throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName("test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        new Expectations(globalTransactionMgr) {
            {
                try {
                    globalTransactionMgr.abortTransaction(db.getId(), anyLong, anyString);
                } catch (UserException e) {
                }
                minTimes = 0;
            }
        };
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        deleteHandler.process(deleteStmt);
        Assert.fail();
    }

    @Test
    public void testQuorumTimeout() throws DdlException, QueryStateException {
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
        try {
            deleteHandler.process(deleteStmt);
        }catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.QUORUM_FINISHED);
        }
    }

    @Test
    public void testNormalTimeout() throws DdlException, QueryStateException {
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

        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }

    @Test(expected = DdlException.class)
    public void testCommitFail(@Mocked MarkedCountDownLatch countDownLatch) throws DdlException, QueryStateException {
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

        new Expectations() {
            {
                try {
                    countDownLatch.await(anyLong, (TimeUnit) any);
                } catch (InterruptedException e) {
                }
                result = false;
            }
        };

        new Expectations(globalTransactionMgr) {
            {
                try {
                    globalTransactionMgr.commitTransaction(anyLong, (List<Table>) any, anyLong, (List<TabletCommitInfo>) any, (TxnCommitAttachment) any);
                } catch (UserException e) {
                }
                result = new UserException("commit fail");
            }
        };

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (DdlException e) {
            Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
            Collection<DeleteJob> jobs = idToDeleteJob.values();
            Assert.assertEquals(1, jobs.size());
            for (DeleteJob job : jobs) {
                Assert.assertEquals(job.getState(), DeleteState.FINISHED);
            }
            throw e;
        } catch (QueryStateException e) {
        }
        Assert.fail();
    }

    @Test
    public void testPublishFail(@Mocked MarkedCountDownLatch countDownLatch, @Mocked AgentTaskExecutor taskExecutor) throws DdlException, QueryStateException {
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

        new Expectations() {
            {
                try {
                    countDownLatch.await(anyLong, (TimeUnit) any);
                } catch (InterruptedException e) {
                }
                result = false;
            }
        };

        new Expectations() {
            {
                AgentTaskExecutor.submit((AgentBatchTask) any);
                minTimes = 0;
            }
        };

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }

    @Test
    public void testNormal(@Mocked MarkedCountDownLatch countDownLatch) throws DdlException, QueryStateException {
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

        new Expectations() {
            {
                try {
                    countDownLatch.await(anyLong, (TimeUnit) any);
                } catch (InterruptedException e) {
                }
                result = false;
            }
        };

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }
        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }
}
