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

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.DeleteJob.DeleteState;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryStateException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DeleteHandlerTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

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
    private Env env;
    @Mocked
    private EditLog editLog;
    @Mocked
    private AgentTaskQueue agentTaskQueue;
    @Mocked
    private AgentTaskExecutor executor;
    @Mocked
    private SystemInfoService systemInfoService;

    private Database db;
    private Auth auth;
    private AccessControllerManager accessManager;

    Analyzer analyzer;

    private GlobalTransactionMgr globalTransactionMgr;
    private TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
    private ConnectContext connectContext = new ConnectContext();

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;

        globalTransactionMgr = new GlobalTransactionMgr(env);
        globalTransactionMgr.setEditLog(editLog);
        deleteHandler = new DeleteHandler();
        accessManager = AccessTestUtil.fetchAdminAccess();
        auth = accessManager.getAuth();
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        db = CatalogMocker.mockDb();
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

        InternalCatalog catalog = Deencapsulation.newInstance(InternalCatalog.class);
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                env.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = db;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = db;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                env.getAuth();
                minTimes = 0;
                result = auth;

                env.getNextId();
                minTimes = 0;
                result = 10L;

                env.getTabletInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                env.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.getAllBackendIds(false);
                minTimes = 0;
                result = Lists.newArrayList(1L);
            }
        };
        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                Env.getCurrentGlobalTransactionMgr();
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

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        new Expectations(globalTransactionMgr) {
            {
                try {
                    globalTransactionMgr.abortTransaction(db.getId(), anyLong, anyString);
                } catch (UserException e) {
                    // CHECKSTYLE IGNORE THIS LINE
                }
                minTimes = 0;
            }
        };
        QueryState state = connectContext.getState();
        deleteHandler.process(deleteStmt, state);
        Assert.assertSame(state.getStateType(), QueryState.MysqlStateType.ERR);
        Assert.fail();
    }

    @Test
    public void testQuorumTimeout() throws DdlException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PARTITION_ID, TABLET_ID);
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

        QueryState state = connectContext.getState();
        deleteHandler.process(deleteStmt, state);
        Assert.assertSame(state.getStateType(), QueryState.MysqlStateType.OK);

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.QUORUM_FINISHED);
        }
    }

    @Test
    public void testNormalTimeout() throws DdlException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PARTITION_ID, TABLET_ID);
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

        QueryState state = connectContext.getState();
        deleteHandler.process(deleteStmt, state);
        Assert.assertSame(state.getStateType(), QueryState.MysqlStateType.OK);

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

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PARTITION_ID, TABLET_ID);
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
                    // CHECKSTYLE IGNORE THIS LINE
                }
                result = false;
            }
        };

        new Expectations(globalTransactionMgr) {
            {
                try {
                    globalTransactionMgr.commitTransaction(anyLong, (List<Table>) any, anyLong, (List<TabletCommitInfo>) any, (TxnCommitAttachment) any);
                } catch (UserException e) {
                    // CHECKSTYLE IGNORE THIS LINE
                }
                result = new UserException("commit fail");
            }
        };

        deleteHandler.process(deleteStmt, connectContext.getState());
        Assert.assertSame(connectContext.getState().getStateType(), QueryState.MysqlStateType.ERR);
    }

    @Test
    public void testPublishFail(@Mocked MarkedCountDownLatch countDownLatch, @Mocked AgentTaskExecutor taskExecutor) throws DdlException, QueryStateException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PARTITION_ID, TABLET_ID);
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
                    // CHECKSTYLE IGNORE THIS LINE
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
        QueryState state = connectContext.getState();
        deleteHandler.process(deleteStmt, state);
        Assert.assertSame(state.getStateType(), QueryState.MysqlStateType.OK);

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

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(internalCtl, "test_db", "test_tbl"),
                new PartitionNames(false, Lists.newArrayList("test_tbl")), binaryPredicate);

        Set<Replica> finishedReplica = Sets.newHashSet();
        finishedReplica.add(new Replica(REPLICA_ID_1, BACKEND_ID_1, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_2, BACKEND_ID_2, 0, Replica.ReplicaState.NORMAL));
        finishedReplica.add(new Replica(REPLICA_ID_3, BACKEND_ID_3, 0, Replica.ReplicaState.NORMAL));
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo(PARTITION_ID, TABLET_ID);
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
                    // CHECKSTYLE IGNORE THIS LINE
                }
                result = false;
            }
        };

        QueryState state = connectContext.getState();
        deleteHandler.process(deleteStmt, state);
        Assert.assertSame(state.getStateType(), QueryState.MysqlStateType.OK);

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(1, jobs.size());
        for (DeleteJob job : jobs) {
            Assert.assertEquals(job.getState(), DeleteState.FINISHED);
        }
    }
}
