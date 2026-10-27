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

package org.apache.doris.qe;

import org.apache.doris.alter.ChangelogColumnIdPreparer;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ChangelogWriteMapping;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TxnWriteSchemaSnapshot;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Exercise the actual VALUES command and stream-load planner on a follower with no local transaction. */
public class TxnValuesWriteSchemaTest extends TestWithFeService {
    private static final String DATABASE = "txn_values_schema";
    private static final String TABLE = "source_table";
    private boolean originalChangelogEnabled;

    @Override
    protected void runBeforeAll() throws Exception {
        originalChangelogEnabled = Config.enable_feature_changelog;
        Config.enable_feature_changelog = true;
        FeConstants.runningUnitTest = true;
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        createTable("create table " + DATABASE + "." + TABLE + " (k int, v int) "
                + "unique key(k) distributed by hash(k) buckets 1 properties("
                + "\"replication_num\"=\"1\", \"enable_unique_key_merge_on_write\"=\"true\","
                + "\"changelog.enable\"=\"true\", \"changelog.need_historical_value\"=\"true\")");
    }

    @Override
    protected void runAfterAll() {
        Config.enable_feature_changelog = originalChangelogEnabled;
    }

    @ParameterizedTest
    @EnumSource(value = FrontendNodeType.class, names = {"FOLLOWER", "OBSERVER"})
    void nonMasterValuesPlansWithMasterSnapshotWithoutLocalTransaction(FrontendNodeType role) throws Exception {
        runFollowerValues(false, false, role);
    }

    @Test
    void rejectedMasterBeginNeverExecutesBackendPlan() throws Exception {
        runFollowerValues(true, false);
    }

    @Test
    void schemaChangeAfterBeginNeverExecutesBackendPlan() throws Exception {
        runFollowerValues(false, true);
    }

    @Test
    void valuesBeginDoesNotWaitForPreparationBehindQueuedWriter() throws Exception {
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DATABASE);
        OlapTable table = (OlapTable) db.getTableOrMetaException(TABLE);
        ReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
        Assertions.assertTrue(lock.isFair());
        GlobalTransactionMgr transactions = new GlobalTransactionMgr(env);
        transactions.setEditLog(env.getEditLog());
        transactions.addDatabaseTransactionMgr(db.getId());
        CountDownLatch readerHeld = new CountDownLatch(1);
        CountDownLatch beginAllowed = new CountDownLatch(1);
        AtomicReference<Thread> writerThread = new AtomicReference<>();
        AtomicReference<Thread> preparerThread = new AtomicReference<>();
        ExecutorService workers = Executors.newFixedThreadPool(3);
        try {
            Future<Long> values = workers.submit(() -> {
                ChangelogColumnIdPreparer.ensureReady(db, table);
                table.readLock();
                try {
                    readerHeld.countDown();
                    Assertions.assertTrue(beginAllowed.await(10, TimeUnit.SECONDS));
                    return transactions.beginTransactionForValues(db.getId(), table.getId(),
                            "values_lock_order_" + UUID.randomUUID(), new TUniqueId(101, 102),
                            new TxnCoordinator(TxnSourceType.FE, 0, "127.0.0.1", 0),
                            LoadJobSourceType.INSERT_STREAMING, 60,
                            TxnWriteSchemaSnapshot.schemaVersions(table));
                } finally {
                    table.readUnlock();
                }
            });
            Assertions.assertTrue(readerHeld.await(10, TimeUnit.SECONDS));
            Future<?> writer = workers.submit(() -> {
                writerThread.set(Thread.currentThread());
                table.writeLock();
                table.writeUnlock();
            });
            awaitQueued(lock, writerThread);
            Future<?> preparation = workers.submit(() -> {
                preparerThread.set(Thread.currentThread());
                ChangelogColumnIdPreparer.ensureReady(db, table);
                return null;
            });
            // Reaching the read-lock queue proves B installed its real IN_FLIGHT future behind C.
            awaitQueued(lock, preparerThread);
            beginAllowed.countDown();
            long txnId = values.get(10, TimeUnit.SECONDS);
            writer.get(10, TimeUnit.SECONDS);
            preparation.get(10, TimeUnit.SECONDS);
            Assertions.assertNotNull(transactions.getTransactionState(db.getId(), txnId)
                    .getWriteSchema(txnId, table.getId()));
        } finally {
            // On the old code, interruption releases A from existing.get(), breaking the cycle.
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static void awaitQueued(ReentrantReadWriteLock lock, AtomicReference<Thread> thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (thread.get() == null || !lock.hasQueuedThread(thread.get())) {
            Assertions.assertTrue(System.nanoTime() < deadline, "Worker did not enter the table lock queue");
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void forwardedValuesPreparesOutsideTableLockBeforeBegin(boolean rejectPreparation) throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DATABASE);
        OlapTable table = (OlapTable) db.getTableOrMetaException(TABLE);
        ReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
        FrontendServiceImpl service = Mockito.spy(new FrontendServiceImpl(ExecuteEnv.getInstance()));
        Mockito.doReturn(true).when(service).checkToken("values-test-token");
        TLoadTxnBeginRequest request = new TLoadTxnBeginRequest()
                .setDb(DATABASE).setTbl(TABLE).setTableId(table.getId())
                .setLabel("values_rpc_prepare_" + UUID.randomUUID()).setToken("values-test-token")
                .setRequestId(new TUniqueId(103, 104)).setTimeout(60)
                .setTxnInsertSchemaVersions(TxnWriteSchemaSnapshot.schemaVersions(table));
        int transactionsBefore = Env.getCurrentGlobalTransactionMgr().getTransactionNum();
        try (MockedStatic<ChangelogColumnIdPreparer> preparer = Mockito.mockStatic(
                ChangelogColumnIdPreparer.class, Mockito.CALLS_REAL_METHODS)) {
            preparer.when(() -> ChangelogColumnIdPreparer.ensureReady(db, table)).thenAnswer(invocation -> {
                Assertions.assertEquals(0, lock.getReadHoldCount());
                Assertions.assertFalse(lock.isWriteLockedByCurrentThread());
                if (rejectPreparation) {
                    throw new UserException("rejected VALUES preparation");
                }
                invocation.callRealMethod();
                return null;
            });
            TLoadTxnBeginResult result = service.loadTxnBegin(request);
            preparer.verify(() -> ChangelogColumnIdPreparer.ensureReady(db, table), Mockito.times(1));
            if (rejectPreparation) {
                Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
                Assertions.assertFalse(result.isSetTxnId());
                Assertions.assertEquals(transactionsBefore, Env.getCurrentGlobalTransactionMgr()
                        .getTransactionNum());
            } else {
                Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode(),
                        result.getStatus().toString());
                Assertions.assertNotNull(Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(db.getId(), result.getTxnId())
                        .getWriteSchema(result.getTxnId(), table.getId()));
            }
        }
    }

    private void runFollowerValues(boolean rejectBegin, boolean changeSchemaAfterBegin) throws Exception {
        runFollowerValues(rejectBegin, changeSchemaAfterBegin, FrontendNodeType.FOLLOWER);
    }

    private void runFollowerValues(boolean rejectBegin, boolean changeSchemaAfterBegin,
            FrontendNodeType role) throws Exception {
        Env env = Env.getCurrentEnv();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DATABASE);
        OlapTable table = (OlapTable) db.getTableOrMetaException(TABLE);
        MaterializedIndexMeta base = table.getIndexMetaByIndexId(table.getBaseIndexId());
        int originalVersion = base.getSchemaVersion();
        FrontendNodeType originalRole = Deencapsulation.getField(env, "feType");
        TransactionEntry previousEntry = connectContext.getTxnEntry();
        GlobalTransactionMgr masterTransactions = new GlobalTransactionMgr(env);
        masterTransactions.setEditLog(env.getEditLog());
        masterTransactions.getTransactionIDGenerator().initTransactionId(900000000L);
        masterTransactions.addDatabaseTransactionMgr(db.getId());
        Map<Long, Integer> expectedVersions = TxnWriteSchemaSnapshot.schemaVersions(table);
        AtomicBoolean beginAcknowledged = new AtomicBoolean();
        AtomicReference<TransactionState> masterState = new AtomicReference<>();
        AtomicReference<TPipelineFragmentParams> executedPlan = new AtomicReference<>();
        AtomicReference<TLoadTxnBeginRequest> beginRequest = new AtomicReference<>();
        BackendServiceProxy backendTransport = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(backendTransport.execPlanFragmentsAsync(Mockito.any(),
                Mockito.any(TPipelineFragmentParamsList.class), Mockito.eq(false))).thenAnswer(invocation -> {
                    Assertions.assertTrue(beginAcknowledged.get(),
                            "BE execution must follow master begin acknowledgement");
                    TPipelineFragmentParamsList params = invocation.getArgument(1);
                    executedPlan.set(params.getParamsList().get(0));
                    return CompletableFuture.completedFuture(InternalService.PExecPlanFragmentResult.newBuilder()
                            .setStatus(Types.PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue())).build());
                });
        String sql = "insert into " + DATABASE + "." + TABLE + " values (1, 2)";
        UUID query = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(query.getMostSignificantBits(), query.getLeastSignificantBits()));
        connectContext.setStatementContext(new StatementContext(connectContext, new OriginStatement(sql, 0)));
        connectContext.setThreadLocalInfo();
        connectContext.getState().reset();
        TransactionEntry entry = new TransactionEntry(new TTxnParams().setNeedTxn(true).setTxnId(-1), db, table);
        entry.setLabel("follower_values_" + query);
        entry.setFirstTxnInsert(true);
        connectContext.setTxnEntry(entry);
        Deencapsulation.setField(env, "feType", role);
        try (MockedStatic<BackendServiceProxy> backendFactory = Mockito.mockStatic(BackendServiceProxy.class);
                MockedConstruction<MasterTxnExecutor> masterTransport = Mockito.mockConstruction(
                        MasterTxnExecutor.class, (mock, context) -> Mockito.when(mock.beginTxn(Mockito.any()))
                                .thenAnswer(invocation -> {
                                    TLoadTxnBeginRequest request = invocation.getArgument(0);
                                    beginRequest.set(request.deepCopy());
                                    Assertions.assertEquals(table.getId(), request.getTableId());
                                    Assertions.assertEquals(expectedVersions, request.getTxnInsertSchemaVersions());
                                    Assertions.assertEquals(connectContext.queryId(), request.getRequestId());
                                    if (rejectBegin) {
                                        throw new TException("master rejected VALUES schema");
                                    }
                                    // Only the RPC transport is replaced: the master still uses its real transaction
                                    // manager and captures the schema itself, in a registry absent on the follower.
                                    Deencapsulation.setField(env, "feType", FrontendNodeType.MASTER);
                                    long txnId;
                                    try {
                                        txnId = masterTransactions.beginTransactionForValues(db.getId(), table.getId(),
                                                request.getLabel(), request.getRequestId(),
                                                new TxnCoordinator(TxnSourceType.FE, 0, "127.0.0.1", 0),
                                                LoadJobSourceType.INSERT_STREAMING, request.getTimeout(),
                                                request.getTxnInsertSchemaVersions());
                                    } finally {
                                        Deencapsulation.setField(env, "feType", role);
                                    }
                                    masterState.set(masterTransactions.getTransactionState(db.getId(), txnId));
                                    Assertions.assertNotNull(masterState.get().getWriteSchema(txnId, table.getId()));
                                    Assertions.assertNull(Env.getCurrentGlobalTransactionMgr()
                                            .getTransactionState(db.getId(), txnId));
                                    if (changeSchemaAfterBegin) {
                                        base.setSchemaVersion(originalVersion + 1);
                                    }
                                    beginAcknowledged.set(true);
                                    return new TLoadTxnBeginResult().setTxnId(txnId).setDbId(db.getId())
                                            .setStatus(new TStatus(TStatusCode.OK));
                                }))) {
            backendFactory.when(BackendServiceProxy::getInstance).thenReturn(backendTransport);
            BatchInsertIntoTableCommand command = Assertions.assertInstanceOf(BatchInsertIntoTableCommand.class,
                    new NereidsParser().parseSingle(sql));
            StmtExecutor executor = new StmtExecutor(connectContext, sql);
            if (rejectBegin || changeSchemaAfterBegin) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> command.run(connectContext, executor));
                Assertions.assertTrue(error.getMessage().contains(rejectBegin
                        ? "master rejected VALUES schema" : "Table schema changed after transaction begin"),
                        error.getMessage());
                Mockito.verifyNoInteractions(backendTransport);
                Assertions.assertNull(executedPlan.get());
            } else {
                command.run(connectContext, executor);
                Assertions.assertNotNull(executedPlan.get());
                TOlapTableSink sink = executedPlan.get().getFragment().getOutputSink().getOlapTableSink();
                TxnWriteSchemaSnapshot snapshot = masterState.get().getWriteSchema(entry.getTxnConf().getTxnId(),
                        table.getId());
                Assertions.assertEquals(expectedVersions, snapshot.getIndexSchemaVersions());
                Map<Long, ChangelogWriteMapping> plannedMappings = new TreeMap<>();
                for (TOlapTableIndexSchema index : sink.getSchema().getIndexes()) {
                    if (index.isSetChangelogColumnMappings()) {
                        plannedMappings.put(index.getId(),
                                ChangelogWriteMapping.fromThrift(index.getChangelogColumnMappings()));
                    }
                }
                Assertions.assertFalse(plannedMappings.isEmpty());
                Assertions.assertEquals(snapshot.getChangelogMappings(), plannedMappings);
                Assertions.assertEquals(1, entry.getRowsInTransaction());
                Assertions.assertNull(Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(db.getId(), entry.getTxnConf().getTxnId()));
            }
            Assertions.assertNotNull(beginRequest.get());
            Assertions.assertEquals(1, masterTransport.constructed().size());
        } finally {
            base.setSchemaVersion(originalVersion);
            Deencapsulation.setField(env, "feType", originalRole);
            connectContext.setTxnEntry(previousEntry);
        }
    }
}
