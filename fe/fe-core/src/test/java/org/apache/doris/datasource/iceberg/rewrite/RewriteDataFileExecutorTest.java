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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache.WritableTableLease;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.scheduler.manager.TransientTaskManager;
import org.apache.doris.transaction.GlobalExternalTransactionInfoMgr;
import org.apache.doris.transaction.Transaction;
import org.apache.doris.transaction.TransactionManager;

import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class RewriteDataFileExecutorTest {

    @Test
    void testCompletedRewriteClearsTransactionRegistries() throws Exception {
        RewriteFixture fixture = new RewriteFixture();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<IcebergUtils> mockedIcebergUtils = Mockito.mockStatic(IcebergUtils.class)) {
            fixture.prepareSuccessfulExecution(mockedEnv, mockedIcebergUtils);

            fixture.executor.executeGroupsConcurrently(Collections.emptyList(), 1L, fixture.lease);

            fixture.assertTransactionRegistriesEmpty();
            InOrder inOrder = Mockito.inOrder(fixture.transaction, fixture.cacheManager);
            inOrder.verify(fixture.transaction).commit();
            inOrder.verify(fixture.cacheManager).invalidateTableCache(fixture.table);
            Mockito.verify(fixture.transaction, Mockito.never()).rollback();
        }
    }

    @Test
    void testPostBeginSetupFailureClearsTransactionRegistries() throws Exception {
        RewriteFixture fixture = new RewriteFixture();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<IcebergUtils> mockedIcebergUtils = Mockito.mockStatic(IcebergUtils.class)) {
            fixture.prepareEnvironment(mockedEnv);
            mockedIcebergUtils.when(() -> IcebergUtils.getSnapshotForWritableLease(fixture.table, fixture.lease))
                    .thenThrow(new IllegalStateException("snapshot setup failed"));

            Assertions.assertThrows(IllegalStateException.class,
                    () -> fixture.executor.executeGroupsConcurrently(
                            Collections.emptyList(), 1L, fixture.lease));

            fixture.assertTransactionRegistriesEmpty();
            Mockito.verify(fixture.transaction).rollback();
            Mockito.verify(fixture.transaction, Mockito.never()).commit();
        }
    }

    @Test
    void testCancelRemovesTableBearingTaskFromManager() throws Exception {
        Env env = Mockito.mock(Env.class);
        TransientTaskManager taskManager = Mockito.mock(TransientTaskManager.class);
        RewriteGroupTask task = Mockito.mock(RewriteGroupTask.class);
        Mockito.when(task.getId()).thenReturn(7L);
        RewriteDataFileExecutor.RewriteResultCollector collector =
                new RewriteDataFileExecutor.RewriteResultCollector(
                        1, Collections.singletonList(task));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getTransientTaskManager()).thenReturn(taskManager);

            collector.cancelAllTasks();

            InOrder inOrder = Mockito.inOrder(taskManager, task);
            inOrder.verify(taskManager).removeMemoryTask(7L);
            inOrder.verify(task).cancel();
        }
    }

    private static class RewriteFixture {
        private static final long TRANSACTION_ID = 7L;

        private final IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        private final ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        private final IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        private final GlobalExternalTransactionInfoMgr globalTransactionManager =
                new GlobalExternalTransactionInfoMgr();
        private final TrackingTransactionManager transactionManager =
                new TrackingTransactionManager(transaction, globalTransactionManager);
        private final WritableTableLease lease = Mockito.mock(WritableTableLease.class);
        private final ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        private final RewriteDataFileExecutor executor = new RewriteDataFileExecutor(table, connectContext);
        private final Env env = Mockito.mock(Env.class);
        private final ExternalMetaCacheMgr cacheManager = Mockito.mock(ExternalMetaCacheMgr.class);

        private RewriteFixture() {
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
        }

        private void prepareEnvironment(MockedStatic<Env> mockedEnv) throws UserException {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheManager);
        }

        private void prepareSuccessfulExecution(MockedStatic<Env> mockedEnv,
                MockedStatic<IcebergUtils> mockedIcebergUtils) throws UserException {
            prepareEnvironment(mockedEnv);
            IcebergSnapshotCacheValue snapshot = Mockito.mock(IcebergSnapshotCacheValue.class);
            mockedIcebergUtils.when(() -> IcebergUtils.getSnapshotForWritableLease(table, lease))
                    .thenReturn(snapshot);
            Mockito.when(lease.getTable()).thenReturn(Mockito.mock(Table.class));
            ComputeGroup computeGroup = Mockito.mock(ComputeGroup.class);
            Mockito.when(connectContext.getComputeGroup()).thenReturn(computeGroup);
            Mockito.when(computeGroup.getBackendList()).thenReturn(Collections.emptyList());
            SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
            Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
            Mockito.when(sessionVariable.getInsertTimeoutS()).thenReturn(1);
        }

        private void assertTransactionRegistriesEmpty() {
            Assertions.assertTrue(transactionManager.transactions.isEmpty());
            Assertions.assertTrue(globalTransactionManager.idToTxn.isEmpty());
        }

        private static class TrackingTransactionManager implements TransactionManager {
            private final Map<Long, Transaction> transactions = new HashMap<>();
            private final Transaction transaction;
            private final GlobalExternalTransactionInfoMgr globalTransactionManager;

            private TrackingTransactionManager(Transaction transaction,
                    GlobalExternalTransactionInfoMgr globalTransactionManager) {
                this.transaction = transaction;
                this.globalTransactionManager = globalTransactionManager;
            }

            @Override
            public long begin() {
                transactions.put(TRANSACTION_ID, transaction);
                globalTransactionManager.putTxnById(TRANSACTION_ID, transaction);
                return TRANSACTION_ID;
            }

            @Override
            public void commit(long id) throws UserException {
                getTransaction(id).commit();
                transactions.remove(id);
                globalTransactionManager.removeTxnById(id);
            }

            @Override
            public void rollback(long id) {
                Transaction registered = transactions.get(id);
                if (registered != null) {
                    registered.rollback();
                }
                transactions.remove(id);
                globalTransactionManager.removeTxnById(id);
            }

            @Override
            public Transaction getTransaction(long id) throws UserException {
                Transaction registered = transactions.get(id);
                if (registered == null) {
                    throw new UserException("Missing transaction " + id);
                }
                return registered;
            }
        }
    }
}
