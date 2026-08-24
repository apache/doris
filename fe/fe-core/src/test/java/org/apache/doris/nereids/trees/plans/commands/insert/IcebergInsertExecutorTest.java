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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.Transaction;
import org.apache.doris.transaction.TransactionManager;

import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class IcebergInsertExecutorTest {

    @Test
    void emptyInsertRollsBackWhenBeginInsertFails() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, 2));
        context.setThreadLocalInfo();
        IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        Mockito.doThrow(new UserException("injected begin failure")).when(transaction)
                .beginInsert(Mockito.any(), Mockito.any(), Mockito.any());
        TestTransactionManager transactionManager = new TestTransactionManager(10L, transaction);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("iceberg");
        Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getId()).thenReturn(3L);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getName()).thenReturn("table");
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<EnvFactory> envFactory = Mockito.mockStatic(EnvFactory.class)) {
            envFactory.when(EnvFactory::getInstance).thenReturn(factory);
            Mockito.when(factory.createCoordinator(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                    .thenReturn(coordinator);
            IcebergInsertExecutor executor = new IcebergInsertExecutor(
                    context, table, Mockito.mock(Table.class), "label", planner,
                    Optional.empty(), true, 4L);
            executor.txnId = 10L;

            Assertions.assertDoesNotThrow(() -> executor.executeEmptyInsert(stmtExecutor));

            Assertions.assertThrows(UserException.class, () -> transactionManager.getTransaction(10L));
            Mockito.verify(transaction).rollback();
        } finally {
            ConnectContext.remove();
        }
    }

    private static class TestTransactionManager implements TransactionManager {
        private final Map<Long, Transaction> transactions = new HashMap<>();

        TestTransactionManager(long id, Transaction transaction) {
            transactions.put(id, transaction);
        }

        @Override
        public long begin() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rollback(long id) {
            Transaction transaction = transactions.remove(id);
            if (transaction != null) {
                transaction.rollback();
            }
        }

        @Override
        public Transaction getTransaction(long id) throws UserException {
            Transaction transaction = transactions.get(id);
            if (transaction == null) {
                throw new UserException("transaction not found");
            }
            return transaction;
        }
    }
}
