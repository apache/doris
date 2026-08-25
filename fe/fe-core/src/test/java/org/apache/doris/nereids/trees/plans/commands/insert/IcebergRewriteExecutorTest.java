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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.transaction.TransactionManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

class IcebergRewriteExecutorTest {

    @Test
    void testGroupCompletionDoesNotCommitParentTransaction() throws Exception {
        try (TestFixture fixture = new TestFixture()) {
            IcebergRewriteExecutor executor = fixture.createExecutor();

            Assertions.assertDoesNotThrow(executor::onComplete);

            Mockito.verify(fixture.transactionManager, Mockito.never()).commit(Mockito.anyLong());
        }
    }

    @Test
    void testGroupFailurePropagatesToRewriteCoordinator() {
        try (TestFixture fixture = new TestFixture()) {
            IcebergRewriteExecutor executor = fixture.createExecutor();
            RuntimeException failure = new RuntimeException("injected group failure");

            RuntimeException propagated = Assertions.assertThrows(
                    RuntimeException.class, () -> executor.onFail(failure));

            Assertions.assertSame(failure, propagated);
            Mockito.verify(fixture.transactionManager, Mockito.never()).rollback(Mockito.anyLong());
        }
    }

    private static class TestFixture implements AutoCloseable {
        private final ConnectContext context = new ConnectContext();
        private final TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        private final IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        private final IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        private final NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        private final Coordinator coordinator = Mockito.mock(Coordinator.class);
        private final MockedStatic<EnvFactory> envFactory = Mockito.mockStatic(EnvFactory.class);

        TestFixture() {
            DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
            Mockito.when(database.getId()).thenReturn(1L);
            Mockito.when(database.getFullName()).thenReturn("db");
            Mockito.when(table.getDatabase()).thenReturn(database);
            Mockito.when(table.getName()).thenReturn("tbl");
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getName()).thenReturn("catalog");
            Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
            Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() { });

            EnvFactory factory = Mockito.mock(EnvFactory.class);
            envFactory.when(EnvFactory::getInstance).thenReturn(factory);
            Mockito.when(factory.createCoordinator(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                    .thenReturn(coordinator);

            context.setThreadLocalInfo();
        }

        IcebergRewriteExecutor createExecutor() {
            return new IcebergRewriteExecutor(context, table, "rewrite", planner,
                    Optional.empty(), false, 1L);
        }

        @Override
        public void close() {
            envFactory.close();
            ConnectContext.remove();
        }
    }
}
