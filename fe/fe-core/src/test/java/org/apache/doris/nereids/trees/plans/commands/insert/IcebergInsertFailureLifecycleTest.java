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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HiveTransactionMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalExternalTransactionInfoMgr;
import org.apache.doris.transaction.IcebergTransactionManager;
import org.apache.doris.transaction.TransactionType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

class IcebergInsertFailureLifecycleTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testNormalInsertBeginFailureClearsLocalAndGlobalRegistriesAfterReset() throws Exception {
        assertBeginFailureClearsRegistries(false);
    }

    @Test
    void testEmptyInsertBeginFailureClearsLocalAndGlobalRegistriesAfterReset() throws Exception {
        assertBeginFailureClearsRegistries(true);
    }

    private void assertBeginFailureClearsRegistries(boolean emptyInsert) throws Exception {
        Env env = Mockito.mock(Env.class);
        GlobalExternalTransactionInfoMgr globalTransactions = new GlobalExternalTransactionInfoMgr();
        Mockito.when(env.getNextId()).thenReturn(101L);
        Mockito.when(env.getGlobalExternalTransactionInfoMgr()).thenReturn(globalTransactions);

        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        HiveTransactionMgr hiveTransactionMgr = Mockito.mock(HiveTransactionMgr.class);
        IcebergTransactionManager transactionManager = new IcebergTransactionManager(metadataOps);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("iceberg");
        Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
        Mockito.when(catalog.getExecutionAuthenticator()).thenThrow(
                new IllegalStateException("catalog reset cleared the live authenticator"));

        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getId()).thenReturn(1L);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getName()).thenReturn("tbl");

        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setQueryId(new TUniqueId(1L, 2L));
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                .thenReturn(coordinator);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<EnvFactory> factoryMock = Mockito.mockStatic(EnvFactory.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentHiveTransactionMgr).thenReturn(hiveTransactionMgr);
            factoryMock.when(EnvFactory::getInstance).thenReturn(factory);

            FailingIcebergExecutor executor = new FailingIcebergExecutor(
                    context, table, emptyInsert);
            executor.beginTransaction();
            long transactionId = executor.getTxnId();
            Assertions.assertNotNull(transactionManager.getTransaction(transactionId));
            Assertions.assertNotNull(globalTransactions.getTxnById(transactionId));

            StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
            if (emptyInsert) {
                executor.executeEmptyInsert(stmtExecutor);
            } else {
                executor.executeSingleInsert(stmtExecutor);
            }

            Assertions.assertThrows(UserException.class,
                    () -> transactionManager.getTransaction(transactionId));
            Assertions.assertThrows(RuntimeException.class,
                    () -> globalTransactions.getTxnById(transactionId));
            Mockito.verify(catalog, Mockito.never()).getExecutionAuthenticator();
            Mockito.verify(coordinator).close();
        }
    }

    private static class FailingIcebergExecutor extends BaseExternalTableInsertExecutor {
        FailingIcebergExecutor(ConnectContext context, IcebergExternalTable table,
                boolean emptyInsert) {
            super(context, table, "label", Mockito.mock(NereidsPlanner.class),
                    Optional.empty(), emptyInsert, -1L);
        }

        @Override
        protected void beforeExec() throws UserException {
            throw new UserException("catalog runtime changed before begin");
        }

        @Override
        protected void doBeforeCommit() {
        }

        @Override
        protected TransactionType transactionType() {
            return TransactionType.ICEBERG;
        }

        @Override
        protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        }
    }
}
