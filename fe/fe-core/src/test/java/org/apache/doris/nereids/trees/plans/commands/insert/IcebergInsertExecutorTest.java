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

import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.transaction.TransactionManager;

import org.apache.iceberg.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

class IcebergInsertExecutorTest {
    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testEmptyOverwriteExecutesAndPassesSessionMode() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDatabase()).thenReturn(Mockito.mock(ExternalDatabase.class));
        TransactionManager manager = Mockito.mock(TransactionManager.class);
        Mockito.when(catalog.getTransactionManager()).thenReturn(manager);
        IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        Mockito.when(manager.getTransaction(Mockito.anyLong())).thenReturn(transaction);
        Table targetTable = Mockito.mock(Table.class);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                .thenReturn(Mockito.mock(Coordinator.class));
        try (MockedStatic<EnvFactory> mockedFactory = Mockito.mockStatic(EnvFactory.class)) {
            mockedFactory.when(EnvFactory::getInstance).thenReturn(factory);
            IcebergInsertCommandContext context = new IcebergInsertCommandContext();
            context.setOverwrite(true);
            for (String mode : new String[] {"static", "dynamic"}) {
                ctx.getSessionVariable().setIcebergPartitionOverwriteMode(mode);
                IcebergInsertExecutor executor = new IcebergInsertExecutor(ctx, table, targetTable,
                        "empty_overwrite", Mockito.mock(NereidsPlanner.class), Optional.of(context), true, 0L);
                Assertions.assertTrue(executor.shouldExecuteEmptyInsert());
                executor.beforeExec();
                Mockito.verify(transaction).beginInsert(table, targetTable, Optional.of(context), mode);
            }
            context.setOverwrite(false);
            IcebergInsertExecutor append = new IcebergInsertExecutor(ctx, table, targetTable,
                    "empty_append", Mockito.mock(NereidsPlanner.class), Optional.of(context), true, 0L);
            Assertions.assertFalse(append.shouldExecuteEmptyInsert());
            context.setOverwrite(true);
            IcebergInsertExecutor nonEmpty = new IcebergInsertExecutor(ctx, table, targetTable,
                    "non_empty", Mockito.mock(NereidsPlanner.class), Optional.of(context), false, 0L);
            Assertions.assertFalse(nonEmpty.shouldExecuteEmptyInsert());
        }
    }
}
