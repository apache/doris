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
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.IcebergTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionManager;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

class IcebergCommitLocationRegistrationTest {
    @Test
    void testInsertRegistersTheBoundSinkBeforeExecution() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.context.getSessionVariable().setIcebergPartitionOverwriteMode("dynamic");
            IcebergInsertExecutor executor = new IcebergInsertExecutor(fixture.context, fixture.table,
                    fixture.nativeTable, "label", Mockito.mock(NereidsPlanner.class), Optional.empty(), false, -1L);
            executor.txnId = 101L;
            IcebergTableSink sink = new IcebergTableSink(fixture.table, fixture.nativeTable);
            executor.finalizeSink(null, sink, null);

            executor.beforeExec();

            Mockito.verify(fixture.transaction).beginInsert(
                    fixture.table, fixture.nativeTable, Optional.empty(), "dynamic");
            Mockito.verify(fixture.transaction).addCommitLocations(sink.toThrift());
            Assertions.assertEquals("file:///tmp/custom/data/",
                    sink.toThrift().getIcebergTableSink().getOriginalOutputPath());
        }
    }

    @Test
    void testEmptyStaticOverwriteDoesNotRequireABeSink() throws Exception {
        try (Fixture fixture = new Fixture()) {
            IcebergInsertCommandContext context = new IcebergInsertCommandContext();
            context.setOverwrite(true);
            context.setStaticPartitionValues(Collections.singletonMap("partition_key", "1"));
            IcebergInsertExecutor executor = new IcebergInsertExecutor(fixture.context, fixture.table,
                    fixture.nativeTable, "label", Mockito.mock(NereidsPlanner.class), Optional.of(context), true, -1L);
            executor.txnId = 101L;
            Assertions.assertTrue(executor.shouldExecuteEmptyInsert());

            executor.beforeExec();

            Mockito.verify(fixture.transaction).beginInsert(
                    fixture.table, fixture.nativeTable, Optional.of(context), "static");
            Mockito.verify(fixture.transaction, Mockito.never()).addCommitLocations(Mockito.any());
        }
    }

    @Test
    void testRewriteRegistersAfterBindingTheSharedTransaction() throws Exception {
        try (Fixture fixture = new Fixture()) {
            IcebergInsertCommandContext context = new IcebergInsertCommandContext();
            context.setRewriting(true);
            IcebergRewriteExecutor executor = new IcebergRewriteExecutor(fixture.context, fixture.table,
                    "label", Mockito.mock(NereidsPlanner.class), Optional.of(context), false, -1L);
            IcebergTableSink sink = new IcebergTableSink(fixture.table, fixture.nativeTable);
            executor.finalizeSink(null, sink, null);
            Mockito.verifyNoInteractions(fixture.transactionManager);

            executor.bindTransaction(101L);

            Mockito.verify(fixture.transaction).addCommitLocations(sink.toThrift());
            Mockito.verify(fixture.coordinator).setTxnId(101L);
            // A group must not take over committing the enclosing rewrite transaction.
            Assertions.assertEquals(-1L, executor.getTxnId());
        }
    }

    private static class Fixture implements AutoCloseable {
        private final ConnectContext context = new ConnectContext();
        private final IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        private final Table nativeTable = Mockito.mock(Table.class);
        private final IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        private final TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        private final Coordinator coordinator = Mockito.mock(Coordinator.class);
        private final MockedStatic<EnvFactory> factoryMock = Mockito.mockStatic(EnvFactory.class);

        Fixture() throws Exception {
            context.setThreadLocalInfo();
            context.setQueryId(new TUniqueId(1L, 2L));
            EnvFactory factory = Mockito.mock(EnvFactory.class);
            factoryMock.when(EnvFactory::getInstance).thenReturn(factory);
            Mockito.when(factory.createCoordinator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                    .thenReturn(coordinator);
            Mockito.when(transactionManager.getTransaction(101L)).thenReturn(transaction);

            CatalogProperty properties = Mockito.mock(CatalogProperty.class);
            Mockito.when(properties.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());
            IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
            Mockito.when(catalog.getName()).thenReturn("iceberg");
            Mockito.when(catalog.getCatalogProperty()).thenReturn(properties);
            Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getDatabase()).thenReturn(Mockito.mock(DatabaseIf.class));
            Mockito.when(table.getName()).thenReturn("table");
            Mockito.when(table.getDbName()).thenReturn("db");

            Mockito.when(nativeTable.schema()).thenReturn(new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get())));
            Mockito.when(nativeTable.spec()).thenReturn(PartitionSpec.unpartitioned());
            Mockito.when(nativeTable.sortOrder()).thenReturn(SortOrder.unsorted());
            Mockito.when(nativeTable.location()).thenReturn("file:///tmp/table");
            Mockito.when(nativeTable.properties()).thenReturn(Collections.singletonMap(
                    TableProperties.WRITE_DATA_LOCATION, "file:///tmp/custom/data/"));
        }

        @Override
        public void close() {
            factoryMock.close();
            ConnectContext.remove();
        }
    }
}
