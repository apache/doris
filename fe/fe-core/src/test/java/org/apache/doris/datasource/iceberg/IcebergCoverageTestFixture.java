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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Supplies the retained catalog generation required by the 4.2 metadata and write entry points. */
final class IcebergCoverageTestFixture implements AutoCloseable {
    final Table table;
    final IcebergMetadataOps ops;
    final IcebergExternalTable dorisTable;
    final Writer writer;
    private final MockedStatic<IcebergUtils> utilities;
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    IcebergCoverageTestFixture(Catalog catalog, Table table) {
        this.table = table;
        IcebergExternalCatalog dorisCatalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.getThreadPoolWithPreAuth()).thenReturn(executor);
        ops = new IcebergMetadataOps(dorisCatalog, catalog);
        dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(dorisCatalog);
        Mockito.when(dorisTable.getName()).thenReturn("events");
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(dorisTable.getRemoteName()).thenReturn("events");
        Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(NameMapping.createForTest("db", "events"));
        IcebergExternalMetaCache.WritableTableLease lease =
                Mockito.mock(IcebergExternalMetaCache.WritableTableLease.class);
        Mockito.when(lease.getTable()).thenReturn(table);
        Mockito.when(lease.getOps()).thenReturn(ops);
        Mockito.when(lease.getAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        utilities = Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS);
        utilities.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, ops)).thenReturn(lease);
        writer = new Writer(table);
    }

    @Override
    public void close() {
        try {
            writer.transaction.rollback();
        } finally {
            utilities.close();
            executor.shutdownNow();
        }
    }

    enum Operation { INSERT, OVERWRITE, DELETE, UPDATE, MERGE }

    static final class WriteContext {
        final Operation operation;
        final IcebergInsertCommandContext insert = new IcebergInsertCommandContext();

        WriteContext(Operation operation, boolean overwrite, Map<String, String> partitions, Optional<String> branch) {
            this.operation = operation;
            insert.setOverwrite(overwrite);
            insert.setStaticPartitionValues(partitions);
            insert.setBranchName(branch);
        }
    }

    final class Writer {
        final IcebergTransaction transaction = new IcebergTransaction(ops);
        private final Table table;
        private Operation operation;

        Writer(Table table) {
            this.table = table;
        }

        void beginWrite(WriteContext context) {
            operation = context.operation;
            try {
                if (context.insert.isStaticPartitionOverwrite()) {
                    // Static overwrite uses the statement-pinned partition spec, not refreshed writer metadata.
                    context.insert.setWriteSchemaContext(Optional.of(IcebergWriteSchemaContext.forSchema(
                            table.schema(), IcebergUtils.getFormatVersion(table), table.spec(), table.sortOrder(),
                            IcebergUtils.getFileFormat(table), MetricsConfig.forTable(table),
                            IcebergUtils.getFileCompress(table), IcebergUtils.dataLocation(table), table.properties(),
                            true, true)));
                }
                switch (operation) {
                    case DELETE:
                        transaction.beginDelete(dorisTable, table);
                        break;
                    case UPDATE:
                    case MERGE:
                        transaction.beginMerge(dorisTable, table, Optional.of(context.insert));
                        break;
                    default:
                        transaction.beginInsert(dorisTable, table, Optional.of(context.insert));
                }
            } catch (Exception failure) {
                throw new RuntimeException(failure.getMessage(), failure);
            }
        }

        void addCommitData(byte[] bytes) {
            try {
                TIcebergCommitData fragment = new TIcebergCommitData();
                new TDeserializer(new TBinaryProtocol.Factory()).deserialize(fragment, bytes);
                transaction.updateIcebergCommitData(Collections.singletonList(fragment));
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        }

        void commit() {
            NameMapping name = NameMapping.createForTest("db", "events");
            try {
                switch (operation) {
                    case DELETE:
                        transaction.finishDelete(name);
                        break;
                    case UPDATE:
                    case MERGE:
                        transaction.finishMerge(name);
                        break;
                    default:
                        transaction.finishInsert(name);
                }
                transaction.commit();
            } catch (Exception failure) {
                throw new RuntimeException(failure.getMessage(), failure);
            }
        }

        long getUpdateCnt() {
            return transaction.getUpdateCnt();
        }
    }
}
