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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.WriteOperation;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Real SDK transactions with a recording metadata refresh model a concurrent encryption change. */
class IcebergEncryptedWriteFenceTest {
    @ParameterizedTest
    @EnumSource(WriteOperation.class)
    void declaredEncryptedTargetsRejectEveryNativeWriteOperation(WriteOperation operation) throws IOException {
        try (Fixture fixture = new Fixture()) {
            fixture.encrypted.set(true);
            assertWriteUnavailable(() -> fixture.begin(operation));
            Assertions.assertEquals(0, fixture.commits.get());
            Assertions.assertNull(fixture.original.currentSnapshot());
        }
    }

    @Test
    void transactionRefreshCannotOpenAnEncryptedNativeSink() throws IOException {
        try (Fixture fixture = new Fixture()) {
            fixture.encryptOnRefresh.set(true);
            assertWriteUnavailable(() -> fixture.begin(WriteOperation.INSERT));
            Assertions.assertEquals(0, fixture.commits.get());
        }
    }

    @Test
    void finalCasRejectsFilesWhenTheTargetBecomesEncryptedAfterPlanning() throws IOException {
        try (Fixture fixture = new Fixture()) {
            Transaction transaction = fixture.open();
            appendNativeFile(transaction);
            fixture.encrypted.set(true);
            assertWriteUnavailable(transaction::commitTransaction);
            Assertions.assertEquals(0, fixture.commits.get());
            Assertions.assertNull(fixture.original.currentSnapshot());
        }
    }

    @Test
    void ordinaryPlaintextTransactionsRemainWritable() throws IOException {
        try (Fixture fixture = new Fixture()) {
            Transaction transaction = fixture.open();
            appendNativeFile(transaction);
            transaction.commitTransaction();
            Assertions.assertEquals(1, fixture.commits.get());
            Assertions.assertNotNull(fixture.original.currentSnapshot());
        }
    }

    private static void appendNativeFile(Transaction transaction) {
        transaction.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("memory:///native-file.parquet").withFileSizeInBytes(100).withRecordCount(1).build()).commit();
    }

    private static void assertWriteUnavailable(Runnable action) {
        RuntimeException error = Assertions.assertThrows(RuntimeException.class, action::run);
        Assertions.assertTrue(error.getMessage().contains("Iceberg encryption [WRITE_UNAVAILABLE]"),
                error.getMessage());
    }

    private static final class Fixture implements AutoCloseable {
        private final InMemoryCatalog catalog = new InMemoryCatalog();
        private final AtomicBoolean encrypted = new AtomicBoolean();
        private final AtomicBoolean encryptOnRefresh = new AtomicBoolean();
        private final AtomicInteger commits = new AtomicInteger();
        private final Table original;
        private final Table table;

        private Fixture() {
            catalog.initialize("fixture", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            original = catalog.createTable(TableIdentifier.of("db", "tbl"),
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                    PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "3"));
            TableOperations delegate = ((HasTableOperations) original).operations();
            TableOperations recording = (TableOperations) Proxy.newProxyInstance(
                    TableOperations.class.getClassLoader(), new Class<?>[] {TableOperations.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("refresh") && encryptOnRefresh.get()) {
                            encrypted.set(true);
                        }
                        if ((method.getName().equals("current") || method.getName().equals("refresh"))
                                && encrypted.get()
                                && !delegate.current().properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY)) {
                            // Publish a genuine concurrent metadata generation, preserving SDK CAS/identity semantics.
                            TableMetadata base = delegate.current();
                            delegate.commit(base, TableMetadata.buildFrom(base).setProperties(
                                    Map.of(TableProperties.ENCRYPTION_TABLE_KEY, "fixture-master-key")).build());
                        }
                        if (method.getName().equals("commit")) {
                            commits.incrementAndGet();
                        }
                        try {
                            return method.invoke(delegate, arguments);
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
            table = new BaseTable(recording, "db.tbl");
        }

        private void begin(WriteOperation operation) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            ops.table = table;
            IcebergConnectorTransaction transaction = new IcebergConnectorTransaction(1, ops,
                    EncryptedManifestFixture.verifiedContext());
            try {
                transaction.beginWrite(new IcebergStatementScopeTest.ScopeSession(1L, "write", ConnectorStatementScope.NONE),
                        "db", "tbl", new IcebergWriteContext(
                        operation, operation == WriteOperation.OVERWRITE, Collections.emptyMap(), Optional.empty()));
            } finally {
                transaction.close();
            }
        }

        private Transaction open() {
            return IcebergConnectorTransaction.openTransaction(table, EncryptedManifestFixture.verifiedContext(),
                    null, false, null, null);
        }

        @Override
        public void close() throws IOException {
            catalog.close();
        }
    }
}
