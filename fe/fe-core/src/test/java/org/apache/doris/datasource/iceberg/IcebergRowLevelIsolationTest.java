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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.iceberg.IcebergCoverageTestFixture.Operation;
import org.apache.doris.datasource.iceberg.IcebergCoverageTestFixture.WriteContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergRowLevelIsolationTest {
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private static final String SEED = "s3://bucket/events/seed.parquet";
    private static final String APPENDED = "s3://bucket/events/appended.parquet";
    private static final String REPLACEMENT = "s3://bucket/events/replacement.parquet";
    private static final String LEGACY_KEY = "delete_isolation_level";

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void missingPropertiesDefaultToSerializable(Operation operation) throws Exception {
        assertConcurrentAppend(operation, Collections.emptyMap(), false);
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void legacySnapshotPermitsConcurrentAppends(Operation operation) throws Exception {
        assertConcurrentAppend(operation, Collections.singletonMap(LEGACY_KEY, "snapshot"), true);
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void standardKeyOverridesInvalidLegacyAndOtherOperationKeys(Operation operation) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(LEGACY_KEY, "invalid");
        properties.put(TableProperties.DELETE_ISOLATION_LEVEL, "invalid");
        properties.put(TableProperties.UPDATE_ISOLATION_LEVEL, "invalid");
        properties.put(TableProperties.MERGE_ISOLATION_LEVEL, "invalid");
        properties.put(isolationKey(operation), "SNAPSHOT");
        assertConcurrentAppend(operation, properties, true);
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void standardSerializableOverridesLegacySnapshot(Operation operation) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(LEGACY_KEY, "snapshot");
        properties.put(isolationKey(operation), "serializable");
        assertConcurrentAppend(operation, properties, false);
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void invalidSelectedPropertyFailsBeforeWritesAndReleasesLease(Operation operation) throws Exception {
        for (String property : Arrays.asList(isolationKey(operation), LEGACY_KEY)) {
            Map<String, String> properties = new HashMap<>();
            properties.put(LEGACY_KEY, "snapshot");
            properties.put(property, "invalid");
            InMemoryCatalog catalog = catalog();
            Table table = create(catalog, properties);
            IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, table);
            IcebergExternalMetaCache.WritableTableLease lease = fixture.lease;
            try (IcebergCoverageTestFixture ignored = fixture) {
                RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                        () -> fixture.writer.beginWrite(context(operation)));
                Assertions.assertTrue(failure.getMessage().contains("Invalid isolation level: invalid"));
                Assertions.assertNull(Deencapsulation.getField(fixture.writer.transaction, "transaction"));
                Assertions.assertTrue(fixture.writer.transaction.getCommitDataList().isEmpty());
                Assertions.assertNull(catalog.loadTable(IDENTIFIER).currentSnapshot());
                Mockito.verify(lease).close();
                // A failed preparation must release the lease so the transaction can be prepared again.
                table.updateProperties().set(property, "snapshot").commit();
                fixture.writer.beginWrite(context(operation));
                Assertions.assertNotNull(Deencapsulation.getField(fixture.writer.transaction, "transaction"));
            }
            Mockito.verify(lease, Mockito.times(2)).close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void isolationIsParsedFromOpenedTransactionMetadata(Operation operation) {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, Collections.singletonMap(isolationKey(operation), "snapshot"));
        Table refreshing = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
                (proxy, method, args) -> {
                    if ("newTransaction".equals(method.getName())) {
                        table.updateProperties().set(isolationKey(operation), "invalid").commit();
                    }
                    try {
                        return method.invoke(table, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        try (IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, refreshing)) {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> fixture.writer.beginWrite(context(operation)));
            Assertions.assertTrue(failure.getMessage().contains("Invalid isolation level: invalid"));
            Assertions.assertNull(Deencapsulation.getField(fixture.writer.transaction, "transaction"));
            Mockito.verify(fixture.lease).close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void commitKeepsBeginTimeIsolationDespitePropertyChanges(Operation operation) throws Exception {
        for (String initial : Arrays.asList("snapshot", "serializable")) {
            InMemoryCatalog catalog = catalog();
            Table table = create(catalog, Collections.singletonMap(isolationKey(operation), initial));
            append(table, SEED);
            try (IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, table)) {
                fixture.writer.beginWrite(context(operation));
                org.apache.iceberg.Transaction opened =
                        Deencapsulation.getField(fixture.writer.transaction, "transaction");
                opened.table().updateProperties().set(isolationKey(operation),
                        "snapshot".equals(initial) ? "invalid" : "snapshot").commit();
                append(catalog.loadTable(IDENTIFIER), APPENDED);
                stageFiles(fixture, operation, SEED);
                assertCommit(catalog, fixture, operation, "snapshot".equals(initial));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"UPDATE", "MERGE"})
    void standardSnapshotWithPinnedWriterMetadataPermitsConcurrentAppend(Operation operation) throws Exception {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, Collections.singletonMap(isolationKey(operation), "snapshot"));
        append(table, SEED);
        try (IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, table)) {
            WriteContext context = context(operation);
            context.insert.setWriteSchemaContext(Optional.of(IcebergWriteSchemaContext.forSchema(
                    table.schema(), 2, table.spec(), table.sortOrder(), IcebergUtils.getFileFormat(table),
                    MetricsConfig.forTable(table), IcebergUtils.getFileCompress(table),
                    IcebergUtils.dataLocation(table), table.properties(), true, true)));
            fixture.writer.beginWrite(context);
            append(catalog.loadTable(IDENTIFIER), APPENDED);
            stageFiles(fixture, operation, SEED);
            assertCommit(catalog, fixture, operation, true);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE", "MERGE"})
    void snapshotStillValidatesReferencedDataFiles(Operation operation) throws Exception {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, Collections.singletonMap(isolationKey(operation), "snapshot"));
        append(table, SEED);
        try (IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, table)) {
            fixture.writer.beginWrite(context(operation));
            Table concurrent = catalog.loadTable(IDENTIFIER);
            try (CloseableIterable<FileScanTask> tasks = concurrent.newScan().planFiles()) {
                concurrent.newDelete().deleteFile(tasks.iterator().next().file()).commit();
            }
            stageFiles(fixture, operation, SEED);
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, fixture.writer::commit);
            Assertions.assertTrue(failure.getMessage().contains("Cannot commit, missing data files: [" + SEED + "]"),
                    failure.getMessage());
            Assertions.assertEquals("delete", catalog.loadTable(IDENTIFIER).currentSnapshot().operation());
        }
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = {"INSERT", "OVERWRITE"})
    void insertsDoNotParseRowLevelProperties(Operation operation) {
        Map<String, String> properties = new HashMap<>();
        for (String property : Arrays.asList(LEGACY_KEY, TableProperties.DELETE_ISOLATION_LEVEL,
                TableProperties.UPDATE_ISOLATION_LEVEL, TableProperties.MERGE_ISOLATION_LEVEL)) {
            properties.put(property, "invalid");
        }
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, properties);
        try (IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, table)) {
            Assertions.assertDoesNotThrow(() -> fixture.writer.beginWrite(context(operation)));
            Assertions.assertNotNull(Deencapsulation.getField(fixture.writer.transaction, "transaction"));
        }
    }

    private static void assertConcurrentAppend(Operation operation, Map<String, String> properties,
            boolean permitsAppend) throws Exception {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, properties);
        append(table, SEED);
        try (IcebergCoverageTestFixture fixture = new IcebergCoverageTestFixture(catalog, table)) {
            fixture.writer.beginWrite(context(operation));
            append(catalog.loadTable(IDENTIFIER), APPENDED);
            stageFiles(fixture, operation, SEED);
            assertCommit(catalog, fixture, operation, permitsAppend);
        }
    }

    private static void assertCommit(InMemoryCatalog catalog, IcebergCoverageTestFixture fixture,
            Operation operation, boolean permitsAppend) throws Exception {
        long before = catalog.loadTable(IDENTIFIER).currentSnapshot().snapshotId();
        if (permitsAppend) {
            fixture.writer.commit();
            Table committed = catalog.loadTable(IDENTIFIER);
            Assertions.assertNotEquals(before, committed.currentSnapshot().snapshotId());
            Assertions.assertEquals("1", committed.currentSnapshot().summary().get("added-delete-files"));
            Set<String> paths = new HashSet<>();
            try (CloseableIterable<FileScanTask> tasks = committed.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    paths.add(task.file().path().toString());
                }
            }
            Set<String> expected = new HashSet<>(Arrays.asList(SEED, APPENDED));
            if (operation != Operation.DELETE) {
                expected.add(REPLACEMENT);
            }
            Assertions.assertEquals(expected, paths);
        } else {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, fixture.writer::commit);
            Assertions.assertTrue(failure.getMessage().contains("Found conflicting files"), failure.getMessage());
            Assertions.assertEquals(before, catalog.loadTable(IDENTIFIER).currentSnapshot().snapshotId());
        }
    }

    private static void stageFiles(IcebergCoverageTestFixture fixture, Operation operation, String referenced)
            throws Exception {
        TIcebergCommitData delete = fragment("s3://bucket/events/delete.parquet", TFileContent.POSITION_DELETES);
        delete.setReferencedDataFilePath(referenced);
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        fixture.writer.addCommitData(serializer.serialize(delete));
        if (operation != Operation.DELETE) {
            fixture.writer.addCommitData(serializer.serialize(fragment(REPLACEMENT, TFileContent.DATA)));
        }
    }

    private static TIcebergCommitData fragment(String path, TFileContent content) {
        TIcebergCommitData fragment = new TIcebergCommitData();
        fragment.setFilePath(path);
        fragment.setFileContent(content);
        fragment.setFileSize(1024);
        fragment.setRowCount(1);
        fragment.setAffectedRows(1);
        return fragment;
    }

    private static WriteContext context(Operation operation) {
        return new WriteContext(operation, operation == Operation.OVERWRITE, Collections.emptyMap(), Optional.empty());
    }

    private static String isolationKey(Operation operation) {
        switch (operation) {
            case DELETE:
                return TableProperties.DELETE_ISOLATION_LEVEL;
            case UPDATE:
                return TableProperties.UPDATE_ISOLATION_LEVEL;
            case MERGE:
                return TableProperties.MERGE_ISOLATION_LEVEL;
            default:
                throw new IllegalArgumentException("Not a row-level operation: " + operation);
        }
    }

    private static void append(Table table, String path) {
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(path)
                .withFileSizeInBytes(1024).withRecordCount(3).build()).commit();
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("isolation", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static Table create(InMemoryCatalog catalog, Map<String, String> properties) {
        Map<String, String> tableProperties = new HashMap<>(properties);
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        return catalog.createTable(IDENTIFIER, new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), tableProperties);
    }
}
