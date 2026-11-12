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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

class DeltaLakeConnectorTest {
    private static final Map<String, Object> VARIANT_PROTOCOL = Map.of(
            "minReaderVersion", 3, "minWriterVersion", 7,
            "readerFeatures", List.of("variantType"), "writerFeatures", List.of("variantType"));

    @TempDir
    Path warehouse;

    @Test
    void providerDiscoversTablesAndReadsSchemaFromDeltaLog() throws Exception {
        ConnectorProvider provider = ServiceLoader.load(ConnectorProvider.class).stream()
                .map(ServiceLoader.Provider::get).filter(value -> "deltalake".equals(value.getType()))
                .findFirst().orElseThrow();
        Assertions.assertNotNull(getClass().getResource("/" + provider.name() + ".conf.template"));
        DeltaLakeTestTables.commit(warehouse.resolve("db/table"), 0, "first",
                new StructType().add("ID", LongType.LONG), Collections.singletonList("ID"));
        Files.createDirectories(warehouse.resolve("db/parquet_only"));
        try (Connector connector = provider.create(Collections.singletonMap("warehouse", warehouse.toString()),
                new RecordingConnectorContext());
                ConnectorMetadata metadata = connector.getMetadata(null)) {
            Assertions.assertEquals(Collections.singletonList("db"), metadata.listDatabaseNames(null));
            Assertions.assertEquals(Collections.singletonList("table"), metadata.listTableNames(null, "db"));
            Assertions.assertTrue(metadata.getTableHandle(null, "db", "parquet_only").isEmpty());
            ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "table").orElseThrow();
            Assertions.assertTrue(connector.ownsHandle(handle));
            ConnectorTableSchema schema = metadata.getTableSchema(null, handle);
            Assertions.assertEquals("ID", schema.getColumns().get(0).getName());
            Assertions.assertEquals("ID", schema.getProperties().get(ConnectorTableSchema.PARTITION_COLUMNS_KEY));
            Assertions.assertEquals(Collections.singleton("ID"), metadata.getColumnHandles(null, handle).keySet());
            Assertions.assertEquals("db", metadata.buildTableDescriptor(null, 1, "table", "db",
                    "table", 1, 42).getHiveTable().getDbName());
        }
    }

    @Test
    void statementPinsHandleSchemaAndMvccAcrossAppends() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType original = new StructType().add("id", LongType.LONG);
        DeltaLakeTestTables.commit(table, 0, "first", original);
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(null)) {
            TestingConnectorSession statement = new TestingConnectorSession("first");
            ConnectorTableHandle first = metadata.getTableHandle(statement, "db", "table").orElseThrow();
            ConnectorMvccSnapshot pin = metadata.beginQuerySnapshot(statement, first).orElseThrow();
            DeltaLakeTestTables.commit(table, 1, "first", original.add("added", StringType.STRING));
            Assertions.assertSame(first, metadata.getTableHandle(statement, "db", "table").orElseThrow());
            Assertions.assertEquals(1, metadata.getTableSchema(statement, first).getColumns().size());
            connector.invalidateTable("db", "table");
            TestingConnectorSession next = new TestingConnectorSession("next");
            ConnectorTableHandle latest = metadata.getTableHandle(next, "db", "table").orElseThrow();
            Assertions.assertEquals(2, metadata.getTableSchema(next, latest).getColumns().size());
            ConnectorTableHandle pinned = metadata.applySnapshot(next, latest, pin);
            Assertions.assertEquals(0, ((DeltaLakeTableHandle) pinned).getVersion());
            Assertions.assertEquals(1, metadata.getTableSchema(next, latest, pin).getColumns().size());
            DeltaLakeTestTables.commit(warehouse.resolve("db/other"), 0, "other", original);
            ConnectorTableHandle other = metadata.getTableHandle(next, "db", "other").orElseThrow();
            Assertions.assertThrows(DorisConnectorException.class, () -> metadata.applySnapshot(next, other, pin));
        }
    }

    @Test
    void readsExplicitVersionsWithTheirOwnSchemaAndColumnHandles() throws Exception {
        Path table = warehouse.resolve("db/table");
        DeltaLakeTestTables.commit(table, 0, "first", new StructType().add("old_name", LongType.LONG));
        DeltaLakeTestTables.commit(table, 1, "first", new StructType().add("new_name", StringType.STRING));
        TestingConnectorSession session = new TestingConnectorSession("versioned-read");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "table").orElseThrow();
            ConnectorMvccSnapshot old = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.snapshotId("0")).orElseThrow();
            Assertions.assertEquals(0, old.getSnapshotId());
            Assertions.assertEquals(0, old.getSchemaId());
            Assertions.assertTrue(old.isSchemaRetained());
            Assertions.assertEquals("old_name", metadata.getTableSchema(session, latest, old)
                    .getColumns().get(0).getName());
            Assertions.assertTrue(metadata.supportsColumnHandleSnapshotPin(session));
            Assertions.assertEquals(Set.of("old_name"), metadata.getColumnHandles(session, latest, old).keySet());
            Assertions.assertSame(metadata.applySnapshot(session, latest, old),
                    metadata.applySnapshot(session, latest, old));
            Assertions.assertEquals(Set.of("new_name"), metadata.getColumnHandles(session, latest).keySet());

            // An explicit version can already exist even when the statement's latest handle predates it.
            DeltaLakeTestTables.commit(table, 2, "first", new StructType().add("later", LongType.LONG));
            ConnectorMvccSnapshot later = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.snapshotId("2")).orElseThrow();
            Assertions.assertEquals(Set.of("later"), metadata.getColumnHandles(session, latest, later).keySet());
            Assertions.assertEquals(1, metadata.beginQuerySnapshot(session, latest).orElseThrow().getSnapshotId());
        }
    }

    @Test
    void rejectsInvalidOrUnavailableVersionsWithoutReadingLatest() throws Exception {
        DeltaLakeTestTables.commit(warehouse.resolve("db/table"), 0, "id", new StructType().add("id", LongType.LONG));
        TestingConnectorSession session = new TestingConnectorSession("unavailable-version");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle table = metadata.getTableHandle(session, "db", "table").orElseThrow();
            for (ConnectorTimeTravelSpec spec : List.of(ConnectorTimeTravelSpec.versionRef("release"),
                    ConnectorTimeTravelSpec.versionRef("-1"), ConnectorTimeTravelSpec.snapshotId("-1"),
                    ConnectorTimeTravelSpec.snapshotId("9223372036854775808"))) {
                DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                        () -> metadata.resolveTimeTravel(session, table, spec));
                Assertions.assertTrue(error.getMessage().contains("non-negative 64-bit integer: "
                        + spec.getStringValue()), error.getMessage());
            }
            KernelException missing = Assertions.assertThrows(KernelException.class, () -> metadata.resolveTimeTravel(
                    session, table, ConnectorTimeTravelSpec.snapshotId("7")));
            Assertions.assertTrue(missing.getMessage().contains("Cannot load table version 7"), missing.getMessage());
            Assertions.assertTrue(metadata.resolveTimeTravel(session, table,
                    ConnectorTimeTravelSpec.timestamp("0", true)).isEmpty());
        }
    }

    @Test
    void rejectsVersionBeforeEarliestRetainedCheckpoint() throws Exception {
        Path log = Files.createDirectories(warehouse.resolve("db/table/_delta_log"));
        Path original = Path.of(getClass().getResource("/deltalake/dv-partitioned-with-checkpoint/_delta_log")
                .toURI());
        for (String file : List.of("_last_checkpoint", "00000000000000000010.checkpoint.parquet",
                "00000000000000000010.json")) {
            Files.copy(original.resolve(file), log.resolve(file));
        }
        TestingConnectorSession session = new TestingConnectorSession("truncated-version");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "table").orElseThrow();
            Assertions.assertEquals(10, metadata.beginQuerySnapshot(session, latest).orElseThrow().getSnapshotId());
            KernelException missing = Assertions.assertThrows(KernelException.class, () -> metadata.resolveTimeTravel(
                    session, latest, ConnectorTimeTravelSpec.snapshotId("9")));
            Assertions.assertTrue(missing.getMessage().contains("version 9"), missing.getMessage());
        }
    }

    @Test
    void checksReaderFeaturesAndIdentityOfRequestedVersion() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType schema = new StructType().add("id", LongType.LONG);
        DeltaLakeTestTables.commit(table, 0, "another-table", schema);
        DeltaLakeTestTables.commit(table, 1, "first", schema, List.of(), VARIANT_PROTOCOL);
        DeltaLakeTestTables.commit(table, 2, "first", schema);
        TestingConnectorSession session = new TestingConnectorSession("unsupported-version");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "table").orElseThrow();
            DorisConnectorException identity = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest, ConnectorTimeTravelSpec.snapshotId("0")));
            Assertions.assertTrue(identity.getMessage().contains("recreated"), identity.getMessage());
            DorisConnectorException features = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest, ConnectorTimeTravelSpec.snapshotId("1")));
            Assertions.assertTrue(features.getMessage().contains("variantType"), features.getMessage());
        }
    }

    @Test
    void verifiesExplicitVersionEvenWhenItEqualsTheCachedLatestVersion() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType schema = new StructType().add("id", LongType.LONG);
        DeltaLakeTestTables.commit(table, 0, "first", schema);
        TestingConnectorSession session = new TestingConnectorSession("recreated-at-version-zero");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "table").orElseThrow();
            DeltaLakeTestTables.commit(table, 0, "replacement", schema);
            DorisConnectorException identity = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest, ConnectorTimeTravelSpec.snapshotId("0")));
            Assertions.assertTrue(identity.getMessage().contains("recreated"), identity.getMessage());
        }
    }

    @Test
    void rejectsUnsupportedLatestBeforeOpeningTheTable() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType schema = new StructType().add("id", LongType.LONG);
        DeltaLakeTestTables.commit(table, 0, "first", schema);
        DeltaLakeTestTables.commit(table, 1, "first", schema, List.of(), VARIANT_PROTOCOL);
        TestingConnectorSession session = new TestingConnectorSession("unsupported-latest");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            DorisConnectorException unsupported = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.getTableHandle(session, "db", "table"));
            Assertions.assertTrue(unsupported.getMessage().contains("Unsupported Delta Lake reader features"),
                    unsupported.getMessage());
            Assertions.assertTrue(unsupported.getMessage().contains("variantType"), unsupported.getMessage());
        }
    }

    @Test
    void closedUnusedConnectorCannotAllocateResources() throws Exception {
        Connector connector = connector();
        connector.close();
        connector.close();
        Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
    }

    @Test
    void statementNamespacesAreTypePrefixed() throws Exception {
        String prefix = new DeltaLakeConnectorProvider().getType() + ".";
        int checked = 0;
        for (Field field : DeltaLakeStatementScope.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class
                    && field.getName().endsWith("_NAMESPACE")) {
                field.setAccessible(true);
                Assertions.assertTrue(((String) field.get(null)).startsWith(prefix), field.getName());
                checked++;
            }
        }
        Assertions.assertTrue(checked > 0);
    }

    private Connector connector() {
        return new DeltaLakeConnectorProvider().create(Collections.singletonMap("warehouse", warehouse.toString()),
                new RecordingConnectorContext());
    }
}
