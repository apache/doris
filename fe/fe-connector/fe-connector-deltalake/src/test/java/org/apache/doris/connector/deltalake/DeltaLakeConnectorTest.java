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
import org.apache.doris.connector.spi.ConnectorSecuritySurface;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

class DeltaLakeConnectorTest {
    private static final Map<String, Object> VARIANT_PROTOCOL = Map.of(
            "minReaderVersion", 3, "minWriterVersion", 7,
            "readerFeatures", List.of("variantType"), "writerFeatures", List.of("variantType"));
    private static final Map<String, Object> ICT_PROTOCOL = Map.of(
            "minReaderVersion", 1, "minWriterVersion", 7,
            "writerFeatures", List.of("inCommitTimestamp"));

    @TempDir
    Path warehouse;

    @Test
    void declaresUcCredentialsAndEndpoints() {
        ConnectorSecuritySurface surface = new DeltaLakeConnectorProvider().securitySurface();
        Assertions.assertTrue(surface.sensitiveKeys().containsAll(
                List.of("deltalake.uc.token", "deltalake.uc.oauth.client-secret")));
        Assertions.assertTrue(surface.ssrfEndpointKeys().containsAll(
                List.of("deltalake.uc.uri", "deltalake.uc.oauth.token-uri")));
    }

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
    void explicitTimeTravelRechecksLatestPolicyEvenForCachedVersions() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType schema = new StructType().add("id", LongType.LONG);
        Map<String, Object> protocol = Map.of("minReaderVersion", 1, "minWriterVersion", 2);
        Map<String, String> longWindow = Map.of("delta.deletedFileRetentionDuration", "interval 30 days");
        long old = System.currentTimeMillis() - Duration.ofDays(10).toMillis();
        DeltaLakeTestTables.commit(table, 0, "first", schema, List.of(), protocol, longWindow, null, old);
        TestingConnectorSession initial = new TestingConnectorSession("initial");
        TestingConnectorSession restricted = new TestingConnectorSession("restricted");
        TestingConnectorSession extended = new TestingConnectorSession("extended");
        List<ConnectorTimeTravelSpec> selectors = List.of(ConnectorTimeTravelSpec.snapshotId("0"),
                ConnectorTimeTravelSpec.timestamp(Long.toString(old), true));
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(initial)) {
            ConnectorTableHandle stale = metadata.getTableHandle(initial, "db", "table").orElseThrow();
            ConnectorMvccSnapshot initialPin = metadata.beginQuerySnapshot(initial, stale).orElseThrow();
            DeltaLakeTestTables.commit(table, 1, "first", schema, List.of(), protocol, Map.of(), null, old + 1000);
            connector.invalidateTable("db", "table");
            ConnectorTableHandle current = metadata.getTableHandle(restricted, "db", "table").orElseThrow();
            // Rehydrating an implicit pin must neither enforce nor bypass explicit time-travel retention.
            metadata.applySnapshot(restricted, current, initialPin);
            for (ConnectorTimeTravelSpec selector : selectors) {
                DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                        () -> metadata.resolveTimeTravel(restricted, stale, selector));
                Assertions.assertTrue(error.getMessage().contains("beyond delta.deletedFileRetentionDuration"));
            }
            DeltaLakeTestTables.commit(table, 2, "first", schema, List.of(), protocol, longWindow, null, old + 2000);
            for (ConnectorTimeTravelSpec selector : selectors) {
                Assertions.assertEquals(0,
                        metadata.resolveTimeTravel(extended, current, selector).orElseThrow().getSnapshotId());
            }
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
        }
    }

    @Test
    void resolvesTimeAsOfByCommitFileTimeAndReusesStatementSnapshot() throws Exception {
        Path table = warehouse.resolve("db/table");
        LocalDate day = LocalDate.now(ZoneOffset.UTC).minusDays(1);
        long t0 = Instant.parse(day + "T00:00:00Z").toEpochMilli();
        long t1 = Instant.parse(day + "T01:00:00Z").toEpochMilli();
        long t2 = Instant.parse(day + "T02:00:00Z").toEpochMilli();
        Path v0File = DeltaLakeTestTables.commit(table, 0, "first",
                new StructType().add("old_name", LongType.LONG), List.of(),
                Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t0);
        DeltaLakeTestTables.commit(table, 1, "first",
                new StructType().add("mid_name", StringType.STRING), List.of(),
                Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t1);
        DeltaLakeTestTables.commit(table, 2, "first",
                new StructType().add("new_name", LongType.LONG), List.of(),
                Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t2);
        TestingConnectorSession session = new TestingConnectorSession("time-as-of-read", "Asia/Shanghai");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "table").orElseThrow();

            // Exact epoch millis matches version 0; applySnapshot reuses the statement-scoped snapshot without re-reading.
            ConnectorMvccSnapshot atT0 = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.timestamp(Long.toString(t0), true)).orElseThrow();
            Assertions.assertEquals(0, atT0.getSnapshotId());
            Assertions.assertEquals(0, atT0.getSchemaId());
            Assertions.assertTrue(atT0.isSchemaRetained());
            Assertions.assertEquals("old_name", metadata.getTableSchema(session, latest, atT0)
                    .getColumns().get(0).getName());
            Assertions.assertEquals(Set.of("old_name"), metadata.getColumnHandles(session, latest, atT0).keySet());

            // Session-timezone datetime between t0 (08:00+08) and t1 (09:00+08) resolves to version 0.
            ConnectorMvccSnapshot betweenT0AndT1 = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.timestamp(day + " 08:30:00", false)).orElseThrow();
            Assertions.assertEquals(0, betweenT0AndT1.getSnapshotId());

            // Explicit UTC offset ignores session timezone and resolves to version 1.
            ConnectorMvccSnapshot atT1 = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.timestamp(day + "T01:00:00Z", false)).orElseThrow();
            Assertions.assertEquals(1, atT1.getSnapshotId());
            Assertions.assertEquals(Set.of("mid_name"), metadata.getColumnHandles(session, latest, atT1).keySet());
            Assertions.assertSame(metadata.applySnapshot(session, latest, atT1),
                    metadata.applySnapshot(session, latest, atT1));

            // Commits added after the statement's latest handle was opened are visible to FOR TIME AS OF.
            long t3 = Instant.parse(day + "T03:00:00Z").toEpochMilli();
            DeltaLakeTestTables.commit(table, 3, "first",
                    new StructType().add("later", LongType.LONG), List.of(),
                    Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t3);
            ConnectorMvccSnapshot atLater = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.timestamp(day + "T03:00:00Z", false)).orElseThrow();
            Assertions.assertEquals(3, atLater.getSnapshotId());
            Assertions.assertEquals(Set.of("later"), metadata.getColumnHandles(session, latest, atLater).keySet());
            Assertions.assertEquals(2, metadata.beginQuerySnapshot(session, latest).orElseThrow().getSnapshotId());

            // Remove version 0 log file to prove applySnapshot reuses the handle seeded by resolveTimeTravel.
            Files.delete(v0File);
            Assertions.assertEquals(Set.of("old_name"), metadata.getColumnHandles(session, latest, atT0).keySet());
        }
    }

    @Test
    void resolvesTimeAsOfUsingInCommitTimestampsOverFileMtime() throws Exception {
        Path table = warehouse.resolve("db/ict_table");
        Map<String, String> ictConfig = Map.of("delta.enableInCommitTimestamps", "true");
        LocalDate day = LocalDate.now(ZoneOffset.UTC).minusDays(1);
        long ict0 = Instant.parse(day + "T00:00:00Z").toEpochMilli();
        long ict1 = Instant.parse(day + "T01:00:00Z").toEpochMilli();
        // Deliberately invert file modification times far in the future so only ICT yields the right versions.
        long mtime0 = Instant.parse(day.plusDays(30) + "T10:00:00Z").toEpochMilli();
        long mtime1 = Instant.parse(day.plusDays(30) + "T09:00:00Z").toEpochMilli();
        DeltaLakeTestTables.commit(table, 0, "ict-id",
                new StructType().add("v0_col", LongType.LONG), List.of(), ICT_PROTOCOL, ictConfig, ict0, mtime0);
        DeltaLakeTestTables.commit(table, 1, "ict-id",
                new StructType().add("v1_col", StringType.STRING), List.of(), ICT_PROTOCOL, ictConfig, ict1, mtime1);

        TestingConnectorSession session = new TestingConnectorSession("ict-time-as-of");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "ict_table").orElseThrow();
            ConnectorMvccSnapshot v0 = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.timestamp(day + " 00:30:00", false)).orElseThrow();
            Assertions.assertEquals(0, v0.getSnapshotId());
            Assertions.assertEquals(Set.of("v0_col"), metadata.getColumnHandles(session, latest, v0).keySet());

            ConnectorMvccSnapshot v1 = metadata.resolveTimeTravel(session, latest,
                    ConnectorTimeTravelSpec.timestamp(day + " 01:00:00", false)).orElseThrow();
            Assertions.assertEquals(1, v1.getSnapshotId());
            Assertions.assertEquals(Set.of("v1_col"), metadata.getColumnHandles(session, latest, v1).keySet());
        }
    }

    @Test
    void rejectsInvalidEarlyRecreatedOrUnsupportedTimestamps() throws Exception {
        Path table = warehouse.resolve("db/table");
        StructType schema = new StructType().add("id", LongType.LONG);
        long t0 = Instant.parse("2026-11-24T00:00:00Z").toEpochMilli();
        long t1 = Instant.parse("2026-11-24T01:00:00Z").toEpochMilli();
        long t2 = Instant.parse("2026-11-24T02:00:00Z").toEpochMilli();
        DeltaLakeTestTables.commit(table, 0, "another-table", schema, List.of(),
                Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t0);
        DeltaLakeTestTables.commit(table, 1, "first", schema, List.of(), VARIANT_PROTOCOL,
                Collections.emptyMap(), null, t1);
        DeltaLakeTestTables.commit(table, 2, "first", schema, List.of(),
                Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t2);
        TestingConnectorSession session = new TestingConnectorSession("timestamp-errors");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle latest = metadata.getTableHandle(session, "db", "table").orElseThrow();

            DorisConnectorException malformed = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest,
                            ConnectorTimeTravelSpec.timestamp("not-a-timestamp", false)));
            Assertions.assertTrue(malformed.getMessage().contains("FOR TIME AS OF"), malformed.getMessage());

            KernelException beforeEarliest = Assertions.assertThrows(KernelException.class,
                    () -> metadata.resolveTimeTravel(session, latest,
                            ConnectorTimeTravelSpec.timestamp(Long.toString(t0 - 1_000), true)));
            Assertions.assertTrue(beforeEarliest.getMessage().contains("before the earliest"),
                    beforeEarliest.getMessage());

            KernelException afterLatest = Assertions.assertThrows(KernelException.class,
                    () -> metadata.resolveTimeTravel(session, latest,
                            ConnectorTimeTravelSpec.timestamp(Long.toString(t2 + 1_000), true)));
            Assertions.assertTrue(afterLatest.getMessage().contains("after the latest"),
                    afterLatest.getMessage());

            DorisConnectorException identity = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest,
                            ConnectorTimeTravelSpec.timestamp(Long.toString(t0), true)));
            Assertions.assertTrue(identity.getMessage().contains("recreated"), identity.getMessage());

            DorisConnectorException features = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest,
                            ConnectorTimeTravelSpec.timestamp(Long.toString(t1), true)));
            Assertions.assertTrue(features.getMessage().contains("variantType"), features.getMessage());

            // Table recreated at latest version between getTableHandle and resolveTimeTravel.
            DeltaLakeTestTables.commit(table, 2, "replacement", schema, List.of(),
                    Map.of("minReaderVersion", 1, "minWriterVersion", 2), Collections.emptyMap(), null, t2);
            DorisConnectorException recreatedLatest = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.resolveTimeTravel(session, latest,
                            ConnectorTimeTravelSpec.timestamp(Long.toString(t2), true)));
            Assertions.assertTrue(recreatedLatest.getMessage().contains("recreated"), recreatedLatest.getMessage());
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
