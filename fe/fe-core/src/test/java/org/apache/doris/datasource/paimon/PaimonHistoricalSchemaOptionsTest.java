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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.paimon.PaimonLatestSnapshotProjectionLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonPartitionInfoLoader;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonHistoricalSchemaOptionsTest {
    @Test
    public void historicalOptionsProjectionKeepsSelectedSchemaAndSnapshot(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "projection_history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT()).primaryKey("id")
                    .option("bucket", "1").option("sequence.field", "value")
                    .option("file.format", "parquet").build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            append(original, GenericRow.of(1, 10));
            original.createTag("before_rename", 1L);
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("value", "renamed_value")), false);
            FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
            append(latest, GenericRow.of(1, 20));
            PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                    new PaimonPartitionInfoLoader(),
                    (mapping, schemaId, generation, retainedTable) -> {
                        Assertions.assertEquals(original.schema().id(), schemaId);
                        Assertions.assertEquals(Arrays.asList("id", "value"),
                                retainedTable.rowType().getFieldNames());
                        return new PaimonSchemaCacheValue(Collections.emptyList(), Collections.emptyList(), null);
                    });
            for (Map<String, String> selector : Arrays.asList(
                    Collections.singletonMap("scan.snapshot-id", "1"),
                    Collections.singletonMap("scan.tag-name", "before_rename"))) {
                FileStoreTable selected = (FileStoreTable) PaimonScanParams.applyOptions(latest, selector);
                PrivilegeChecker checker = Mockito.mock(PrivilegeChecker.class);
                Mockito.doThrow(new SecurityException("read-only principal")).when(checker)
                        .assertCanInsert(Mockito.any());
                selected = PrivilegedFileStoreTable.wrap(selected, checker, id);
                PaimonSnapshotCacheValue projection = loader.load(
                        NameMapping.createForTest("db", "projection_history"), selected);
                Assertions.assertEquals(1L, projection.getSnapshot().getSnapshotId());
                Assertions.assertEquals(original.schema().id(), projection.getSnapshot().getSchemaId());
                Assertions.assertEquals(Collections.singletonList("1:10"),
                        readRows(projection.getSnapshot().getTable()));
            }
        }
    }

    @Test
    public void historicalAppendReadKeepsCompatibleBucketMode(@TempDir Path warehouse) throws Exception {
        for (int bucket : new int[] {-1, 2}) {
            try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                    new org.apache.paimon.fs.Path(warehouse.resolve("buckets_" + bucket).toUri()))) {
                catalog.createDatabase("db", false);
                Identifier id = Identifier.create("db", "append_history");
                Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT())
                        .column("value", DataTypes.INT()).option("bucket", Integer.toString(bucket))
                        .option("file.format", "parquet");
                if (bucket > 0) {
                    schema.option("bucket-key", "id");
                }
                catalog.createTable(id, schema.build(), false);
                FileStoreTable original = (FileStoreTable) catalog.getTable(id);
                List<String> expected = new ArrayList<>();
                BatchWriteBuilder builder = original.newBatchWriteBuilder();
                try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                    for (int value = 1; value <= 16; value++) {
                        write.write(GenericRow.of(value, value * 10));
                        expected.add(value + ":" + value * 10);
                    }
                    commit.commit(write.prepareCommit());
                }
                Collections.sort(expected);
                if (bucket > 0) {
                    Assertions.assertTrue(original.newReadBuilder().newScan().plan().splits().stream()
                            .map(split -> ((org.apache.paimon.table.source.DataSplit) split).bucket())
                            .distinct().count() > 1);
                }
                catalog.alterTable(id, Collections.singletonList(
                        SchemaChange.addColumn("extra", DataTypes.INT())), false);
                FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
                TableSchema historical = original.schemaManager().schema(original.schema().id());
                Map<String, String> legacyOptions = new HashMap<>(historical.options());
                // Older append schemas can advertise fixed buckets without an explicit bucket key.
                legacyOptions.put("bucket", Integer.toString(bucket > 0 ? bucket : 1));
                legacyOptions.remove("bucket-key");
                if (bucket > 0) {
                    legacyOptions.put("bucket-key", "");
                }
                original.fileIO().overwriteFileUtf8(original.schemaManager().toSchemaPath(historical.id()),
                        historical.copy(legacyOptions).toString());

                Table selected = PaimonScanParams.applyOptions(latest,
                        Collections.singletonMap("scan.snapshot-id", "1"));
                Assertions.assertEquals(Arrays.asList("id", "value"), selected.rowType().getFieldNames());
                Assertions.assertEquals("-1", selected.options().get("bucket"));
                Assertions.assertEquals(expected, readRows(selected));
            }
        }
    }

    @Test
    public void snapshotReadDoesNotRestoreRemovedStartupMode(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "removed_startup_mode");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT()).primaryKey("id")
                    .option("bucket", "1").option("file.format", "parquet")
                    .option("scan.mode", "latest-full").build(), false);
            append((FileStoreTable) catalog.getTable(id), GenericRow.of(1, 10));
            catalog.alterTable(id, Collections.singletonList(SchemaChange.removeOption("scan.mode")), false);
            Table latest = catalog.getTable(id);
            Assertions.assertFalse(latest.options().containsKey("scan.mode"));
            Map<String, String> selector = Collections.singletonMap("scan.snapshot-id", "1");
            // Removing a startup mode must remain effective when a historical schema is restored.
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(latest.copy(selector)));
            Table historical = PaimonScanParams.copyWithHistoricalSchema(latest, selector);
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(historical));
        }
    }

    @Test
    public void historicalFallbackPriorityIsStableAt64(@TempDir Path warehouse) throws Exception {
        checkHistoricalFallbackPriority(warehouse, 64);
    }

    @Test
    public void historicalFallbackPriorityIsStableAt128(@TempDir Path warehouse) throws Exception {
        checkHistoricalFallbackPriority(warehouse, 128);
    }

    private void checkHistoricalFallbackPriority(Path warehouse, int capacity) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "branch_priority_history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT().notNull()).primaryKey("id", "value").partitionKeys("id")
                    .option("bucket", "1").option("file.format", "parquet").build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            original.createBranch("backup");
            Identifier backupId = new Identifier("db", "branch_priority_history", "backup");
            append((FileStoreTable) catalog.getTable(backupId), GenericRow.of(1, 20));
            catalog.alterTable(id, Arrays.asList(SchemaChange.setOption("scan.primary-branch", "backup"),
                    SchemaChange.setOption("scan.manifest.parallelism", "128")), false);
            FallbackReadFileStoreTable primary = (FallbackReadFileStoreTable) catalog.getTable(id);
            append(primary.wrapped(), GenericRow.of(1, 10));
            Assertions.assertEquals(Collections.singletonList("1:20"), readRows(primary));
            catalog.alterTable(id, Arrays.asList(SchemaChange.removeOption("scan.primary-branch"),
                    SchemaChange.setOption("scan.fallback-branch", "backup")), false);
            FallbackReadFileStoreTable latest = (FallbackReadFileStoreTable) catalog.getTable(id);
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(latest));
            Table historical = PaimonScanParams.copyWithHistoricalSchema(latest,
                    Collections.singletonMap("scan.snapshot-id", "1"));
            Assertions.assertEquals("backup", historical.options().get("scan.fallback-branch"));
            Assertions.assertFalse(historical.options().containsKey("scan.primary-branch"));
            // The same overlapping partition must choose the same branch on different FE capacities.
            Table normalized = PaimonReaderOptions.runtimeSafeTable(historical, capacity);
            Assertions.assertEquals(Integer.toString(capacity), normalized.options().get("scan.manifest.parallelism"));
            Assertions.assertEquals("backup", normalized.options().get("scan.fallback-branch"));
            Assertions.assertFalse(normalized.options().containsKey("scan.primary-branch"));
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(normalized));
        }
    }

    @Test
    public void snapshotUsesHistoricalSequenceFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "snapshot");
    }

    @Test
    public void timestampUsesHistoricalSequenceFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "timestamp");
    }

    @Test
    public void tagUsesHistoricalSequenceFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "tag");
    }

    @Test
    public void optionsUsesHistoricalSequenceFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "options");
    }

    @Test
    public void currentReadUsesRenamedSequenceFieldFromWarmTable(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "current");
    }

    @Test
    public void snapshotUsesHistoricalAggregationFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, true, "snapshot");
    }

    @Test
    public void timestampUsesHistoricalAggregationFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, true, "timestamp");
    }

    @Test
    public void tagUsesHistoricalAggregationFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, true, "tag");
    }

    @Test
    public void optionsUsesHistoricalAggregationFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, true, "options");
    }

    @Test
    public void currentReadUsesRenamedAggregationFieldFromWarmTable(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, true, "current");
    }

    @Test
    public void readerOptionsKeepSchemaOnlySequenceRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "current_options");
    }

    @Test
    public void readerOptionsKeepSchemaOnlyAggregationRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, true, "current_options");
    }

    @Test
    public void systemOptionsUsesHistoricalSequenceFieldAfterRename(@TempDir Path warehouse) throws Exception {
        checkRenamedField(warehouse, false, "system");
    }

    @Test
    public void historicalSchemaKeepsExplicitCatalogReaderOption(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "reader_history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT().notNull()).primaryKey("id", "value").partitionKeys("id")
                    .option("bucket", "1").option("sequence.field", "value")
                    .option("read.batch-size", "64").option("file.format", "parquet").build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            append(original, GenericRow.of(1, 10));
            catalog.alterTable(id, Arrays.asList(SchemaChange.renameColumn("value", "renamed_value"),
                    SchemaChange.setOption("read.batch-size", "32")), false);
            Map<String, String> catalogOptions = Collections.singletonMap("read.batch-size", "32");
            Table decorated = PaimonScanParams.copyWithHistoricalSchema(catalog.getTable(id), catalogOptions);
            Table scan = PaimonScanParams.copyWithHistoricalSchema(decorated,
                    PaimonScanParams.withCatalogOptions(Collections.singletonMap("scan.snapshot-id", "1"),
                            catalogOptions));
            // Equal current/catalog values still carry an explicit override of the historical default.
            Assertions.assertEquals("32", scan.options().get("read.batch-size"));
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(scan));
        }
    }

    @Test
    public void fallbackBranchesRestoreIndependentHistoricalOptions(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "fallback_history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT()).primaryKey("id")
                    .option("bucket", "1").option("sequence.field", "value")
                    .option("file.format", "parquet").build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            original.createBranch("backup");
            Identifier backupId = new Identifier("db", "fallback_history", "backup");
            FileStoreTable backup = (FileStoreTable) catalog.getTable(backupId);
            append(backup, GenericRow.of(2, 8));
            append(original, GenericRow.of(1, 10));
            catalog.alterTable(backupId, Collections.singletonList(
                    SchemaChange.renameColumn("value", "renamed_value")), false);
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("value", "renamed_value")), false);
            FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
            FileStoreTable latestBackup = (FileStoreTable) catalog.getTable(backupId);
            append(latestBackup, GenericRow.of(2, 18));
            append(latest, GenericRow.of(1, 20));
            FallbackReadFileStoreTable pair = new FallbackReadFileStoreTable(latest, latestBackup, false);
            Map<String, String> options = new HashMap<>();
            options.put("scan.snapshot-id", "1");
            options.put("branch", "main");
            FallbackReadFileStoreTable historical = (FallbackReadFileStoreTable)
                    PaimonScanParams.copyWithHistoricalSchema(pair, options);
            Assertions.assertEquals("value", historical.wrapped().options().get("sequence.field"));
            Assertions.assertEquals("value", historical.other().options().get("sequence.field"));
            Assertions.assertEquals("backup", historical.other().coreOptions().branch());
            Assertions.assertSame(latest.catalogEnvironment(), historical.wrapped().catalogEnvironment());
            Assertions.assertSame(latestBackup.catalogEnvironment(), historical.other().catalogEnvironment());
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(historical.wrapped()));
            Assertions.assertEquals(Collections.singletonList("2:8"), readRows(historical.other()));
        }
    }

    @Test
    public void timestampOnEmptyTableKeepsSdkBehavior(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "empty_history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            Table table = catalog.getTable(id);
            Map<String, String> options = Collections.singletonMap("scan.timestamp-millis", "1");
            Table selected = PaimonScanParams.copyWithHistoricalSchema(table, options);
            Assertions.assertEquals(table.copy(options).rowType(), selected.rowType());
            IllegalArgumentException expected = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> readRows(table.copy(options)));
            IllegalArgumentException actual = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> readRows(selected));
            Assertions.assertEquals(expected.getMessage(), actual.getMessage());
        }
    }

    @Test
    public void serializedSchemaOnlyRenameKeepsStatementSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkSerializedTransport(warehouse, true, false);
    }

    @Test
    public void serializedHistoricalReadKeepsHistoricalSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkSerializedTransport(warehouse, false, false);
    }

    @Test
    public void serializedAggregationRenameKeepsStatementSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkSerializedTransport(warehouse, true, true);
    }

    @Test
    public void serializedAggregationHistoryKeepsHistoricalSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkSerializedTransport(warehouse, false, true);
    }

    private void checkSerializedTransport(Path warehouse, boolean current, boolean aggregate) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "transport_history");
            Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT()).primaryKey("id")
                    .option("bucket", "1").option("file.format", "parquet");
            if (aggregate) {
                schema.option("merge-engine", "aggregation").option("fields.value.aggregate-function", "sum");
            } else {
                schema.option("sequence.field", "value");
            }
            catalog.createTable(id, schema.build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(id);
            append(original, GenericRow.of(1, 10));
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("value", "renamed_value")), false);
            FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
            FileStoreTable scan = (FileStoreTable) (current
                    ? latest.copyWithoutTimeTravel(Collections.singletonMap("scan.snapshot-id", "1"))
                    : PaimonScanParams.copyWithHistoricalSchema(latest,
                            Collections.singletonMap("scan.snapshot-id", "1")));
            String expectedName = current ? "renamed_value" : "value";
            Assertions.assertEquals(Arrays.asList("id", expectedName), scan.rowType().getFieldNames());
            // The 4.2 scan node serializes its processed table directly, retaining the bound schema.
            FileStoreTable backend = InstantiationUtil.deserializeObject(
                    InstantiationUtil.serializeObject(scan),
                    getClass().getClassLoader());
            // Unlike the connector rebuild path, 4.2 transports the catalog loader with the table.
            Assertions.assertNotNull(backend.catalogEnvironment().catalogLoader());
            Assertions.assertEquals(scan.schema(), backend.schema());
            Assertions.assertEquals("1", backend.options().get("scan.snapshot-id"));
            if (aggregate) {
                Assertions.assertEquals("sum", backend.options().get("fields." + expectedName + ".aggregate-function"));
            } else {
                Assertions.assertEquals(expectedName, backend.options().get("sequence.field"));
            }
            Assertions.assertEquals(Collections.singletonList("1:10"), readRows(backend));
        }
    }

    private void checkRenamedField(Path warehouse, boolean aggregate, String selector) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "field_history");
            Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("value", DataTypes.INT()).primaryKey("id")
                    .option("bucket", "1").option("file.format", "parquet");
            if (aggregate) {
                schema.option("merge-engine", "aggregation")
                        .option("fields.value.aggregate-function", "sum");
            } else {
                schema.option("sequence.field", "value");
            }
            catalog.createTable(id, schema.build(), false);
            FileStoreTable warm = (FileStoreTable) catalog.getTable(id);
            append(warm, GenericRow.of(1, 10));
            // A second value proves that the configured merge rule remains effective during reads.
            append(warm, GenericRow.of(1, 5));
            long snapshotId = warm.snapshotManager().latestSnapshotId();
            long timestamp = warm.snapshotManager().snapshot(snapshotId).timeMillis();
            warm.createTag("before_rename", snapshotId);
            // Read-optimized views can omit un-compacted files; preserve their real SDK baseline.
            List<String> systemBaseline = "system".equals(selector)
                    ? readRows(SystemTableLoader.load("ro", warm)) : Collections.emptyList();
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("value", "renamed_value")), false);
            FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
            boolean readerOptions = "current_options".equals(selector);
            if (!readerOptions) {
                append(latest, GenericRow.of(1, 20));
            }
            String historicalRow = aggregate ? "1:15" : "1:10";
            String latestRow = readerOptions ? historicalRow : aggregate ? "1:35" : "1:20";
            Assertions.assertEquals(Collections.singletonList(latestRow), readRows(latest));

            boolean current = selector.startsWith("current");
            Table scan = current ? latest.copyWithoutTimeTravel(
                    Collections.singletonMap("scan.snapshot-id", Long.toString(latest.latestSnapshot().get().id())))
                    : PaimonScanParams.applyOptions(latest, selector(selector, snapshotId, timestamp));
            if (readerOptions) {
                scan = PaimonScanParams.applyOptions(scan, PaimonScanParams.pinOptionsToSnapshot(
                        Collections.singletonMap("read.batch-size", "8"), latest.latestSnapshot().get().id()));
            }
            if ("system".equals(selector)) {
                scan = SystemTableLoader.load("ro", (FileStoreTable) scan);
            }
            String expectedName = current ? "renamed_value" : "value";
            String expectedRow = current ? latestRow : historicalRow;
            Assertions.assertEquals(Arrays.asList("id", expectedName), scan.rowType().getFieldNames());
            if (aggregate) {
                Assertions.assertEquals("sum", scan.options().get("fields." + expectedName + ".aggregate-function"));
            } else {
                Assertions.assertEquals(expectedName, scan.options().get("sequence.field"));
            }
            List<String> expectedRows = "system".equals(selector)
                    ? systemBaseline : Collections.singletonList(expectedRow);
            Assertions.assertEquals(expectedRows, readRows(scan));
            Assertions.assertEquals(expectedRows, readRows(InstantiationUtil.deserializeObject(
                    InstantiationUtil.serializeObject(scan), getClass().getClassLoader())));
        }
    }

    private static Map<String, String> selector(String selector, long snapshotId, long timestamp) {
        switch (selector) {
            case "timestamp":
                return Collections.singletonMap("scan.timestamp-millis", Long.toString(timestamp));
            case "tag":
                return Collections.singletonMap("scan.tag-name", "before_rename");
            case "snapshot":
            case "system":
            case "options":
                return Collections.singletonMap("scan.snapshot-id", Long.toString(snapshotId));
            default:
                throw new IllegalArgumentException("Unexpected test selector: " + selector);
        }
    }

    private static void append(FileStoreTable table, GenericRow row) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }

    private static List<String> readRows(Table table) throws Exception {
        List<String> rows = new ArrayList<>();
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = table.newReadBuilder().newRead().createReader(split)) {
                reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getInt(1)));
            }
        }
        Collections.sort(rows);
        return rows;
    }
}
