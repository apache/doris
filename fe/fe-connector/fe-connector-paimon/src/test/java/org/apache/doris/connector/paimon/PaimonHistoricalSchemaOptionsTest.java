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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonHistoricalSchemaOptionsTest {
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
            Map<String, String> properties = Collections.singletonMap("paimon.table-option.read.batch-size", "32");
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(
                    catalog, Collections.singletonMap("read.batch-size", "32"));
            PaimonCatalogProperties catalogProperties = PaimonCatalogProperties.of(properties);
            PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                    ops, catalogProperties, new RecordingConnectorContext());
            PaimonTableHandle handle = (PaimonTableHandle) metadata.getTableHandle(null, "db", "reader_history").get();
            ConnectorMvccSnapshot snapshot = metadata.resolveTimeTravel(null, handle,
                    ConnectorTimeTravelSpec.snapshotId("1")).get();
            PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snapshot);
            Assertions.assertEquals(Collections.singletonList("id=1"), metadata.listPartitionNames(null, pinned));
            Assertions.assertTrue(metadata.getTableStatistics(null, handle, snapshot).isPresent());
            Table scan = new PaimonScanPlanProvider(catalogProperties, ops).resolveScanTable(pinned);
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
    public void catalogBackedSchemaOnlyRenameKeepsStatementSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkCatalogBackedTransport(warehouse, true, false);
    }

    @Test
    public void catalogBackedHistoricalReadKeepsHistoricalSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkCatalogBackedTransport(warehouse, false, false);
    }

    @Test
    public void catalogBackedAggregationRenameKeepsStatementSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkCatalogBackedTransport(warehouse, true, true);
    }

    @Test
    public void catalogBackedAggregationHistoryKeepsHistoricalSchemaOnBackend(@TempDir Path warehouse)
            throws Exception {
        checkCatalogBackedTransport(warehouse, false, true);
    }

    private void checkCatalogBackedTransport(Path warehouse, boolean current, boolean aggregate) throws Exception {
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
            CatalogEnvironment environment = new CatalogEnvironment(id, null, () -> catalog,
                    null, null, null, false, false);
            latest = FileStoreTableFactory.createWithoutFallbackBranch(latest.fileIO(), latest.location(),
                    latest.schema(), new Options(), environment);
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonCatalogProperties properties = PaimonCatalogProperties.of(Collections.emptyMap());
            PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                    ops, properties, new RecordingConnectorContext());
            PaimonTableHandle handle = new PaimonTableHandle("db", "transport_history",
                    Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(latest);
            ConnectorMvccSnapshot snapshot = current ? metadata.beginQuerySnapshot(null, handle).get()
                    : metadata.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.snapshotId("1")).get();
            PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snapshot);
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(properties, ops);
            FileStoreTable scan = (FileStoreTable) provider.resolveScanTable(pinned);
            String expectedName = current ? "renamed_value" : "value";
            Assertions.assertNotNull(scan.catalogEnvironment().catalogLoader());
            Assertions.assertEquals(Arrays.asList("id", expectedName), scan.rowType().getFieldNames());
            // A filesystem-only fixture skips loader removal; serialize the catalog-backed path too.
            FileStoreTable backend = InstantiationUtil.deserializeObject(
                    InstantiationUtil.serializeObject(provider.tableForBackend(pinned, scan)),
                    getClass().getClassLoader());
            Assertions.assertNull(backend.catalogEnvironment().catalogLoader());
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
            append(latest, GenericRow.of(1, 20));
            String historicalRow = aggregate ? "1:15" : "1:10";
            String latestRow = aggregate ? "1:35" : "1:20";
            Assertions.assertEquals(Collections.singletonList(latestRow), readRows(latest));

            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonCatalogProperties properties = PaimonCatalogProperties.of(Collections.emptyMap());
            PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                    ops, properties, new RecordingConnectorContext());
            PaimonTableHandle handle = new PaimonTableHandle("db", "field_history",
                    Collections.emptyList(), Collections.emptyList());
            boolean current = "current".equals(selector);
            handle.setPaimonTable(current ? warm : latest);
            // Current reads rebind a warm table; explicit selectors must restore historical field options.
            ConnectorMvccSnapshot snapshot = current
                    ? metadata.beginQuerySnapshot(null, handle).get()
                    : metadata.resolveTimeTravel(null, handle, selector(selector, snapshotId, timestamp)).get();
            if ("system".equals(selector)) {
                handle = (PaimonTableHandle) metadata.getSysTableHandle(null, handle, "ro").get();
            }
            Assertions.assertEquals(current ? "renamed_value" : "value",
                    metadata.getTableSchema(null, handle, snapshot).getColumns().get(1).getName());
            PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snapshot);
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(properties, ops);
            Table scan = provider.resolveScanTable(pinned);
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
            Assertions.assertEquals(expectedRows, readRows(provider.tableForBackend(pinned, scan)));
        }
    }

    private static ConnectorTimeTravelSpec selector(String selector, long snapshotId, long timestamp) {
        switch (selector) {
            case "snapshot":
                return ConnectorTimeTravelSpec.snapshotId(Long.toString(snapshotId));
            case "timestamp":
                return ConnectorTimeTravelSpec.timestamp(Long.toString(timestamp), true);
            case "tag":
                return ConnectorTimeTravelSpec.tag("before_rename");
            case "system":
            case "options":
                return ConnectorTimeTravelSpec.options(
                        Collections.singletonMap("scan.snapshot-id", Long.toString(snapshotId)));
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
