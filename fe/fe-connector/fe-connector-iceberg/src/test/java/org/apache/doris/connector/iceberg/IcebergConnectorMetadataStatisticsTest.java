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

import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

/**
 * Unit tests for FIX-H4: {@link IcebergConnectorMetadata#getTableStatistics}.
 *
 * <p>Without the override, IcebergConnectorMetadata inherited {@code ConnectorStatisticsOps}'s
 * {@code Optional.empty()}, so every iceberg base table reported row count -1 to the FE optimizer
 * (cardinality collapses to 1, join reorder disabled, SHOW TABLE STATUS = -1). The fix mirrors
 * {@code PaimonConnectorMetadata.getTableStatistics} in structure but uses the legacy iceberg FORMULA
 * ({@code IcebergUtils.getIcebergRowCount}: currentSnapshot summary {@code total-records -
 * total-position-deletes}). Tests run against a real {@link InMemoryCatalog} table (no Mockito), the
 * {@link RecordingIcebergCatalogOps} fake serving it through the seam.
 */
public class IcebergConnectorMetadataStatisticsTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    // ---------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------

    /** A fresh (empty) v2 InMemoryCatalog table db1.t1 — v2 so row-level delete files are legal. */
    private static Table newTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(
                TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
    }

    private static DataFile dataFile(String path, long records) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path).withFileSizeInBytes(100).withRecordCount(records)
                .withFormat(FileFormat.PARQUET).build();
    }

    private static DeleteFile positionDeletes(String path, long records) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(path).withFileSizeInBytes(50).withRecordCount(records)
                .withFormat(FileFormat.PARQUET).build();
    }

    private static DeleteFile equalityDeletes(String path, long records) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(1)
                .withPath(path).withFileSizeInBytes(50).withRecordCount(records)
                .withFormat(FileFormat.PARQUET).build();
    }

    private static IcebergConnectorMetadata metadataFor(Table table, RecordingIcebergCatalogOps ops) {
        ops.table = table;
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    private static ConnectorTableHandle handle() {
        return new IcebergTableHandle("db1", "t1");
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    @Test
    public void rowCountFromTotalRecords() {
        Table table = newTable();
        table.newAppend().appendFile(dataFile("/data/f1.parquet", 100)).commit();

        Optional<ConnectorTableStatistics> stats =
                metadataFor(table, new RecordingIcebergCatalogOps()).getTableStatistics(null, handle());

        // WHY: a populated table must report its real row count, not UNKNOWN, or the CBO collapses cardinality
        // to 1 (the whole point of the fix).
        Assertions.assertTrue(stats.isPresent(), "a positive row count must be reported, not UNKNOWN");
        Assertions.assertEquals(100L, stats.get().getRowCount());
        // Legacy getIcebergRowCount computes ONLY row count; data size stays unknown (-1).
        Assertions.assertEquals(-1L, stats.get().getDataSize());
    }

    @Test
    public void rowCountNetsOutPositionDeletes() {
        Table table = newTable();
        table.newAppend().appendFile(dataFile("/data/f1.parquet", 100)).commit();
        table.newRowDelta().addDeletes(positionDeletes("/data/pd1.parquet", 30)).commit();

        Optional<ConnectorTableStatistics> stats =
                metadataFor(table, new RecordingIcebergCatalogOps()).getTableStatistics(null, handle());

        // WHY: legacy formula is total-records - total-position-deletes. MUTATION: dropping the subtraction
        // yields 100, not 70.
        Assertions.assertTrue(stats.isPresent());
        Assertions.assertEquals(70L, stats.get().getRowCount());
    }

    @Test
    public void emptyTableReportsUnknown() {
        Table table = newTable(); // no snapshot at all

        Optional<ConnectorTableStatistics> stats =
                metadataFor(table, new RecordingIcebergCatalogOps()).getTableStatistics(null, handle());

        // WHY: legacy getIcebergRowCount returns UNKNOWN when currentSnapshot() == null; the connector maps
        // that to empty so the FE keeps UNKNOWN.
        Assertions.assertFalse(stats.isPresent(), "an empty table (no snapshot) must degrade to UNKNOWN");
    }

    @Test
    public void zeroNetRowsReportUnknown() {
        Table table = newTable();
        table.newAppend().appendFile(dataFile("/data/f1.parquet", 100)).commit();
        table.newRowDelta().addDeletes(positionDeletes("/data/pd1.parquet", 100)).commit();

        Optional<ConnectorTableStatistics> stats =
                metadataFor(table, new RecordingIcebergCatalogOps()).getTableStatistics(null, handle());

        // WHY: net 0 rows must report UNKNOWN, not 0. Legacy data-table consumer was `rowCount > 0 ? .. : UNKNOWN`,
        // but the NEW consumer (PluginDrivenExternalTable.fetchRowCount) takes the value whenever >= 0 — so a 0
        // returned as Optional.of(0) would surface as 0. MUTATION: changing the `> 0` gate to `>= 0` surfaces 0.
        Assertions.assertFalse(stats.isPresent(), "0 net rows must map to UNKNOWN, not 0");
    }

    @Test
    public void systemTableReportsUnknownWithoutLoading() {
        Table table = newTable();
        table.newAppend().appendFile(dataFile("/data/f1.parquet", 100)).commit();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        IcebergConnectorMetadata metadata = metadataFor(table, ops);
        ConnectorTableHandle sysHandle = metadata.getSysTableHandle(null, handle(), "snapshots").get();

        Optional<ConnectorTableStatistics> stats = metadata.getTableStatistics(null, sysHandle);

        // WHY: legacy IcebergSysExternalTable.fetchRowCount is unconditionally UNKNOWN — a deliberate divergence
        // from paimon, which DOES report sys-table counts. A metadata table's "rows" are not data rows, and a
        // sys handle would otherwise load the BASE table and misreport its 100. MUTATION: removing the
        // isSystemTable() guard loads the base table -> present(100); the empty assertion fails. The null
        // lastLoadTable additionally proves the guard short-circuits BEFORE the seam load.
        Assertions.assertFalse(stats.isPresent(), "system tables must report UNKNOWN (legacy parity)");
        Assertions.assertNull(ops.lastLoadTable, "the sys-table guard must short-circuit before loadTable");
    }

    @Test
    public void loadFailureDegradesToUnknown() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.throwOnLoadTable = true;
        IcebergConnectorMetadata metadata =
                new IcebergConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());

        // WHY: a statistics miss must NEVER break query planning. MUTATION: letting the exception propagate
        // makes assertDoesNotThrow fail.
        Optional<ConnectorTableStatistics> stats = Assertions.assertDoesNotThrow(
                () -> metadata.getTableStatistics(null, handle()));
        Assertions.assertFalse(stats.isPresent(), "a load failure must degrade to UNKNOWN, not throw");
    }

    @Test
    public void equalityDeletesDoNotGateTableStatistics() {
        Table table = newTable();
        table.newAppend().appendFile(dataFile("/data/f1.parquet", 100)).commit();
        table.newRowDelta().addDeletes(equalityDeletes("/data/ed1.parquet", 5)).commit();

        Optional<ConnectorTableStatistics> stats =
                metadataFor(table, new RecordingIcebergCatalogOps()).getTableStatistics(null, handle());

        // WHY: table statistics use total-records - total-position-deletes ONLY. The COUNT(*)-pushdown formula
        // (IcebergScanPlanProvider.getCountFromSnapshot) returns -1 when equality deletes exist; misusing THAT
        // here would report UNKNOWN for any table with equality deletes. MUTATION: routing through the equality
        // gate -> empty; this asserts present(100) (equality deletes are NOT subtracted by the legacy stat).
        Assertions.assertTrue(stats.isPresent(), "equality deletes must not blank out table statistics");
        Assertions.assertEquals(100L, stats.get().getRowCount());
    }
}
