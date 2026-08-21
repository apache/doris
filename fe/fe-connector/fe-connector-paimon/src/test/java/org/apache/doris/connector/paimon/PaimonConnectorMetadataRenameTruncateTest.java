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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@code ALTER TABLE ... RENAME} and {@code TRUNCATE TABLE} for {@link PaimonConnectorMetadata}, exercised
 * against a REAL {@link FileSystemCatalog} (no mock, no env gate): builds a table, inserts data, renames or
 * truncates it through the connector, and reads the rows BACK (or re-lists the catalog) to prove the effect.
 *
 * <p>Each test uses its own {@code @TempDir} warehouse, so they are independent and safe to re-run.
 */
public class PaimonConnectorMetadataRenameTruncateTest {

    private static final Identifier TABLE = Identifier.create("db", "t");

    private static PaimonConnectorMetadata metadata(Catalog catalog) {
        return new PaimonConnectorMetadata(
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog),
                PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext());
    }

    /** Two-column table partitioned by STRING {@code region}: (id INT, region STRING). */
    private static Catalog regionPartitionedCatalog(Path warehouse) throws Exception {
        Catalog catalog = new FileSystemCatalog(
                LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()));
        catalog.createDatabase("db", false);
        catalog.createTable(TABLE, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("region", DataTypes.STRING())
                .partitionKeys("region")
                .build(), false);
        return catalog;
    }

    /** Writes one row (id, region) to the table identified by {@code id} in a single batch commit. */
    private static void insert(Catalog catalog, Identifier tableId, int id, String region) throws Exception {
        Table table = catalog.getTable(tableId);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(id, BinaryString.fromString(region)));
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    /** All (id:region) rows currently visible in the table identified by {@code id}, sorted for stability. */
    private static List<String> readRows(Catalog catalog, Identifier tableId) throws Exception {
        ReadBuilder builder = catalog.getTable(tableId).newReadBuilder();
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getString(1)));
        }
        Collections.sort(rows);
        return rows;
    }

    private static PaimonTableHandle regionHandle(Catalog catalog, Identifier tableId) throws Exception {
        PaimonTableHandle handle = new PaimonTableHandle(
                tableId.getDatabaseName(), tableId.getTableName(),
                Collections.singletonList("region"), Collections.emptyList());
        handle.setPaimonTable(catalog.getTable(tableId));
        return handle;
    }

    // ─────────────────────────────── RENAME TABLE ───────────────────────────────

    @Test
    public void renamesTableAndKeepsData(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, TABLE, 1, "cn");
            insert(catalog, TABLE, 2, "us");

            metadata(catalog).renameTable(null, regionHandle(catalog, TABLE), "t2");

            Identifier renamed = Identifier.create("db", "t2");
            // WHY: the old name must be gone and the new name must serve the SAME data. MUTATION: renaming
            // only in the metadata layer without calling the real catalog rename leaves "t" resolvable ->
            // the not-exist assertion below goes red.
            Assertions.assertThrows(Catalog.TableNotExistException.class, () -> catalog.getTable(TABLE));
            Assertions.assertEquals(Arrays.asList("1:cn", "2:us"), readRows(catalog, renamed));
        }
    }

    @Test
    public void renameToAnExistingNameThrows(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            Identifier other = Identifier.create("db", "t2");
            catalog.createTable(other, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("region", DataTypes.STRING())
                    .partitionKeys("region")
                    .build(), false);

            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, () ->
                    metadata(catalog).renameTable(null, regionHandle(catalog, TABLE), "t2"));
            Assertions.assertTrue(e.getMessage().contains("t2"),
                    "the error must name the rename target, was: " + e.getMessage());
            // The source table must be untouched by the failed rename.
            Assertions.assertDoesNotThrow(() -> catalog.getTable(TABLE));
        }
    }

    @Test
    public void renameOfflineSeamPropagatesSourceMissing() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableNotExistOnRename = true;
        FakePaimonTable table = new FakePaimonTable(
                "t", org.apache.paimon.types.RowType.builder()
                        .field("id", DataTypes.INT())
                        .build(),
                Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db", "t", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, () ->
                new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()),
                        new RecordingConnectorContext())
                        .renameTable(null, handle, "t2"));
        Assertions.assertTrue(e.getMessage().contains("db.t"),
                "the error must name the source table, was: " + e.getMessage());
        Assertions.assertEquals(Identifier.create("db", "t"), ops.lastRenameFromTableId);
        Assertions.assertEquals(Identifier.create("db", "t2"), ops.lastRenameToTableId);
        Assertions.assertFalse(ops.lastRenameTableIgnoreIfNotExists,
                "a rename of a handle fe-core already resolved must not silently ignore a missing source");
    }

    // ────────────────────────────── TRUNCATE TABLE ──────────────────────────────

    @Test
    public void truncatesWholeTableAndKeepsSchema(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, TABLE, 1, "cn");
            insert(catalog, TABLE, 2, "us");
            Assertions.assertEquals(Arrays.asList("1:cn", "2:us"), readRows(catalog, TABLE));

            PaimonConnectorMetadata metadata = metadata(catalog);
            // null partitions == whole-table truncate.
            metadata.truncateTable(null, regionHandle(catalog, TABLE), null);

            Assertions.assertEquals(Collections.emptyList(), readRows(catalog, TABLE),
                    "every row must be gone after a whole-table truncate");
            // The table definition (partition spec, schema) must survive: a subsequent insert still works.
            insert(catalog, TABLE, 3, "eu");
            Assertions.assertEquals(Collections.singletonList("3:eu"), readRows(catalog, TABLE));
        }
    }

    @Test
    public void truncatesWholeTableWithEmptyPartitionsList(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, TABLE, 1, "cn");

            // An empty (non-null) list must also mean whole-table truncate, matching the SPI contract.
            metadata(catalog).truncateTable(null, regionHandle(catalog, TABLE), Collections.emptyList());

            Assertions.assertEquals(Collections.emptyList(), readRows(catalog, TABLE));
        }
    }

    @Test
    public void truncatesOnlyNamedPartitions(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, TABLE, 1, "cn");
            insert(catalog, TABLE, 2, "cn");
            insert(catalog, TABLE, 3, "us");

            PaimonConnectorMetadata metadata = metadata(catalog);
            metadata.truncateTable(null, regionHandle(catalog, TABLE),
                    Collections.singletonList("region=cn"));

            Assertions.assertEquals(Collections.singletonList("3:us"), readRows(catalog, TABLE),
                    "only the us partition should remain after truncating region=cn");
        }
    }

    @Test
    public void truncatingAMissingNamedPartitionThrows(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, TABLE, 1, "cn");

            // WHY: TRUNCATE TABLE t PARTITION (p) on an absent partition is an error, not a silent no-op —
            // the same contract DROP PARTITION (without IF EXISTS) already enforces.
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, () ->
                    metadata(catalog).truncateTable(null, regionHandle(catalog, TABLE),
                            Collections.singletonList("region=zz")));
            Assertions.assertTrue(e.getMessage().contains("region=zz"),
                    "the error must name the missing partition, was: " + e.getMessage());
            Assertions.assertEquals(Collections.singletonList("1:cn"), readRows(catalog, TABLE));
        }
    }

    @Test
    public void truncatingPartitionsOfANonPartitionedTableIsRejected(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(
                LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            catalog.createTable(TABLE, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("region", DataTypes.STRING())
                    .build(), false);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(catalog.getTable(TABLE));

            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, () ->
                    metadata(catalog).truncateTable(null, handle,
                            Collections.singletonList("region=cn")));
            Assertions.assertTrue(e.getMessage().contains("non-partitioned"),
                    "message should explain the table is not partitioned, was: " + e.getMessage());
        }
    }

    @Test
    public void wholeTableTruncateOfANonPartitionedTableWorks(@TempDir Path warehouse) throws Exception {
        // No partitions means TABLE-level truncate, which is independent of the DROP PARTITION machinery and
        // must NOT be rejected as "non-partitioned" — that gate only applies to a NAMED-partition request.
        try (Catalog catalog = new FileSystemCatalog(
                LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            catalog.createTable(TABLE, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("region", DataTypes.STRING())
                    .build(), false);
            PaimonTableHandle handle = new PaimonTableHandle(
                    "db", "t", Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(catalog.getTable(TABLE));
            Table table = catalog.getTable(TABLE);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = writeBuilder.newWrite()) {
                write.write(GenericRow.of(1, BinaryString.fromString("cn")));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = writeBuilder.newCommit()) {
                    commit.commit(messages);
                }
            }

            Assertions.assertDoesNotThrow(() -> metadata(catalog).truncateTable(null, handle, null));
            Assertions.assertEquals(Collections.emptyList(), readRows(catalog, TABLE));
        }
    }

    // ─────────── offline seam assertions: whole-table truncate calls the right seam method ───────────

    @Test
    public void wholeTableTruncateOfflineSeamCallsTruncateTableNotTruncatePartitions() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t", org.apache.paimon.types.RowType.builder()
                        .field("id", DataTypes.INT())
                        .build(),
                Collections.emptyList(), Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db", "t", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext())
                .truncateTable(null, handle, null);

        // WHY: a whole-table truncate must go through the DEDICATED truncateTable seam, not accidentally
        // fall into the dropPartitions/truncatePartitions path (which has no partition names to resolve
        // for a null request and would misbehave). MUTATION: routing null-partitions to truncatePartitions
        // with an empty spec list -> this call-log assertion goes red.
        Assertions.assertEquals(Identifier.create("db", "t"), ops.lastTruncatedTableId);
        Assertions.assertTrue(ops.log.contains("truncateTable:db.t"));
        Assertions.assertFalse(ops.log.stream().anyMatch(entry -> entry.startsWith("truncatePartitions:")),
                "a whole-table truncate must not call truncatePartitions");
    }
}
