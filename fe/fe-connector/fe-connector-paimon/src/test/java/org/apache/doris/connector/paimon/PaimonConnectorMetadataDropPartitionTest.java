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
import org.apache.paimon.utils.DateTimeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code ALTER TABLE ... DROP PARTITION} for {@link PaimonConnectorMetadata}, exercised against a REAL
 * {@link FileSystemCatalog} (no mock, no env gate): the offline seam-recording assertions live at the
 * bottom, but the primary cases build a partitioned table, insert data, drop a partition through the
 * connector, and read the rows BACK to prove the targeted partition's data is gone while every other
 * partition survives.
 *
 * <p>Each test uses its own {@code @TempDir} warehouse, so they are independent and safe to re-run.
 */
public class PaimonConnectorMetadataDropPartitionTest {

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

    /** Writes one row (id, region) to the table in a single batch commit. */
    private static void insert(Catalog catalog, int id, String region) throws Exception {
        Table table = catalog.getTable(TABLE);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(id, BinaryString.fromString(region)));
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    /** All (id:region) rows currently visible in the table, sorted for stable comparison. */
    private static List<String> readRows(Catalog catalog) throws Exception {
        ReadBuilder builder = catalog.getTable(TABLE).newReadBuilder();
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getString(1)));
        }
        Collections.sort(rows);
        return rows;
    }

    private static PaimonTableHandle regionHandle(Catalog catalog) throws Exception {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db", "t", Collections.singletonList("region"), Collections.emptyList());
        handle.setPaimonTable(catalog.getTable(TABLE));
        return handle;
    }

    @Test
    public void dropsTargetPartitionAndKeepsTheRest(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, 1, "cn");
            insert(catalog, 2, "cn");
            insert(catalog, 3, "us");
            Assertions.assertEquals(Arrays.asList("1:cn", "2:cn", "3:us"), readRows(catalog));

            PaimonConnectorMetadata metadata = metadata(catalog);
            // WHY: DROP PARTITION region=cn must clear ONLY the cn rows. MUTATION: truncating the wrong
            // spec (or truncating the whole table) leaves the surviving-row assertion red.
            metadata.dropPartitions(null, regionHandle(catalog),
                    Collections.singletonList("region=cn"), false);

            Assertions.assertEquals(Collections.singletonList("3:us"), readRows(catalog),
                    "only the us partition should remain");
            // The connector's own listing must no longer report the dropped partition.
            List<String> names = metadata.listPartitionNames(null, regionHandle(catalog));
            Assertions.assertEquals(Collections.singletonList("region=us"), names);
        }
    }

    @Test
    public void dropMultiplePartitionsInOneCall(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, 1, "cn");
            insert(catalog, 2, "us");
            insert(catalog, 3, "eu");

            PaimonConnectorMetadata metadata = metadata(catalog);
            metadata.dropPartitions(null, regionHandle(catalog),
                    Arrays.asList("region=cn", "region=eu"), false);

            Assertions.assertEquals(Collections.singletonList("2:us"), readRows(catalog));
        }
    }

    @Test
    public void missingPartitionWithoutIfExistsThrowsAndCommitsNothing(@TempDir Path warehouse)
            throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, 1, "cn");

            PaimonConnectorMetadata metadata = metadata(catalog);
            // WHY: DROP PARTITION (no IF EXISTS) on an absent partition must fail loud, mirroring the
            // internal-table ERR_DROP_PARTITION_NON_EXISTENT. MUTATION: swallowing the missing partition
            // (or handing the unknown spec to truncatePartitions, which silently no-ops) -> no throw -> red.
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, () ->
                    metadata.dropPartitions(null, regionHandle(catalog),
                            Collections.singletonList("region=zz"), false));
            Assertions.assertTrue(e.getMessage().contains("region=zz"),
                    "the error must name the missing partition, was: " + e.getMessage());

            // The existing partition must be untouched (the pre-flight existence check rejects before commit).
            Assertions.assertEquals(Collections.singletonList("1:cn"), readRows(catalog));
        }
    }

    @Test
    public void missingPartitionWithIfExistsIsSilentNoOp(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, 1, "cn");

            PaimonConnectorMetadata metadata = metadata(catalog);
            // IF EXISTS: an absent partition is a no-op, leaving all data in place and creating no commit.
            Assertions.assertDoesNotThrow(() ->
                    metadata.dropPartitions(null, regionHandle(catalog),
                            Collections.singletonList("region=zz"), true));

            Assertions.assertEquals(Collections.singletonList("1:cn"), readRows(catalog));
        }
    }

    @Test
    public void ifExistsDropsThePresentAndSkipsTheAbsent(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = regionPartitionedCatalog(warehouse)) {
            insert(catalog, 1, "cn");
            insert(catalog, 2, "us");

            PaimonConnectorMetadata metadata = metadata(catalog);
            // A mixed batch under IF EXISTS: cn is present (dropped), zz is absent (skipped), us survives.
            metadata.dropPartitions(null, regionHandle(catalog),
                    Arrays.asList("region=cn", "region=zz"), true);

            Assertions.assertEquals(Collections.singletonList("2:us"), readRows(catalog));
        }
    }

    @Test
    public void nonPartitionedTableIsRejected(@TempDir Path warehouse) throws Exception {
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
                    metadata(catalog).dropPartitions(null, handle,
                            Collections.singletonList("region=cn"), false));
            Assertions.assertTrue(e.getMessage().contains("non-partitioned"),
                    "message should explain the table is not partitioned, was: " + e.getMessage());
        }
    }

    // ─────────── offline seam assertions: the NATIVE spec handed to truncatePartitions ───────────
    // truncatePartitions matches partition FILES against paimon's raw spec (DATE=epoch-day,
    // null=partition.default-name), NOT the rendered display value. These pin that the DROP name is
    // resolved back to the raw spec, using the offline seam fake so the exact argument is inspectable.

    @Test
    public void resolvesLegacyDateNameBackToEpochDaySpec() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t", org.apache.paimon.types.RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("dt", DataTypes.DATE())
                        .build(),
                Collections.singletonList("dt"), Collections.emptyList());
        table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
        ops.table = table;
        int epochDay = 19723; // DateTimeUtils.formatDate(19723) == 2024-01-01
        Map<String, String> rawSpec = new LinkedHashMap<>();
        rawSpec.put("dt", String.valueOf(epochDay));
        ops.partitions = Collections.singletonList(
                new org.apache.paimon.partition.Partition(rawSpec, 1L, 1L, 1, 1L, 1, true));

        PaimonTableHandle handle = new PaimonTableHandle(
                "db", "t", Collections.singletonList("dt"), Collections.emptyList());
        handle.setPaimonTable(table);

        String displayName = "dt=" + DateTimeUtils.formatDate(epochDay); // dt=2024-01-01
        new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext())
                .dropPartitions(null, handle, Collections.singletonList(displayName), false);

        // WHY: the truncate spec must be paimon's RAW epoch-day value (19723), not the rendered
        // "2024-01-01" — otherwise truncatePartitions matches no files and silently drops nothing.
        // MUTATION: passing the rendered display value as the spec -> value "2024-01-01" -> red.
        Assertions.assertEquals(1, ops.lastTruncatedPartitionSpecs.size());
        Assertions.assertEquals(String.valueOf(epochDay),
                ops.lastTruncatedPartitionSpecs.get(0).get("dt"));
        Assertions.assertTrue(ops.log.contains("truncatePartitions:db.t,specs=1"));
    }
}
