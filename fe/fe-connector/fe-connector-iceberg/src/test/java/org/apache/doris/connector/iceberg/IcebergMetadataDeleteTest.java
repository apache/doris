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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Pins {@link IcebergConnectorMetadata#deleteUsingMetadata}: the whole-file {@code DELETE} fast path that
 * Spark / StarRocks / Trino use for whole-partition and whole-file predicates.
 *
 * <p>Mirrors the no-Mockito, real-{@link InMemoryCatalog} style of {@code IcebergConnectorTransactionTest}: the
 * delete is committed against a real iceberg table, but — unlike a row-level delete — it never reads a data
 * file, so the fixtures only need data-file metadata (path + record count), not real parquet files.</p>
 */
public class IcebergMetadataDeleteTest {

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db1", "t1");
    private static final ConnectorTableHandle HANDLE = new IcebergTableHandle("db1", "t1");
    private static final ConnectorSession SESSION = new FakeSession("UTC");

    @Test
    public void wholePartitionDeleteDropsMatchingFilesOnly() {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(partitionedDataFile(spec, "s3://b/db1/t1/region=us/a.parquet", 10L, "region=us"))
                .appendFile(partitionedDataFile(spec, "s3://b/db1/t1/region=us/b.parquet", 10L, "region=us"))
                .appendFile(partitionedDataFile(spec, "s3://b/db1/t1/region=eu/c.parquet", 20L, "region=eu"))
                .commit();

        OptionalLong deleted = metadata(catalog).deleteUsingMetadata(SESSION, HANDLE, Optional.of(
                new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("region", ConnectorType.of("STRING")), ConnectorLiteral.ofString("us"))));

        Assertions.assertEquals(OptionalLong.of(20L), deleted,
                "the two region=us files (10 records each) are the deleted rows");
        Assertions.assertEquals(Collections.singletonList("s3://b/db1/t1/region=eu/c.parquet"),
                dataFilePaths(reload(catalog)),
                "a whole-partition delete removes only the matching data files");
        Assertions.assertEquals(0, liveDeleteFileCount(reload(catalog)),
                "a metadata delete writes no delete files (no position deletes, no deletion vectors)");
    }

    @Test
    public void unfilteredDeleteRemovesEveryFile() {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L))
                .appendFile(dataFile(spec, "s3://b/db1/t1/b.parquet", 5L))
                .commit();

        // The engine passes no predicate for "DELETE FROM t": every row of the table is deleted.
        OptionalLong deleted = metadata(catalog).deleteUsingMetadata(SESSION, HANDLE, Optional.empty());

        Assertions.assertEquals(OptionalLong.of(15L), deleted);
        Assertions.assertTrue(dataFilePaths(reload(catalog)).isEmpty(),
                "an unfiltered metadata delete drops every file");
    }

    @Test
    public void partialMatchFallsBackWithoutCommitting() {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        // No column metrics on the file: nothing proves that ALL of its rows satisfy `id > 5`.
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();

        OptionalLong deleted = metadata(catalog).deleteUsingMetadata(SESSION, HANDLE, Optional.of(
                new ConnectorComparison(ConnectorComparison.Operator.GT,
                        new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5L))));

        Assertions.assertFalse(deleted.isPresent(),
                "a candidate file that is only partially matched must fall back to the row-level delete");
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId(),
                "the fallback must not commit anything");
    }

    @Test
    public void unpushablePredicateFallsBack() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(),
                props("format-version", "2"));
        table.newAppend().appendFile(dataFile(PartitionSpec.unpartitioned(), "s3://b/db1/t1/a.parquet", 10L))
                .commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();

        // A column the table does not have: the strict matrix cannot lower it, and dropping it would delete
        // every row of every file.
        OptionalLong deleted = metadata(catalog).deleteUsingMetadata(SESSION, HANDLE, Optional.of(
                new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("no_such_column", ConnectorType.of("BIGINT")), ConnectorLiteral.ofLong(1L))));

        Assertions.assertFalse(deleted.isPresent(), "an unliftable predicate must not be treated as always-true");
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
    }

    @Test
    public void deletedRowCountExcludesRowsHiddenByExistingDeleteFiles() {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "3"));
        table.newAppend()
                .appendFile(partitionedDataFile(spec, "s3://b/db1/t1/region=us/a.parquet", 10L, "region=us"))
                .commit();
        // Three of the ten rows are already deleted by a deletion vector for that data file.
        table.newRowDelta().addDeletes(deletionVector(spec, "s3://b/db1/t1/dv.puffin",
                "s3://b/db1/t1/region=us/a.parquet", 3L)).commit();

        OptionalLong deleted = metadata(catalog).deleteUsingMetadata(SESSION, HANDLE, Optional.of(
                new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("region", ConnectorType.of("STRING")), ConnectorLiteral.ofString("us"))));

        Assertions.assertEquals(OptionalLong.of(7L), deleted,
                "deleted rows = the dropped file's records minus the rows its deletion vector already hid");
        Assertions.assertTrue(dataFilePaths(reload(catalog)).isEmpty());
        Assertions.assertEquals(0, liveDeleteFileCount(reload(catalog)),
                "the deletion vector of the dropped data file is removed as well");
    }

    private static IcebergConnectorMetadata metadata(InMemoryCatalog catalog) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = reload(catalog);
        return new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext());
    }

    /** Re-reads the table so the assertions observe the snapshot the metadata delete committed. */
    private static Table reload(InMemoryCatalog catalog) {
        return catalog.loadTable(TABLE_ID);
    }

    private static List<String> dataFilePaths(Table table) {
        List<String> paths = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().path().toString());
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        Collections.sort(paths);
        return paths;
    }

    /** Live delete-file entries in the current snapshot (an emptied manifest may legitimately remain). */
    private static int liveDeleteFileCount(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return 0;
        }
        int live = 0;
        for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
            try (ManifestReader<DeleteFile> reader =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                live += Iterables.size(reader);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return live;
    }

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));

    private static InMemoryCatalog freshCatalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog;
    }

    private static Map<String, String> props(String... kv) {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            properties.put(kv[i], kv[i + 1]);
        }
        return properties;
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long records) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static DataFile partitionedDataFile(PartitionSpec spec, String path, long records,
            String partitionPath) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath(partitionPath)
                .build();
    }

    /** A v3 deletion vector for {@code referencedDataFile} that already hides {@code cardinality} rows. */
    private static DeleteFile deletionVector(PartitionSpec spec, String puffin, String referencedDataFile,
            long cardinality) {
        return FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(puffin)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(128L)
                .withRecordCount(cardinality)
                .withReferencedDataFile(referencedDataFile)
                .withContentOffset(0L)
                .withContentSizeInBytes(64L)
                .build();
    }

    /** Minimal {@link ConnectorSession} exposing a time zone (partition-timestamp parsing); no Mockito. */
    private static final class FakeSession implements ConnectorSession {
        private final String timeZone;

        FakeSession(String timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return timeZone;
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public long getCatalogId() {
            return 0;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
