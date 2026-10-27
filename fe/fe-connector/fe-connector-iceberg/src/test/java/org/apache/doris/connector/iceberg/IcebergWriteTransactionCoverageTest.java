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
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergWriteTransactionCoverageTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "region", Types.StringType.get()));
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private static final String SEED = "s3://bucket/events/seed.parquet";
    private static final String ADDED = "s3://bucket/events/added.parquet";
    private static final String DELETED = "s3://bucket/events/deleted.parquet";

    @Test
    public void appendV1PreservesHistoricalSnapshot() throws IOException {
        assertAppendPreservesHistory(1);
    }

    @Test
    public void appendV2PreservesHistoricalSnapshot() throws IOException {
        assertAppendPreservesHistory(2);
    }

    @Test
    public void appendV3PreservesHistoricalSnapshot() throws IOException {
        assertAppendPreservesHistory(3);
    }

    private void assertAppendPreservesHistory(int formatVersion) throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, formatVersion, false);
        table.newAppend().appendFile(file(table.spec(), SEED, 2, null)).commit();
        long previousSnapshot = table.currentSnapshot().snapshotId();

        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", context(WriteOperation.INSERT));
        transaction.addCommitData(serialize(data(ADDED, 3)));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(paths(SEED, ADDED), paths(current, null, null));
        Assertions.assertEquals(paths(SEED), paths(current, null, previousSnapshot));
        Assertions.assertEquals(3, transaction.getUpdateCnt());
        Assertions.assertEquals("5", current.currentSnapshot().summary().get("total-records"));
    }

    @Test
    public void dynamicOverwriteReplacesEveryTouchedPartitionOnly() throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, true);
        String west = "s3://bucket/events/west.parquet";
        String east = "s3://bucket/events/east.parquet";
        String north = "s3://bucket/events/north.parquet";
        table.newAppend().appendFile(file(table.spec(), west, 2, "west"))
                .appendFile(file(table.spec(), east, 3, "east"))
                .appendFile(file(table.spec(), north, 4, "north")).commit();
        long previousSnapshot = table.currentSnapshot().snapshotId();

        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session("dynamic"), "db", "events", context(WriteOperation.OVERWRITE));
        TIcebergCommitData westReplacement = data(ADDED, 1);
        westReplacement.setPartitionValues(Collections.singletonList("west"));
        TIcebergCommitData eastReplacement = data(SEED, 2);
        eastReplacement.setPartitionValues(Collections.singletonList("east"));
        transaction.addCommitData(serialize(westReplacement));
        transaction.addCommitData(serialize(eastReplacement));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(paths(ADDED, SEED, north), paths(current, null, null));
        Assertions.assertEquals(paths(west, east, north), paths(current, null, previousSnapshot));
    }

    @Test
    public void staticPartitionOverwriteOnBranchPreservesMainAndOtherPartitions() throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, true);
        table.newAppend().appendFile(file(table.spec(), SEED, 2, "west"))
                .appendFile(file(table.spec(), ADDED, 3, "east")).commit();
        table.manageSnapshots().createBranch("audit", table.currentSnapshot().snapshotId()).commit();
        IcebergWriteSchemaContext schema = IcebergWriteSchemaContext.create(
                table, table.name(), Optional.of("audit"), false, false);
        IcebergWriteContext writeContext = new IcebergWriteContext(WriteOperation.OVERWRITE, true,
                Collections.singletonMap("region", "west"), Optional.of("audit"), -1L, schema);

        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", writeContext);
        TIcebergCommitData replacement = data(DELETED, 1);
        replacement.setPartitionValues(Collections.singletonList("west"));
        transaction.addCommitData(serialize(replacement));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(paths(SEED, ADDED), paths(current, null, null));
        Assertions.assertEquals(paths(DELETED, ADDED), paths(current, "audit", null));
    }

    @Test
    public void branchAppendDoesNotPublishConcurrentMainFilesToBranch() throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, false);
        table.newAppend().appendFile(file(table.spec(), SEED, 1, null)).commit();
        table.manageSnapshots().createBranch("audit", table.currentSnapshot().snapshotId()).commit();
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", new IcebergWriteContext(
                WriteOperation.INSERT, false, Collections.emptyMap(), Optional.of("audit")));
        catalog.loadTable(IDENTIFIER).newAppend().appendFile(file(table.spec(), ADDED, 1, null)).commit();
        transaction.addCommitData(serialize(data(DELETED, 1)));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(paths(SEED, ADDED), paths(current, null, null));
        Assertions.assertEquals(paths(SEED, DELETED), paths(current, "audit", null));
    }

    @Test
    public void updatePublishesDataAndDeletesInOneSnapshot() throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, false);
        table.newAppend().appendFile(file(table.spec(), SEED, 3, null)).commit();
        long previousSnapshot = table.currentSnapshot().snapshotId();
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", context(WriteOperation.UPDATE));
        transaction.addCommitData(serialize(data(ADDED, 2)));
        transaction.addCommitData(serialize(delete(DELETED, 2, SEED)));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(Long.valueOf(previousSnapshot), current.currentSnapshot().parentId());
        Assertions.assertEquals(paths(SEED, ADDED), paths(current, null, null));
        Assertions.assertEquals(paths(SEED), paths(current, null, previousSnapshot));
        Assertions.assertEquals("1", current.currentSnapshot().summary().get("added-data-files"));
        Assertions.assertEquals("1", current.currentSnapshot().summary().get("added-delete-files"));
        Assertions.assertEquals(2, transaction.getUpdateCnt());
    }

    @Test
    public void mergeWithOnlyInsertActionsCommitsWithoutDeleteFiles() throws IOException {
        InMemoryCatalog catalog = catalog();
        create(catalog, 2, false);
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", context(WriteOperation.MERGE));
        transaction.addCommitData(serialize(data(ADDED, 3)));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(paths(ADDED), paths(current, null, null));
        Assertions.assertEquals("0", current.currentSnapshot().summary().get("total-delete-files"));
        Assertions.assertEquals(3, transaction.getUpdateCnt());
    }

    @Test
    public void mergeWithOnlyDeleteActionsPreservesOriginalDataFile() throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, false);
        table.newAppend().appendFile(file(table.spec(), SEED, 3, null)).commit();
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", context(WriteOperation.MERGE));
        transaction.addCommitData(serialize(delete(DELETED, 2, SEED)));
        transaction.commit();

        Table current = catalog.loadTable(IDENTIFIER);
        Assertions.assertEquals(paths(SEED), paths(current, null, null));
        Assertions.assertEquals("1", current.currentSnapshot().summary().get("total-delete-files"));
        Assertions.assertEquals("1", current.currentSnapshot().summary().get("total-data-files"));
        Assertions.assertEquals(2, transaction.getUpdateCnt());
    }

    @Test
    public void updateV1IsRejectedBeforeAnySnapshotIsPublished() {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 1, false);
        IcebergConnectorTransaction transaction = transaction(table);
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> transaction.beginWrite(session(null), "db", "events", context(WriteOperation.UPDATE)));
        Assertions.assertTrue(failure.getMessage().contains("format version 2 or higher"));
        Assertions.assertNull(catalog.loadTable(IDENTIFIER).currentSnapshot());
    }

    @Test
    public void malformedDeleteSpecLeavesPublishedSnapshotUntouched() {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, true);
        table.newAppend().appendFile(file(table.spec(), SEED, 3, "west")).commit();
        long previousSnapshot = table.currentSnapshot().snapshotId();
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(null), "db", "events", context(WriteOperation.DELETE));
        TIcebergCommitData fragment = delete(DELETED, 1, SEED);
        fragment.setPartitionSpecId(99);
        fragment.setPartitionValues(Collections.singletonList("west"));
        transaction.addCommitData(serialize(fragment));

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, transaction::commit);
        Assertions.assertTrue(failure.getMessage().contains("Unknown partition spec id 99"));
        Assertions.assertEquals(previousSnapshot, catalog.loadTable(IDENTIFIER).currentSnapshot().snapshotId());
    }

    @Test
    public void beginCanRetryAfterCatalogFailureWithoutKeepingPartialState() throws IOException {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog, 2, false);
        RecordingIcebergCatalogOps operations = new RecordingIcebergCatalogOps();
        operations.table = table;
        operations.throwOnLoadTable = true;
        IcebergConnectorTransaction transaction = new IcebergConnectorTransaction(
                7, operations, new RecordingConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> transaction.beginWrite(session(null), "db", "events", context(WriteOperation.INSERT)));
        Assertions.assertNull(transaction.getTransaction());
        Assertions.assertNull(transaction.getTable());
        operations.throwOnLoadTable = false;
        transaction.beginWrite(session(null), "db", "events", context(WriteOperation.INSERT));
        transaction.addCommitData(serialize(data(ADDED, 1)));
        transaction.commit();
        Assertions.assertEquals(paths(ADDED), paths(catalog.loadTable(IDENTIFIER), null, null));
    }

    @Test
    public void closeBeforeBeginPreventsCatalogAccess() {
        RecordingIcebergCatalogOps operations = new RecordingIcebergCatalogOps();
        IcebergConnectorTransaction transaction = new IcebergConnectorTransaction(
                7, operations, new RecordingConnectorContext());
        transaction.close();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> transaction.beginWrite(session(null), "db", "events", context(WriteOperation.INSERT)));
        Assertions.assertTrue(operations.log.isEmpty());
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("coverage", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static Table create(InMemoryCatalog catalog, int version, boolean partitioned) {
        PartitionSpec spec = partitioned
                ? PartitionSpec.builderFor(SCHEMA).identity("region").build() : PartitionSpec.unpartitioned();
        return catalog.createTable(IDENTIFIER, SCHEMA, spec,
                Collections.singletonMap("format-version", Integer.toString(version)));
    }

    private static DataFile file(PartitionSpec spec, String path, long rows, String region) {
        DataFiles.Builder builder = DataFiles.builder(spec).withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024).withRecordCount(rows);
        if (region != null) {
            builder.withPartitionPath("region=" + region);
        }
        return builder.build();
    }

    private static Set<String> paths(String... paths) {
        return new HashSet<>(Arrays.asList(paths));
    }

    private static Set<String> paths(Table table, String branch, Long snapshot) throws IOException {
        org.apache.iceberg.TableScan scan = table.newScan();
        if (branch != null) {
            scan = scan.useRef(branch);
        }
        if (snapshot != null) {
            scan = scan.useSnapshot(snapshot);
        }
        Set<String> paths = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().path().toString());
            }
        }
        return paths;
    }

    private static IcebergConnectorTransaction transaction(Table table) {
        RecordingIcebergCatalogOps operations = new RecordingIcebergCatalogOps();
        operations.table = table;
        return new IcebergConnectorTransaction(7, operations, new RecordingConnectorContext());
    }

    private static IcebergWriteContext context(WriteOperation operation) {
        return new IcebergWriteContext(operation, operation == WriteOperation.OVERWRITE,
                Collections.emptyMap(), Optional.empty());
    }

    private static TIcebergCommitData data(String path, long rows) {
        TIcebergCommitData fragment = new TIcebergCommitData();
        fragment.setFilePath(path);
        fragment.setFileSize(1024);
        fragment.setRowCount(rows);
        fragment.setAffectedRows(rows);
        fragment.setFileContent(TFileContent.DATA);
        return fragment;
    }

    private static TIcebergCommitData delete(String path, long rows, String referencedDataFile) {
        TIcebergCommitData fragment = data(path, rows);
        fragment.setFileContent(TFileContent.POSITION_DELETES);
        fragment.setReferencedDataFilePath(referencedDataFile);
        return fragment;
    }

    private static byte[] serialize(TIcebergCommitData fragment) {
        try {
            return new TSerializer(new TBinaryProtocol.Factory()).serialize(fragment);
        } catch (TException exception) {
            throw new AssertionError(exception);
        }
    }

    private static ConnectorSession session(String overwriteMode) {
        Map<String, String> properties = new HashMap<>();
        if (overwriteMode != null) {
            properties.put("iceberg_partition_overwrite_mode", overwriteMode);
        }
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "coverage";
            }

            @Override
            public String getUser() {
                return "test";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public String getCatalogName() {
                return "coverage";
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return type.cast(properties.get(name));
            }
        };
    }
}
