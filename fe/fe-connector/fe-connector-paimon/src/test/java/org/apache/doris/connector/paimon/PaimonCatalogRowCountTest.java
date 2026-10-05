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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Snapshot statistics must not open manifests or construct a split plan. */
public class PaimonCatalogRowCountTest {
    @TempDir
    Path warehouse;

    private final PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(null);

    @Test
    public void latestAppendAndPrimaryKeyCountsNeedNoManifests() throws Exception {
        for (boolean primaryKey : new boolean[] {false, true}) {
            FileStoreTable table = newTable("latest_" + primaryKey, primaryKey);
            snapshot(table, 1, 10L);
            snapshot(table, 2, 25L);
            Assertions.assertEquals(25L, ops.rowCount(table));
        }
    }

    @Test
    public void historicalSnapshotAndTimestampUseSelectedCount() throws Exception {
        FileStoreTable table = newTable("history", true);
        snapshot(table, 1, 10L);
        snapshot(table, 2, 25L);
        Assertions.assertEquals(10L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.snapshot-id", "1"))));
        Assertions.assertEquals(10L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.timestamp-millis", "1500"))));
    }

    @Test
    public void tagSurvivesExpiredSnapshot() throws Exception {
        FileStoreTable table = newTable("tag", false);
        snapshot(table, 1, 10L);
        table.createTag("retained", 1L);
        snapshot(table, 2, 25L);
        table.fileIO().delete(table.snapshotManager().snapshotPath(1), false);
        Assertions.assertEquals(10L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.tag-name", "retained"))));
    }

    @Test
    public void branchUsesItsOwnSnapshot() throws Exception {
        FileStoreTable table = newTable("branch", false);
        snapshot(table, 1, 10L);
        table.createBranch("dev");
        FileStoreTable branch = table.switchToBranch("dev");
        snapshot(branch, 1, 7L);
        snapshot(table, 2, 25L);
        Assertions.assertEquals(7L, ops.rowCount(branch));
        Assertions.assertEquals(25L, ops.rowCount(table));
    }

    @Test
    public void emptyAndLegacySnapshotsDoNotFallBackToPlanning() throws Exception {
        FileStoreTable table = newTable("empty", false);
        Assertions.assertEquals(-1L, ops.rowCount(table));
        snapshot(table, 1, null);
        Assertions.assertEquals(-1L, ops.rowCount(table));
        snapshot(table, 2, 0L);
        Assertions.assertEquals(0L, ops.rowCount(table));
    }

    @Test
    public void partialScansDoNotUseWholeSnapshotCount() throws Exception {
        FileStoreTable table = newTable("partial", false);
        snapshot(table, 1, 10L);
        snapshot(table, 2, 25L);
        Assertions.assertEquals(-1L, ops.rowCount(table.copy(
                Collections.singletonMap("incremental-between", "1,2"))));
        Assertions.assertEquals(-1L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.file-creation-time-millis", "1500"))));
        Assertions.assertEquals(-1L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.mode", "compacted-full"))));
    }

    @Test
    public void systemAndFallbackTablesDoNotUseBaseCount() throws Exception {
        FileStoreTable table = newTable("system", false);
        snapshot(table, 1, 10L);
        Assertions.assertEquals(-1L, ops.rowCount(new SnapshotsTable(table)));
        Assertions.assertEquals(-1L, ops.rowCount(new FakePaimonTable("format",
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                Collections.emptyList(), Collections.emptyList())));
        FileStoreTable fallback = newTable("fallback", false);
        snapshot(fallback, 1, 20L);
        Assertions.assertEquals(-1L, ops.rowCount(new FallbackReadFileStoreTable(table, fallback)));
    }

    @Test
    public void committedFilesExcludedByBatchScanReturnUnknown() throws Exception {
        for (Map<String, String> options : List.of(
                Collections.singletonMap("deletion-vectors.enabled", "true"),
                Collections.singletonMap("merge-engine", "first-row"),
                Collections.singletonMap("bucket", "-2"))) {
            ManifestGuardFileIO fileIO = new ManifestGuardFileIO();
            fileIO.rejectManifests = false;
            FileStoreTable table = newTable(options.keySet().iterator().next(), true, options, fileIO);
            append(table);
            Assertions.assertEquals(1L, table.latestSnapshot().get().totalRecordCount().longValue());
            Assertions.assertEquals(0L, table.newScan().plan().splits().stream().mapToLong(Split::rowCount).sum(),
                    "The committed file must be invisible to the ordinary batch scan");

            FileStoreTable compactScan = table.copy(Collections.singletonMap("batch-scan-mode", "compact"));
            fileIO.rejectManifests = true;
            Assertions.assertEquals(-1L, ops.rowCount(table));
            Assertions.assertEquals(options.containsKey("bucket") ? -1L : 1L, ops.rowCount(compactScan));
        }
    }

    @Test
    public void committedOrdinaryTableKeepsSnapshotEstimate() throws Exception {
        ManifestGuardFileIO fileIO = new ManifestGuardFileIO();
        fileIO.rejectManifests = false;
        FileStoreTable table = newTable("committed", true, Collections.emptyMap(), fileIO);
        append(table);
        Assertions.assertEquals(1L, table.newScan().plan().splits().stream().mapToLong(Split::rowCount).sum());
        fileIO.rejectManifests = true;
        Assertions.assertEquals(1L, ops.rowCount(table));
    }

    @Test
    public void preservedPrivilegeWrapperNeedsOnlySelect() throws Exception {
        FileStoreTable table = newTable("privileged", false);
        snapshot(table, 1, 10L);
        AtomicInteger selectChecks = new AtomicInteger();
        AtomicBoolean denySelect = new AtomicBoolean();
        PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(), new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("assertCanInsert")) {
                        throw new SecurityException("INSERT denied");
                    }
                    if (method.getName().equals("assertCanSelect")
                            || method.getName().equals("assertCanSelectOrInsert")) {
                        selectChecks.incrementAndGet();
                        if (denySelect.get()) {
                            throw new SecurityException("SELECT denied");
                        }
                    }
                    return null;
                });
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(table, checker, Identifier.create("db", "t"));
        Assertions.assertSame(privileged, PaimonReaderOptions.runtimeSafeTable(privileged));
        PaimonConnectorMetadata metadata = metadata();
        PaimonTableHandle handle = handle(privileged);
        Assertions.assertEquals(10L, metadata.getTableStatistics(null, handle).get().getRowCount());
        Assertions.assertEquals(10L, metadata.getTableStatistics(null, handle,
                ConnectorMvccSnapshot.builder().snapshotId(1L).property("scan.snapshot-id", "1").build())
                .get().getRowCount());
        Assertions.assertTrue(selectChecks.get() >= 2);
        denySelect.set(true);
        Assertions.assertThrows(SecurityException.class, () -> ops.rowCount(privileged));
    }

    @Test
    public void catalogQueryAuthorizationControlsSnapshotStatistics() throws Exception {
        FileStoreTable base = newTable("query_auth", false,
                Collections.singletonMap("query-auth.enabled", "true"), new ManifestGuardFileIO());
        snapshot(base, 1, 10L);
        Identifier identifier = Identifier.create("db", "t");
        AtomicInteger authCalls = new AtomicInteger();
        AtomicBoolean denyQuery = new AtomicBoolean();
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), base.location()) {
            @Override
            public List<String> authTableQuery(Identifier requested, List<String> select) {
                Assertions.assertEquals(identifier, requested);
                Assertions.assertNull(select, "Statistics must preserve the old all-column authorization");
                authCalls.incrementAndGet();
                if (denyQuery.get()) {
                    throw new Catalog.TableNoPermissionException(requested);
                }
                return Collections.emptyList();
            }

            @Override
            public Optional<TableSnapshot> loadSnapshot(Identifier requested) {
                return Optional.of(new TableSnapshot(base.snapshotManager().snapshot(1L), 0L, 0L, 0L, 0L));
            }
        }) {
            // Like a REST-loaded table, this has a catalog loader but no privilege wrapper.
            CatalogEnvironment environment = new CatalogEnvironment(
                    identifier, null, () -> catalog, null, null, false);
            FileStoreTable table = FileStoreTableFactory.create(
                    base.fileIO(), base.location(), base.schema(), environment);
            PaimonConnectorMetadata metadata = metadata();
            PaimonTableHandle handle = handle(table);
            ConnectorMvccSnapshot pinned = ConnectorMvccSnapshot.builder()
                    .snapshotId(1L).property("scan.snapshot-id", "1").build();

            Assertions.assertEquals(10L, metadata.getTableStatistics(null, handle).get().getRowCount());
            Assertions.assertEquals(10L, metadata.getTableStatistics(null, handle, pinned).get().getRowCount());
            Assertions.assertEquals(2, authCalls.get());

            denyQuery.set(true);
            RuntimeException denied = Assertions.assertThrows(RuntimeException.class, () -> ops.rowCount(table));
            Assertions.assertInstanceOf(Catalog.TableNoPermissionException.class, denied.getCause());
            Assertions.assertFalse(metadata.getTableStatistics(null, handle).isPresent());
            Assertions.assertFalse(metadata.getTableStatistics(null, handle, pinned).isPresent());
            Assertions.assertEquals(5, authCalls.get());

            FileStoreTable disabled = table.copy(Collections.singletonMap("query-auth.enabled", "false"));
            Assertions.assertEquals(10L, ops.rowCount(disabled));
            Assertions.assertEquals(5, authCalls.get(), "Disabled query authorization must not contact the catalog");
        }
    }

    @Test
    public void normalizedFileCreationHandlesReturnUnknown() throws Exception {
        ManifestGuardFileIO fileIO = new ManifestGuardFileIO();
        fileIO.rejectManifests = false;
        FileStoreTable table = newTable("creation", false, Collections.emptyMap(), fileIO);
        append(table);
        fileIO.rejectManifests = true;
        PaimonConnectorMetadata metadata = metadata();
        for (String key : new String[] {"scan.file-creation-time-millis", "scan.creation-time-millis"}) {
            // A threshold before the first snapshot forces creation-time's file-filter fallback.
            Map<String, String> resolved = PaimonScanParams.markAsOptions(PaimonScanParams.resolveOptions(
                    table, Collections.singletonMap(key, "1")));
            Assertions.assertTrue(PaimonScanParams.getPinnedFileCreationTime(resolved).isPresent());
            FileStoreTable selected = (FileStoreTable) PaimonScanParams.applyOptions(table, resolved);
            Assertions.assertEquals(CoreOptions.StartupMode.FROM_SNAPSHOT, selected.coreOptions().startupMode());
            Assertions.assertEquals(1L, ops.rowCount(selected), "The Table alone has lost the file filter");
            ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(1L)
                    .properties(resolved).build();
            Assertions.assertFalse(metadata.getTableStatistics(null, handle(table), snapshot).isPresent());
            Assertions.assertFalse(metadata.getTableStatistics(null,
                    handle(table).withScanOptions(resolved)).isPresent());
        }
    }

    private PaimonConnectorMetadata metadata() {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext());
    }

    private PaimonTableHandle handle(FileStoreTable table) {
        PaimonTableHandle handle = new PaimonTableHandle("db", "t", table.partitionKeys(), table.primaryKeys());
        handle.setPaimonTable(table);
        return handle;
    }

    private void append(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1));
            commit.commit(write.prepareCommit());
        }
    }

    private FileStoreTable newTable(String name, boolean primaryKey) throws Exception {
        return newTable(name, primaryKey, Collections.emptyMap(), new ManifestGuardFileIO());
    }

    private FileStoreTable newTable(String name, boolean primaryKey,
            Map<String, String> options, LocalFileIO fileIO) throws Exception {
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(warehouse.resolve(name).toUri());
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT())
                .option("file.format", "parquet").option("write-only", "true")
                .option("scan.manifest.parallelism", "1");
        if (primaryKey) {
            schema.primaryKey("id").option("bucket", "1");
        }
        options.forEach(schema::option);
        new SchemaManager(fileIO, path).createTable(schema.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void snapshot(FileStoreTable table, long id, Long count) throws IOException {
        // Deliberately no manifest files: a return to split planning must fail this test.
        Snapshot snapshot = new Snapshot(id, 0L, "unused-base", null, "unused-delta", null,
                null, null, null, "test", id, Snapshot.CommitKind.APPEND, id * 1000,
                Collections.emptyMap(), count, count, null, null, null, null, null);
        table.fileIO().mkdirs(table.snapshotManager().snapshotPath(id).getParent());
        table.fileIO().overwriteFileUtf8(table.snapshotManager().snapshotPath(id), snapshot.toJson());
        table.snapshotManager().commitLatestHint(id);
    }

    private static class ManifestGuardFileIO extends LocalFileIO {
        private boolean rejectManifests = true;

        @Override
        public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
            Assertions.assertFalse(rejectManifests && path.toString().contains("/manifest/"),
                    "Row count estimation must not read manifests: " + path);
            return super.newInputStream(path);
        }
    }
}
