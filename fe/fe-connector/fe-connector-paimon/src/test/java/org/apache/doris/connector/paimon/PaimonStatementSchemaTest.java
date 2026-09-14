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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ForwardingConnectorContext;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PaimonStatementSchemaTest {

    @Test
    public void latestSchemaSurvivesExternalRecreationWithReusedId(@TempDir Path warehouse) throws Exception {
        checkExternalRecreation(warehouse, false);
    }

    @Test
    public void latestSchemaAndDataSurviveExternalRecreationWithReusedIds(@TempDir Path warehouse) throws Exception {
        checkExternalRecreation(warehouse, true);
    }

    private void checkExternalRecreation(Path warehouse, boolean withData) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("old_name", DataTypes.INT()).build(), false);
            if (withData) {
                append((FileStoreTable) catalog.getTable(id), GenericRow.of(1));
            }
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonCatalogProperties props = PaimonCatalogProperties.of(Collections.emptyMap());
            PaimonSchemaAtMemo memo = new PaimonSchemaAtMemo(1000);
            PaimonLatestSnapshotCache cache = new PaimonLatestSnapshotCache(0, 1000);
            PaimonTableHandle firstHandle = new PaimonTableHandle("db", "t",
                    Collections.emptyList(), Collections.emptyList());
            firstHandle.setPaimonTable(catalog.getTable(id));
            PaimonConnectorMetadata first = new PaimonConnectorMetadata(
                    ops, props, new RecordingConnectorContext(), memo, cache);
            ConnectorMvccSnapshot oldPin = first.beginQuerySnapshot(null, firstHandle).get();
            Assertions.assertEquals("old_name", first.getTableSchema(null, firstHandle, oldPin)
                    .getColumns().get(0).getName());
            // Warm the historical memo independently; latest reads must not reuse it after recreation.
            new PaimonConnectorMetadata(ops, props, new RecordingConnectorContext(), memo, cache)
                    .getTableSchema(null, firstHandle, oldPin);
            Assertions.assertEquals(1, memo.size());
            catalog.dropTable(id, false);
            catalog.createTable(id, Schema.newBuilder().column("new_name", DataTypes.INT()).build(), false);
            if (withData) {
                append((FileStoreTable) catalog.getTable(id), GenericRow.of(2));
            }
            PaimonTableHandle secondHandle = new PaimonTableHandle("db", "t",
                    Collections.emptyList(), Collections.emptyList());
            secondHandle.setPaimonTable(catalog.getTable(id));
            PaimonConnectorMetadata second = new PaimonConnectorMetadata(
                    ops, props, new RecordingConnectorContext(), memo, cache);
            ConnectorMvccSnapshot newPin = second.beginQuerySnapshot(null, secondHandle).get();
            Assertions.assertEquals(oldPin.getSchemaId(), newPin.getSchemaId());
            Assertions.assertEquals(oldPin.getSnapshotId(), newPin.getSnapshotId());
            PaimonTableHandle pinned = (PaimonTableHandle) second.applySnapshot(null, secondHandle, newPin);
            Table scan = new PaimonScanPlanProvider(props, ops).resolveScanTable(pinned);
            Assertions.assertEquals("new_name", scan.rowType().getFieldNames().get(0));
            if (withData) {
                Assertions.assertEquals(Collections.singletonList(2), readIds((FileStoreTable) scan));
            }
            Assertions.assertTrue(second.getColumnHandles(null, secondHandle, newPin).containsKey("new_name"));
            Assertions.assertFalse(second.getColumnHandles(null, secondHandle, newPin).containsKey("old_name"));
            Assertions.assertEquals("new_name", second.getTableSchema(null, secondHandle, newPin)
                    .getColumns().get(0).getName(), "Recreated latest metadata must match the scan table");
        }
    }

    @Test
    public void latestCacheHitCapturesSchemaInsideAuth(@TempDir Path warehouse) throws Exception {
        checkAuthenticatedSchemaRead(warehouse, "capture");
    }

    @Test
    public void pinnedSchemaMaterializesInsideAuth(@TempDir Path warehouse) throws Exception {
        checkAuthenticatedSchemaRead(warehouse, "materialize");
    }

    @Test
    public void scanRestoresSchemaInsideAuth(@TempDir Path warehouse) throws Exception {
        checkAuthenticatedSchemaRead(warehouse, "restore");
    }

    @Test
    public void systemOptionsScanPropertiesRestoreSchemaInsideAuth(@TempDir Path warehouse) throws Exception {
        checkAuthenticatedSchemaRead(warehouse, "system-properties");
    }

    private static final class SchemaGuardFileIO extends LocalFileIO {
        // The FE-only assertion must not be serialized into the table sent to the backend.
        private final transient Runnable checkSchemaRead;

        private SchemaGuardFileIO(Runnable checkSchemaRead) {
            this.checkSchemaRead = checkSchemaRead;
        }

        @Override
        public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
            if (checkSchemaRead != null && path.toString().contains("/schema")) {
                checkSchemaRead.run();
            }
            return super.newInputStream(path);
        }
    }

    private void checkAuthenticatedSchemaRead(Path warehouse, String operation) throws Exception {
        AtomicBoolean enforceScope = new AtomicBoolean();
        ThreadLocal<Boolean> authenticated = ThreadLocal.withInitial(() -> false);
        AtomicInteger reads = new AtomicInteger();
        ClassLoader pluginLoader = new ClassLoader(getClass().getClassLoader()) {};
        ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();
        FileIO guarded = new SchemaGuardFileIO(() -> {
            if (enforceScope.get()) {
                Assertions.assertTrue(authenticated.get(), "schema FileIO must run inside auth");
                Assertions.assertSame(pluginLoader, Thread.currentThread().getContextClassLoader());
                reads.incrementAndGet();
            }
        });
        ConnectorContext context = new TcclPinningConnectorContext(
                new ForwardingConnectorContext(new RecordingConnectorContext()) {
                    @Override
                    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
                        boolean previous = authenticated.get();
                        authenticated.set(true);
                        try {
                            return task.call();
                        } finally {
                            authenticated.set(previous);
                        }
                    }
                }, pluginLoader, () -> null);
        try (Catalog catalog = new FileSystemCatalog(guarded,
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("old_name", DataTypes.INT())
                    .option("scan.manifest.parallelism", "1").build(), false);
            FileStoreTable warm = (FileStoreTable) catalog.getTable(id);
            PaimonTableHandle handle = new PaimonTableHandle("db", "t",
                    Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(warm);
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonLatestSnapshotCache cache = new PaimonLatestSnapshotCache(100, 1000);
            PaimonCatalogProperties props = PaimonCatalogProperties.of(Collections.emptyMap());
            new PaimonConnectorMetadata(ops, props, context, new PaimonSchemaAtMemo(1000), cache)
                    .beginQuerySnapshot(null, handle);
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("old_name", "bound_name")), false);
            long schemaId = ((FileStoreTable) catalog.getTable(id)).schema().id();
            PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                    ops, props, context, new PaimonSchemaAtMemo(1000), cache);
            ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(-1L)
                    .schemaId(schemaId).build();
            if (operation.equals("system-properties")) {
                snapshot = metadata.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.options(
                        Collections.singletonMap("scan.plan-sort-partition", "true"), -1L)).get();
                handle = (PaimonTableHandle) metadata.getSysTableHandle(null, handle, "ro").get();
            }
            enforceScope.set(true);
            if (operation.equals("capture")) {
                Assertions.assertEquals(schemaId, metadata.beginQuerySnapshot(null, handle).get().getSchemaId());
            } else if (operation.equals("materialize")) {
                Assertions.assertEquals("bound_name",
                        metadata.getTableSchema(null, handle, snapshot).getColumns().get(0).getName());
            } else {
                PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snapshot);
                PaimonScanPlanProvider provider = new PaimonScanPlanProvider(props, ops, context);
                Assertions.assertEquals(Collections.singletonList("bound_name"),
                        provider.resolveScanTable(pinned).rowType().getFieldNames());
                if (operation.equals("system-properties")) {
                    // Exercise both source restorations without the pre-existing native history-dictionary IO.
                    Map<String, String> scanProperties = provider.getScanNodeProperties(
                            session(true), pinned, Collections.emptyList(), Optional.empty());
                    Assertions.assertTrue(scanProperties.get("paimon.options_json")
                            .contains("doris.serialized-system-source"));
                }
            }
            Assertions.assertTrue(reads.get() > 0, "the assertion must exercise real schema-file IO");
            Assertions.assertFalse(authenticated.get());
            Assertions.assertSame(callerLoader, Thread.currentThread().getContextClassLoader());
            enforceScope.set(false);
        }
    }

    @Test
    public void warmTableKeepsExactStatementSchema(@TempDir Path warehouse) throws Exception {
        checkSchemaMutation(warehouse, false, false);
    }

    @Test
    public void warmOptionsTableKeepsExactStatementSchema(@TempDir Path warehouse) throws Exception {
        checkSchemaMutation(warehouse, true, false);
    }

    @Test
    public void systemOptionsKeepsExactStatementSchema(@TempDir Path warehouse) throws Exception {
        checkSchemaMutation(warehouse, true, true);
    }

    @Test
    public void warmKeyRenameUsesBoundSchemaOptions(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .column("old_key", DataTypes.INT().notNull()).primaryKey("id", "old_key")
                    .option("bucket", "1").option("bucket-key", "old_key")
                    .option("sequence.field", "old_key").option("file.format", "parquet").build(), false);
            FileStoreTable warm = (FileStoreTable) catalog.getTable(id);
            append(warm, GenericRow.of(1, 10));
            warm = warm.copyWithoutTimeTravel(Collections.singletonMap("read.batch-size", "64"));
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("old_key", "bound_key")), false);
            long boundId = ((FileStoreTable) catalog.getTable(id)).schema().id();
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("bound_key", "later_key")), false);
            for (FileStoreTable loaded : Arrays.asList(warm, (FileStoreTable) catalog.getTable(id))) {
                FileStoreTable pinned = PaimonScanParams.applyOptionsWithoutTimeTravel(loaded,
                        PaimonScanParams.withBoundSchema(
                                PaimonScanParams.pinOptionsToSnapshot(Collections.emptyMap(), 1), boundId));
                Assertions.assertEquals("bound_key", pinned.options().get("bucket-key"));
                Assertions.assertEquals("bound_key", pinned.options().get("sequence.field"));
                Assertions.assertEquals(Collections.singletonList(1), readIds(pinned));
                if (loaded == warm) {
                    Assertions.assertEquals("64", pinned.options().get("read.batch-size"));
                }
            }
        }
    }

    @Test
    public void stalePrivilegedFallbackKeepsBranchProvenance(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT())
                    .column("value", DataTypes.INT()).partitionKeys("id")
                    .option("file.format", "parquet").option("scan.manifest.parallelism", "1").build(), false);
            FileStoreTable warm = (FileStoreTable) catalog.getTable(id);
            warm.createBranch("backup");
            FileStoreTable fallback = warm.switchToBranch("backup");
            append(fallback, GenericRow.of(2, 20));
            append(warm, GenericRow.of(1, 10));
            catalog.alterTable(id, Collections.singletonList(SchemaChange.setOption("read.batch-size", "32")),
                    false);
            FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
            AtomicInteger selectChecks = new AtomicInteger();
            AtomicBoolean denied = new AtomicBoolean();
            PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                    PrivilegeChecker.class.getClassLoader(), new Class<?>[] {PrivilegeChecker.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("assertCanSelect")) {
                            selectChecks.incrementAndGet();
                            if (denied.get()) {
                                throw new SecurityException("Select denied");
                            }
                        }
                        return null;
                    });
            FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                    new FallbackReadFileStoreTable(warm, fallback), checker, id);
            FileStoreTable pinned = PaimonScanParams.applyOptionsWithoutTimeTravel(privileged,
                    PaimonScanParams.withBoundSchema(
                            PaimonScanParams.pinOptionsToSnapshot(Collections.emptyMap(), 1), latest.schema().id()));
            Assertions.assertInstanceOf(PrivilegedFileStoreTable.class, pinned);
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable)
                    ((DelegatedFileStoreTable) pinned).wrapped();
            Assertions.assertEquals("backup", pair.fallback().coreOptions().branch());
            Assertions.assertEquals(fallback.schema().id(), pair.fallback().schema().id());
            Assertions.assertEquals(latest.schema().id(), pair.wrapped().schema().id());
            List<Integer> ids = readIds(pinned);
            Collections.sort(ids);
            Assertions.assertEquals(Arrays.asList(1, 2), ids);
            Assertions.assertTrue(selectChecks.get() >= 2);
            denied.set(true);
            Assertions.assertThrows(SecurityException.class, pinned::newScan);
            Assertions.assertThrows(SecurityException.class, pinned::newRead);
        }
    }

    private void checkSchemaMutation(Path warehouse, boolean options, boolean system) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT())
                    .column("old_name", DataTypes.INT()).option("file.format", "parquet").build(), false);
            FileStoreTable warm = (FileStoreTable) catalog.getTable(id);
            append(warm, GenericRow.of(1, 10));
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("old_name", "bound_name")), false);
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(ops,
                    PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                    PaimonCatalogProperties.of(Collections.emptyMap()), ops);
            PaimonTableHandle handle = new PaimonTableHandle("db", "t",
                    Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(warm);
            ConnectorMvccSnapshot snapshot = metadata.beginQuerySnapshot(null, handle).get();
            if (options) {
                snapshot = metadata.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.options(
                        Collections.singletonMap("scan.plan-sort-partition", "true"),
                        snapshot.getSnapshotId())).get();
            }
            if (system) {
                handle = (PaimonTableHandle) metadata.getSysTableHandle(null, handle, "ro").get();
            }
            Assertions.assertTrue(metadata.getTableSchema(null, handle, snapshot).getColumns().stream()
                    .map(ConnectorColumn::getName).collect(Collectors.toList()).contains("bound_name"));
            // The second ALTER must not change a statement that already bound the first rename.
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.renameColumn("bound_name", "later_name")), false);
            PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snapshot);
            Table scan = provider.resolveScanTable(pinned);
            Assertions.assertEquals(Arrays.asList("id", "bound_name"), scan.rowType().getFieldNames());
            List<ConnectorColumnHandle> columns = Arrays.asList(new PaimonColumnHandle("id", 0),
                    new PaimonColumnHandle("bound_name", 1));
            Assertions.assertFalse(provider.planScan(session(),
                    ConnectorScanRequest.builder(pinned, columns).build()).isEmpty());
            Map<String, String> properties = provider.getScanNodeProperties(session(), pinned, columns,
                    Optional.empty());
            TFileScanRangeParams params = new TFileScanRangeParams();
            PaimonScanPlanProvider.applySchemaEvolutionParam(params, properties.get("paimon.schema_evolution"));
            Assertions.assertEquals("bound_name", params.getHistorySchemaInfo().get(0)
                    .getRootField().getFields().get(1).getFieldPtr().getName());
            Table backend = InstantiationUtil.deserializeObject(
                    InstantiationUtil.serializeObject(provider.tableForBackend(pinned, scan)),
                    getClass().getClassLoader());
            Assertions.assertEquals(scan.rowType(), backend.rowType());
            Assertions.assertEquals(Collections.singletonList(1), readIds(backend));
        }
    }

    @Test
    public void branchCommitAfterResolutionIsInvisible(@TempDir Path warehouse) throws Exception {
        checkBranchMutation(warehouse, false);
    }

    @Test
    public void emptyBranchRemainsEmptyAfterFirstCommit(@TempDir Path warehouse) throws Exception {
        checkBranchMutation(warehouse, true);
    }

    private void checkBranchMutation(Path warehouse, boolean empty) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT())
                    .option("file.format", "parquet").build(), false);
            FileStoreTable base = (FileStoreTable) catalog.getTable(id);
            base.branchManager().createBranch("dev");
            Identifier branchId = new Identifier("db", "t", "dev");
            FileStoreTable branch = (FileStoreTable) catalog.getTable(branchId);
            if (!empty) {
                append(branch, GenericRow.of(1));
            }
            catalog.alterTable(branchId, Collections.singletonList(
                    SchemaChange.addColumn("bound_name", DataTypes.INT())), false);
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(ops,
                    PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
            PaimonTableHandle handle = new PaimonTableHandle("db", "t",
                    Collections.emptyList(), Collections.emptyList());
            handle.setPaimonTable(base);
            ConnectorMvccSnapshot snapshot = metadata.resolveTimeTravel(null, handle,
                    ConnectorTimeTravelSpec.branch("dev")).get();
            catalog.alterTable(branchId, Collections.singletonList(
                    SchemaChange.renameColumn("bound_name", "later_name")), false);
            append((FileStoreTable) catalog.getTable(branchId), GenericRow.of(2, 20));
            PaimonTableHandle pinned = (PaimonTableHandle) metadata.applySnapshot(null, handle, snapshot);
            PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                    PaimonCatalogProperties.of(Collections.emptyMap()), ops);
            Assertions.assertEquals(Arrays.asList("id", "bound_name"),
                    provider.resolveScanTable(pinned).rowType().getFieldNames());
            if (empty) {
                Assertions.assertTrue(provider.planScan(session(), ConnectorScanRequest.builder(pinned,
                        Collections.singletonList(new PaimonColumnHandle("id", 0))).build()).isEmpty());
            } else {
                Table scan = provider.resolveScanTable(pinned);
                Assertions.assertEquals(Collections.singletonList(1), readIds(scan));
                Table backend = InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(provider.tableForBackend(pinned, scan)),
                        getClass().getClassLoader());
                Assertions.assertEquals(Collections.singletonList(1), readIds(backend));
            }
        }
    }

    private static void append(FileStoreTable table, GenericRow row) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }

    private static List<Integer> readIds(Table table) throws Exception {
        List<Integer> ids = new ArrayList<>();
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = table.newReadBuilder().newRead().createReader(split)) {
                reader.forEachRemaining(row -> ids.add(row.getInt(0)));
            }
        }
        Collections.sort(ids);
        return ids;
    }

    private static ConnectorSession session() {
        return session(false);
    }

    private static ConnectorSession session(boolean forceJni) {
        return new ConnectorSession() {
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
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return Collections.singletonMap("force_jni_scanner", Boolean.toString(forceJni));
            }
        };
    }

}
