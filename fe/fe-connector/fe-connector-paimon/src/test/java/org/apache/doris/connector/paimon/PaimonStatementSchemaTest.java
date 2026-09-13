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
import org.apache.doris.connector.spi.ConnectorSession;
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

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PaimonStatementSchemaTest {
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
                return Collections.emptyMap();
            }
        };
    }

}
