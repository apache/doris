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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Real SDK tables exercise scan, metadata and COUNT entry points without reading data files. */
public class IcebergEncryptionReadTest {
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db", "tbl");

    @Test
    public void encryptedReadsAreDisabledAcrossPlanningRoutes() throws Exception {
        for (boolean populated : new boolean[] {false, true}) {
            for (boolean cached : new boolean[] {false, true}) {
                IcebergTableCache cache = new IcebergTableCache(100, 10);
                try (InMemoryCatalog catalog = catalog()) {
                    Table table = encryptedView(table(catalog, populated), 3);
                    RecordingIcebergCatalogOps ops = ops(table);
                    IcebergScanPlanProvider provider = new IcebergScanPlanProvider(properties("hms"),
                            ignored -> ops, context(Map.of()), null, cached ? cache : null);
                    for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE,
                            new TestStatementScope())) {
                        ConnectorSession session = new IcebergStatementScopeTest.ScopeSession(1L, "query", scope);
                        try {
                            reject("READ_DISABLED", () -> provider.planScan(session, request(HANDLE, false)));
                            reject("READ_DISABLED", () -> provider.planScan(session, request(HANDLE, true)));
                            reject("READ_DISABLED", () -> provider.canServeMetadataOnlyCount(
                                    session, HANDLE, Optional.empty()));
                            reject("READ_DISABLED", () -> provider.streamingSplitEstimate(
                                    session, HANDLE, Optional.empty(), false));
                            reject("READ_DISABLED", () -> provider.streamSplits(session, request(HANDLE, false)));
                            for (String name : List.of("files", "snapshots", "position_deletes", "labels")) {
                                IcebergTableHandle sys = IcebergTableHandle.forSystemTable(
                                        "db", "tbl", name, -1L, null, -1L);
                                reject("READ_DISABLED", () -> provider.planScan(session, request(sys, false)));
                            }
                        } finally {
                            scope.closeAll();
                        }
                    }
                } finally {
                    cache.close();
                }
            }
        }
    }

    @Test
    public void enablingReadStillRequiresSupportedCatalogAndBackendCapability() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = encryptedView(table(catalog, true), 3);
            for (String flavor : List.of("hms", "rest", "hadoop", "jdbc", "glue", "dlf", "s3tables")) {
                IcebergScanPlanProvider provider = new IcebergScanPlanProvider(properties(flavor),
                        ops(table), context(Map.of("enable_encryption_read", "true")));
                reject(flavor.equals("hms") ? "BACKEND_CAPABILITY_UNAVAILABLE" : "UNSUPPORTED_CATALOG",
                        () -> provider.planScan(null, request(HANDLE, true)));
            }
        }
    }

    @Test
    public void unsupportedTableVersionHasAnExplicitError() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = encryptedView(table(catalog, false), 2);
            IcebergScanPlanProvider provider = new IcebergScanPlanProvider(properties("hms"),
                    ops(table), context(Map.of("enable_encryption_read", "true")));
            reject("UNSUPPORTED_TABLE_FORMAT", () -> provider.planScan(null, request(HANDLE, false)));
        }
    }

    @Test
    public void metadataBindingCannotBypassReadChecks() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = encryptedView(table(catalog, true), 3);
            IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                    ops(table), properties("hms"), context(Map.of()));
            reject("READ_DISABLED", () -> metadata.getTableSchema(null, HANDLE));
            reject("READ_DISABLED", () -> metadata.beginQuerySnapshot(null, HANDLE));
            IcebergTableHandle sys = IcebergTableHandle.forSystemTable("db", "tbl", "files", -1L, null, -1L);
            reject("READ_DISABLED", () -> metadata.getTableSchema(null, sys));
        }
    }

    @Test
    public void retainedEncryptionKeysAndEmptySnapshotPinsCannotBypassReadChecks() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = table(catalog, false);
            ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(
                    TableMetadataParser.toJson(((BaseTable) table).operations().current()));
            ObjectNode key = json.putArray("encryption-keys").addObject();
            key.put("key-id", "test-key");
            key.put("encrypted-key-metadata", "AA==");
            Table retained = new BaseTable(new StaticTableOperations(
                    TableMetadataParser.fromJson(json.toString()), table.io()), "db.tbl");
            IcebergScanPlanProvider provider = new IcebergScanPlanProvider(properties("hms"), ops(retained));
            IcebergTableHandle empty = HANDLE.withSnapshot(-1L, null, 0L);
            reject("READ_DISABLED", () -> provider.planScan(null, request(empty, false)));
            reject("READ_DISABLED", () -> provider.streamSplits(null, request(empty, false)));
            reject("READ_DISABLED", () -> provider.planScan(null, request(HANDLE, true)));
        }
    }

    @Test
    public void customEncryptionManagerIsDetectedWithoutTableProperties() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = table(catalog, false);
            StaticTableOperations operations = new StaticTableOperations(
                    ((BaseTable) table).operations().current(), new NoReadFileIO()) {
                @Override
                public EncryptionManager encryption() {
                    return new EncryptionManager() {
                        @Override
                        public InputFile decrypt(EncryptedInputFile input) {
                            throw new AssertionError("Admission must not decrypt files");
                        }

                        @Override
                        public EncryptedOutputFile encrypt(OutputFile output) {
                            throw new AssertionError("Read admission must not encrypt files");
                        }
                    };
                }
            };
            Table custom = new BaseTable(operations, "db.tbl");
            reject("READ_DISABLED", () -> new IcebergScanPlanProvider(properties("hms"), ops(custom))
                    .planScan(null, request(HANDLE, false)));
            reject("UNSUPPORTED_ENCRYPTION_MANAGER", () -> new IcebergScanPlanProvider(properties("hms"),
                    ops(custom), context(Map.of("enable_encryption_read", "true")))
                    .planScan(null, request(HANDLE, false)));
        }
    }

    @Test
    public void unverifiedBackendRejectsBeforeKmsManagerInitialization() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            BaseTable encrypted = (BaseTable) encryptedView(table(catalog, false), 3);
            Table table = new BaseTable(new StaticTableOperations(
                    encrypted.operations().current(), new NoReadFileIO()) {
                @Override
                public EncryptionManager encryption() {
                    throw new AssertionError("Unverified execution must not initialize a KMS client");
                }
            }, "db.tbl");
            IcebergScanPlanProvider provider = new IcebergScanPlanProvider(properties("hms"), ops(table),
                    context(Map.of("enable_encryption_read", "true")));
            reject("BACKEND_CAPABILITY_UNAVAILABLE", () -> provider.planScan(null, request(HANDLE, false)));
        }
    }

    @Test
    public void plaintextScansAndCountsKeepWorkingWithEitherSwitchValue() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = table(catalog, true);
            for (String enabled : List.of("false", "true")) {
                IcebergScanPlanProvider provider = new IcebergScanPlanProvider(properties("hadoop"),
                        ops(table), context(Map.of("enable_encryption_read", enabled)));
                Assertions.assertEquals(1, provider.planScan(null, request(HANDLE, false)).size());
                Assertions.assertEquals(10L, provider.planScan(null, request(HANDLE, true))
                        .get(0).getPushDownRowCount());
                Assertions.assertTrue(provider.canServeMetadataOnlyCount(null, HANDLE, Optional.empty()));
            }
        }
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static Table table(InMemoryCatalog catalog, boolean populated) {
        Table table = catalog.createTable(TableIdentifier.of("db", "tbl"), SCHEMA, PartitionSpec.unpartitioned(),
                null, Map.of("format-version", "3"));
        if (populated) {
            table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("memory:///data.parquet")
                    .withFormat("parquet").withFileSizeInBytes(128).withRecordCount(10).build()).commit();
        }
        return table;
    }

    private static Table encryptedView(Table table, int version) throws Exception {
        // Metadata-only fixtures must not need a KMS or a usable key to exercise admission control.
        TableMetadata metadata = ((BaseTable) table).operations().current();
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(TableMetadataParser.toJson(metadata));
        json.put("format-version", version);
        json.with("properties").put(TableProperties.ENCRYPTION_TABLE_KEY, "test-key");
        metadata = TableMetadataParser.fromJson(json.toString());
        return new BaseTable(new StaticTableOperations(metadata, new NoReadFileIO()), "db.tbl");
    }

    private static RecordingIcebergCatalogOps ops(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        ops.tableExists = true;
        return ops;
    }

    private static IcebergCatalogProperties properties(String flavor) {
        return IcebergCatalogProperties.of(Map.of("iceberg.catalog.type", flavor));
    }

    private static ConnectorContext context(Map<String, String> config) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getConnectorConfig() {
                return config;
            }
        };
    }

    private static ConnectorScanRequest request(IcebergTableHandle handle, boolean count) {
        return ConnectorScanRequest.builder(handle, List.of()).countPushdown(count).build();
    }

    private static void reject(String classification, Executable action) {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, action);
        Assertions.assertTrue(failure.getMessage().contains("Iceberg encryption [" + classification + "]"),
                failure::getMessage);
        Assertions.assertFalse(failure.getMessage().contains("test-key"), "key identifiers must not be disclosed");
    }

    private static final class NoReadFileIO implements FileIO {
        @Override
        public InputFile newInputFile(String path) {
            throw new AssertionError("Admission must precede manifest and data I/O");
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new AssertionError("Read admission must not write files");
        }

        @Override
        public void deleteFile(String path) {
            throw new AssertionError("Read admission must not delete files");
        }
    }
}
