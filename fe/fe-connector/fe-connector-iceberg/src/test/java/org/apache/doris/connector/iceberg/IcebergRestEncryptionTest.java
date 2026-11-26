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

import org.apache.doris.connector.spi.DorisConnectorException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergRestEncryptionTest {
    private static final TableIdentifier ID = TableIdentifier.of("db", "encrypted");

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restTablesInitializeEncryptionLazilyAndCloseOwnedClients(boolean tableConfig) throws Exception {
        FixtureKms.initialized.set(0);
        FixtureKms.closed.set(0);
        try (Fixture fixture = new Fixture(tableConfig)) {
            Table table = fixture.load();
            Assertions.assertEquals(0, FixtureKms.initialized.get(), "loading must not initialize KMS");
            Assertions.assertInstanceOf(StandardEncryptionManager.class, table.encryption());
            Assertions.assertEquals(1, FixtureKms.initialized.get());
            Assertions.assertEquals(tableConfig ? "table" : "catalog", FixtureKms.scope.get());
            Assertions.assertSame(table.encryption(), table.encryption());
            FileIO rawCatalogIo = fixture.catalogIo;
            TrackingFileIO rawTableIo = (TrackingFileIO) ((IcebergRestEncryptionFileIO) table.io()).delegate();
            table.io().close();
            table.io().close();
            Assertions.assertEquals(tableConfig ? 1 : 0, rawTableIo.closes.get(),
                    "only table-specific FileIO belongs to the table lease");
            Assertions.assertThrows(IllegalStateException.class, table::encryption);
            Assertions.assertEquals(1, FixtureKms.closed.get(), "one owner closes each KMS client once");
            if (rawCatalogIo instanceof TrackingFileIO) {
                Assertions.assertEquals(0, ((TrackingFileIO) rawCatalogIo).closes.get(),
                        "closing one table must not close the shared catalog FileIO");
            }
        }
        Assertions.assertEquals(1, FixtureKms.closed.get());
    }

    @Test
    void restReadsEncryptedManifestsFilesAndHistoricalSnapshotsAcrossRefresh() throws Exception {
        FixtureKms.initialized.set(0);
        FixtureKms.denied.set(false);
        FixtureKms.wrongKey.set(false);
        try (MemoryEncryptedTable writer = new MemoryEncryptedTable()) {
            writer.append();
            long firstSnapshot = writer.current().currentSnapshot().snapshotId();
            writer.append();
            Assertions.assertFalse(writer.current().encryptionKeys().isEmpty());
            try (Fixture fixture = new Fixture(true, writer.current())) {
                Table table = fixture.load();
                Assertions.assertEquals(0, FixtureKms.initialized.get());
                Assertions.assertEquals(2, filesAndPayloads(table, null));
                Assertions.assertEquals(1, filesAndPayloads(table, firstSnapshot));
                EncryptionManager first = table.encryption();
                writer.append();
                fixture.metadata.set(writer.current());
                table.refresh();
                Assertions.assertEquals(3, filesAndPayloads(table, null));
                Assertions.assertEquals(1, filesAndPayloads(table, firstSnapshot));
                Assertions.assertNotSame(first, table.encryption(), "refresh must publish the new encrypted key chain");
                Assertions.assertEquals(1, FixtureKms.initialized.get(), "refresh reuses the table-owned KMS client");
                Assertions.assertThrows(UnsupportedOperationException.class,
                        () -> table.io().newOutputFile("memory://unsupported-write"));
                Assertions.assertThrows(UnsupportedOperationException.class,
                        () -> table.io().deleteFile("memory://unsupported-delete"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restMetadataDecryptionRejectsDeniedOrWrongKeys(boolean wrongKey) throws Exception {
        FixtureKms.denied.set(false);
        FixtureKms.wrongKey.set(false);
        try (MemoryEncryptedTable writer = new MemoryEncryptedTable()) {
            writer.append();
            try (Fixture fixture = new Fixture(true, writer.current())) {
                Table table = fixture.load();
                FixtureKms.denied.set(!wrongKey);
                FixtureKms.wrongKey.set(wrongKey);
                RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                        () -> filesAndPayloads(table, null));
                Throwable cause = failure;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                if (wrongKey) {
                    Assertions.assertInstanceOf(javax.crypto.AEADBadTagException.class, cause);
                } else {
                    Assertions.assertEquals("Fixture KMS denied decryption", cause.getMessage());
                }
            }
        } finally {
            FixtureKms.denied.set(false);
            FixtureKms.wrongKey.set(false);
        }
    }

    @Test
    void parallelManifestReadersInitializeOneClientAndCloseItOnce() throws Exception {
        FixtureKms.initialized.set(0);
        FixtureKms.closed.set(0);
        try (Fixture fixture = new Fixture(false)) {
            Table table = fixture.load();
            var workers = Executors.newFixedThreadPool(4);
            try {
                var results = new ArrayList<java.util.concurrent.Future<EncryptionManager>>();
                for (int i = 0; i < 16; i++) {
                    results.add(workers.submit(table::encryption));
                }
                for (var result : results) {
                    Assertions.assertSame(table.encryption(), result.get(10, TimeUnit.SECONDS));
                }
                Assertions.assertEquals(1, FixtureKms.initialized.get());
            } finally {
                workers.shutdownNow();
                Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
        Assertions.assertEquals(1, FixtureKms.closed.get());
    }

    @Test
    void plaintextRestTablesDoNotAcquireAnEncryptionManager() throws Exception {
        FixtureKms.initialized.set(0);
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(
                TableMetadataParser.toJson(Fixture.emptyEncryptedMetadata()));
        json.with("properties").remove("encryption.key-id");
        try (Fixture fixture = new Fixture(false, TableMetadataParser.fromJson(json.toString()))) {
            Table table = fixture.load();
            Assertions.assertSame(fixture.catalogIo, table.io());
            Assertions.assertInstanceOf(org.apache.iceberg.encryption.PlaintextEncryptionManager.class,
                    table.encryption());
            Assertions.assertEquals(0, FixtureKms.initialized.get());
        }
    }

    @Test
    void missingKmsConfigurationCannotFallBackToPlaintext() throws Exception {
        FixtureKms.initialized.set(0);
        try (Fixture fixture = new Fixture(false, Fixture.emptyEncryptedMetadata(), false)) {
            Table table = fixture.load();
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, table::encryption);
            Assertions.assertEquals("Iceberg encryption [MISSING_KMS]", failure.getMessage());
            Assertions.assertEquals(0, FixtureKms.initialized.get());
        }
    }

    @Test
    void retainedKeysCannotFallBackToPlaintextWithoutACurrentWriteKey() throws Exception {
        try (MemoryEncryptedTable writer = new MemoryEncryptedTable()) {
            writer.append();
            ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(TableMetadataParser.toJson(writer.current()));
            json.with("properties").remove("encryption.key-id");
            try (Fixture fixture = new Fixture(false, TableMetadataParser.fromJson(json.toString()))) {
                DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                        fixture.load()::encryption);
                Assertions.assertEquals("Iceberg encryption [MISSING_TABLE_KEY]", failure.getMessage());
            }
        }
    }

    @Test
    void encryptedRestTransactionsRejectBeforeInitializingKms() throws Exception {
        FixtureKms.initialized.set(0);
        try (Fixture fixture = new Fixture(false)) {
            Table table = fixture.load();
            Assertions.assertThrows(UnsupportedOperationException.class, table::newTransaction);
            Assertions.assertEquals(0, FixtureKms.initialized.get());
        }
    }

    private static int filesAndPayloads(Table table, Long snapshot) throws Exception {
        var scan = table.newScan();
        if (snapshot != null) {
            scan = scan.useSnapshot(snapshot);
        }
        int files = 0;
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                Assertions.assertNotNull(task.file().keyMetadata());
                try (var input = table.io().newInputFile(task.file()).newStream()) {
                    Assertions.assertArrayEquals(new byte[] {1, 2, 3, 4}, input.readAllBytes());
                }
                var delete = FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                        .withPath(task.file().location()).withFormat("parquet").withRecordCount(1)
                        .withFileSizeInBytes(task.file().fileSizeInBytes())
                        .withEncryptionKeyMetadata(task.file().keyMetadata()).build();
                try (var input = table.io().newInputFile(delete).newStream()) {
                    Assertions.assertArrayEquals(new byte[] {1, 2, 3, 4}, input.readAllBytes());
                }
                files++;
            }
        }
        return files;
    }

    private static final class MemoryEncryptedTable implements TableOperations, AutoCloseable {
        private final InMemoryFileIO raw = new InMemoryFileIO();
        private final StandardEncryptionManager manager = new StandardEncryptionManager(
                List.of(), "fixture-key", 16, new FixtureKms());
        private final FileIO io = EncryptingFileIO.combine(raw, manager);
        private TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), SortOrder.unsorted(), "memory://rest-" + UUID.randomUUID(),
                Map.of("format-version", "3", "encryption.key-id", "fixture-key"));

        private void append() throws Exception {
            String path = metadata.location() + "/" + UUID.randomUUID() + ".parquet";
            NativeEncryptionOutputFile output = manager.encrypt(raw.newOutputFile(path));
            try (var stream = output.encryptingOutputFile().create()) {
                stream.write(new byte[] {1, 2, 3, 4});
            }
            long length = raw.newInputFile(path).getLength();
            DataFile data = DataFiles.builder(PartitionSpec.unpartitioned()).withPath(path)
                    .withFormat("parquet").withFileSizeInBytes(length).withRecordCount(1)
                    .withEncryptionKeyMetadata(output.keyMetadata().copyWithLength(length)).build();
            new BaseTable(this, "fixture").newFastAppend().appendFile(data).commit();
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            Assertions.assertSame(metadata, base);
            // REST refresh identifies a new metadata generation by its metadata-location, as a real catalog does.
            metadata = TableMetadata.buildFrom(updated).discardChanges()
                    .withMetadataLocation(updated.location() + "/metadata/" + UUID.randomUUID() + ".metadata.json")
                    .build();
        }

        @Override
        public FileIO io() {
            return io;
        }

        @Override
        public EncryptionManager encryption() {
            return manager;
        }

        @Override
        public String metadataFileLocation(String name) {
            return metadata.location() + "/metadata/" + name;
        }

        @Override
        public LocationProvider locationProvider() {
            return LocationProviders.locationsFor(metadata.location(), metadata.properties());
        }

        @Override
        public void close() {
            raw.close();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final HttpServer server;
        private final IcebergConnector connector;
        private final Catalog catalog;
        private final FileIO catalogIo;
        private final List<Table> loaded = new ArrayList<>();

        private final AtomicReference<TableMetadata> metadata;
        private final boolean tableConfig;

        private Fixture(boolean tableConfig) throws Exception {
            this(tableConfig, emptyEncryptedMetadata());
        }

        private static TableMetadata emptyEncryptedMetadata() throws Exception {
            try (InMemoryCatalog storage = new InMemoryCatalog()) {
                storage.initialize("fixture", Map.of());
                storage.createNamespace(Namespace.of("db"));
                BaseTable table = (BaseTable) storage.createTable(ID,
                        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                        PartitionSpec.unpartitioned(), null, Map.of("format-version", "3"));
                ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(
                        TableMetadataParser.toJson(table.operations().current()));
                json.with("properties").put("encryption.key-id", "fixture-key");
                return TableMetadataParser.fromJson(json.toString());
            }
        }

        private Fixture(boolean tableConfig, TableMetadata initialMetadata) throws Exception {
            this(tableConfig, initialMetadata, true);
        }

        private Fixture(boolean tableConfig, TableMetadata initialMetadata, boolean configuredKms) throws Exception {
            this.tableConfig = tableConfig;
            metadata = new AtomicReference<>(initialMetadata);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/", exchange -> {
                try (exchange) {
                    exchange.getRequestBody().close();
                    byte[] body;
                    if (exchange.getRequestURI().getPath().equals("/v1/config")) {
                        body = "{\"defaults\":{},\"overrides\":{\"fixture.kms.scope\":\"catalog\"}}"
                                .getBytes(StandardCharsets.UTF_8);
                    } else {
                        LoadTableResponse.Builder load = LoadTableResponse.builder().withTableMetadata(metadata.get());
                        if (this.tableConfig) {
                            load.addConfig("fixture.kms.scope", "table");
                        }
                        body = LoadTableResponseParser.toJson(load.build()).getBytes(StandardCharsets.UTF_8);
                    }
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                }
            });
            server.start();
            Map<String, String> properties = new HashMap<>(Map.of("iceberg.catalog.type", "rest",
                    "iceberg.rest.uri", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "rest.auth.type", "none", "io-impl", TrackingFileIO.class.getName()));
            if (configuredKms) {
                properties.put(CatalogProperties.ENCRYPTION_KMS_IMPL, FixtureKms.class.getName());
            }
            connector = new IcebergConnector(properties, new RecordingConnectorContext());
            Method get = IcebergConnector.class.getDeclaredMethod("getOrCreateCatalog");
            get.setAccessible(true);
            catalog = (Catalog) get.invoke(connector);
            Field session = IcebergConnector.class.getDeclaredField("restSessionCatalog");
            session.setAccessible(true);
            catalogIo = IcebergConnector.restCatalogFileIO(session.get(connector));
        }

        private Table load() {
            Table table = catalog.loadTable(ID);
            loaded.add(table);
            return table;
        }

        @Override
        public void close() throws Exception {
            try {
                for (Table table : loaded) {
                    IcebergConnector.cachedTableCleanup(table, "rest").run();
                }
                connector.close();
            } finally {
                server.stop(0);
            }
        }
    }

    public static class FixtureKms implements KeyManagementClient {
        private static final AtomicInteger initialized = new AtomicInteger();
        private static final AtomicInteger closed = new AtomicInteger();
        private static final AtomicReference<String> scope = new AtomicReference<>();
        private static final AtomicBoolean denied = new AtomicBoolean();
        private static final AtomicBoolean wrongKey = new AtomicBoolean();

        @Override
        public void initialize(Map<String, String> properties) {
            Assertions.assertTrue(properties.containsKey("fixture.kms.scope"));
            scope.set(properties.get("fixture.kms.scope"));
            initialized.incrementAndGet();
        }

        @Override
        public ByteBuffer wrapKey(ByteBuffer key, String keyId) {
            return key.duplicate();
        }

        @Override
        public ByteBuffer unwrapKey(ByteBuffer key, String keyId) {
            if (denied.get()) {
                throw new IllegalStateException("Fixture KMS denied decryption");
            }
            byte[] bytes = new byte[key.remaining()];
            key.duplicate().get(bytes);
            if (wrongKey.get()) {
                bytes[0] ^= 1;
            }
            return ByteBuffer.wrap(bytes);
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }

    public static class TrackingFileIO implements FileIO {
        private final InMemoryFileIO delegate = new InMemoryFileIO();
        private final AtomicInteger closes = new AtomicInteger();
        private Map<String, String> properties = Map.of();

        @Override
        public void initialize(Map<String, String> config) {
            properties = Map.copyOf(config);
        }

        @Override
        public Map<String, String> properties() {
            return properties;
        }

        @Override
        public InputFile newInputFile(String path) {
            Assertions.assertFalse(path.endsWith(".metadata.json"), "REST metadata must come from the catalog response");
            return delegate.newInputFile(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            delegate.close();
        }
    }
}
