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

package org.apache.doris.iceberg;

import org.apache.doris.jni.spi.utils.OffHeap;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergMetadataTaskLifecycleTest {
    public static class TrackingFileIO extends HadoopFileIO {
        private static final AtomicReference<TrackingFileIO> LAST_CLOSED = new AtomicReference<>();
        private int closes;

        @Override
        public void close() {
            closes++;
            LAST_CLOSED.set(this);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILES", "ENTRIES", "ALL_MANIFESTS"})
    void closesOnlyTheDeserializedTaskOwnerOnce(String tableType, @TempDir Path warehouse) throws Exception {
        try (HadoopCatalog catalog = catalog(warehouse)) {
            FileScanTask task = task(catalog, warehouse, tableType);
            TrackingFileIO original = owner(task);
            IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(16, params(task, "valid"));
            TrackingFileIO restored = owner(restoredTask(scanner));
            Assertions.assertNotSame(original, restored);

            scanner.close();
            scanner.close();

            Assertions.assertEquals(1, restored.closes);
            Assertions.assertEquals(0, original.closes);
        }
    }

    @Test
    void constructorFailureClosesTheDeserializedOwner(@TempDir Path warehouse) throws Exception {
        try (HadoopCatalog catalog = catalog(warehouse)) {
            FileScanTask task = task(catalog, warehouse, "FILES");
            TrackingFileIO original = owner(task);
            TrackingFileIO.LAST_CLOSED.set(null);

            Assertions.assertThrows(NullPointerException.class,
                    () -> new IcebergSysTableJniScanner(16, params(task, "missing")));

            Assertions.assertNotNull(TrackingFileIO.LAST_CLOSED.get());
            Assertions.assertNotSame(original, TrackingFileIO.LAST_CLOSED.get());
            Assertions.assertEquals(0, original.closes);
        }
    }

    @Test
    void constructorFailureDoesNotDispatchToAnUninitializedSubclass(@TempDir Path warehouse) throws Exception {
        try (HadoopCatalog catalog = catalog(warehouse)) {
            FileScanTask task = task(catalog, warehouse, "FILES");
            TrackingFileIO original = owner(task);
            TrackingFileIO.LAST_CLOSED.set(null);

            Assertions.assertThrows(NullPointerException.class,
                    () -> new IcebergSysTableJniScanner(16, params(task, "missing")) {
                        @Override
                        protected void closeInternal() throws IOException {
                            throw new IOException("subclass is not initialized");
                        }
                    });

            Assertions.assertNotNull(TrackingFileIO.LAST_CLOSED.get());
            Assertions.assertNotSame(original, TrackingFileIO.LAST_CLOSED.get());
            Assertions.assertEquals(0, original.closes);
        }
    }

    @Test
    void staticMetadataRowsRemainReadableWithoutAnEmbeddedOwner(@TempDir Path warehouse) throws Exception {
        OffHeap.setTesting();
        try (HadoopCatalog catalog = catalog(warehouse)) {
            FileScanTask task = task(catalog, warehouse, "SNAPSHOTS");
            Map<String, String> parameters = ImmutableMap.of(
                    "serialized_split", SerializationUtil.serializeToBase64(task),
                    "required_fields", "snapshot_id", "required_types", "bigint", "time_zone", "UTC");
            IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(16, parameters);
            try {
                scanner.open();
                Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                Assertions.assertEquals(1, scanner.getTable().getNumRows());
                scanner.releaseTable();
                Assertions.assertEquals(0, scanner.getNextBatchMeta());
            } finally {
                scanner.releaseTable();
                scanner.close();
                scanner.close();
            }
        }
    }

    @Test
    void readerCloseFailureStillReleasesTheOwner(@TempDir Path warehouse) throws Exception {
        OffHeap.setTesting();
        try (HadoopCatalog catalog = catalog(warehouse)) {
            FileScanTask task = task(catalog, warehouse, "FILES");
            IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(16, params(task, "valid"));
            TrackingFileIO restored = owner(restoredTask(scanner));
            scanner.open();
            failReaderClose(scanner);

            IOException failure = Assertions.assertThrows(IOException.class, scanner::close);
            Assertions.assertEquals("synthetic reader close failure", failure.getMessage());
            Assertions.assertEquals(1, restored.closes);
            Assertions.assertDoesNotThrow(scanner::close);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"complete", "open-failure", "reader-close-failure"})
    void cancelsRealVendedGcsRefreshOnEveryExit(String exit, @TempDir Path warehouse) throws Exception {
        OffHeap.setTesting();
        try (HadoopCatalog catalog = catalog(warehouse)) {
            FileScanTask task = task(catalog, warehouse, "FILES");
            Field manifestField = task.getClass().getDeclaredField("manifest");
            manifestField.setAccessible(true);
            ManifestFile manifest = (ManifestFile) manifestField.get(task);
            byte[] bytes = "open-failure".equals(exit) ? new byte[] {1, 2, 3}
                    : Files.readAllBytes(Paths.get(URI.create(manifest.path())));
            AtomicReference<String> authorization = new AtomicReference<>();
            AtomicInteger refreshes = new AtomicInteger();
            HttpServer server = manifestServer(bytes, authorization, refreshes);
            server.start();
            FileIO original = null;
            FileIO restored = null;
            IcebergSysTableJniScanner scanner = null;
            try {
                String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
                String location = "gs://test-bucket/manifest.avro";
                original = (FileIO) Class.forName("org.apache.iceberg.gcp.gcs.GCSFileIO")
                        .getConstructor().newInstance();
                Map<String, String> properties = new HashMap<>();
                properties.put("gcs.project-id", "test-project");
                properties.put("gcs.service.host", endpoint);
                properties.put("gcs.oauth2.token", "synthetic-vended-token");
                properties.put("gcs.oauth2.refresh-credentials-endpoint", endpoint + "/refresh");
                properties.put("uri", endpoint);
                original.initialize(properties);
                ((SupportsStorageCredentials) original).setCredentials(Collections.singletonList(
                        StorageCredential.create("gs://test-bucket", ImmutableMap.of(
                                "gcs.oauth2.token", "synthetic-vended-token",
                                "gcs.oauth2.token-expires-at", String.valueOf(System.currentTimeMillis() + 3600000)))));
                // Use an actual SDK manifest task and Avro reader; only the fixture's storage location changes.
                ManifestFile remote = manifest.copy();
                ((StructLike) remote).set(0, location);
                manifestField.set(task, remote);
                Field ioField = task.getClass().getDeclaredField("io");
                ioField.setAccessible(true);
                ioField.set(task, original);
                scanner = new IcebergSysTableJniScanner(16, params(task, "valid"));
                restored = fileIO(restoredTask(scanner));
                Assertions.assertNotSame(original, restored);
                Assertions.assertNull(refreshFuture(restored));

                ScheduledFuture<?> refresh;
                if ("open-failure".equals(exit)) {
                    restored.newInputFile(location, bytes.length);
                    refresh = refreshFuture(restored);
                    Assertions.assertNotNull(refresh);
                    Assertions.assertThrows(IOException.class, scanner::open);
                } else {
                    scanner.open();
                    refresh = refreshFuture(restored);
                    Assertions.assertNotNull(refresh);
                    Assertions.assertFalse(refresh.isDone());
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertEquals(1, scanner.getTable().getNumRows());
                    scanner.releaseTable();
                    Assertions.assertEquals("Bearer synthetic-vended-token", authorization.get());
                    if ("reader-close-failure".equals(exit)) {
                        failReaderClose(scanner);
                        Assertions.assertThrows(IOException.class, scanner::close);
                    } else {
                        scanner.close();
                    }
                }
                Assertions.assertTrue(refresh.isCancelled());
                Assertions.assertTrue(resourceClosed(restored));
                Assertions.assertFalse(resourceClosed(original));
                Assertions.assertEquals(0, refreshes.get());
                Assertions.assertDoesNotThrow(scanner::close);
            } finally {
                try {
                    if (scanner != null) {
                        scanner.releaseTable();
                        try {
                            scanner.close();
                        } catch (IOException ignored) {
                            // The baseline may still throw the deliberately injected reader-close failure.
                        }
                    }
                } finally {
                    try {
                        // Release the clone on the unfixed baseline so a failing regression leaves no job behind.
                        if (restored != null) {
                            restored.close();
                        }
                    } finally {
                        try {
                            if (original != null) {
                                original.close();
                            }
                        } finally {
                            server.stop(0);
                        }
                    }
                }
            }
        }
    }

    private static HttpServer manifestServer(byte[] content, AtomicReference<String> authorization,
            AtomicInteger refreshes) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                if (exchange.getRequestURI().getPath().contains("/refresh")) {
                    refreshes.incrementAndGet();
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                int start = 0;
                int end = content.length - 1;
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range != null) {
                    String[] positions = range.substring("bytes=".length()).split("-", -1);
                    start = Integer.parseInt(positions[0]);
                    if (!positions[1].isEmpty()) {
                        end = Math.min(end, Integer.parseInt(positions[1]));
                    }
                }
                if (start >= content.length) {
                    exchange.getResponseHeaders().set("Content-Range", "bytes */" + content.length);
                    exchange.sendResponseHeaders(416, -1);
                    return;
                }
                byte[] body = Arrays.copyOfRange(content, start, end + 1);
                exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + content.length);
                exchange.sendResponseHeaders(range == null ? 200 : 206, body.length);
                exchange.getResponseBody().write(body);
            } finally {
                exchange.close();
            }
        });
        return server;
    }

    private static void failReaderClose(IcebergSysTableJniScanner scanner) throws Exception {
        Field field = IcebergSysTableJniScanner.class.getDeclaredField("reader");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        CloseableIterator<StructLike> reader = (CloseableIterator<StructLike>) field.get(scanner);
        field.set(scanner, new CloseableIterator<StructLike>() {
            @Override
            public boolean hasNext() {
                return reader.hasNext();
            }

            @Override
            public StructLike next() {
                return reader.next();
            }

            @Override
            public void close() throws IOException {
                reader.close();
                throw new IOException("synthetic reader close failure");
            }
        });
    }

    private static ScheduledFuture<?> refreshFuture(FileIO io) throws Exception {
        Field field = io.getClass().getDeclaredField("refreshFuture");
        field.setAccessible(true);
        return (ScheduledFuture<?>) field.get(io);
    }

    private static boolean resourceClosed(FileIO io) throws Exception {
        Field field = io.getClass().getDeclaredField("isResourceClosed");
        field.setAccessible(true);
        return ((AtomicBoolean) field.get(io)).get();
    }

    private static HadoopCatalog catalog(Path warehouse) {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("test", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toUri().toString(),
                CatalogProperties.FILE_IO_IMPL, TrackingFileIO.class.getName()));
        return catalog;
    }

    private static FileScanTask task(HadoopCatalog catalog, Path warehouse, String tableType) throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = catalog.createTable(TableIdentifier.of("db", "t"), schema, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(warehouse.resolve("data.parquet").toString())
                .withFileSizeInBytes(10).withRecordCount(1).build()).commit();
        Table metadata = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.valueOf(tableType));
        try (CloseableIterable<FileScanTask> tasks = metadata.newScan().planFiles()) {
            return tasks.iterator().next();
        }
    }

    private static TrackingFileIO owner(FileScanTask task) throws Exception {
        return (TrackingFileIO) fileIO(task);
    }

    private static FileIO fileIO(FileScanTask task) throws Exception {
        Method method = task.getClass().getDeclaredMethod("io");
        method.setAccessible(true);
        return (FileIO) method.invoke(task);
    }

    private static Map<String, String> params(FileScanTask task, String field) {
        String selected = "valid".equals(field) ? task.schema().columns().get(0).name() : field;
        String type = task.schema().findField(selected) == null ? "bigint"
                : Types.IntegerType.get().equals(task.schema().findField(selected).type()) ? "int" : "string";
        return ImmutableMap.of("serialized_split", SerializationUtil.serializeToBase64(task),
                "required_fields", selected, "required_types", type, "time_zone", "UTC");
    }

    private static FileScanTask restoredTask(IcebergSysTableJniScanner scanner) throws Exception {
        Field field = IcebergSysTableJniScanner.class.getDeclaredField("scanTask");
        field.setAccessible(true);
        return (FileScanTask) field.get(scanner);
    }
}
