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

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergSerializedManifestTaskTest {
    private static final String ACCOUNT_HOST = "account.dfs.core.windows.net";
    private static final String WAREHOUSE = "abfss://container@" + ACCOUNT_HOST + "/warehouse";
    private static final String SAS_TOKEN = "sv=2024-11-04&sp=r&sig=serialized-task";
    private static final Instant FILE_IO_EXPIRY = Instant.parse("2030-01-01T00:00:00Z");
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    @Test
    @Timeout(60)
    void allManifestsReadsWithOnlyTheFileIoSerializedInsideItsTask() throws Exception {
        withManifestTask(Map.of(), Clock.systemUTC(), (scanner, manifest, snapshot, queries, rangeReads) -> {
            scanner.open();
            long address = scanner.getNextBatchMeta();
            Assertions.assertNotEquals(0, address);
            // Scanner output is Java -> BE; createReadableTable is the different C++ -> Java
            // layout. Read the scanner-owned vector without interpreting it as that protocol.
            VectorTable batch = scanner.getTable();
            Assertions.assertEquals(1, batch.getNumRows());
            Object[][] columns = batch.getMaterializedData();
            Assertions.assertArrayEquals(new Object[] {manifest.path()}, columns[0]);
            Assertions.assertArrayEquals(new Object[] {1}, columns[1]);
            Assertions.assertArrayEquals(new Object[] {snapshot.snapshotId()}, columns[2]);
            scanner.resetTable();
            Assertions.assertEquals(0, scanner.getNextBatchMeta());
            Assertions.assertTrue(rangeReads.get() > 0, "the scanner must read Avro bytes through ADLS range GET");
            Assertions.assertFalse(queries.isEmpty());
            Assertions.assertTrue(queries.stream().allMatch(query -> query != null
                    && query.contains("sig=serialized-task")), "every ADLS request must carry the task's SAS");
        });
    }

    @Test
    @Timeout(60)
    void obsoleteNativeSidebandCannotReplaceTheSerializedFileIoCredential() throws Exception {
        withManifestTask(Map.of(
                "hadoop.provider", "azure", "hadoop.AZURE_AUTH_TYPE", "SAS",
                "hadoop.AZURE_ACCOUNT_NAME", "account", "hadoop.AZURE_SAS_TOKEN", "sig=obsolete-sideband",
                "hadoop.AZURE_SAS_EXPIRY_MS", "1"), Clock.systemUTC(),
                (scanner, manifest, snapshot, queries, rangeReads) -> {
                    scanner.open();
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertArrayEquals(new Object[] {manifest.path()},
                            scanner.getTable().getMaterializedData()[0]);
                    Assertions.assertTrue(rangeReads.get() > 0);
                    Assertions.assertTrue(queries.stream().allMatch(query -> query != null
                            && query.contains("sig=serialized-task") && !query.contains("obsolete-sideband")));
                });
    }

    @Test
    @Timeout(60)
    void expiryIsCheckedAtOpenAfterTheSerializedTaskHasWaitedInTheQueue() throws Exception {
        MutableClock clock = new MutableClock(FILE_IO_EXPIRY.minusSeconds(1));
        withManifestTask(Map.of("file_io_expiry_ms", Long.toString(FILE_IO_EXPIRY.toEpochMilli())), clock,
                (scanner, manifest, snapshot, queries, rangeReads) -> {
                    // Construction succeeds while the credential is still valid. Queue time then
                    // advances the clock to the expiry boundary without sleeping or contacting Azure.
                    Assertions.assertTrue(queries.isEmpty());
                    clock.setInstant(FILE_IO_EXPIRY);
                    IOException error = Assertions.assertThrows(IOException.class, scanner::open);
                    Assertions.assertTrue(error.getMessage().contains("expired"));
                    Assertions.assertFalse(error.getMessage().contains(SAS_TOKEN));
                    Assertions.assertTrue(queries.isEmpty(), "An expired task must not issue even a HEAD request");
                    Assertions.assertEquals(0, rangeReads.get());
                });
    }

    @Test
    @Timeout(60)
    void unexpiredTimestampAllowsReadingTheSerializedTask() throws Exception {
        Clock clock = Clock.fixed(FILE_IO_EXPIRY.minusSeconds(1), ZoneOffset.UTC);
        withManifestTask(Map.of("file_io_expiry_ms", Long.toString(FILE_IO_EXPIRY.toEpochMilli())), clock,
                (scanner, manifest, snapshot, queries, rangeReads) -> {
                    scanner.open();
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertEquals(1, scanner.getTable().getNumRows());
                    Assertions.assertArrayEquals(new Object[] {manifest.path()},
                            scanner.getTable().getMaterializedData()[0]);
                    Assertions.assertTrue(rangeReads.get() > 0);
                });
    }

    @Test
    @Timeout(60)
    void sharedKeyRemainsInsideTheSerializedFileIo() throws Exception {
        withManifestTask(MetadataTableType.ALL_MANIFESTS,
                Map.of("hadoop.provider", "s3", "hadoop.AWS_ACCESS_KEY", "unrelated-sideband"),
                Clock.systemUTC(), Map.of("adls.auth.shared-key.account.name", "account",
                        "adls.auth.shared-key.account.key", "dW5pdC10ZXN0LXNoYXJlZC1rZXk="),
                (scanner, manifest, snapshot, queries, rangeReads) -> {
                    scanner.open();
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertArrayEquals(new Object[] {manifest.path()},
                            scanner.getTable().getMaterializedData()[0]);
                    Assertions.assertTrue(rangeReads.get() > 0);
                    Assertions.assertTrue(queries.stream().allMatch(query -> query == null || !query.contains("sig=")));
                });
    }

    @Test
    @Timeout(60)
    void invalidTimestampIsRejectedWithoutEchoingItsValue() throws Exception {
        String invalidTimestamp = "unknown-value-must-not-appear-in-diagnostics";
        withManifestTask(Map.of("file_io_expiry_ms", invalidTimestamp), Clock.systemUTC(),
                (scanner, manifest, snapshot, queries, rangeReads) -> {
                    IOException error = Assertions.assertThrows(IOException.class, scanner::open);
                    Assertions.assertEquals("Invalid Iceberg FileIO expiry timestamp", error.getMessage());
                    Assertions.assertFalse(error.getMessage().contains(invalidTimestamp));
                    Assertions.assertFalse(error.getMessage().contains(SAS_TOKEN));
                    Assertions.assertNull(error.getCause(), "NumberFormatException exposes its input in the cause");
                    Assertions.assertTrue(queries.isEmpty(), "An invalid timestamp must fail before any HEAD request");
                    Assertions.assertEquals(0, rangeReads.get());
                });
    }

    @Test
    @Timeout(60)
    void staticDataTaskWithoutExpiryReadsMaterializedRowsWithoutStorageAccess() throws Exception {
        MutableClock clock = new MutableClock(FILE_IO_EXPIRY.minusSeconds(1));
        withManifestTask(MetadataTableType.MANIFESTS, Map.of(), clock,
                (scanner, manifest, snapshot, queries, rangeReads) -> {
                    // Iceberg's MANIFESTS task has already materialized its rows during planning.
                    // The planner's FileIO can expire now without affecting this serialized StaticDataTask.
                    int planningRequests = queries.size();
                    int planningRangeReads = rangeReads.get();
                    Assertions.assertTrue(planningRequests > 0);
                    clock.setInstant(FILE_IO_EXPIRY.plusSeconds(1));
                    scanner.open();
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertEquals(1, scanner.getTable().getNumRows());
                    Object[][] columns = scanner.getTable().getMaterializedData();
                    Assertions.assertArrayEquals(new Object[] {manifest.path()}, columns[0]);
                    Assertions.assertArrayEquals(new Object[] {1}, columns[1]);
                    scanner.resetTable();
                    Assertions.assertEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertEquals(planningRequests, queries.size());
                    Assertions.assertEquals(planningRangeReads, rangeReads.get());
                });
    }

    private static void withManifestTask(Map<String, String> extraParams, Clock clock, ScannerCheck check)
            throws Exception {
        withManifestTask(MetadataTableType.ALL_MANIFESTS, extraParams, clock, check);
    }

    private static void withManifestTask(MetadataTableType tableType, Map<String, String> extraParams,
            Clock clock, ScannerCheck check) throws Exception {
        withManifestTask(tableType, extraParams, clock, Map.of(
                "adls.sas-token." + ACCOUNT_HOST, SAS_TOKEN,
                "adls.sas-token-expires-at-ms." + ACCOUNT_HOST, Long.toString(FILE_IO_EXPIRY.toEpochMilli())), check);
    }

    private static void withManifestTask(MetadataTableType tableType, Map<String, String> extraParams,
            Clock clock, Map<String, String> authentication, ScannerCheck check) throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog(); ADLSFileIO fileIO = new ADLSFileIO()) {
            // Generate the real Avro manifest list through Iceberg's append API, without cloud writes.
            catalog.initialize("serialized-manifest-test", Map.of("warehouse", WAREHOUSE));
            catalog.createNamespace(Namespace.of("db"));
            Table source = catalog.createTable(
                    TableIdentifier.of("db", "table"), SCHEMA, PartitionSpec.unpartitioned());
            source.newFastAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(source.location() + "/data/one.parquet")
                    .withFileSizeInBytes(16)
                    .withRecordCount(3)
                    .build()).commit();
            Snapshot snapshot = source.currentSnapshot();
            ManifestFile manifest = snapshot.dataManifests(source.io()).get(0);
            byte[] manifestList;
            try (SeekableInputStream input = source.io().newInputFile(snapshot.manifestListLocation()).newStream()) {
                manifestList = input.readAllBytes();
            }

            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            List<String> queries = new CopyOnWriteArrayList<>();
            AtomicInteger rangeReads = new AtomicInteger();
            String manifestListPath = "/container" + URI.create(snapshot.manifestListLocation()).getPath();
            server.createContext(manifestListPath, exchange -> {
                if (authentication.containsKey("adls.auth.shared-key.account.key")) {
                    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                    if (authorization == null || !authorization.startsWith("SharedKey account:")) {
                        exchange.sendResponseHeaders(403, -1);
                        exchange.close();
                        return;
                    }
                }
                serve(exchange, manifestList, queries, rangeReads);
            });
            server.start();
            try {
                Map<String, String> fileIOProperties = new HashMap<>(authentication);
                fileIOProperties.put("adls.connection-string." + ACCOUNT_HOST,
                        "http://127.0.0.1:" + server.getAddress().getPort());
                fileIO.initialize(fileIOProperties);
                TableMetadata metadata = ((BaseTable) source).operations().current();
                Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "azure-table");
                Table metadataTable = MetadataTableUtils.createMetadataTableInstance(table, tableType);
                boolean staticTask = tableType == MetadataTableType.MANIFESTS;
                String[] requiredFields = staticTask ? new String[] {"path", "added_data_files_count"}
                        : new String[] {"path", "added_data_files_count", "reference_snapshot_id"};
                String serializedTask;
                try (CloseableIterable<FileScanTask> tasks = metadataTable.newScan().select(requiredFields).planFiles();
                        CloseableIterator<FileScanTask> iterator = tasks.iterator()) {
                    Assertions.assertTrue(iterator.hasNext());
                    FileScanTask task = iterator.next();
                    Assertions.assertTrue(task.isDataTask());
                    if (staticTask) {
                        Assertions.assertEquals("org.apache.iceberg.StaticDataTask", task.getClass().getName());
                    }
                    serializedTask = SerializationUtil.serializeToBase64(task);
                    Assertions.assertFalse(iterator.hasNext());
                }
                if (!staticTask) {
                    Assertions.assertTrue(queries.isEmpty(),
                            "planning and serialization must leave the Azure FileIO cold");
                }

                // No provider, AZURE_* or hadoop.* parameters: credentials must come from task FileIO.
                Map<String, String> params = new HashMap<>(Map.of(
                        "serialized_split", serializedTask,
                        "required_fields", String.join(",", requiredFields),
                        "required_types", staticTask ? "string#int" : "string#int#bigint",
                        "time_zone", "UTC"));
                params.putAll(extraParams);
                OffHeap.setTesting();
                IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(8, params, clock);
                try {
                    check.run(scanner, manifest, snapshot, queries, rangeReads);
                } finally {
                    scanner.releaseTable();
                    scanner.close();
                }
            } finally {
                server.stop(0);
            }
        }
    }

    @FunctionalInterface
    private interface ScannerCheck {
        void run(IcebergSysTableJniScanner scanner, ManifestFile manifest, Snapshot snapshot,
                List<String> queries, AtomicInteger rangeReads) throws Exception;
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> current;
        private final ZoneId zone;

        MutableClock(Instant instant) {
            this(new AtomicReference<>(instant), ZoneOffset.UTC);
        }

        private MutableClock(AtomicReference<Instant> current, ZoneId zone) {
            this.current = current;
            this.zone = zone;
        }

        void setInstant(Instant instant) {
            current.set(instant);
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId newZone) {
            return new MutableClock(current, newZone);
        }

        @Override
        public Instant instant() {
            return current.get();
        }
    }

    private static void serve(HttpExchange exchange, byte[] bytes, List<String> queries, AtomicInteger rangeReads)
            throws IOException {
        try {
            queries.add(exchange.getRequestURI().getRawQuery());
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.getResponseHeaders().set("Content-Length", Integer.toString(bytes.length));
            exchange.getResponseHeaders().set("ETag", "\"manifest-list-etag\"");
            exchange.getResponseHeaders().set("Last-Modified", "Wed, 09 Sep 2026 00:00:00 GMT");
            exchange.getResponseHeaders().set("x-ms-blob-type", "BlockBlob");
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range == null) {
                    range = exchange.getRequestHeaders().getFirst("x-ms-range");
                }
                int start = 0;
                int end = bytes.length - 1;
                if (range != null) {
                    rangeReads.incrementAndGet();
                    String[] bounds = range.substring("bytes=".length()).split("-", 2);
                    start = Integer.parseInt(bounds[0]);
                    if (!bounds[1].isEmpty()) {
                        end = Math.min(end, Integer.parseInt(bounds[1]));
                    }
                    exchange.getResponseHeaders().set("Content-Range",
                            "bytes " + start + "-" + end + "/" + bytes.length);
                }
                int length = end - start + 1;
                exchange.getResponseHeaders().set("Content-Length", Integer.toString(length));
                exchange.sendResponseHeaders(range == null ? 200 : 206, length);
                exchange.getResponseBody().write(bytes, start, length);
            }
        } finally {
            exchange.close();
        }
    }
}
