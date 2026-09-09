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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergSerializedManifestTaskTest {
    private static final String ACCOUNT_HOST = "account.dfs.core.windows.net";
    private static final String WAREHOUSE = "abfss://container@" + ACCOUNT_HOST + "/warehouse";
    private static final String SAS_TOKEN = "sv=2024-11-04&sp=r&sig=serialized-task";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    @Test
    @Timeout(60)
    void allManifestsReadsWithOnlyTheFileIoSerializedInsideItsTask() throws Exception {
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
            server.createContext(manifestListPath, exchange -> serve(exchange, manifestList, queries, rangeReads));
            server.start();
            try {
                fileIO.initialize(Map.of(
                        "adls.sas-token." + ACCOUNT_HOST, SAS_TOKEN,
                        "adls.connection-string." + ACCOUNT_HOST,
                        "http://127.0.0.1:" + server.getAddress().getPort()));
                TableMetadata metadata = ((BaseTable) source).operations().current();
                Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "azure-table");
                Table allManifests = MetadataTableUtils.createMetadataTableInstance(
                        table, MetadataTableType.ALL_MANIFESTS);
                String serializedTask;
                try (CloseableIterable<FileScanTask> tasks = allManifests.newScan()
                        .select("path", "added_data_files_count", "reference_snapshot_id").planFiles();
                        CloseableIterator<FileScanTask> iterator = tasks.iterator()) {
                    Assertions.assertTrue(iterator.hasNext());
                    FileScanTask task = iterator.next();
                    Assertions.assertTrue(task.isDataTask());
                    serializedTask = SerializationUtil.serializeToBase64(task);
                    Assertions.assertFalse(iterator.hasNext());
                }
                Assertions.assertTrue(queries.isEmpty(),
                        "planning and serialization must leave the Azure FileIO cold");

                // No provider, AZURE_* or hadoop.* parameters: credentials must come from task FileIO.
                Map<String, String> params = Map.of(
                        "serialized_split", serializedTask,
                        "required_fields", "path,added_data_files_count,reference_snapshot_id",
                        "required_types", "string#int#bigint",
                        "time_zone", "UTC");
                OffHeap.setTesting();
                IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(8, params);
                try {
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
                } finally {
                    scanner.releaseTable();
                    scanner.close();
                }
                Assertions.assertTrue(rangeReads.get() > 0, "the scanner must read Avro bytes through ADLS range GET");
                Assertions.assertFalse(queries.isEmpty());
                Assertions.assertTrue(queries.stream().allMatch(query -> query != null
                        && query.contains("sig=serialized-task")), "every ADLS request must carry the task's SAS");
            } finally {
                server.stop(0);
            }
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
