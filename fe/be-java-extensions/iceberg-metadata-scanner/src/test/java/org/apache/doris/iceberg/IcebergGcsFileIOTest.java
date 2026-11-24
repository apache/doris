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

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergGcsFileIOTest {
    @Test
    void loadsGcsRuntimeDependencies() {
        ClassLoader loader = getClass().getClassLoader();
        Assertions.assertDoesNotThrow(() -> Class.forName("org.apache.iceberg.gcp.gcs.GCSFileIO", true, loader));
        Assertions.assertDoesNotThrow(() -> Class.forName("com.google.cloud.storage.StorageOptions", true, loader));
        Assertions.assertDoesNotThrow(() -> Class.forName("com.google.auth.oauth2.GoogleCredentials", true, loader));
    }

    @Test
    void deserializesGcsFileIoAndCreatesStorageClient() throws Exception {
        // A system-table split carries its FileIO; loading FE's copy alone cannot validate the BE closure.
        Class<?> type = Assertions.assertDoesNotThrow(() -> Class.forName("org.apache.iceberg.gcp.gcs.GCSFileIO"));
        try (FileIO original = (FileIO) type.getConstructor().newInstance()) {
            original.initialize(ImmutableMap.of("gcs.project-id", "test-project", "gcs.no-auth", "true"));
            try (FileIO restored = SerializationUtil.deserializeFromBase64(
                    SerializationUtil.serializeToBase64(original))) {
                Assertions.assertEquals(type, restored.getClass());
                Assertions.assertEquals("test-project", restored.properties().get("gcs.project-id"));
                Object client = type.getMethod("client").invoke(restored);
                Assertions.assertNotNull(client);
                ((AutoCloseable) client).close();
            }
        }
    }

    @Test
    void readsWithASerializedFreshTokenWithoutLoadingFeAuthentication() throws Exception {
        byte[] content = "manifest-content".getBytes(StandardCharsets.UTF_8);
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicInteger refreshRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                if (exchange.getRequestURI().getPath().startsWith("/refresh")) {
                    refreshRequests.incrementAndGet();
                    exchange.sendResponseHeaders(500, -1);
                } else {
                    exchange.sendResponseHeaders(200, content.length);
                    exchange.getResponseBody().write(content);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            Map<String, String> properties = new HashMap<>();
            properties.put("gcs.project-id", "test-project");
            properties.put("gcs.service.host", endpoint);
            properties.put("gcs.oauth2.token", "test-vended-token");
            properties.put("gcs.oauth2.token-expires-at", String.valueOf(System.currentTimeMillis() + 3600000));
            properties.put("gcs.oauth2.refresh-credentials-endpoint", endpoint + "/refresh");
            properties.put("uri", endpoint);
            // Catalog authentication belongs to FE and must not be needed for a fresh storage-token read.
            properties.put("rest.auth.type", "example.FeOnlyAuthManager");
            Class<?> type = Assertions.assertDoesNotThrow(() -> Class.forName("org.apache.iceberg.gcp.gcs.GCSFileIO"));
            try (FileIO original = (FileIO) type.getConstructor().newInstance()) {
                original.initialize(properties);
                try (FileIO restored = SerializationUtil.deserializeFromBase64(
                        SerializationUtil.serializeToBase64(original));
                        SeekableInputStream input = restored.newInputFile("gs://test-bucket/manifest.avro", content.length)
                                .newStream()) {
                    byte[] actual = new byte[content.length];
                    new DataInputStream(input).readFully(actual);
                    Assertions.assertArrayEquals(content, actual);
                    Assertions.assertEquals("Bearer test-vended-token", authorization.get());
                    Assertions.assertEquals(0, refreshRequests.get());
                }
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void scansFilesMetadataAfterSharedFileIoSerialization(@TempDir Path warehouse) throws Exception {
        OffHeap.setTesting();
        try (HadoopCatalog catalog = new HadoopCatalog()) {
            catalog.setConf(new Configuration());
            catalog.initialize("test", ImmutableMap.of(
                    CatalogProperties.WAREHOUSE_LOCATION, warehouse.toUri().toString(),
                    CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.NativeGcsResolvingFileIO"));
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
            Table table = catalog.createTable(TableIdentifier.of("db", "t"), schema, PartitionSpec.unpartitioned());
            table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(warehouse.resolve("data.parquet").toString())
                    .withFileSizeInBytes(10).withRecordCount(1).build()).commit();
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
            try (CloseableIterable<FileScanTask> tasks = metadata.newScan()
                    .project(new Schema(metadata.schema().findField("record_count"))).planFiles()) {
                FileScanTask task = tasks.iterator().next();
                Map<String, String> params = ImmutableMap.of(
                        "serialized_split", SerializationUtil.serializeToBase64(task),
                        "required_fields", "record_count", "required_types", "bigint", "time_zone", "UTC");
                IcebergSysTableJniScanner scanner = new IcebergSysTableJniScanner(16, params);
                try {
                    scanner.open();
                    Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
                    Assertions.assertEquals(1, scanner.getTable().getNumRows());
                    scanner.releaseTable();
                    Assertions.assertEquals(0, scanner.getNextBatchMeta());
                } finally {
                    scanner.releaseTable();
                    scanner.close();
                }
            }
        }
    }

}
