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

package org.apache.doris.connector;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.properties.FileSystemProperties;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** Official Iceberg/Azure SDK reads; only the storage HTTP service is replaced. */
class IcebergAzureFileIOIntegrationTest {
    private static final String TOKEN = "sv=2024-11-04&sp=r&sig=unit-test-signature&se=2100-01-01T00:00:00Z";
    private static final String LOCATION = "abfs://container@127.0.0.1/table";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    @Test
    void staticSasAuthenticatesAnOfficialFileIOMetadataRead() throws Exception {
        TableMetadata expected = TableMetadata.newTableMetadata(
                SCHEMA, PartitionSpec.unpartitioned(), LOCATION, Collections.emptyMap());
        byte[] bytes = TableMetadataParser.toJson(expected).getBytes(StandardCharsets.UTF_8);
        CopyOnWriteArrayList<String> queries = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/container/table/v1.metadata.json", exchange -> serve(exchange, bytes, queries));
        server.start();
        try (ADLSFileIO fileIO = new ADLSFileIO()) {
            FileSystemProperties storage = StorageAdapter.ofProvider("AZURE", Map.of(
                    "azure.account_name", "account",
                    "azure.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "azure.sas_token", TOKEN)).getSpiProperties();
            Map<String, String> fileIOProperties = storage.toIcebergFileIOProperties();
            // Fail before opening an unconfigured SDK client: no ambient credential discovery is allowed.
            Assertions.assertEquals(TOKEN, fileIOProperties.get("adls.sas-token.127.0.0.1"));
            fileIO.initialize(fileIOProperties);

            TableMetadata actual = TableMetadataParser.read(fileIO, LOCATION + "/v1.metadata.json");

            Assertions.assertEquals(expected.uuid(), actual.uuid());
            Assertions.assertEquals(expected.schema().asStruct(), actual.schema().asStruct());
            Assertions.assertFalse(queries.isEmpty(), "metadata must be read from the storage service");
            Assertions.assertTrue(queries.stream().allMatch(query -> query != null
                    && query.contains("sig=unit-test-signature")), "every storage request must carry SAS");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void sharedKeyAuthenticatesAnOfficialFileIOMetadataRead() throws Exception {
        String key = "dW5pdC10ZXN0LXNoYXJlZC1rZXk=";
        TableMetadata expected = TableMetadata.newTableMetadata(
                SCHEMA, PartitionSpec.unpartitioned(), LOCATION, Collections.emptyMap());
        byte[] bytes = TableMetadataParser.toJson(expected).getBytes(StandardCharsets.UTF_8);
        CopyOnWriteArrayList<String> queries = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> authorizations = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/container/table/v1.metadata.json", exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            serve(exchange, bytes, queries);
        });
        server.start();
        try (ADLSFileIO fileIO = new ADLSFileIO()) {
            FileSystemProperties storage = StorageAdapter.ofProvider("AZURE", Map.of(
                    "azure.account_name", "account",
                    "azure.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "azure.account_key", key)).getSpiProperties();
            Map<String, String> fileIOProperties = storage.toIcebergFileIOProperties();
            Assertions.assertEquals(key, fileIOProperties.get("adls.auth.shared-key.account.key"));
            fileIO.initialize(fileIOProperties);

            TableMetadata actual = TableMetadataParser.read(fileIO, LOCATION + "/v1.metadata.json");

            Assertions.assertEquals(expected.uuid(), actual.uuid());
            Assertions.assertEquals(expected.schema().asStruct(), actual.schema().asStruct());
            Assertions.assertFalse(authorizations.isEmpty());
            Assertions.assertTrue(authorizations.stream().allMatch(authorization -> authorization != null
                    && authorization.startsWith("SharedKey account:")), "every request must be signed with SharedKey");
            Assertions.assertTrue(queries.stream().allMatch(query -> query == null || !query.contains("sig=")));
        } finally {
            server.stop(0);
        }
    }

    private static void serve(HttpExchange exchange, byte[] bytes, CopyOnWriteArrayList<String> queries)
            throws IOException {
        try {
            queries.add(exchange.getRequestURI().getRawQuery());
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Content-Length", Integer.toString(bytes.length));
            exchange.getResponseHeaders().set("ETag", "\"unit-test-etag\"");
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
