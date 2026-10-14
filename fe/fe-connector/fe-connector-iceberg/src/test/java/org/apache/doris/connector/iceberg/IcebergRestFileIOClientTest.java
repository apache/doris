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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
class IcebergRestFileIOClientTest {
    private static final String TABLE_PATH = "v1/namespaces/db/tables/t";
    private static final String ETAG = "\"table-version-1\"";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    private HttpServer server;
    private String uri;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/config", exchange ->
                respond(exchange, 200, "{\"defaults\":{},\"overrides\":{}}"));
        server.start();
        uri = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3://bucket/table", "abfss://container@account.dfs.core.windows.net/table"})
    void catalogLoadsTableThroughResponseAdapter(String location) throws Exception {
        LoadTableResponse response = tableResponse(location);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger adaptations = new AtomicInteger();
        server.createContext("/" + TABLE_PATH, exchange -> {
            tableRequests.incrementAndGet();
            respond(exchange, 200, LoadTableResponseParser.toJson(response));
        });

        // Exercise the real SDK loadTable -> withAuthSession -> get(responseHeaders) path.
        // The adapter selects an in-memory FileIO so no S3/Azure environment is required.
        try (RESTCatalog catalog = new RESTCatalog(options -> new IcebergRestFileIOClient(
                HTTPClient.builder(options).uri(uri).build(), result -> {
                    if (!(result instanceof LoadTableResponse)) {
                        return result;
                    }
                    adaptations.incrementAndGet();
                    return LoadTableResponse.builder()
                            .withTableMetadata(((LoadTableResponse) result).tableMetadata())
                            .addConfig(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName())
                            .build();
                }))) {
            catalog.initialize("response-headers-test", Map.of("uri", uri, "rest.auth.type", "none"));
            Table first = catalog.loadTable(TableIdentifier.of("db", "t"));
            Assertions.assertEquals(location, first.location());
            Assertions.assertEquals(SCHEMA.asStruct(), first.schema().asStruct());
            Assertions.assertInstanceOf(InMemoryFileIO.class, first.io());
            Assertions.assertEquals(1, tableRequests.get());
            Assertions.assertEquals(1, adaptations.get());

            Table second = catalog.loadTable(TableIdentifier.of("db", "t"));
            Assertions.assertEquals(location, second.location());
            Assertions.assertEquals(SCHEMA.asStruct(), second.schema().asStruct());
            Assertions.assertEquals(2, tableRequests.get());
            Assertions.assertInstanceOf(InMemoryFileIO.class, second.io());
            Assertions.assertEquals(2, adaptations.get());
        }
    }

    @Test
    void authenticatedGetForwardsRequestAndResponseHeadersAndAdaptsPayload() throws Exception {
        LoadTableResponse response = tableResponse("s3://bucket/table");
        LoadTableResponse adapted = tableResponse("s3://bucket/adapted");
        AtomicReference<String> requestHeader = new AtomicReference<>();
        AtomicReference<String> query = new AtomicReference<>();
        AtomicInteger adaptations = new AtomicInteger();
        server.createContext("/" + TABLE_PATH, exchange -> {
            requestHeader.set(exchange.getRequestHeaders().getFirst("X-Test-Request"));
            query.set(exchange.getRequestURI().getRawQuery());
            exchange.getResponseHeaders().set("ETag", ETAG);
            respond(exchange, 200, LoadTableResponseParser.toJson(response));
        });
        Map<String, String> responseHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try (RESTClient root = new IcebergRestFileIOClient(HTTPClient.builder(Map.of()).uri(uri).build(), result -> {
            Assertions.assertEquals(response.tableMetadata().location(),
                    ((LoadTableResponse) result).tableMetadata().location());
            adaptations.incrementAndGet();
            return adapted;
        }); RESTClient client = root.withAuthSession(AuthSession.EMPTY)) {
            // The Supplier overload must dispatch to the same response-header-aware implementation.
            LoadTableResponse result = client.get(TABLE_PATH, Map.of("snapshots", "all"),
                    LoadTableResponse.class, () -> Map.of("X-Test-Request", "request-value"),
                    ErrorHandlers.tableErrorHandler(), responseHeaders::putAll);
            Assertions.assertSame(adapted, result);
            Assertions.assertEquals("request-value", requestHeader.get());
            Assertions.assertEquals("snapshots=all", query.get());
            Assertions.assertEquals(ETAG, responseHeaders.get("ETag"));
            Assertions.assertEquals(1, adaptations.get());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {204, 304})
    void emptyResponseDoesNotInvokeResponseAdapter(int status) throws Exception {
        AtomicReference<String> conditionalHeader = new AtomicReference<>();
        server.createContext("/" + TABLE_PATH, exchange -> {
            conditionalHeader.set(exchange.getRequestHeaders().getFirst("If-None-Match"));
            exchange.getResponseHeaders().set("X-Test-Response", "empty");
            respond(exchange, status, "");
        });
        Map<String, String> responseHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try (RESTClient client = new IcebergRestFileIOClient(
                HTTPClient.builder(Map.of()).uri(uri).withAuthSession(AuthSession.EMPTY).build(),
                result -> Assertions.fail("No content must not be adapted"))) {
            Assertions.assertNull(client.get(TABLE_PATH, Map.of(), LoadTableResponse.class,
                    Map.of("If-None-Match", ETAG), ErrorHandlers.tableErrorHandler(), responseHeaders::putAll));
            Assertions.assertEquals(ETAG, conditionalHeader.get());
            Assertions.assertEquals("empty", responseHeaders.get("X-Test-Response"));
        }
    }

    @Test
    void missingTableRetainsSdkErrorHandling() throws Exception {
        server.createContext("/" + TABLE_PATH, exchange -> respond(exchange, 404,
                "{\"error\":{\"message\":\"table is missing\",\"type\":\"NoSuchTableException\",\"code\":404}}"));
        try (RESTClient client = new IcebergRestFileIOClient(
                HTTPClient.builder(Map.of()).uri(uri).withAuthSession(AuthSession.EMPTY).build(),
                result -> Assertions.fail("Failed responses must not be adapted"))) {
            NoSuchTableException error = Assertions.assertThrows(NoSuchTableException.class,
                    () -> client.get(TABLE_PATH, Map.of(), LoadTableResponse.class, Map.of(),
                            ErrorHandlers.tableErrorHandler(), headers -> {}));
            Assertions.assertTrue(error.getMessage().contains("table is missing"));
        }
    }

    private static LoadTableResponse tableResponse(String location) {
        return LoadTableResponse.builder().withTableMetadata(TableMetadata.newTableMetadata(
                SCHEMA, PartitionSpec.unpartitioned(), location, Map.of())).build();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        try (HttpExchange response = exchange) {
            if (status == 204 || status == 304) {
                response.sendResponseHeaders(status, -1);
            } else {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                response.getResponseHeaders().set("Content-Type", "application/json");
                response.sendResponseHeaders(status, bytes.length);
                response.getResponseBody().write(bytes);
            }
        }
    }
}
