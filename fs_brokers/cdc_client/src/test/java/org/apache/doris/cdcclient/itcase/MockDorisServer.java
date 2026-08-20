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

package org.apache.doris.cdcclient.itcase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A tiny in-process stand-in for the Doris BE stream-load endpoint and the FE commit-offset
 * endpoint, so the from-to {@code writeRecords} path can be exercised without a real Doris cluster.
 *
 * <ul>
 *   <li>{@code PUT /api/{db}/{table}/_stream_load} — captures the newline-delimited JSON rows and
 *       replies with a Success stream-load result.
 *   <li>{@code POST /api/streaming/commit_offset} — captures the committed offset payload and
 *       replies {@code {"code":0}}.
 * </ul>
 */
final class MockDorisServer implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpServer server;
    private final List<String> loadedRecords = Collections.synchronizedList(new ArrayList<>());
    private final List<String> executedDdls = Collections.synchronizedList(new ArrayList<>());
    private final List<String> ddlResponses = Collections.synchronizedList(new ArrayList<>());
    private final Set<String> schemaColumns = Collections.synchronizedSet(new LinkedHashSet<>());
    private final AtomicInteger ddlRequestCount = new AtomicInteger();
    private final AtomicInteger schemaRequestCount = new AtomicInteger();
    private final AtomicInteger failedCommitOffsetCount = new AtomicInteger();
    private final AtomicBoolean blockNextDdlResponse = new AtomicBoolean();
    private final AtomicBoolean failCommitOffset = new AtomicBoolean();
    private volatile String committedOffset;
    private volatile boolean partialDdlRetryScenario;
    private volatile CountDownLatch ddlRequestArrived;
    private volatile CountDownLatch releaseDdlResponse;

    MockDorisServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/", this::handle);
        server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        byte[] body = exchange.getRequestBody().readAllBytes();
        String response;
        if (path.endsWith("/_stream_load")) {
            int rows = 0;
            for (String line : new String(body, StandardCharsets.UTF_8).split("\n")) {
                if (!line.isEmpty()) {
                    loadedRecords.add(line);
                    rows++;
                }
            }
            response =
                    String.format(
                            "{\"Status\":\"Success\",\"NumberTotalRows\":%d,\"NumberLoadedRows\":%d,"
                                    + "\"NumberFilteredRows\":0,\"LoadBytes\":%d}",
                            rows, rows, body.length);
        } else if (path.endsWith("/commit_offset")) {
            if (failCommitOffset.get()) {
                failedCommitOffsetCount.incrementAndGet();
                response = "{\"code\":1,\"msg\":\"injected commit offset failure\"}";
            } else {
                this.committedOffset = new String(body, StandardCharsets.UTF_8);
                response = "{\"code\":0,\"msg\":\"ok\"}";
            }
        } else if (path.endsWith("/_schema")) {
            schemaRequestCount.incrementAndGet();
            List<String> properties = new ArrayList<>();
            synchronized (schemaColumns) {
                for (String column : schemaColumns) {
                    properties.add("{\"name\":\"" + column + "\"}");
                }
            }
            response =
                    "{\"code\":0,\"data\":{\"status\":200,\"properties\":["
                            + String.join(",", properties)
                            + "]}}";
        } else if (path.contains("/api/query/")) {
            // FE schema-change endpoint: body is {"stmt":"<DDL>"}
            JsonNode node = MAPPER.readTree(body);
            executedDdls.add(node.path("stmt").asText(""));
            if (blockNextDdlResponse.compareAndSet(true, false)) {
                ddlRequestArrived.countDown();
                try {
                    if (!releaseDdlResponse.await(30, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting to release blocked DDL response");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to release DDL response", e);
                }
            }
            int requestNumber = ddlRequestCount.incrementAndGet();
            if (partialDdlRetryScenario && requestNumber == 1) {
                schemaColumns.add("age");
                response = "{\"code\":0,\"msg\":\"ok\"}";
            } else if (partialDdlRetryScenario && requestNumber == 2) {
                response = "{\"code\":1,\"msg\":\"injected second DDL failure\"}";
            } else if (partialDdlRetryScenario && requestNumber == 3) {
                response =
                        "{\"code\":1,\"msg\":\"Can not add column which already exists\"}";
            } else if (partialDdlRetryScenario && requestNumber == 4) {
                schemaColumns.add("city");
                response = "{\"code\":1,\"msg\":\"unrecognized DDL response\"}";
            } else {
                response = applyDdlToMockSchema(node.path("stmt").asText(""));
            }
            ddlResponses.add(response);
        } else {
            response = "{\"code\":-1,\"msg\":\"unknown path " + path + "\"}";
        }
        byte[] out = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, out.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(out);
        }
    }

    /** host:port the cdc_client should be pointed at (serves both the BE and FE roles). */
    String hostPort() {
        return "127.0.0.1:" + server.getAddress().getPort();
    }

    int port() {
        return server.getAddress().getPort();
    }

    /** All JSON rows received via stream-load, in arrival order. */
    List<String> loadedRecords() {
        return new ArrayList<>(loadedRecords);
    }

    /** The last committed offset payload (JSON), or null if commit was never called. */
    String committedOffset() {
        return committedOffset;
    }

    /** All DDL statements executed via the FE query endpoint, in arrival order. */
    List<String> executedDdls() {
        return new ArrayList<>(executedDdls);
    }

    /** FE query endpoint responses for schema-change DDLs, in arrival order. */
    List<String> ddlResponses() {
        return new ArrayList<>(ddlResponses);
    }

    int failedCommitOffsetCount() {
        return failedCommitOffsetCount.get();
    }

    int schemaRequestCount() {
        return schemaRequestCount.get();
    }

    /** Exercise retry idempotency through both known error text and schema verification. */
    void enablePartialDdlRetryScenario() {
        partialDdlRetryScenario = true;
        ddlRequestCount.set(0);
        schemaRequestCount.set(0);
        schemaColumns.clear();
    }

    /** Force commit_offset to fail without updating the committed payload. */
    void failCommitOffset() {
        failCommitOffset.set(true);
    }

    /** Allow commit_offset to succeed again. */
    void allowCommitOffset() {
        failCommitOffset.set(false);
    }

    /** Block the next DDL response until the test explicitly releases it. */
    void blockNextDdlResponse() {
        ddlRequestArrived = new CountDownLatch(1);
        releaseDdlResponse = new CountDownLatch(1);
        blockNextDdlResponse.set(true);
    }

    boolean awaitBlockedDdlRequest(Duration timeout) throws InterruptedException {
        return ddlRequestArrived.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    void releaseBlockedDdlResponse() {
        releaseDdlResponse.countDown();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private String applyDdlToMockSchema(String ddl) {
        String upper = ddl.toUpperCase();
        String column;
        if (upper.contains("ADD COLUMN")) {
            column = extractColumnName(ddl, "ADD COLUMN");
            if (column != null && !schemaColumns.add(column)) {
                return "{\"code\":1,\"msg\":\"Can not add column which already exists\"}";
            }
            return "{\"code\":0,\"msg\":\"ok\"}";
        }
        if (upper.contains("DROP COLUMN")) {
            column = extractColumnName(ddl, "DROP COLUMN");
            if (column != null && !schemaColumns.remove(column)) {
                return "{\"code\":1,\"msg\":\"Column does not exists\"}";
            }
            return "{\"code\":0,\"msg\":\"ok\"}";
        }
        return "{\"code\":0,\"msg\":\"ok\"}";
    }

    private String extractColumnName(String ddl, String keyword) {
        int idx = ddl.toUpperCase().indexOf(keyword);
        if (idx < 0) {
            return null;
        }
        String tail = ddl.substring(idx + keyword.length()).trim();
        if (tail.startsWith("`")) {
            int end = tail.indexOf('`', 1);
            return end > 1 ? tail.substring(1, end) : null;
        }
        int end = tail.indexOf(' ');
        return end > 0 ? tail.substring(0, end) : tail;
    }
}
