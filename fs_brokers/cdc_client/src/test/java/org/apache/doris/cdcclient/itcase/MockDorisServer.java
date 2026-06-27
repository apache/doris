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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    private volatile String committedOffset;

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
            this.committedOffset = new String(body, StandardCharsets.UTF_8);
            response = "{\"code\":0,\"msg\":\"ok\"}";
        } else if (path.contains("/api/query/")) {
            // FE schema-change endpoint: body is {"stmt":"<DDL>"}
            JsonNode node = MAPPER.readTree(body);
            executedDdls.add(node.path("stmt").asText(""));
            response = "{\"code\":0,\"msg\":\"ok\"}";
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

    @Override
    public void close() {
        server.stop(0);
    }
}
