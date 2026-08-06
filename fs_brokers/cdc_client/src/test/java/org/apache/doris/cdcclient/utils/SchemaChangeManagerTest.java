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

package org.apache.doris.cdcclient.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

class SchemaChangeManagerTest {
    private HttpServer server;
    private String feAddr;
    private final AtomicInteger schemaRequests = new AtomicInteger();

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        feAddr = "127.0.0.1:" + server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void addColumnTreatsUnknownErrorAsIdempotentWhenColumnExists() throws Exception {
        respondToDdlWithUnknownError();
        respondToSchemaWithColumns("id", "new_col");

        SchemaChangeManager.execute(
                feAddr,
                "target_db",
                "token",
                SchemaChangeOperation.addColumn(
                        "target_table",
                        "new_col",
                        "ALTER TABLE `target_db`.`target_table` ADD COLUMN `new_col` INT"));

        assertThat(schemaRequests).hasValue(1);
    }

    @Test
    void dropColumnTreatsUnknownErrorAsIdempotentWhenColumnIsAbsent() throws Exception {
        respondToDdlWithUnknownError();
        respondToSchemaWithColumns("id");

        SchemaChangeManager.execute(
                feAddr,
                "target_db",
                "token",
                SchemaChangeOperation.dropColumn(
                        "target_table",
                        "old_col",
                        "ALTER TABLE `target_db`.`target_table` DROP COLUMN `old_col`"));

        assertThat(schemaRequests).hasValue(1);
    }

    @Test
    void addColumnKeepsFailureWhenColumnIsAbsent() throws Exception {
        respondToDdlWithUnknownError();
        respondToSchemaWithColumns("id");
        SchemaChangeOperation operation =
                SchemaChangeOperation.addColumn(
                        "target_table",
                        "new_col",
                        "ALTER TABLE `target_db`.`target_table` ADD COLUMN `new_col` INT");

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.execute(
                                        feAddr, "target_db", "token", operation))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to execute schema change");
    }

    @Test
    void dropColumnKeepsFailureWhenColumnStillExists() throws Exception {
        respondToDdlWithUnknownError();
        respondToSchemaWithColumns("id", "old_col");
        SchemaChangeOperation operation =
                SchemaChangeOperation.dropColumn(
                        "target_table",
                        "old_col",
                        "ALTER TABLE `target_db`.`target_table` DROP COLUMN `old_col`");

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.execute(
                                        feAddr, "target_db", "token", operation))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to execute schema change");
    }

    @Test
    void schemaQueryFailureKeepsOriginalDdlFailure() throws Exception {
        respondToDdlWithUnknownError();
        server.createContext(
                "/api/target_db/target_table/_schema",
                exchange -> respond(exchange, "{\"code\":1,\"msg\":\"schema unavailable\"}"));
        SchemaChangeOperation operation =
                SchemaChangeOperation.addColumn(
                        "target_table",
                        "new_col",
                        "ALTER TABLE `target_db`.`target_table` ADD COLUMN `new_col` INT");

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.execute(
                                        feAddr, "target_db", "token", operation))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Column operation cannot be applied")
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
    }

    @Test
    void successfulDdlDoesNotQuerySchema() throws Exception {
        server.createContext(
                "/api/query/default_cluster/target_db",
                exchange -> respond(exchange, "{\"code\":0,\"msg\":\"success\"}"));

        SchemaChangeManager.execute(
                feAddr,
                "target_db",
                "token",
                SchemaChangeOperation.addColumn(
                        "target_table",
                        "new_col",
                        "ALTER TABLE `target_db`.`target_table` ADD COLUMN `new_col` INT"));

        assertThat(schemaRequests).hasValue(0);
    }

    private void respondToDdlWithUnknownError() {
        server.createContext(
                "/api/query/default_cluster/target_db",
                exchange ->
                        respond(
                                exchange,
                                "{\"code\":1,\"msg\":\"Column operation cannot be applied\"}"));
    }

    private void respondToSchemaWithColumns(String... columns) {
        server.createContext(
                "/api/target_db/target_table/_schema",
                exchange -> {
                    schemaRequests.incrementAndGet();
                    StringBuilder properties = new StringBuilder();
                    for (String column : columns) {
                        if (properties.length() > 0) {
                            properties.append(',');
                        }
                        properties.append("{\"name\":\"").append(column).append("\"}");
                    }
                    respond(
                            exchange,
                            "{\"code\":0,\"data\":{\"status\":200,\"properties\":["
                                    + properties
                                    + "]}}");
                });
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
