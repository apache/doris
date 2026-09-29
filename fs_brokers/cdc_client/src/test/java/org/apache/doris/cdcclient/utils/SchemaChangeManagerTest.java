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
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

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
        respondToSchemaWithColumns("id", "NEW_COL");

        SchemaChangeManager.execute(
                feAddr,
                "target_db",
                "token",
                "123",
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
                "123",
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
                                        feAddr, "target_db", "token", "123", operation))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to execute schema change");
    }

    @Test
    void dropColumnKeepsFailureWhenColumnStillExists() throws Exception {
        respondToDdlWithUnknownError();
        respondToSchemaWithColumns("id", "OLD_COL");
        SchemaChangeOperation operation =
                SchemaChangeOperation.dropColumn(
                        "target_table",
                        "old_col",
                        "ALTER TABLE `target_db`.`target_table` DROP COLUMN `old_col`");

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.execute(
                                        feAddr, "target_db", "token", "123", operation))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to execute schema change");
    }

    @Test
    void schemaQueryFailureKeepsOriginalDdlFailure() throws Exception {
        respondToDdlWithUnknownError();
        server.createContext(
                "/api/streaming/schema/target_db/target_table",
                exchange -> respond(exchange, "{\"code\":1,\"msg\":\"schema unavailable\"}"));
        SchemaChangeOperation operation =
                SchemaChangeOperation.addColumn(
                        "target_table",
                        "new_col",
                        "ALTER TABLE `target_db`.`target_table` ADD COLUMN `new_col` INT");

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.execute(
                                        feAddr, "target_db", "token", "123", operation))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Column operation cannot be applied")
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
    }

    @Test
    void successfulDdlDoesNotQuerySchema() throws Exception {
        server.createContext(
                "/api/streaming/schema_change",
                exchange -> respond(exchange, "{\"code\":0,\"msg\":\"success\"}"));

        SchemaChangeManager.execute(
                feAddr,
                "target_db",
                "token",
                "123",
                SchemaChangeOperation.addColumn(
                        "target_table",
                        "new_col",
                        "ALTER TABLE `target_db`.`target_table` ADD COLUMN `new_col` INT"));

        assertThat(schemaRequests).hasValue(0);
    }

    @Test
    void validateTargetSchemasRejectsMissingSourceColumns() {
        respondToSchemaWithColumns("id");

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.validateTargetSchemas(
                                        feAddr, "target_db", "token", "123",
                                        sourceSchemas("target_table"), Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("target_db.target_table: missing columns [secret]");
    }

    @Test
    void validateTargetSchemasAllowsDifferentTypesKeysAndExtraColumns() throws Exception {
        server.createContext(
                "/api/streaming/schema/target_db/target_table",
                exchange -> {
                    schemaRequests.incrementAndGet();
                    respond(exchange, "{\"code\":0,\"data\":{\"status\":200,\"properties\":["
                            + "{\"name\":\"ID\",\"type\":\"STRING\",\"is_key\":\"No\"},"
                            + "{\"name\":\"extra\",\"type\":\"INT\",\"is_key\":\"Yes\"}]}}");
                });

        SchemaChangeManager.validateTargetSchemas(
                feAddr, "target_db", "token", "123", sourceSchemas("source_table"),
                Map.of("table.source_table.target_table", "target_table",
                        "table.source_table.exclude_columns", "secret"));

        assertThat(schemaRequests).hasValue(1);
    }

    @Test
    void validateTargetSchemasPropagatesQueryFailure() {
        server.createContext(
                "/api/streaming/schema/target_db/target_table",
                exchange -> respond(exchange, "{\"code\":1,\"msg\":\"schema unavailable\"}"));

        assertThatThrownBy(
                        () ->
                                SchemaChangeManager.validateTargetSchemas(
                                        feAddr, "target_db", "token", "123",
                                        sourceSchemas("target_table"), Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to query Doris table schema")
                .hasMessageContaining("schema unavailable");
    }

    private static Map<TableId, TableChanges.TableChange> sourceSchemas(String tableName) {
        TableId tableId = new TableId("source_db", null, tableName);
        Table table = Table.editor().tableId(tableId)
                .addColumns(
                        Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create(),
                        Column.editor().name("secret").type("INT").jdbcType(Types.INTEGER).create())
                .setPrimaryKeyNames("id")
                .create();
        return Map.of(tableId, new TableChanges.TableChange(TableChanges.TableChangeType.ALTER, table));
    }

    private void respondToDdlWithUnknownError() {
        server.createContext(
                "/api/streaming/schema_change",
                exchange ->
                        respond(
                                exchange,
                                "{\"code\":1,\"msg\":\"Column operation cannot be applied\"}"));
    }

    private void respondToSchemaWithColumns(String... columns) {
        server.createContext(
                "/api/streaming/schema/target_db/target_table",
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
        assertThat(exchange.getRequestHeaders().getFirst("token")).isEqualTo("token");
        assertThat(exchange.getRequestHeaders().getFirst("jobId")).isEqualTo("123");
        assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isNull();
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
