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

package org.apache.doris.cdcclient.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.doris.cdcclient.source.deserialize.DeserializeResult;
import org.apache.doris.job.cdc.request.WriteRecordRequest;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Map;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

class PipelineCoordinatorSchemaChangeTest {
    private static final String RECOVERY_HINT =
            "Review and adjust the target schema as needed. "
                    + "RESUME confirms acceptance of unsupported changes for this recovery round.";

    @Test
    void initialFailureKeepsReasonAndExplainsResumeConfirmation() throws Exception {
        DeserializeResult result = unsupportedChange();
        IOException failure = validateFailure(new WriteRecordRequest(), result);

        assertThat(ExceptionUtils.getRootCauseMessage(failure))
                .contains("[SCHEMA_CHANGE_UNSUPPORTED]", result.getUnsupportedReason())
                .endsWith(RECOVERY_HINT);
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @Test
    void resumedFailureKeepsMissingColumnsAndConfirmation() throws Exception {
        assertResumeFailure(
                "{\"code\":0,\"data\":{\"status\":200,\"properties\":[{\"name\":\"id\"}]}}",
                "target_db.mapped_t1: missing columns [city]");
    }

    @Test
    void resumedFailureKeepsSchemaQueryErrorAndConfirmation() throws Exception {
        assertResumeFailure(
                "{\"code\":1,\"msg\":\"schema unavailable\"}", "schema unavailable");
    }

    private static void assertResumeFailure(String responseBody, String detail) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/api/streaming/schema/target_db/mapped_t1",
                exchange -> {
                    byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                    exchange.close();
                });
        server.start();
        try {
            WriteRecordRequest request = new WriteRecordRequest();
            request.setFrontendAddress("127.0.0.1:" + server.getAddress().getPort());
            request.setTargetDb("target_db");
            request.setToken("token");
            request.setJobId("123");
            request.setConfig(Map.of("table.t1.target_table", "mapped_t1"));
            request.setTolerateSchemaChange(true);
            DeserializeResult result = unsupportedChange();

            IOException failure = validateFailure(request, result);

            // FE classifies the root message, so a nested cause must not hide the marker.
            assertThat(ExceptionUtils.getRootCauseMessage(failure))
                    .contains("[SCHEMA_CHANGE_UNSUPPORTED]", result.getUnsupportedReason(),
                            "Doris target:", detail)
                    .endsWith(RECOVERY_HINT);
            assertThat(failure.getSuppressed()).hasSize(1);
            assertThat(failure.getSuppressed()[0]).hasMessageContaining(detail);
        } finally {
            server.stop(0);
        }
    }

    private static IOException validateFailure(WriteRecordRequest request, DeserializeResult result)
            throws Exception {
        Method validate = PipelineCoordinator.class.getDeclaredMethod(
                "validateUnsupportedSchemaChange", WriteRecordRequest.class, DeserializeResult.class);
        validate.setAccessible(true);
        InvocationTargetException failure = assertThrows(
                InvocationTargetException.class,
                () -> validate.invoke(new PipelineCoordinator(1), request, result));
        assertThat(failure.getCause()).isInstanceOf(IOException.class);
        return (IOException) failure.getCause();
    }

    private static DeserializeResult unsupportedChange() {
        TableId tableId = new TableId("source_db", null, "t1");
        Table before = Table.editor().tableId(tableId)
                .addColumn(Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create())
                .create();
        Table after = before.edit()
                .addColumn(Column.editor().name("city").type("INT").jdbcType(Types.INTEGER).create())
                .create();
        return DeserializeResult.unsupportedSchemaChange(
                "MODIFY COLUMN is not supported (id)",
                Map.of(tableId, new TableChanges.TableChange(TableChanges.TableChangeType.ALTER, before)),
                Map.of(tableId, new TableChanges.TableChange(TableChanges.TableChangeType.ALTER, after)),
                "target_db", Map.of("t1", "mapped_t1"),
                "ALTER TABLE t1 ADD COLUMN city INT, MODIFY COLUMN id BIGINT");
    }
}
