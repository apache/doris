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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.utframe.TestWithFeService;

import com.google.protobuf.Any;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class DorisFlightSqlSchemaTest extends TestWithFeService {
    private DorisFlightSqlProducer producer;
    private CallContext callContext;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("schema_test");
        connectContext.setDatabase("schema_test");
        createTable("CREATE TABLE schema_input (id BIGINT NOT NULL, name VARCHAR(20)) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        connectContext = Mockito.spy(connectContext);
        Mockito.doReturn(new FlightSqlChannel()).when(connectContext).getFlightSqlChannel();
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessions.getConnectContext("schema-peer")).thenReturn(connectContext);
        callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("schema-peer");
        producer = new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 0), sessions);
    }

    @Override
    protected void runAfterAll() throws Exception {
        producer.close();
        connectContext.getFlightSqlChannel().close();
    }

    private Schema schema(String sql) {
        return producer.getSchemaStatement(CommandStatementQuery.newBuilder().setQuery(sql).build(),
                callContext, FlightDescriptor.command(new byte[0])).getSchema();
    }

    private ActionCreatePreparedStatementResult prepare(String sql) throws Exception {
        CompletableFuture<ActionCreatePreparedStatementResult> result = new CompletableFuture<>();
        producer.createPreparedStatement(ActionCreatePreparedStatementRequest.newBuilder().setQuery(sql).build(),
                callContext, new StreamListener<Result>() {
                    private ActionCreatePreparedStatementResult response;

                    @Override
                    public void onNext(Result value) {
                        try {
                            response = Any.parseFrom(value.getBody()).unpack(ActionCreatePreparedStatementResult.class);
                        } catch (Exception e) {
                            result.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        result.completeExceptionally(error);
                    }

                    @Override
                    public void onCompleted() {
                        result.complete(response);
                    }
                });
        return result.get(30, java.util.concurrent.TimeUnit.SECONDS);
    }

    private Schema preparedSchema(String sql) throws Exception {
        ActionCreatePreparedStatementResult result = prepare(sql);
        String handle = result.getPreparedStatementHandle().toStringUtf8();
        connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
        byte[] bytes = result.getDatasetSchema().toByteArray();
        return MessageSerializer.deserializeSchema(new ReadChannel(
                Channels.newChannel(new ByteArrayInputStream(bytes))));
    }

    @Test
    void prepareReturnsAnalyzedOutput() throws Exception {
        Schema schema = preparedSchema("SELECT id AS key_alias, name FROM schema_input");
        Assertions.assertEquals(Arrays.asList("key_alias", "name"), schema.getFields().stream()
                .map(Field::getName).collect(Collectors.toList()));
        Assertions.assertEquals(new ArrowType.Int(64, true), schema.getFields().get(0).getType());
        Assertions.assertFalse(schema.getFields().get(0).isNullable());
        Assertions.assertEquals(new ArrowType.Utf8(), schema.getFields().get(1).getType());
        Assertions.assertTrue(schema.getFields().get(1).isNullable());
    }

    @Test
    void getSchemaAndPrepareAgree() throws Exception {
        String sql = "SELECT CAST(7 AS BIGINT) AS id, CAST('2025-01-02 03:04:05.123456' "
                + "AS DATETIME(6)) AS event_time, ARRAY('first', 'second') AS tags";
        Schema schema = schema(sql);
        Assertions.assertEquals(schema, preparedSchema(sql));
        Assertions.assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
                schema.getFields().get(1).getType());
        Assertions.assertEquals(new ArrowType.List(), schema.getFields().get(2).getType());
        Assertions.assertEquals(new ArrowType.Utf8(), schema.getFields().get(2).getChildren().get(0).getType());
    }

    @Test
    void invalidSqlIsRejectedAndConnectionRecovers() throws Exception {
        for (String sql : Arrays.asList("SELEC 1", "SELECT missing_column FROM schema_input")) {
            Assertions.assertThrows(FlightRuntimeException.class, () -> schema(sql));
            Mockito.clearInvocations(connectContext);
            Assertions.assertThrows(java.util.concurrent.ExecutionException.class, () -> prepare(sql));
            Mockito.verify(connectContext, Mockito.never()).addPreparedQuery(Mockito.anyString(), Mockito.anyString());
            Assertions.assertEquals("id", preparedSchema("SELECT id FROM schema_input")
                    .getFields().get(0).getName());
        }
    }

    @Test
    void showVariablesUsesFeResultTypes() throws Exception {
        String sql = "SHOW VARIABLES LIKE 'query_timeout'";
        Schema schema = schema(sql);
        Assertions.assertEquals(schema, preparedSchema(sql));
        Assertions.assertEquals(Arrays.asList("Variable_name", "Value", "Default_Value", "Changed"),
                schema.getFields().stream().map(Field::getName).collect(Collectors.toList()));
        Assertions.assertTrue(schema.getFields().stream().allMatch(f -> f.getType().equals(new ArrowType.Utf8())));
    }

    @Test
    void metadataDoesNotExecuteOrChangeSession() {
        SessionVariable session = connectContext.getSessionVariable();
        StatementContext statement = connectContext.getStatementContext();
        ConnectContext threadContext = ConnectContext.get();
        int timeout = session.getQueryTimeoutS();
        schema("SELECT /*+ SET_VAR(query_timeout=17) */ id FROM schema_input");
        Assertions.assertSame(session, connectContext.getSessionVariable());
        Assertions.assertEquals(timeout, session.getQueryTimeoutS());
        Assertions.assertSame(statement, connectContext.getStatementContext());
        Assertions.assertSame(threadContext, ConnectContext.get());
        Assertions.assertEquals("StatusResult", schema("SET query_timeout=17").getFields().get(0).getName());
        Assertions.assertEquals(timeout, session.getQueryTimeoutS());
        Assertions.assertThrows(FlightRuntimeException.class, () -> schema("SELECT 1; SELECT 2"));
        Assertions.assertEquals(MysqlCommand.COM_SLEEP, connectContext.getCommand());
    }

    @Test
    void createPreparedStatementDoesNotLeakChannelAllocator() throws Exception {
        // Each prepare used to allocate an off-heap placeholder vector. Repeated prepares must stay bounded.
        for (int i = 0; i < 100; ++i) {
            preparedSchema("SELECT " + i + " AS id");
            Assertions.assertEquals(0, connectContext.getFlightSqlChannel().getAllocatedMemory());
            Assertions.assertEquals(0, connectContext.getFlightSqlChannel().resultNum());
        }
    }

    @Test
    void getSchemaPreparedStatementValidatesHandle() throws Exception {
        ActionCreatePreparedStatementResult result = prepare("SELECT id FROM schema_input");
        CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
        Assertions.assertEquals(schema("SELECT id FROM schema_input"), producer.getSchemaPreparedStatement(
                command, callContext, FlightDescriptor.command(new byte[0])).getSchema());
        String handle = result.getPreparedStatementHandle().toStringUtf8();
        connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
        FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class,
                () -> producer.getSchemaPreparedStatement(command, callContext, FlightDescriptor.command(new byte[0])));
        Assertions.assertEquals(FlightStatusCode.NOT_FOUND, failure.status().code());
    }

    @Test
    void schemaDiscoveryChecksTablePrivileges() {
        UserIdentity originalUser = connectContext.getCurrentUserIdentity();
        connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("schema_reader", "%"));
        try {
            for (String sql : Arrays.asList("SELECT id FROM schema_input",
                    "WITH source AS (SELECT id FROM schema_input) SELECT * FROM source",
                    "SELECT (SELECT MAX(id) FROM schema_input) AS value",
                    "SELECT SUM(id) FROM schema_input")) {
                FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class,
                        () -> schema(sql), sql);
                Assertions.assertTrue(failure.getMessage().contains("denied"), failure.getMessage());
            }
        } finally {
            connectContext.setCurrentUserIdentity(originalUser);
        }
    }

    @Test
    void prepareKeepsCommandAndUsePrefixCompatibility() throws Exception {
        Assertions.assertEquals("StatusResult", preparedSchema("CREATE DATABASE schema_not_created")
                .getFields().get(0).getName());
        Assertions.assertFalse(connectContext.getCurrentCatalog().getDb("schema_not_created").isPresent());
        String previousDatabase = connectContext.getDatabase();
        connectContext.setDatabase("information_schema");
        try {
            Assertions.assertEquals("id", preparedSchema("USE schema_test; SELECT id FROM schema_input")
                    .getFields().get(0).getName());
            Assertions.assertEquals("information_schema", connectContext.getDatabase());
        } finally {
            connectContext.setDatabase(previousDatabase);
        }
    }

    @Test
    void nestedFieldsAndLogicalTypeMetadata() {
        Schema result = schema("SELECT MAP('key', CAST(1 AS LARGEINT)) AS properties, "
                + "NAMED_STRUCT('value', CAST(1 AS DECIMAL(18,4))) AS record, "
                + "CAST(1 AS LARGEINT) AS large_value");
        Field map = result.getFields().get(0);
        Assertions.assertEquals(new ArrowType.Map(false), map.getType());
        Field entries = map.getChildren().get(0);
        Assertions.assertFalse(entries.isNullable());
        Assertions.assertFalse(entries.getChildren().get(0).isNullable());
        Assertions.assertEquals(new ArrowType.Utf8(), entries.getChildren().get(1).getType());
        Assertions.assertEquals(new ArrowType.Decimal(18, 4, 128),
                result.getFields().get(1).getChildren().get(0).getType());
        Assertions.assertEquals("LARGEINT", result.getFields().get(2).getMetadata().get("doris_type"));
    }

    @Test
    void informationSchemaAndUnevaluatedExpression() {
        Schema catalog = schema("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME "
                + "FROM information_schema.tables WHERE 1=0");
        Assertions.assertEquals(Arrays.asList("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"),
                catalog.getFields().stream().map(Field::getName).collect(Collectors.toList()));
        // A schema-only request must never construct the executor that could evaluate SLEEP or launch fragments.
        try (MockedConstruction<StmtExecutor> executors =
                Mockito.mockConstruction(StmtExecutor.class)) {
            Assertions.assertEquals(new ArrowType.Bool(), schema("SELECT sleep(30) AS sleeping")
                    .getFields().get(0).getType());
            Assertions.assertTrue(executors.constructed().isEmpty());
        }
    }

    @Test
    void aggregateAndCteSchemas() {
        Assertions.assertEquals(Arrays.asList("total", "count_value"),
                schema("SELECT SUM(id) AS total, COUNT(*) AS count_value FROM schema_input")
                        .getFields().stream().map(Field::getName).collect(Collectors.toList()));
        Assertions.assertEquals("id", schema("WITH source AS (SELECT id FROM schema_input) SELECT * FROM source")
                .getFields().get(0).getName());
    }

    @Test
    void showTableLabelsUseResolvedDatabase() {
        Assertions.assertEquals("Tables_in_schema_test", schema("SHOW TABLES").getFields().get(0).getName());
    }
}
