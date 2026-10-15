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
import org.apache.doris.catalog.Column;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
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
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DorisFlightSqlSchemaTest extends TestWithFeService {
    private DorisFlightSqlProducer producer;
    private CallContext callContext;
    private boolean previousUnitTest;

    public static class SchemaCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return Collections.singletonMap("db1", Collections.emptyMap());
        }
    }

    @Override
    protected void runBeforeAll() throws Exception {
        previousUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        createDatabase("schema_test");
        connectContext.setDatabase("schema_test");
        createTable("CREATE TABLE schema_input (id BIGINT NOT NULL, name VARCHAR(20)) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createCatalog("CREATE CATALOG schema_catalog PROPERTIES ('type'='test', "
                + "'catalog_provider.class'='" + SchemaCatalogProvider.class.getName() + "')");
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
        FeConstants.runningUnitTest = previousUnitTest;
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
        for (String sql : Arrays.asList("SELEC 1", "SELECT missing_column FROM schema_input",
                "SELECT CAST(? AS BIGINT) FROM missing_schema_table")) {
            FlightRuntimeException schemaFailure = Assertions.assertThrows(
                    FlightRuntimeException.class, () -> schema(sql));
            Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT, schemaFailure.status().code());
            Mockito.clearInvocations(connectContext);
            java.util.concurrent.ExecutionException prepareFailure = Assertions.assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> prepare(sql));
            Assertions.assertTrue(prepareFailure.getCause() instanceof FlightRuntimeException);
            Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT,
                    ((FlightRuntimeException) prepareFailure.getCause()).status().code());
            Mockito.verify(connectContext, Mockito.never()).addPreparedQuery(Mockito.anyString(), Mockito.anyString(), Mockito.any());
            Mockito.verify(connectContext, Mockito.never()).addPreparedQuery(
                    Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyInt());
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

    @Test
    void schemaConvertsDialectBeforeParsing() {
        String query = "dialect_only_query";
        try (MockedStatic<SqlDialectHelper> converter = Mockito.mockStatic(SqlDialectHelper.class)) {
            converter.when(() -> SqlDialectHelper.convertSqlByDialect(Mockito.eq(query), Mockito.any()))
                    .thenReturn("SELECT id FROM schema_input");
            Assertions.assertEquals("id", schema(query).getFields().get(0).getName());
            converter.verify(() -> SqlDialectHelper.convertSqlByDialect(Mockito.eq(query), Mockito.any()));
        }
    }

    @Test
    void switchUsesRememberedDatabaseWithoutChangingSession() throws Exception {
        String database = connectContext.getDatabase();
        String remembered = connectContext.getLastDBOfCatalog("internal");
        try {
            connectContext.addLastDBOfCatalog("internal", "information_schema");
            Assertions.assertEquals("id", preparedSchema(
                    "USE schema_catalog.db1; SWITCH internal; SELECT id FROM schema_input")
                    .getFields().get(0).getName());
            connectContext.addLastDBOfCatalog("internal", "schema_test");
            connectContext.changeDefaultCatalog("schema_catalog");
            connectContext.setDatabase("db1");
            Assertions.assertEquals("id", preparedSchema("SWITCH internal; SELECT id FROM schema_input")
                    .getFields().get(0).getName());
            Assertions.assertEquals("schema_catalog", connectContext.getDefaultCatalog());
            Assertions.assertEquals("db1", connectContext.getDatabase());
            Assertions.assertEquals("schema_test", connectContext.getLastDBOfCatalog("internal"));
        } finally {
            connectContext.changeDefaultCatalog("internal");
            connectContext.setDatabase(database);
            connectContext.addLastDBOfCatalog("internal", remembered == null ? "" : remembered);
        }
    }

    @Test
    void commandsWithUnknownOrDynamicMetadataAreNotOkResults() {
        for (String query : Arrays.asList("SHOW PYTHON PACKAGES IN '3.11'", "DESC schema_input ALL",
                "SHOW DATA FROM schema_input", "SHOW PARTITIONS FROM schema_input", "SHOW QUERY STATS",
                "COPY INTO schema_input FROM @~", "WARM UP CLUSTER target WITH CLUSTER source")) {
            FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> schema(query), query);
            Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, error.status().code(), query);
            java.util.concurrent.ExecutionException prepareError = Assertions.assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> prepare(query), query);
            Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED,
                    ((FlightRuntimeException) prepareError.getCause()).status().code(), query);
        }
        Assertions.assertEquals(6, schema("SHOW FRONTEND CONFIG").getFields().size());
    }

    @Test
    void dynamicShowCreateAndProcCanBePrepared() throws Exception {
        Assertions.assertEquals(Arrays.asList("Table", "Create Table"),
                preparedSchema("SHOW CREATE TABLE schema_input").getFields().stream()
                        .map(Field::getName).collect(Collectors.toList()));
        Assertions.assertEquals(schema("SHOW PROC '/'"), preparedSchema("SHOW PROC '/'"));
        Assertions.assertFalse(schema("SHOW PROC '/'").getFields().isEmpty());
    }

    @Test
    void aggregateStateUsesSerializedStringType() throws Exception {
        String query = "SELECT group_concat_state('x') AS s";
        Assertions.assertEquals(new ArrowType.Utf8(), schema(query).getFields().get(0).getType());
        Assertions.assertEquals(schema(query), preparedSchema(query));
    }

    @Test
    void preparedHandleCannotChangeNamespace() throws Exception {
        ActionCreatePreparedStatementResult result = prepare("SELECT id FROM schema_input");
        CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
        String database = connectContext.getDatabase();
        connectContext.setDatabase("information_schema");
        try {
            FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getSchemaPreparedStatement(command, callContext, FlightDescriptor.command(new byte[0])));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
            error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoPreparedStatement(command, callContext,
                            FlightDescriptor.command(new byte[0])));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
        } finally {
            connectContext.setDatabase(database);
            String handle = result.getPreparedStatementHandle().toStringUtf8();
            connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
        }
    }

    @Test
    void executionCannotMutateSchemaAnalysisSessionClone() throws Exception {
        SessionVariable original = connectContext.getSessionVariable();
        int timeout = original.getQueryTimeoutS();
        CountDownLatch cloned = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch executing = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        Mockito.doAnswer(invocation -> {
            invocation.callRealMethod();
            if (invocation.getArgument(0) != original) {
                cloned.countDown();
                Assertions.assertTrue(release.await(10, java.util.concurrent.TimeUnit.SECONDS));
            }
            return null;
        }).when(connectContext).setSessionVariable(Mockito.any());
        try {
            Future<?> analysis = workers.submit(() -> schema("SELECT 1"));
            Assertions.assertTrue(cloned.await(10, java.util.concurrent.TimeUnit.SECONDS));
            Future<?> execution = workers.submit(() -> {
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            Mockito.doAnswer(invocation -> {
                                connectContext.getSessionVariable().setQueryTimeoutS(17);
                                connectContext.setReturnResultFromLocal(true);
                                return null;
                            }).when(processor).handleQuery(Mockito.anyString());
                        })) {
                    executing.countDown();
                    producer.getFlightInfoStatement(CommandStatementQuery.newBuilder()
                            .setQuery("SET query_timeout=17").build(), callContext,
                            FlightDescriptor.command(new byte[0]));
                }
            });
            Assertions.assertTrue(executing.await(10, java.util.concurrent.TimeUnit.SECONDS));
            // Execution must wait until schema analysis restores the real session variables.
            Assertions.assertThrows(TimeoutException.class,
                    () -> execution.get(200, java.util.concurrent.TimeUnit.MILLISECONDS));
            release.countDown();
            analysis.get(10, java.util.concurrent.TimeUnit.SECONDS);
            execution.get(10, java.util.concurrent.TimeUnit.SECONDS);
            Assertions.assertSame(original, connectContext.getSessionVariable());
            Assertions.assertEquals(17, original.getQueryTimeoutS());
        } finally {
            release.countDown();
            workers.shutdown();
            Assertions.assertTrue(workers.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
            Mockito.doCallRealMethod().when(connectContext).setSessionVariable(Mockito.any());
            original.setQueryTimeoutS(timeout);
            connectContext.getFlightSqlChannel().reset();
        }
    }

    @Test
    void preparedSchemaCannotDriftWithSqlMode() throws Exception {
        long mode = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(0);
            for (boolean fetchSchema : Arrays.asList(true, false)) {
                ActionCreatePreparedStatementResult result = prepare("SELECT 1 || 2 AS x");
                CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
                connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT);
                Assertions.assertEquals(new ArrowType.Utf8(), schema("SELECT 1 || 2 AS x")
                        .getFields().get(0).getType());
                FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class, () -> {
                    if (fetchSchema) {
                        producer.getSchemaPreparedStatement(command, callContext, FlightDescriptor.command(new byte[0]));
                    } else {
                        producer.getFlightInfoPreparedStatement(command, callContext,
                                FlightDescriptor.command(new byte[0]));
                    }
                });
                Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
                String handle = result.getPreparedStatementHandle().toStringUtf8();
                Assertions.assertNull(connectContext.getPreparedQuery(handle.substring(handle.indexOf(':') + 1)));
                connectContext.getSessionVariable().setSqlMode(0);
            }
        } finally {
            connectContext.getSessionVariable().setSqlMode(mode);
        }
    }

    @Test
    void preparedHandleSurvivesSettingsThatDoNotChangeSchema() throws Exception {
        int timeout = connectContext.getSessionVariable().getQueryTimeoutS();
        ActionCreatePreparedStatementResult result = prepare("SELECT 1 AS x");
        CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
        try {
            connectContext.getSessionVariable().setQueryTimeoutS(timeout + 1);
            Assertions.assertEquals(schema("SELECT 1 AS x"), producer.getSchemaPreparedStatement(
                    command, callContext, FlightDescriptor.command(new byte[0])).getSchema());
        } finally {
            connectContext.getSessionVariable().setQueryTimeoutS(timeout);
            String handle = result.getPreparedStatementHandle().toStringUtf8();
            connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
        }
    }

    @Test
    void dialectParseFailureRetriesOriginalOnlyWhenEnabled() {
        SessionVariable session = connectContext.getSessionVariable();
        boolean retry = session.retryOriginSqlOnConvertFail;
        String query = "SELECT id FROM schema_input";
        try (MockedStatic<SqlDialectHelper> converter = Mockito.mockStatic(SqlDialectHelper.class)) {
            converter.when(() -> SqlDialectHelper.convertSqlByDialect(Mockito.eq(query), Mockito.any()))
                    .thenReturn("SELECT FROM");
            session.retryOriginSqlOnConvertFail = true;
            Assertions.assertEquals("id", schema(query).getFields().get(0).getName());
            Assertions.assertSame(session, connectContext.getSessionVariable());
            session.retryOriginSqlOnConvertFail = false;
            Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT,
                    Assertions.assertThrows(FlightRuntimeException.class, () -> schema(query)).status().code());
        } finally {
            session.retryOriginSqlOnConvertFail = retry;
        }
    }

    @Test
    void procSchemaDoesNotFetchRowsOrTraverseRemoteNodes() throws Exception {
        ProcService service = Mockito.mock(ProcService.class);
        ProcNodeInterface node = Mockito.mock(ProcNodeInterface.class);
        Mockito.when(service.open(Mockito.anyString())).thenReturn(node);
        Mockito.when(node.fetchResult()).thenThrow(new AssertionError("Schema discovery fetched PROC rows"));
        try (MockedStatic<ProcService> services = Mockito.mockStatic(ProcService.class)) {
            services.when(ProcService::getInstance).thenReturn(service);
            Assertions.assertEquals("name", schema("SHOW PROC '/'").getFields().get(0).getName());
            Assertions.assertEquals("QueryId", schema("SHOW PROC '/current_queries'")
                    .getFields().get(0).getName());
            Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, Assertions.assertThrows(
                    FlightRuntimeException.class, () -> schema("SHOW PROC '/dbs/1/2/index_schema/3'"))
                    .status().code());
            Mockito.verifyNoInteractions(service, node);
        }
    }

    @Test
    void lanceIndexSchemaFollowsGenericDdlStatusSchema() throws Exception {
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.doReturn(catalog).when(connectContext).getCatalog("schema_lance");
        for (String query : Arrays.asList(
                "CREATE INDEX idx ON schema_lance.db1.source (v) USING ANN",
                "CREATE INDEX IF NOT EXISTS idx ON schema_lance.db1.source (v) USING ANN",
                "DROP INDEX IF EXISTS idx ON schema_lance.db1.source")) {
            Assertions.assertEquals("StatusResult", schema(query).getFields().get(0).getName(), query);
        }
        Assertions.assertEquals("StatusResult", schema("CREATE INDEX idx ON schema_input (name) USING INVERTED")
                .getFields().get(0).getName());
    }

    @Test
    void forwardedMasterRejectionFailsFlightExecutionInsteadOfOk() throws Exception {
        // What a follower FE holds after forwarding a valid Lance index DDL that the master
        // rejected: the follower-local state stays OK (the MySQL path returns the master's ERR
        // packet verbatim and the audit log reads the proxy status), so only the proxy fields
        // describe the outcome. Flight must not turn that combination into a success result.
        StmtExecutor rejected = Mockito.mock(StmtExecutor.class);
        Mockito.when(rejected.hasForwardedToMaster()).thenReturn(true);
        Mockito.when(rejected.getProxyStatusCode())
                .thenReturn(ErrorCode.ERR_LANCE_INDEX_OPERATION_NOT_SUPPORTED.getCode());
        Mockito.when(rejected.getProxyErrMsg())
                .thenReturn("CREATE INDEX is not supported for Lance catalog tables");
        StmtExecutor accepted = Mockito.mock(StmtExecutor.class);
        Mockito.when(accepted.hasForwardedToMaster()).thenReturn(true);
        Mockito.when(accepted.getProxyStatusCode()).thenReturn(0);
        final StmtExecutor[] forwarded = {rejected};
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.doReturn(catalog).when(connectContext).getCatalog("schema_lance");
        StmtExecutor previous = connectContext.getExecutor();
        String ddl = "CREATE INDEX idx ON schema_lance.db1.source (v) USING ANN";
        ActionCreatePreparedStatementResult result = prepare(ddl);
        CommandPreparedStatementQuery prepared = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
        try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (processor, context) -> {
                    Mockito.doAnswer(invocation -> {
                        // Reset like the real processor's prepare() so each execution starts from
                        // a clean local state, whatever the previous one set.
                        connectContext.getState().reset();
                        connectContext.setReturnResultFromLocal(true);
                        connectContext.setExecutor(forwarded[0]);
                        return null;
                    }).when(processor).handleQuery(Mockito.anyString());
                    Mockito.doAnswer(invocation -> {
                        connectContext.getState().reset();
                        connectContext.setReturnResultFromLocal(true);
                        connectContext.setExecutor(forwarded[0]);
                        return null;
                    }).when(processor).handleQuery(Mockito.anyString(), Mockito.any());
                })) {
            // Direct Execute of a forwarded statement the master rejected.
            FlightRuntimeException direct = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoStatement(CommandStatementQuery.newBuilder()
                            .setQuery(ddl).build(), callContext, FlightDescriptor.command(new byte[0])));
            Assertions.assertEquals(FlightStatusCode.INTERNAL, direct.status().code());
            Assertions.assertTrue(direct.status().description()
                    .contains("CREATE INDEX is not supported for Lance catalog tables"), direct.toString());
            Assertions.assertEquals(0, connectContext.getFlightSqlChannel().resultNum());

            // Prepared Execute of the same statement fails the same way, and the handle survives:
            // the rejection is an execution outcome, not a schema change.
            FlightRuntimeException viaPrepared = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoPreparedStatement(prepared, callContext,
                            FlightDescriptor.command(new byte[0])));
            Assertions.assertEquals(FlightStatusCode.INTERNAL, viaPrepared.status().code());
            Assertions.assertTrue(viaPrepared.status().description()
                    .contains("forwarded statement failed on master FE"), viaPrepared.toString());
            Assertions.assertEquals(0, connectContext.getFlightSqlChannel().resultNum());
            String handle = result.getPreparedStatementHandle().toStringUtf8();
            Assertions.assertNotNull(connectContext.getPreparedQuery(handle.substring(handle.indexOf(':') + 1)));

            // A forwarded statement the master accepted still produces the OK StatusResult.
            forwarded[0] = accepted;
            Assertions.assertEquals("StatusResult", producer.getFlightInfoStatement(
                    CommandStatementQuery.newBuilder().setQuery(ddl).build(), callContext,
                    FlightDescriptor.command(new byte[0])).getSchema().getFields().get(0).getName());
            Assertions.assertEquals(1, connectContext.getFlightSqlChannel().resultNum());
        } finally {
            connectContext.setExecutor(previous);
            connectContext.getFlightSqlChannel().reset();
            connectContext.getState().reset();
            String handle = result.getPreparedStatementHandle().toStringUtf8();
            connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
        }
    }

    @Test
    void procedureCommandsExposeFixedHeaders() throws Exception {
        Assertions.assertEquals(Arrays.asList("Procedure", "Create Procedure"),
                preparedSchema("SHOW CREATE PROCEDURE schema_proc").getFields().stream()
                        .map(Field::getName).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("ProcedureName", "CatalogId", "DbId", "DbName", "PackageName",
                "OwnerName", "CreateTime", "ModifyTime"), preparedSchema("SHOW PROCEDURE STATUS")
                .getFields().stream().map(Field::getName).collect(Collectors.toList()));
    }

    @Test
    void noRowCommandsRemainPreparableWithoutExecution() throws Exception {
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class)) {
            for (String query : Arrays.asList("INSERT INTO schema_input VALUES (1, 'x')",
                    "INSERT OVERWRITE TABLE schema_input SELECT * FROM schema_input",
                    "UPDATE schema_input SET name='x' WHERE id=1", "DELETE FROM schema_input WHERE id=1",
                    "MERGE INTO schema_input t USING schema_input s ON t.id=s.id WHEN MATCHED THEN DELETE",
                    "KILL QUERY 123", "BEGIN", "COMMIT", "ROLLBACK")) {
                Assertions.assertEquals("StatusResult", preparedSchema(query).getFields().get(0).getName(), query);
                Assertions.assertEquals(schema(query), preparedSchema(query));
            }
            Assertions.assertTrue(executors.constructed().isEmpty());
        }
    }

    @Test
    void warmupSelectHasStatisticsInsteadOfOkHeader() throws Exception {
        String query = "WARM UP SELECT * FROM schema_input";
        Assertions.assertEquals(Arrays.asList("BackendId", "ScanRows", "ScanBytes", "ScanBytesFromLocalStorage",
                "ScanBytesFromRemoteStorage", "BytesWriteIntoCache"), preparedSchema(query).getFields().stream()
                .map(Field::getName).collect(Collectors.toList()));
        Assertions.assertEquals(schema(query), preparedSchema(query));
    }

    @Test
    void explainAndReplayExposeHeadersWithoutRunningPlanner() throws Exception {
        Assertions.assertEquals("Explain String(Nereids Planner)",
                preparedSchema("EXPLAIN SELECT 1").getFields().get(0).getName());
        Assertions.assertEquals("Plan Replayer dump url",
                preparedSchema("PLAN REPLAYER DUMP SELECT 1").getFields().get(0).getName());
        // PLAN PROCESS currently has no Flight result serialization, unlike ordinary EXPLAIN.
        Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, Assertions.assertThrows(
                FlightRuntimeException.class, () -> schema("EXPLAIN PLAN PROCESS SELECT 1")).status().code());
    }

    @Test
    void preparedNamespaceTransitionIsReusable() throws Exception {
        String database = connectContext.getDatabase();
        try {
            for (String query : Arrays.asList("USE information_schema",
                    "USE information_schema; SELECT TABLE_NAME FROM tables WHERE 1=0")) {
                connectContext.setDatabase(database);
                Schema expected = schema(query);
                ActionCreatePreparedStatementResult result = prepare(query);
                CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            Mockito.doAnswer(invocation -> {
                                connectContext.setDatabase("information_schema");
                                connectContext.setReturnResultFromLocal(false);
                                return null;
                            }).when(processor).handleQuery(Mockito.anyString());
                            Mockito.when(processor.getArrowSchema()).thenReturn(expected);
                        })) {
                    for (int i = 0; i < 2; ++i) {
                        Assertions.assertEquals(expected, producer.getFlightInfoPreparedStatement(command,
                                callContext, FlightDescriptor.command(new byte[0])).getSchema());
                        Assertions.assertEquals(expected, producer.getSchemaPreparedStatement(command,
                                callContext, FlightDescriptor.command(new byte[0])).getSchema());
                    }
                } finally {
                    String handle = result.getPreparedStatementHandle().toStringUtf8();
                    connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
                }
            }
        } finally {
            connectContext.setDatabase(database);
        }
    }

    @Test
    void executionSchemaDriftExpiresHandleAndCleansResults() throws Exception {
        Schema changed = new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("new_column", new ArrowType.Utf8())));
        StmtExecutor previous = connectContext.getExecutor();
        try {
            for (boolean remote : Arrays.asList(true, false)) {
                ActionCreatePreparedStatementResult result = prepare("SELECT id FROM schema_input");
                CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
                StmtExecutor executor = Mockito.mock(StmtExecutor.class);
                FlightSqlConnectProcessor cleanup = new FlightSqlConnectProcessor(connectContext);
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            // Simulate replanning after another session changes the table, after schema reanalysis.
                            Mockito.doAnswer(invocation -> {
                                connectContext.setReturnResultFromLocal(!remote);
                                connectContext.setExecutor(executor);
                                connectContext.addFlightSqlDeferredExecutor(executor);
                                return null;
                            }).when(processor).handleQuery(Mockito.anyString());
                            Mockito.when(processor.getArrowSchema()).thenReturn(changed);
                            Mockito.doAnswer(invocation -> {
                                cleanup.close();
                                return null;
                            }).when(processor).close();
                        })) {
                    FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                            () -> producer.getFlightInfoPreparedStatement(command, callContext,
                                    FlightDescriptor.command(new byte[0])));
                    Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
                    Assertions.assertNull(connectContext.getExecutor());
                    Mockito.verify(processors.constructed().get(0)).close();
                    InOrder cleanupOrder = Mockito.inOrder(executor);
                    cleanupOrder.verify(executor).cancel(Mockito.any(Status.class));
                    cleanupOrder.verify(executor).finalizeArrowFlightQuery();
                    Assertions.assertEquals(0, connectContext.getFlightSqlChannel().resultNum());
                    Assertions.assertTrue(connectContext.getFlightSqlEndpointsLocations().isEmpty());
                    String handle = result.getPreparedStatementHandle().toStringUtf8();
                    Assertions.assertNull(connectContext.getPreparedQuery(handle.substring(handle.indexOf(':') + 1)));
                    connectContext.closeFlightSqlDeferredExecutors();
                    Mockito.verify(executor, Mockito.times(1)).finalizeArrowFlightQuery();
                } finally {
                    connectContext.closeFlightSqlDeferredExecutors();
                    connectContext.getFlightSqlChannel().reset();
                    String handle = result.getPreparedStatementHandle().toStringUtf8();
                    connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
                }
            }
        } finally {
            connectContext.setExecutor(previous);
        }
    }

    @Test
    void backendWireLabelsAndNarrowedNullabilityDoNotExpirePreparedQueries() throws Exception {
        Schema actual = new Schema(Arrays.asList(
                Field.notNullable("Int64_0", new ArrowType.Int(64, true)),
                Field.notNullable("String_1", new ArrowType.Utf8())));
        assertPreparedExecutionSchema("SELECT id, name FROM schema_input WHERE name IS NOT NULL",
                Arrays.asList("id", "name"), actual, true);
    }

    @Test
    void actualSemanticRenameAndNullableWideningStillExpirePreparedQueries() throws Exception {
        Schema renamed = new Schema(Collections.singletonList(
                Field.notNullable("Int64_0", new ArrowType.Int(64, true))));
        assertPreparedExecutionSchema("SELECT id FROM schema_input", Collections.singletonList("renamed"),
                renamed, false);
        Schema widened = new Schema(Collections.singletonList(
                Field.nullable("id", new ArrowType.Int(64, true))));
        assertPreparedExecutionSchema("SELECT id FROM schema_input", Collections.singletonList("id"),
                widened, false);
    }

    private void assertPreparedExecutionSchema(String query, List<String> labels, Schema actual, boolean compatible)
            throws Exception {
        StmtExecutor previous = connectContext.getExecutor();
        ActionCreatePreparedStatementResult result = prepare(query);
        CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        LogicalPlanAdapter statement = Mockito.mock(LogicalPlanAdapter.class);
        Mockito.when(statement.getColLabels()).thenReturn(new ArrayList<>(labels));
        Mockito.when(executor.getParsedStmt()).thenReturn(statement);
        FlightSqlConnectProcessor cleanup = new FlightSqlConnectProcessor(connectContext);
        try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (processor, context) -> {
                    Mockito.doAnswer(invocation -> {
                        connectContext.setExecutor(executor);
                        connectContext.addFlightSqlDeferredExecutor(executor);
                        connectContext.setReturnResultFromLocal(false);
                        return null;
                    }).when(processor).handleQuery(Mockito.anyString());
                    Mockito.when(processor.getArrowSchema()).thenReturn(actual);
                    // Exercise real processor cleanup: it clears the context's executor before validation.
                    Mockito.doAnswer(invocation -> {
                        cleanup.close();
                        return null;
                    }).when(processor).close();
                })) {
            if (compatible) {
                Assertions.assertEquals(actual, producer.getFlightInfoPreparedStatement(command,
                        callContext, FlightDescriptor.command(new byte[0])).getSchema());
                Assertions.assertEquals(schema(query), producer.getSchemaPreparedStatement(command,
                        callContext, FlightDescriptor.command(new byte[0])).getSchema());
                Mockito.verify(executor, Mockito.never()).cancel(Mockito.any(Status.class));
                Mockito.verify(executor, Mockito.never()).finalizeArrowFlightQuery();
            } else {
                FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                        () -> producer.getFlightInfoPreparedStatement(command, callContext,
                                FlightDescriptor.command(new byte[0])));
                Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
                InOrder cleanupOrder = Mockito.inOrder(executor);
                cleanupOrder.verify(executor).cancel(Mockito.any(Status.class));
                cleanupOrder.verify(executor).finalizeArrowFlightQuery();
            }
            Assertions.assertNull(connectContext.getExecutor());
            Mockito.verify(processors.constructed().get(0)).close();
        } finally {
            connectContext.closeFlightSqlDeferredExecutors();
            connectContext.setExecutor(previous);
            String handle = result.getPreparedStatementHandle().toStringUtf8();
            connectContext.removePreparedQuery(handle.substring(handle.indexOf(':') + 1));
        }
    }

}
