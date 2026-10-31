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

package org.apache.doris.arrowflight;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
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
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DorisFlightSqlSchemaTest extends TestWithFeService {
    private DorisFlightSqlProducer producer;
    private CallContext callContext;
    private ConnectContext flightContext;
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
        // A Flight SQL session of the same user, settings and database as the test's MySQL context.
        ConnectContext flight = ConnectContext.forFlight("schema-peer");
        flight.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flight.setRemoteIP(connectContext.getRemoteIP());
        flight.setEnv(Env.getCurrentEnv());
        flight.setSessionVariable(VariableMgr.cloneSessionVariable(connectContext.getSessionVariable()));
        flight.setDatabase("schema_test");
        flightContext = Mockito.spy(flight);
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessions.getConnectContext("schema-peer")).thenReturn(flightContext);
        callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("schema-peer");
        producer = new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 0), sessions);
    }

    @Override
    protected void runAfterAll() throws Exception {
        FeConstants.runningUnitTest = previousUnitTest;
        producer.close();
        flightContext.getFlightSqlChannel().close();
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

    private static String preparedId(ActionCreatePreparedStatementResult result) {
        String handle = result.getPreparedStatementHandle().toStringUtf8();
        return handle.substring(handle.indexOf(':') + 1);
    }

    private static Schema datasetSchema(ActionCreatePreparedStatementResult result) throws IOException {
        byte[] bytes = result.getDatasetSchema().toByteArray();
        return MessageSerializer.deserializeSchema(new ReadChannel(
                Channels.newChannel(new ByteArrayInputStream(bytes))));
    }

    private Schema preparedSchema(String sql) throws Exception {
        ActionCreatePreparedStatementResult result = prepare(sql);
        flightContext.removePreparedQuery(preparedId(result));
        return datasetSchema(result);
    }

    private static CommandPreparedStatementQuery command(ActionCreatePreparedStatementResult result) {
        return CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(result.getPreparedStatementHandle()).build();
    }

    // The final planner's parse result of a query that ran, with its result labels.
    private static StmtExecutor executorWithLabels(String... labels) {
        LogicalPlanAdapter statement = Mockito.mock(LogicalPlanAdapter.class);
        Mockito.when(statement.getColLabels()).thenReturn(new ArrayList<>(Arrays.asList(labels)));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getParsedStmt()).thenReturn(statement);
        return executor;
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
            FlightRuntimeException schemaFailure = Assertions.assertThrows(
                    FlightRuntimeException.class, () -> schema(sql));
            Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT, schemaFailure.status().code());
            Mockito.clearInvocations(flightContext);
            ExecutionException prepareFailure = Assertions.assertThrows(ExecutionException.class,
                    () -> prepare(sql));
            Assertions.assertTrue(prepareFailure.getCause() instanceof FlightRuntimeException);
            Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT,
                    ((FlightRuntimeException) prepareFailure.getCause()).status().code());
            Mockito.verify(flightContext, Mockito.never()).addPreparedQuery(Mockito.anyString(), Mockito.any());
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
        SessionVariable session = flightContext.getSessionVariable();
        StatementContext statement = flightContext.getStatementContext();
        ConnectContext threadContext = ConnectContext.get();
        int timeout = session.getQueryTimeoutS();
        schema("SELECT /*+ SET_VAR(query_timeout=17) */ id FROM schema_input");
        Assertions.assertSame(session, flightContext.getSessionVariable());
        Assertions.assertEquals(timeout, session.getQueryTimeoutS());
        Assertions.assertSame(statement, flightContext.getStatementContext());
        Assertions.assertSame(threadContext, ConnectContext.get());
        Assertions.assertEquals("StatusResult", schema("SET query_timeout=17").getFields().get(0).getName());
        Assertions.assertEquals(timeout, session.getQueryTimeoutS());
        Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, Assertions.assertThrows(
                FlightRuntimeException.class, () -> schema("SELECT 1; SELECT 2")).status().code());
        Assertions.assertEquals(MysqlCommand.COM_SLEEP, flightContext.getCommand());
    }

    @Test
    void createPreparedStatementDoesNotLeakChannelAllocator() throws Exception {
        // Each prepare used to allocate an off-heap placeholder vector. Repeated prepares must stay
        // bounded, for an analyzed statement and for one that keeps the placeholder alike.
        for (int i = 0; i < 100; ++i) {
            Assertions.assertEquals("id", preparedSchema("SELECT " + i + " AS id").getFields().get(0).getName());
            Assertions.assertEquals("ResultMeta", preparedSchema("SET @v = " + i + "; SELECT @v")
                    .getFields().get(0).getName());
            Assertions.assertEquals(0, flightContext.getFlightSqlChannel().getAllocatedMemory());
            Assertions.assertEquals(0, flightContext.getFlightSqlChannel().resultNum());
        }
    }

    @Test
    void getSchemaPreparedStatementValidatesHandle() throws Exception {
        ActionCreatePreparedStatementResult result = prepare("SELECT id FROM schema_input");
        CommandPreparedStatementQuery command = command(result);
        Assertions.assertEquals(schema("SELECT id FROM schema_input"), producer.getSchemaPreparedStatement(
                command, callContext, FlightDescriptor.command(new byte[0])).getSchema());
        flightContext.removePreparedQuery(preparedId(result));
        FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class,
                () -> producer.getSchemaPreparedStatement(command, callContext, FlightDescriptor.command(new byte[0])));
        Assertions.assertEquals(FlightStatusCode.NOT_FOUND, failure.status().code());
    }

    @Test
    void preparedHandleMustBelongToTheCallingSession() throws Exception {
        ActionCreatePreparedStatementResult result = prepare("SELECT id FROM schema_input");
        try {
            // Another session's handle for the same id, and this session's prefix without an id.
            for (String handle : Arrays.asList("other-peer:" + preparedId(result), "schema-peer:")) {
                CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(ByteString.copyFromUtf8(handle)).build();
                Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT, Assertions.assertThrows(
                        FlightRuntimeException.class, () -> producer.getSchemaPreparedStatement(command,
                                callContext, FlightDescriptor.command(new byte[0]))).status().code(), handle);
                Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT, Assertions.assertThrows(
                        FlightRuntimeException.class, () -> producer.getFlightInfoPreparedStatement(command,
                                callContext, FlightDescriptor.command(new byte[0]))).status().code(), handle);
                CompletableFuture<Void> closed = new CompletableFuture<>();
                producer.closePreparedStatement(ActionClosePreparedStatementRequest.newBuilder()
                        .setPreparedStatementHandle(ByteString.copyFromUtf8(handle)).build(), callContext,
                        new StreamListener<Result>() {
                            @Override
                            public void onNext(Result value) {
                            }

                            @Override
                            public void onError(Throwable error) {
                                closed.completeExceptionally(error);
                            }

                            @Override
                            public void onCompleted() {
                                closed.complete(null);
                            }
                        });
                ExecutionException closeFailure = Assertions.assertThrows(ExecutionException.class,
                        () -> closed.get(30, java.util.concurrent.TimeUnit.SECONDS));
                Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT,
                        ((FlightRuntimeException) closeFailure.getCause()).status().code(), handle);
            }
            // None of them touched this session's handle.
            Assertions.assertNotNull(flightContext.getPreparedQuery(preparedId(result)));
        } finally {
            flightContext.removePreparedQuery(preparedId(result));
        }
    }

    // The privilege check of the analyzed plan denies sql: Prepare cannot tell whether the executed plan is
    // denied too, so it keeps the placeholder and leaves the decision to execution, and GetSchema reports the
    // denial without a schema.
    private void assertPrivilegesLeftToExecution(String sql) throws Exception {
        FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class, () -> schema(sql), sql);
        Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, failure.status().code(), sql);
        Assertions.assertTrue(failure.getMessage().contains("denied"), failure.getMessage());
        ActionCreatePreparedStatementResult result = prepare(sql);
        try {
            Assertions.assertEquals(Collections.singletonList("ResultMeta"), datasetSchema(result).getFields()
                    .stream().map(Field::getName).collect(Collectors.toList()), sql);
            Assertions.assertNull(flightContext.getPreparedQuery(preparedId(result)).getSchema(), sql);
        } finally {
            flightContext.removePreparedQuery(preparedId(result));
        }
    }

    @Test
    void schemaDiscoveryChecksTablePrivileges() throws Exception {
        UserIdentity originalUser = flightContext.getCurrentUserIdentity();
        flightContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("schema_reader", "%"));
        try {
            for (String sql : Arrays.asList("SELECT id FROM schema_input",
                    "WITH source AS (SELECT id FROM schema_input) SELECT * FROM source",
                    "SELECT (SELECT MAX(id) FROM schema_input) AS value",
                    "SELECT SUM(id) FROM schema_input")) {
                assertPrivilegesLeftToExecution(sql);
            }
        } finally {
            flightContext.setCurrentUserIdentity(originalUser);
        }
    }

    @Test
    void columnGrantsAreCheckedAsExecutionChecksThem() throws Exception {
        addUser("schema_column_reader", true);
        grantPriv("GRANT SELECT_PRIV(id) ON schema_test.schema_input TO 'schema_column_reader'@'%'");
        UserIdentity reader = UserIdentity.createAnalyzedUserIdentWithIp("schema_column_reader", "%");
        UserIdentity originalFlightUser = flightContext.getCurrentUserIdentity();
        UserIdentity originalUser = connectContext.getCurrentUserIdentity();
        // connectContext plans as execution does (PlanChecker runs the whole rewriter), flightContext discovers.
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(reader);
        flightContext.setCurrentUserIdentity(reader);
        try {
            // Execution inlines a CTE used once before it checks privileges, so only the columns read are checked.
            for (String sql : Arrays.asList("WITH c AS (SELECT id, name FROM schema_input) SELECT id FROM c",
                    "WITH c AS (SELECT * FROM schema_input) SELECT id FROM c",
                    "WITH c AS (SELECT id, name FROM (SELECT id, name FROM schema_input) d) SELECT id FROM c",
                    "WITH c AS (SELECT id, MAX(name) AS top FROM schema_input GROUP BY id) SELECT id FROM c")) {
                PlanChecker.from(connectContext).parse(sql).analyze().rewrite();
                Assertions.assertEquals(Collections.singletonList("id"), schema(sql).getFields().stream()
                        .map(Field::getName).collect(Collectors.toList()), sql);
                ActionCreatePreparedStatementResult result = prepare(sql);
                try {
                    Assertions.assertEquals(schema(sql), datasetSchema(result), sql);
                    Assertions.assertNotNull(flightContext.getPreparedQuery(preparedId(result)).getSchema(), sql);
                } finally {
                    flightContext.removePreparedQuery(preparedId(result));
                }
            }
            // Execution denies these as well: a column without the grant, and a CTE used twice, which it does not
            // inline and checks on every column the CTE selects.
            for (String sql : Arrays.asList("SELECT name FROM schema_input",
                    "WITH c AS (SELECT id, name FROM schema_input) SELECT a.id FROM c a JOIN c b ON a.id = b.id")) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> PlanChecker.from(connectContext).parse(sql).analyze().rewrite(), sql);
                assertPrivilegesLeftToExecution(sql);
            }
            // Execution drops an ORDER BY under a derived table before it checks privileges, while the analyzed
            // plan still sorts on the column without the grant: Prepare must not refuse what execution runs.
            String sorted = "SELECT id FROM (SELECT id, name FROM schema_input ORDER BY name) d";
            PlanChecker.from(connectContext).parse(sorted).analyze().rewrite();
            assertPrivilegesLeftToExecution(sorted);
        } finally {
            flightContext.setCurrentUserIdentity(originalFlightUser);
            connectContext.setCurrentUserIdentity(originalUser);
        }
    }

    @Test
    void prepareKeepsCommandAndUsePrefixCompatibility() throws Exception {
        Assertions.assertEquals("StatusResult", preparedSchema("CREATE DATABASE schema_not_created")
                .getFields().get(0).getName());
        Assertions.assertFalse(flightContext.getCurrentCatalog().getDb("schema_not_created").isPresent());
        String previousDatabase = flightContext.getDatabase();
        flightContext.setDatabase("information_schema");
        try {
            Assertions.assertEquals("id", preparedSchema("USE schema_test; SELECT id FROM schema_input")
                    .getFields().get(0).getName());
            Assertions.assertEquals("information_schema", flightContext.getDatabase());
        } finally {
            flightContext.setDatabase(previousDatabase);
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
        // Execution marks logical types at every depth (withDorisTypeMetadata), so Prepare does too.
        Assertions.assertEquals("LARGEINT", entries.getChildren().get(1).getMetadata().get("doris_type"));
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
        String database = flightContext.getDatabase();
        String remembered = flightContext.getLastDBOfCatalog("internal");
        try {
            flightContext.addLastDBOfCatalog("internal", "information_schema");
            Assertions.assertEquals("id", preparedSchema(
                    "USE schema_catalog.db1; SWITCH internal; SELECT id FROM schema_input")
                    .getFields().get(0).getName());
            flightContext.addLastDBOfCatalog("internal", "schema_test");
            flightContext.changeDefaultCatalog("schema_catalog");
            flightContext.setDatabase("db1");
            Assertions.assertEquals("id", preparedSchema("SWITCH internal; SELECT id FROM schema_input")
                    .getFields().get(0).getName());
            Assertions.assertEquals("schema_catalog", flightContext.getDefaultCatalog());
            Assertions.assertEquals("db1", flightContext.getDatabase());
            Assertions.assertEquals("schema_test", flightContext.getLastDBOfCatalog("internal"));
        } finally {
            flightContext.changeDefaultCatalog("internal");
            flightContext.setDatabase(database);
            flightContext.addLastDBOfCatalog("internal", remembered == null ? "" : remembered);
        }
    }

    private void assertKeepsPlaceholder(String query) throws Exception {
        FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                () -> schema(query), query);
        Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, error.status().code(), query);
        // The Arrow Flight SQL JDBC driver prepares every statement and does not fall back when Prepare
        // refuses one: such a statement keeps the placeholder schema, and its handle has nothing to check.
        ActionCreatePreparedStatementResult result = prepare(query);
        try {
            Assertions.assertEquals(Collections.singletonList("ResultMeta"), datasetSchema(result).getFields()
                    .stream().map(Field::getName).collect(Collectors.toList()), query);
            Assertions.assertNull(flightContext.getPreparedQuery(preparedId(result)).getSchema(), query);
            Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, Assertions.assertThrows(
                    FlightRuntimeException.class, () -> producer.getSchemaPreparedStatement(command(result),
                            callContext, FlightDescriptor.command(new byte[0]))).status().code(), query);
        } finally {
            flightContext.removePreparedQuery(preparedId(result));
        }
    }

    @Test
    void commandsWithUnknownOrDynamicMetadataKeepThePlaceholder() throws Exception {
        for (String query : Arrays.asList("SHOW PYTHON PACKAGES IN '3.11'", "DESC schema_input ALL",
                "SHOW DATA FROM schema_input", "SHOW PARTITIONS FROM schema_input", "SHOW QUERY STATS",
                "COPY INTO schema_input FROM @~", "WARM UP CLUSTER target WITH CLUSTER source",
                "WARM UP SELECT * FROM schema_input", "SET @multi = 2; SELECT @multi")) {
            assertKeepsPlaceholder(query);
        }
        Assertions.assertEquals(6, schema("SHOW FRONTEND CONFIG").getFields().size());
    }

    @Test
    void untypedNullResultsKeepThePlaceholder() throws Exception {
        // BE sends an untyped NULL column as nullable Bool, not as the Arrow Null of NULL_TYPE: an advertised
        // Null would expire the handle on every execution, so such a result is not described, at any depth.
        for (String query : Arrays.asList("SELECT NULL", "SELECT id, NULL AS n FROM schema_input ORDER BY id",
                "SELECT ARRAY(NULL) AS a", "SELECT MAP('k', NULL) AS m", "SELECT NAMED_STRUCT('n', NULL) AS s")) {
            assertKeepsPlaceholder(query);
        }
        // A NULL with a type is described as that type.
        Assertions.assertEquals(new ArrowType.Utf8(), preparedSchema("SELECT CAST(NULL AS VARCHAR(20)) AS name")
                .getFields().get(0).getType());
    }

    @Test
    void placeholderHandleRunsUncheckedInAnyNamespace() throws Exception {
        String database = flightContext.getDatabase();
        ActionCreatePreparedStatementResult result = prepare("DESC schema_input");
        List<String> executed = new ArrayList<>();
        try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (processor, context) -> {
                    Mockito.doAnswer(invocation -> {
                        executed.add(invocation.getArgument(0));
                        flightContext.getState().reset();
                        FlightProtocolAdapter.of(flightContext).beforeStatement(flightContext);
                        return null;
                    }).when(processor).handleQuery(Mockito.anyString());
                })) {
            // Not analyzed again, and not tied to the namespace it was prepared in: it runs as before.
            flightContext.setDatabase("information_schema");
            Assertions.assertEquals("StatusResult", producer.getFlightInfoPreparedStatement(command(result),
                    callContext, FlightDescriptor.command(new byte[0])).getSchema().getFields().get(0).getName());
            Assertions.assertEquals(Collections.singletonList("DESC schema_input"), executed);
            Assertions.assertNotNull(flightContext.getPreparedQuery(preparedId(result)));
        } finally {
            flightContext.setDatabase(database);
            flightContext.getFlightSqlChannel().reset();
            flightContext.removePreparedQuery(preparedId(result));
        }
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
        CommandPreparedStatementQuery command = command(result);
        String database = flightContext.getDatabase();
        flightContext.setDatabase("information_schema");
        try {
            FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getSchemaPreparedStatement(command, callContext,
                            FlightDescriptor.command(new byte[0])));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
            error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoPreparedStatement(command, callContext,
                            FlightDescriptor.command(new byte[0])));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
        } finally {
            flightContext.setDatabase(database);
            flightContext.removePreparedQuery(preparedId(result));
        }
    }

    @Test
    void executionCannotMutateSchemaAnalysisSessionClone() throws Exception {
        SessionVariable original = flightContext.getSessionVariable();
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
        }).when(flightContext).setSessionVariable(Mockito.any());
        try {
            Future<?> analysis = workers.submit(() -> schema("SELECT 1"));
            Assertions.assertTrue(cloned.await(10, java.util.concurrent.TimeUnit.SECONDS));
            Future<?> execution = workers.submit(() -> {
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            Mockito.doAnswer(invocation -> {
                                flightContext.getState().reset();
                                flightContext.getSessionVariable().setQueryTimeoutS(17);
                                FlightProtocolAdapter.of(flightContext).beforeStatement(flightContext);
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
            // Execution waits for the session's command lock until the analysis put the session variables back.
            Assertions.assertThrows(TimeoutException.class,
                    () -> execution.get(200, java.util.concurrent.TimeUnit.MILLISECONDS));
            release.countDown();
            analysis.get(10, java.util.concurrent.TimeUnit.SECONDS);
            execution.get(10, java.util.concurrent.TimeUnit.SECONDS);
            Assertions.assertSame(original, flightContext.getSessionVariable());
            Assertions.assertEquals(17, original.getQueryTimeoutS());
        } finally {
            release.countDown();
            workers.shutdown();
            Assertions.assertTrue(workers.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
            Mockito.doCallRealMethod().when(flightContext).setSessionVariable(Mockito.any());
            original.setQueryTimeoutS(timeout);
            flightContext.getFlightSqlChannel().reset();
        }
    }

    @Test
    void preparedSchemaCannotDriftWithSqlMode() throws Exception {
        long mode = flightContext.getSessionVariable().getSqlMode();
        try {
            flightContext.getSessionVariable().setSqlMode(0);
            for (boolean fetchSchema : Arrays.asList(true, false)) {
                ActionCreatePreparedStatementResult result = prepare("SELECT 1 || 2 AS x");
                CommandPreparedStatementQuery command = command(result);
                flightContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT);
                Assertions.assertEquals(new ArrowType.Utf8(), schema("SELECT 1 || 2 AS x")
                        .getFields().get(0).getType());
                FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class, () -> {
                    if (fetchSchema) {
                        producer.getSchemaPreparedStatement(command, callContext,
                                FlightDescriptor.command(new byte[0]));
                    } else {
                        producer.getFlightInfoPreparedStatement(command, callContext,
                                FlightDescriptor.command(new byte[0]));
                    }
                });
                Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
                Assertions.assertNull(flightContext.getPreparedQuery(preparedId(result)));
                flightContext.getSessionVariable().setSqlMode(0);
            }
        } finally {
            flightContext.getSessionVariable().setSqlMode(mode);
        }
    }

    @Test
    void preparedHandleSurvivesSettingsThatDoNotChangeSchema() throws Exception {
        int timeout = flightContext.getSessionVariable().getQueryTimeoutS();
        ActionCreatePreparedStatementResult result = prepare("SELECT 1 AS x");
        try {
            flightContext.getSessionVariable().setQueryTimeoutS(timeout + 1);
            Assertions.assertEquals(schema("SELECT 1 AS x"), producer.getSchemaPreparedStatement(
                    command(result), callContext, FlightDescriptor.command(new byte[0])).getSchema());
        } finally {
            flightContext.getSessionVariable().setQueryTimeoutS(timeout);
            flightContext.removePreparedQuery(preparedId(result));
        }
    }

    @Test
    void dialectParseFailureRetriesOriginalOnlyWhenEnabled() {
        SessionVariable session = flightContext.getSessionVariable();
        boolean retry = session.retryOriginSqlOnConvertFail;
        String query = "SELECT id FROM schema_input";
        try (MockedStatic<SqlDialectHelper> converter = Mockito.mockStatic(SqlDialectHelper.class)) {
            converter.when(() -> SqlDialectHelper.convertSqlByDialect(Mockito.eq(query), Mockito.any()))
                    .thenReturn("SELECT FROM");
            session.retryOriginSqlOnConvertFail = true;
            Assertions.assertEquals("id", schema(query).getFields().get(0).getName());
            Assertions.assertSame(session, flightContext.getSessionVariable());
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
    void procedureStatusExposesFixedHeader() throws Exception {
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
    void warmupSelectRunsUncheckedWhateverItsSourceHolds() throws Exception {
        // WARM UP SELECT answers OK when planning proves its source empty, and its statistics otherwise.
        List<String> statistics = Arrays.asList("BackendId", "ScanRows", "ScanBytes", "ScanBytesFromLocalStorage",
                "ScanBytesFromRemoteStorage", "BytesWriteIntoCache");
        ActionCreatePreparedStatementResult result = prepare("WARM UP SELECT * FROM schema_input");
        try {
            for (boolean emptySource : Arrays.asList(true, false)) {
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            Mockito.doAnswer(invocation -> {
                                flightContext.getState().reset();
                                FlightProtocolAdapter.of(flightContext).beforeStatement(flightContext);
                                if (!emptySource) {
                                    ShowResultSetMetaData.Builder header = ShowResultSetMetaData.builder();
                                    statistics.forEach(name -> header.addColumn(new Column(name, Type.STRING)));
                                    flightContext.setQueryId(new TUniqueId(48, 1));
                                    flightContext.getFlightSqlChannel().addResult(
                                            DebugUtil.printId(flightContext.queryId()), invocation.getArgument(0),
                                            new ShowResultSet(header.build(), Collections.emptyList()));
                                }
                                return null;
                            }).when(processor).handleQuery(Mockito.anyString());
                        })) {
                    Schema executed = producer.getFlightInfoPreparedStatement(command(result), callContext,
                            FlightDescriptor.command(new byte[0])).getSchema();
                    Assertions.assertEquals(emptySource ? Collections.singletonList("StatusResult") : statistics,
                            executed.getFields().stream().map(Field::getName).collect(Collectors.toList()));
                    Assertions.assertNotNull(flightContext.getPreparedQuery(preparedId(result)));
                } finally {
                    flightContext.getFlightSqlChannel().reset();
                }
            }
        } finally {
            flightContext.removePreparedQuery(preparedId(result));
        }
    }

    @Test
    void explainExposesHeadersWithoutRunningPlanner() throws Exception {
        Assertions.assertEquals("Explain String(Nereids Planner)",
                preparedSchema("EXPLAIN SELECT 1").getFields().get(0).getName());
        // The header StmtExecutor sends for the plan process, which Flight serializes as it does a SHOW.
        Assertions.assertEquals(Arrays.asList("Rule", "Before", "After"),
                preparedSchema("EXPLAIN PLAN PROCESS SELECT 1").getFields().stream()
                        .map(Field::getName).collect(Collectors.toList()));
    }

    @Test
    void preparedNamespaceTransitionIsReusable() throws Exception {
        String database = flightContext.getDatabase();
        try {
            for (String query : Arrays.asList("USE information_schema",
                    "USE information_schema; SELECT TABLE_NAME FROM tables WHERE 1=0")) {
                flightContext.setDatabase(database);
                Schema expected = schema(query);
                boolean remote = query.contains("SELECT");
                ActionCreatePreparedStatementResult result = prepare(query);
                CommandPreparedStatementQuery command = command(result);
                StmtExecutor executor = executorWithLabels("TABLE_NAME");
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            Mockito.doAnswer(invocation -> {
                                FlightProtocolAdapter adapter = FlightProtocolAdapter.of(flightContext);
                                flightContext.getState().reset();
                                flightContext.setDatabase("information_schema");
                                adapter.beforeStatement(flightContext);
                                if (remote) {
                                    adapter.beforeQuery(flightContext);
                                    flightContext.setExecutor(executor);
                                }
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
                    flightContext.setExecutor(null);
                    flightContext.getFlightSqlChannel().reset();
                    flightContext.removePreparedQuery(preparedId(result));
                }
            }
        } finally {
            flightContext.setDatabase(database);
        }
    }

    @Test
    void executionSchemaDriftExpiresHandleAndCleansResults() throws Exception {
        Schema changed = new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(64, true)),
                Field.nullable("new_column", new ArrowType.Utf8())));
        StmtExecutor previous = flightContext.getExecutor();
        try {
            for (boolean remote : Arrays.asList(true, false)) {
                ActionCreatePreparedStatementResult result = prepare("SELECT id FROM schema_input");
                CommandPreparedStatementQuery command = command(result);
                StmtExecutor executor = executorWithLabels("id", "new_column");
                FlightSqlConnectProcessor cleanup = new FlightSqlConnectProcessor(flightContext);
                try (MockedConstruction<FlightSqlConnectProcessor> processors = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, context) -> {
                            // Simulate replanning after another session changes the table, after schema reanalysis.
                            Mockito.doAnswer(invocation -> {
                                FlightProtocolAdapter adapter = FlightProtocolAdapter.of(flightContext);
                                flightContext.getState().reset();
                                adapter.beforeStatement(flightContext);
                                if (remote) {
                                    adapter.beforeQuery(flightContext);
                                }
                                flightContext.setExecutor(executor);
                                flightContext.addFlightSqlDeferredExecutor(executor);
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
                    Assertions.assertNull(flightContext.getExecutor());
                    Mockito.verify(processors.constructed().get(0)).close();
                    InOrder cleanupOrder = Mockito.inOrder(executor);
                    cleanupOrder.verify(executor).cancel(Mockito.any(Status.class));
                    cleanupOrder.verify(executor).finalizeArrowFlightQuery();
                    Assertions.assertEquals(0, flightContext.getFlightSqlChannel().resultNum());
                    Assertions.assertTrue(flightContext.getFlightSqlEndpointsLocations().isEmpty());
                    Assertions.assertNull(flightContext.getPreparedQuery(preparedId(result)));
                    flightContext.closeFlightSqlDeferredExecutors();
                    Mockito.verify(executor, Mockito.times(1)).finalizeArrowFlightQuery();
                } finally {
                    flightContext.closeFlightSqlDeferredExecutors();
                    flightContext.getFlightSqlChannel().reset();
                    flightContext.removePreparedQuery(preparedId(result));
                }
            }
        } finally {
            flightContext.setExecutor(previous);
        }
    }
}
