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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FlightSqlPreparedQueryTest extends TestWithFeService {
    private RootAllocator allocator;
    private FlightServer server;
    private FlightClient client;
    private FlightSqlClient sqlClient;
    private DorisFlightSqlProducer producer;
    private FlightSqlConnectContext flightContext;

    @Override
    protected void runBeforeAll() throws Exception {
        allocator = new RootAllocator();
        flightContext = new FlightSqlConnectContext("parameter-peer");
        flightContext.setEnv(connectContext.getEnv());
        flightContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        flightContext.setSessionVariable(connectContext.getSessionVariable());
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessions.getConnectContext("parameter-peer")).thenReturn(flightContext);
        producer = new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 0), sessions);
        server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer)
                .headerAuthenticator(headers -> new CallHeaderAuthenticator.AuthResult() {
                    @Override
                    public String getPeerIdentity() {
                        return "parameter-peer";
                    }
                }).build().start();
        client = FlightClient.builder(allocator, server.getLocation()).build();
        sqlClient = new FlightSqlClient(client);
    }

    @Override
    protected void runAfterAll() throws Exception {
        sqlClient.close();
        server.close();
        Field executor = DorisFlightSqlProducer.class.getDeclaredField("executorService");
        executor.setAccessible(true);
        ((ExecutorService) executor.get(producer)).shutdownNow();
        producer.close();
        flightContext.getFlightSqlChannel().close();
        allocator.close();
    }

    private CommandPreparedStatementQuery prepare(String sql) throws Exception {
        Action action = new Action("CreatePreparedStatement", Any.pack(
                ActionCreatePreparedStatementRequest.newBuilder().setQuery(sql).build()).toByteArray());
        ActionCreatePreparedStatementResult prepared = Any.parseFrom(client.doAction(action).next().getBody())
                .unpack(ActionCreatePreparedStatementResult.class);
        Assertions.assertTrue(prepared.getDatasetSchema().isEmpty());
        return CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(prepared.getPreparedStatementHandle()).build();
    }

    private void bind(CommandPreparedStatementQuery command, VectorSchemaRoot root) {
        AsyncPutListener listener = new AsyncPutListener();
        FlightClient.ClientStreamListener writer = client.startPut(
                FlightDescriptor.command(Any.pack(command).toByteArray()), root, listener);
        writer.putNext();
        writer.completed();
        listener.getResult();
    }

    private String id(CommandPreparedStatementQuery command) {
        return command.getPreparedStatementHandle().toStringUtf8().substring("parameter-peer:".length());
    }

    private Schema schema(CommandPreparedStatementQuery command) {
        return client.getSchema(FlightDescriptor.command(Any.pack(command).toByteArray())).getSchema();
    }

    @Test
    public void bindsAndRebindsIntegerQueryOverFlight() throws Exception {
        CommandPreparedStatementQuery command = prepare("SELECT CAST(? AS BIGINT) AS value");
        try (BigIntVector value = new BigIntVector("parameter", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(value)) {
            for (long expected : new long[] {42, -7, Long.MAX_VALUE}) {
                value.setSafe(0, expected);
                root.setRowCount(1);
                bind(command, root);
                Assertions.assertEquals(new ArrowType.Int(64, true), schema(command).getFields().get(0).getType());
                Assertions.assertEquals(expected, flightContext.getPreparedQueryParameters(id(command)).get(0).getValue());
            }
        } finally {
            flightContext.removePreparedQuery(id(command));
        }
    }

    @Test
    public void bindsMixedTypesAndPreservesQuotedText() throws Exception {
        String query = "SELECT CAST(? AS BIGINT) AS n, CAST(? AS STRING) AS text, CAST(? AS DOUBLE) AS fraction";
        CommandPreparedStatementQuery command = prepare(query);
        try (BigIntVector n = new BigIntVector("n", allocator);
                VarCharVector text = new VarCharVector("text", allocator);
                Float8Vector fraction = new Float8Vector("fraction", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(n, text, fraction)) {
            String expected = "中文 ' ? \\ ; SELECT 2";
            n.setSafe(0, 17);
            text.setSafe(0, expected.getBytes(StandardCharsets.UTF_8));
            fraction.setSafe(0, 2.5);
            root.setRowCount(1);
            bind(command, root);
            List<Literal> parameters = flightContext.getPreparedQueryParameters(id(command));
            Assertions.assertEquals(17L, parameters.get(0).getValue());
            Assertions.assertEquals(expected, parameters.get(1).getValue());
            Assertions.assertEquals(2.5, parameters.get(2).getValue());
            Assertions.assertEquals(3, schema(command).getFields().size());
            // Exercise the execution parser hook without requiring a BE in the FE unit test fixture.
            try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(flightContext)) {
                Field bindings = FlightSqlConnectProcessor.class.getDeclaredField("parameters");
                bindings.setAccessible(true);
                bindings.set(processor, parameters);
                List<StatementBase> statements = processor.parseWithFallback(query, query,
                        flightContext.getSessionVariable());
                StatementContext statement = ((LogicalPlanAdapter) statements.get(0))
                        .getStatementContext();
                try {
                    for (int i = 0; i < parameters.size(); i++) {
                        Assertions.assertEquals(parameters.get(i), statement.getIdToPlaceholderRealExpr().get(
                                statement.getPlaceholders().get(i).getPlaceholderId()));
                    }
                } finally {
                    statement.close();
                }
            }
        } finally {
            flightContext.removePreparedQuery(id(command));
        }
    }

    @Test
    public void failedRebindInvalidatesOldValuesAndConnectionRecovers() throws Exception {
        CommandPreparedStatementQuery command = prepare("SELECT ? AS value");
        try (IntVector value = new IntVector("value", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(value)) {
            value.setSafe(0, 1);
            root.setRowCount(1);
            bind(command, root);
            value.setSafe(1, 2);
            root.setRowCount(2);
            FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> bind(command, root));
            Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, failure.status().code());
            Assertions.assertNull(flightContext.getPreparedQueryParameters(id(command)));
            Assertions.assertThrows(FlightRuntimeException.class, () -> schema(command));
            root.setRowCount(1);
            value.setSafe(0, 3);
            bind(command, root);
            Assertions.assertEquals(new ArrowType.Int(32, true), schema(command).getFields().get(0).getType());
            value.setNull(0);
            bind(command, root);
            Assertions.assertTrue(flightContext.getPreparedQueryParameters(id(command)).get(0) instanceof NullLiteral);
        } finally {
            flightContext.removePreparedQuery(id(command));
        }
        Assertions.assertNotNull(sqlClient.getExecuteSchema("SELECT 1"));
    }

    @Test
    public void convertsDetachedValuesAndTypedNulls() {
        try (IntVector value = new IntVector("value", allocator);
                VarCharVector text = new VarCharVector("text", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(value, text)) {
            value.setSafe(0, 12);
            text.setNull(0);
            root.setRowCount(1);
            List<Literal> literals = FlightSqlParameters.convert(root);
            value.setSafe(0, 99);
            Assertions.assertEquals(12, literals.get(0).getValue());
            Assertions.assertTrue(literals.get(1) instanceof NullLiteral);
            Assertions.assertEquals(StringType.INSTANCE, literals.get(1).getDataType());
        }
    }

    @Test
    public void validatesWholeUploadInsteadOfUsingFirstBatch() {
        try (IntVector value = new IntVector("value", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(value)) {
            value.setSafe(0, 1);
            root.setRowCount(1);
            FlightStream stream = Mockito.mock(FlightStream.class);
            Mockito.when(stream.next()).thenReturn(true, true, false);
            Mockito.when(stream.getRoot()).thenReturn(root);
            Assertions.assertThrows(Exception.class, () -> FlightSqlParameters.read(stream, 1));
            Mockito.verify(stream, Mockito.times(3)).next();
            FlightStream empty = Mockito.mock(FlightStream.class);
            Assertions.assertThrows(Exception.class, () -> FlightSqlParameters.read(empty, 1));
            Assertions.assertEquals(Arrays.asList(), FlightSqlParameters.read(empty, 0));
        }
    }

    @Test
    public void limitsRetainedParametersAcrossHandlesAndReleasesTheirBudget() {
        ConnectContext connection = new ConnectContext();
        List<Literal> parameters = Arrays.asList(
                new StringLiteral("x".repeat(512 * 1024)));
        for (int i = 0; i < 15; i++) {
            connection.addPreparedQuery("p" + i, "SELECT ?", null, 1);
            connection.setPreparedQueryParameters("p" + i, parameters, null);
        }
        connection.addPreparedQuery("overflow", "SELECT ?", null, 1);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> connection.setPreparedQueryParameters("overflow", parameters, null));
        connection.removePreparedQuery("p0");
        connection.setPreparedQueryParameters("overflow", parameters, null);
        connection.beginPreparedQueryBinding("overflow");
        connection.addPreparedQuery("replacement", "SELECT ?", null, 1);
        connection.setPreparedQueryParameters("replacement", parameters, null);
        long upload = connection.beginPreparedQueryBinding("replacement");
        connection.clearPreparedQueries();
        Assertions.assertFalse(connection.isPreparedQueryBindingCurrent("replacement", upload));
        Assertions.assertNull(connection.getPreparedQuery("replacement"));
        connection.addPreparedQuery("fresh", "SELECT ?", null, 1);
        connection.setPreparedQueryParameters("fresh", parameters, null);
    }

    @Test
    public void rejectsInvalidValuesAndUnsupportedTypes() {
        try (VarCharVector text = new VarCharVector("text", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(text)) {
            text.setSafe(0, new byte[] {(byte) 0xc3, (byte) 0x28});
            root.setRowCount(1);
            Assertions.assertThrows(FlightRuntimeException.class, () -> FlightSqlParameters.convert(root));
        }
        try (Float8Vector number = new Float8Vector("number", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(number)) {
            number.setSafe(0, Double.POSITIVE_INFINITY);
            root.setRowCount(1);
            Assertions.assertThrows(FlightRuntimeException.class, () -> FlightSqlParameters.convert(root));
        }
        try (VarBinaryVector binary =
                new VarBinaryVector("binary", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(binary)) {
            binary.setSafe(0, new byte[] {0, 1});
            root.setRowCount(1);
            FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> FlightSqlParameters.convert(root));
            Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, error.status().code());
        }
    }

    @Test
    public void rejectsForwardingInsteadOfLosingTypedBindings() throws Exception {
        flightContext.setThreadLocalInfo();
        LogicalPlanAdapter statement = (LogicalPlanAdapter) new NereidsParser()
                .parseSQL("SELECT ?", flightContext.getSessionVariable()).get(0);
        try {
            FlightSqlParameters.bind(statement.getStatementContext(), Arrays.asList(
                    new BigIntLiteral(7)));
            StmtExecutor executor = new StmtExecutor(flightContext, statement);
            Method forward = StmtExecutor.class
                    .getDeclaredMethod("forwardToMaster");
            forward.setAccessible(true);
            InvocationTargetException failure = Assertions.assertThrows(
                    InvocationTargetException.class, () -> forward.invoke(executor));
            Assertions.assertTrue(failure.getCause().getMessage().contains("connect to master FE"));
        } finally {
            statement.getStatementContext().close();
            ConnectContext.remove();
        }
    }

    @Test
    public void preservesDecimalAndTemporalPrecision() {
        try (DecimalVector decimal =
                new DecimalVector("decimal", allocator, 20, 6);
                DateDayVector date = new DateDayVector("date", allocator);
                TimeStampMicroVector timestamp =
                        new TimeStampMicroVector("timestamp", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(decimal, date, timestamp)) {
            BigDecimal expected = new BigDecimal("12345678901234.567890");
            decimal.setSafe(0, expected);
            date.setSafe(0, -1);
            timestamp.setSafe(0, -1);
            root.setRowCount(1);
            List<Literal> values = FlightSqlParameters.convert(root);
            Assertions.assertEquals(expected, values.get(0).getValue());
            Assertions.assertEquals("1969-12-31", values.get(1).getStringValue());
            Assertions.assertEquals("1969-12-31 23:59:59.999999", values.get(2).getStringValue());
            date.setSafe(0, Integer.MAX_VALUE);
            Assertions.assertThrows(FlightRuntimeException.class, () -> FlightSqlParameters.convert(root));
        }
    }

    @Test
    public void rejectsForeignAndClosedHandles() throws Exception {
        CommandPreparedStatementQuery command = prepare("SELECT ?");
        CommandPreparedStatementQuery foreign = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8("another-peer:handle")).build();
        FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class, () -> schema(foreign));
        Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT, failure.status().code());
        ActionClosePreparedStatementRequest close =
                ActionClosePreparedStatementRequest.newBuilder()
                        .setPreparedStatementHandle(command.getPreparedStatementHandle()).build();
        client.doAction(new Action("ClosePreparedStatement", Any.pack(close).toByteArray())).forEachRemaining(r -> { });
        FlightRuntimeException closed = Assertions.assertThrows(FlightRuntimeException.class, () -> schema(command));
        Assertions.assertEquals(FlightStatusCode.NOT_FOUND, closed.status().code());
    }

    @Test
    public void bindsRangePredicatesAndRejectsWrongParameterCount() throws Exception {
        CommandPreparedStatementQuery command = prepare("SELECT number FROM numbers(\"number\"=\"10\") "
                + "WHERE number >= ? AND number < ? ORDER BY number");
        try (BigIntVector lower = new BigIntVector("lower", allocator);
                BigIntVector upper = new BigIntVector("upper", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(lower, upper)) {
            lower.setSafe(0, 2);
            upper.setSafe(0, 5);
            root.setRowCount(1);
            bind(command, root);
            Assertions.assertEquals(new ArrowType.Int(64, true), schema(command).getFields().get(0).getType());
        }
        try (IntVector only = new IntVector("only", allocator);
                VectorSchemaRoot wrong = VectorSchemaRoot.of(only)) {
            only.setSafe(0, 1);
            wrong.setRowCount(1);
            FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> bind(command, wrong));
            Assertions.assertEquals(FlightStatusCode.INVALID_ARGUMENT, error.status().code());
            Assertions.assertNull(flightContext.getPreparedQueryParameters(id(command)));
        } finally {
            flightContext.removePreparedQuery(id(command));
        }
    }

    @Test
    public void sessionCloseDoesNotWaitForExecutionMonitor() throws Exception {
        ConnectContext connection = new ConnectContext();
        connection.addPreparedQuery("p", "SELECT ?", null, 1);
        long upload = connection.beginPreparedQueryBinding("p");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            synchronized (connection) {
                executor.submit(connection::closePreparedQueries).get(5, TimeUnit.SECONDS);
            }
            Assertions.assertFalse(connection.isPreparedQueryBindingCurrent("p", upload));
            Assertions.assertThrows(IllegalStateException.class,
                    () -> connection.addPreparedQuery("new", "SELECT ?", null, 1));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void closedHandleCannotCommitAnUploadOrLeaveUnreachableResults() throws Exception {
        ConnectContext connection = new ConnectContext();
        connection.addPreparedQuery("p", "SELECT ?", null, 1);
        long version = connection.beginPreparedQueryBinding("p");
        connection.closePreparedQueries();
        Assertions.assertFalse(connection.setPreparedQueryParameters("p", Arrays.asList(new BigIntLiteral(1)),
                null, version));
        Assertions.assertEquals(-1, connection.getPreparedQueryParameterCount("p"));
        Assertions.assertNull(connection.getPreparedQueryParameters("p"));
        Assertions.assertEquals(-1, connection.beginPreparedQueryBinding("p"));
        String query = "SET query_timeout = 300";
        flightContext.getState().reset();
        flightContext.addPreparedQuery("close-race", query, FlightSqlQuerySchema.analyze(flightContext, query));
        CallContext call = Mockito.mock(CallContext.class);
        Mockito.when(call.peerIdentity()).thenReturn("parameter-peer");
        CommandPreparedStatementQuery command = CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8("parameter-peer:close-race")).build();
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        boolean previousLocal = flightContext.isReturnResultFromLocal();
        try (MockedConstruction<FlightSqlConnectProcessor> ignored = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (processor, context) -> {
                    Mockito.doAnswer(invocation -> {
                        flightContext.clearPreparedQueries();
                        flightContext.setReturnResultFromLocal(true);
                        flightContext.addFlightSqlDeferredExecutor(deferred);
                        return null;
                    }).when(processor).handleQuery(Mockito.anyString());
                })) {
            FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoPreparedStatement(
                            command, call, FlightDescriptor.command(Any.pack(command).toByteArray())));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, error.status().code());
            Assertions.assertEquals(0, flightContext.getFlightSqlChannel().resultNum());
            Mockito.verify(deferred).finalizeArrowFlightQuery();
        } finally {
            flightContext.setReturnResultFromLocal(previousLocal);
            flightContext.closeFlightSqlDeferredExecutors();
            flightContext.getFlightSqlChannel().reset();
        }
    }

}
