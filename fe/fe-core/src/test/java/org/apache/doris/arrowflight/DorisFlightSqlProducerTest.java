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
import org.apache.doris.arrowflight.auth2.FlightAuthResult;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.results.FlightSqlChannel;
import org.apache.doris.arrowflight.sessions.FlightSessionsInConnectPool;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.IncrWindowNotReadyException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.trees.plans.commands.utils.KillUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectPoolTestSupport;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.GetSessionOptionsResult;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.SetSessionOptionsResult.ErrorValue;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DorisFlightSqlProducerTest {

    private boolean prevRunningUnitTest;

    @Test
    public void testWindowNotReadyHasRetryableFlightStatusAndStableBusinessCode() {
        QueryState state = new QueryState();
        IncrWindowNotReadyException error = new IncrWindowNotReadyException(2000, 100, 1000);
        state.setError(error.getMysqlErrorCode(), error.getDetailMessage());
        FlightRuntimeException result = DorisFlightSqlProducer.queryFailure(state, state.getErrorMessage(), error);
        Assertions.assertEquals(FlightStatusCode.UNAVAILABLE, result.status().code());
        Assertions.assertEquals(Integer.toString(ErrorCode.ERR_INCR_WINDOW_NOT_READY.getCode()),
                result.status().metadata().get("doris-error-code"));
        Assertions.assertTrue(result.status().description().contains("requestedEndTimestampMs=2000"));
        Assertions.assertTrue(result.status().description().contains("retryAfterMs=1000"));
        state.setError(ErrorCode.ERR_UNKNOWN_ERROR, "other failure");
        Assertions.assertEquals(FlightStatusCode.INTERNAL,
                DorisFlightSqlProducer.queryFailure(state, "other failure", error).status().code());
    }

    @Test
    public void testGetFlightInfoPreservesBothWindowErrors() throws Exception {
        for (ErrorCode code : new ErrorCode[] {ErrorCode.ERR_INCR_WINDOW_NOT_READY,
                ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT}) {
            QueryState state = new QueryState();
            IncrWindowNotReadyException error = new IncrWindowNotReadyException(code, "test reason",
                    2000, 3000, 100, 1000, 5000);
            state.setError(error.getMysqlErrorCode(), error.getDetailMessage());
            FlightRuntimeException failure = DorisFlightSqlProducer.queryFailure(state, state.getErrorMessage(), error);
            Assertions.assertEquals(FlightStatusCode.UNAVAILABLE, failure.status().code());
            Assertions.assertEquals(Integer.toString(code.getCode()), failure.status().metadata().get("doris-error-code"));
            Assertions.assertEquals(code.name(), failure.status().metadata().get("doris-error-name"));
            Assertions.assertTrue(failure.status().description().contains("currentTSO=3000"));
            Assertions.assertTrue(failure.status().description().contains("committedTSO=100"));
            Assertions.assertTrue(failure.status().description().contains("timeoutMs=5000"));
            Assertions.assertSame(failure, getFlightInfoFailure(failure));
        }
    }

    // A Flight status chosen by the session layer reaches the client as is, whatever it is: the
    // session's execution owner (UNAVAILABLE), a closed session (UNAUTHENTICATED), a refused session
    // (RESOURCE_EXHAUSTED). Only a non-Flight failure is wrapped as INTERNAL.
    @Test
    public void testGetFlightInfoPassesOtherFlightErrorsThrough() throws Exception {
        for (CallStatus status : new CallStatus[] {CallStatus.INTERNAL, CallStatus.UNAVAILABLE,
                CallStatus.INVALID_ARGUMENT, CallStatus.UNAUTHENTICATED, CallStatus.RESOURCE_EXHAUSTED}) {
            FlightRuntimeException failure = status.withDescription("other flight failure").toRuntimeException();
            Assertions.assertSame(failure, getFlightInfoFailure(failure));
        }
    }

    @Test
    public void testGetFlightInfoPassesOtherBusinessErrorsThrough() throws Exception {
        ErrorFlightMetadata metadata = new ErrorFlightMetadata();
        metadata.insert("doris-error-code", Integer.toString(ErrorCode.ERR_UNKNOWN_ERROR.getCode()));
        FlightRuntimeException failure = CallStatus.UNAVAILABLE.withDescription("other business failure")
                .withMetadata(metadata).toRuntimeException();

        Assertions.assertSame(failure, getFlightInfoFailure(failure));
    }

    @Test
    public void testGetFlightInfoWrapsNonFlightErrors() throws Exception {
        RuntimeException failure = new RuntimeException("session lookup failed");
        FlightRuntimeException result = getFlightInfoFailure(failure);
        Assertions.assertEquals(FlightStatusCode.INTERNAL, result.status().code());
        Assertions.assertSame(failure, result.getCause());
        Assertions.assertEquals("get flight info statement failed, " + failure.getMessage(),
                result.status().description());
        Assertions.assertFalse(result.status().metadata().containsKey("doris-error-code"));
    }

    private FlightRuntimeException getFlightInfoFailure(RuntimeException failure) throws Exception {
        FlightSessionsManager sessionsManager = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessionsManager.getConnectContext("token")).thenThrow(failure);
        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("token");
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsManager)) {
            CommandStatementQuery request = CommandStatementQuery.newBuilder().setQuery("select 1").build();
            return Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoStatement(request, callContext, FlightDescriptor.command(new byte[0])));
        }
    }

    @BeforeEach
    public void setUp() {
        // ConnectContext.init() only reaches Env when this is false; keep it true so the
        // context can be built without a running FE.
        prevRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = prevRunningUnitTest;
    }

    /**
     * Regression test for the FE direct-memory leak in {@code createPreparedStatement}
     * (issue apache/doris#65305, fixed by PR #65311).
     *
     * <p>Before that fix, each prepare allocated a {@link org.apache.arrow.vector.VectorSchemaRoot}
     * via {@code FlightSqlChannel.createOneOneSchemaRoot("ResultMeta", ...)} and read only its
     * {@code Schema}, never closing the root, leaking one off-heap {@code VarCharVector} buffer per
     * prepare. Because Arrow Flight has no parameter binding, every client query triggers a fresh
     * prepare, so the leak grows monotonically until {@code MaxDirectMemorySize} is exhausted.
     *
     * <p>Prepare now advertises the analyzed result schema; that {@code ResultMeta} placeholder is left
     * for a statement whose schema only running it can tell, such as a request that sets a variable
     * before its query. This test drives {@code createPreparedStatement} for such statements against a
     * real {@link FlightSqlChannel} many times and asserts that each one gets the placeholder and the
     * channel's Arrow allocator holds zero bytes afterwards.
     */
    @Test
    public void createPreparedStatementDoesNotLeakChannelAllocator() throws Exception {
        // A real flight session context owns a real FlightSqlChannel (and thus a real Arrow allocator),
        // so allocator bookkeeping is exercised for real instead of mocked away.
        ConnectContext connectContext = ConnectContext.forFlight("test-peer-identity");
        FlightSqlChannel channel = connectContext.getFlightSqlChannel();
        Assertions.assertEquals(0L, channel.getAllocatedMemory(), "channel allocator should start empty");

        FlightSessionsManager sessionsManager = new FlightSessionsManager() {
            @Override
            public String openSession(FlightAuthResult authResult) {
                throw new UnsupportedOperationException("not exercised by this test");
            }

            @Override
            public ConnectContext getConnectContext(String peerIdentity) {
                return connectContext;
            }

            @Override
            public void closeConnectContext(String peerIdentity) {
                // not exercised by this test
            }
        };
        DorisFlightSqlProducer producer =
                new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 9090), sessionsManager);

        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("test-peer-identity");

        final int rounds = 100;
        AtomicInteger errors = new AtomicInteger(0);
        try {
            // createPreparedStatement runs asynchronously and mutates a shared, non-thread-safe
            // ConnectContext, so drive it serially: each prepare completes before the next is issued.
            // The leak, if any, still accumulates on the single channel allocator across rounds.
            for (int i = 0; i < rounds; i++) {
                CountDownLatch finished = new CountDownLatch(1);
                StreamListener<Result> listener = new StreamListener<Result>() {
                    @Override
                    public void onNext(Result val) {
                        try {
                            ByteString dataset = Any.parseFrom(val.getBody())
                                    .unpack(ActionCreatePreparedStatementResult.class).getDatasetSchema();
                            Schema schema = MessageSerializer.deserializeSchema(new ReadChannel(
                                    Channels.newChannel(new ByteArrayInputStream(dataset.toByteArray()))));
                            if (!"ResultMeta".equals(schema.getFields().get(0).getName())) {
                                errors.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        errors.incrementAndGet();
                        finished.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        finished.countDown();
                    }
                };
                ActionCreatePreparedStatementRequest request = ActionCreatePreparedStatementRequest.newBuilder()
                        .setQuery("set @id = " + i + "; select * from t where id = @id").build();
                producer.createPreparedStatement(request, callContext, listener);
                Assertions.assertTrue(finished.await(30, TimeUnit.SECONDS), "createPreparedStatement #" + i + " did not finish in time");
            }

            // Guard against a false pass: if a prepare failed before reaching the placeholder, no buffer
            // would be leaked and the memory assertion below could not detect a regression.
            Assertions.assertEquals(0, errors.get(),
                    "every createPreparedStatement call should answer with the placeholder schema");
            // Every temporary VectorSchemaRoot must have been closed, so the channel's Arrow allocator
            // is back to zero. Reverting the fix leaves `rounds` ResultMeta buffers allocated here.
            Assertions.assertEquals(0L, channel.getAllocatedMemory(), "createPreparedStatement leaked off-heap memory in the channel allocator");
        } finally {
            producer.close();
        }
    }

    @Test
    public void testSelfKillConnectionKeepsItsNormalResultTicketUntilDoGet() throws Exception {
        verifySelfKillResponse(true);
    }

    @Test
    public void testSelfKillQueryPreservesTheSessionAndResultTicket() throws Exception {
        verifySelfKillResponse(false);
    }

    private void verifySelfKillResponse(boolean killConnection) throws Exception {
        ConnectContext ctx = ConnectPoolTestSupport.flightSession(ConnectPoolTestSupport.envAllowing(100),
                UserIdentity.ROOT, "self-kill-token");
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, ctx.registerClientSession(scheduler.getConnectPoolMgr()));
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx));
        try (MockedConstruction<FlightSqlConnectProcessor> mocked = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (mock, construction) -> {
                    Mockito.doAnswer(call -> {
                        ctx.setExecutor(running);
                        KillUtils.killByConnectionId(ctx, killConnection, ctx.getConnectionId());
                        Assertions.assertEquals(killConnection, ctx.isClosingOrClosed());
                        return null;
                    }).when(mock).handleQuery(Mockito.anyString());
                })) {
            CommandStatementQuery request = CommandStatementQuery.newBuilder().setQuery(
                    "KILL " + (killConnection ? "CONNECTION " : "QUERY ") + ctx.getConnectionId()).build();
            FlightInfo info = producer.getFlightInfoStatement(request, callOf("self-kill-token"),
                    FlightDescriptor.command(new byte[0]));
            Assertions.assertEquals(new Schema(Collections.singletonList(
                    Field.nullable("StatusResult", new ArrowType.Utf8()))), info.getSchema());
            Assertions.assertEquals(1, info.getEndpoints().size());
            Assertions.assertEquals(-1, info.getRecords());
            Assertions.assertEquals(-1, info.getBytes());
            Assertions.assertSame(ctx, scheduler.getContextWithPeerIdentity("self-kill-token"));
            Assertions.assertTrue(ctx.getFlightSqlChannel().getAllocatedMemory() > 0);
            if (killConnection) {
                Assertions.assertTrue(ctx.isClosingAfterResponse());
                FlightInfo decoded = FlightInfo.deserialize(info.serialize());
                Assertions.assertEquals(info.getSchema(), decoded.getSchema());
                Assertions.assertEquals(1, decoded.getEndpoints().size());
                Mockito.verify(running, Mockito.never()).cancel(Mockito.any(org.apache.doris.common.Status.class));
                FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                        () -> producer.getFlightInfoStatement(request, callOf("self-kill-token"),
                                FlightDescriptor.command(new byte[0])));
                Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, error.status().code());
            }
            Assertions.assertEquals(Collections.singletonList("0"), consume(producer, info));
            Assertions.assertEquals(0, ctx.getFlightSqlChannel().getAllocatedMemory());
            if (killConnection) {
                Assertions.assertNull(scheduler.getContextWithPeerIdentity("self-kill-token"));
            } else {
                Assertions.assertEquals("next command", FlightProtocolAdapter.of(ctx)
                        .callCommand(ctx, () -> "next command"));
                Assertions.assertSame(ctx, scheduler.getContextWithPeerIdentity("self-kill-token"));
                Mockito.verify(running).cancel(Mockito.any(org.apache.doris.common.Status.class));
            }
        } finally {
            ctx.cleanup();
            producer.close();
        }
    }

    private static List<String> consume(DorisFlightSqlProducer producer, FlightInfo info) throws Exception {
        TicketStatementQuery ticket = Any.parseFrom(info.getEndpoints().get(0).getTicket().getBytes())
                .unpack(TicketStatementQuery.class);
        List<String> rows = new ArrayList<>();
        AtomicReference<VectorSchemaRoot> root = new AtomicReference<>();
        ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
        Mockito.doAnswer(invocation -> {
            root.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(Mockito.any(VectorSchemaRoot.class));
        Mockito.doAnswer(invocation -> {
            for (int i = 0; i < root.get().getRowCount(); i++) {
                rows.add(root.get().getVector(0).getObject(i).toString());
            }
            return null;
        }).when(listener).putNext();
        producer.getStreamStatement(ticket, callOf("reader"), listener);
        Mockito.verify(listener).completed();
        Mockito.verify(listener, Mockito.never()).error(Mockito.any(Throwable.class));
        return rows;
    }

    @Test
    public void testAfterResponsePreservesActualSchemaAndRowsUntilDoGet() throws Exception {
        ConnectContext ctx = ConnectPoolTestSupport.flightSession(ConnectPoolTestSupport.envAllowing(100),
                UserIdentity.ROOT, "rows-token");
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, ctx.registerClientSession(scheduler.getConnectPoolMgr()));
        ShowResultSet rows = new ShowResultSet(ShowResultSetMetaData.builder()
                .addColumn(new Column("payload", ScalarType.createVarchar(20))).build(),
                Collections.singletonList(Collections.singletonList("kept value")));
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), new FlightSessionsInConnectPool(scheduler));
                MockedConstruction<FlightSqlConnectProcessor> ignored = Mockito.mockConstruction(
                        FlightSqlConnectProcessor.class, (processor, construction) -> {
                            Mockito.doAnswer(invocation -> {
                                ctx.setQueryId(new TUniqueId(5, 7));
                                ctx.getFlightSqlChannel().addResult(DebugUtil.printId(ctx.queryId()), "SHOW", rows);
                                ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
                                return null;
                            }).when(processor).handleQuery(Mockito.anyString());
                        })) {
            FlightInfo info = producer.getFlightInfoStatement(CommandStatementQuery.newBuilder()
                    .setQuery("SHOW").build(), callOf("rows-token"), FlightDescriptor.command(new byte[0]));
            Assertions.assertEquals(new Schema(Collections.singletonList(
                    Field.nullable("payload", new ArrowType.Utf8()))), info.getSchema());
            Assertions.assertEquals(1, info.getEndpoints().size());
            Assertions.assertEquals(-1, info.getRecords());
            Assertions.assertEquals(-1, info.getBytes());
            Assertions.assertSame(ctx, scheduler.getContextWithPeerIdentity("rows-token"));
            Assertions.assertTrue(ctx.getFlightSqlChannel().getAllocatedMemory() > 0);
            Assertions.assertEquals(Collections.singletonList("kept value"), consume(producer, info));
            Assertions.assertEquals(0, ctx.getFlightSqlChannel().getAllocatedMemory());
            Assertions.assertNull(scheduler.getContextWithPeerIdentity("rows-token"));
        } finally {
            ctx.cleanup();
        }
    }

    @Test
    public void testInvalidAndRepeatedTicketsDoNotConsumeAnotherResult() throws Exception {
        ConnectContext ctx = ConnectContext.forFlight("result-token");
        FlightSqlChannel channel = ctx.getFlightSqlChannel();
        channel.addOKResult("first", "SELECT 1");
        channel.addOKResult("second", "SELECT 2");
        ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx))) {
            ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
            FlightRuntimeException absent = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getStreamStatement(ticket("result-token", "missing"), callOf("reader"), listener));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, absent.status().code());
            Assertions.assertEquals(2, channel.resultNum());
            Mockito.verifyNoInteractions(listener);

            producer.getStreamStatement(ticket("result-token", "first"), callOf("reader"), listener);
            Assertions.assertEquals(1, channel.resultNum());
            FlightRuntimeException duplicate = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getStreamStatement(ticket("result-token", "first"), callOf("reader"), listener));
            Assertions.assertEquals(FlightStatusCode.NOT_FOUND, duplicate.status().code());
            Assertions.assertNotNull(channel.getResult("second"));
            Assertions.assertTrue(channel.getAllocatedMemory() > 0);
            producer.getStreamStatement(ticket("result-token", "second"), callOf("reader"), listener);
            Assertions.assertEquals(0, channel.getAllocatedMemory());
        } finally {
            ctx.cleanup();
        }
    }

    @Test
    public void testDoGetDiscardsItsResultEvenWhenListenerCallbacksThrow() throws Exception {
        for (String failingCallback : new String[] {"start", "putNext", "error", "completed"}) {
            ConnectContext ctx = ConnectContext.forFlight("result-token");
            FlightSqlChannel channel = ctx.getFlightSqlChannel();
            channel.addOKResult("first", "SELECT 1");
            channel.addOKResult("second", "SELECT 2");
            ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
            try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                    Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx))) {
                ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
                IllegalStateException failure = new IllegalStateException("listener " + failingCallback);
                switch (failingCallback) {
                    case "start":
                        Mockito.doThrow(failure).when(listener).start(Mockito.any(VectorSchemaRoot.class));
                        break;
                    case "putNext":
                        Mockito.doThrow(failure).when(listener).putNext();
                        break;
                    case "error":
                        Mockito.doThrow(failure).when(listener).putNext();
                        Mockito.doThrow(failure).when(listener).error(Mockito.any(Throwable.class));
                        break;
                    case "completed":
                        Mockito.doThrow(failure).when(listener).completed();
                        break;
                    default:
                        throw new AssertionError(failingCallback);
                }
                Assertions.assertThrows(RuntimeException.class, () -> producer.getStreamStatement(
                        ticket("result-token", "first"), callOf("reader"), listener));
                Assertions.assertNull(channel.getResult("first"), failingCallback);
                Assertions.assertNotNull(channel.getResult("second"), failingCallback);
                Assertions.assertEquals(1, channel.resultNum(), failingCallback);
                FlightProtocolAdapter.of(ctx).runResult(ctx, () -> channel.invalidate("second"));
                Assertions.assertEquals(0, channel.getAllocatedMemory(), failingCallback);
            } finally {
                ctx.cleanup();
            }
        }
    }

    @Test
    public void testStaticMetadataDoesNotWaitForCommandOrRenewSession() throws Exception {
        ConnectContext ctx = Mockito.spy(ConnectContext.forFlight("metadata-token"));
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx));
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> owner = workers.submit(() -> {
                ctx.runCommand(() -> {
                    entered.countDown();
                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                });
                return null;
            });
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<?> metadata = workers.submit(() -> {
                FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
                Assertions.assertEquals(1, producer.getFlightInfoCatalogs(CommandGetCatalogs.getDefaultInstance(),
                        callOf("metadata-token"), descriptor).getEndpoints().size());
                Assertions.assertEquals(1, producer.getFlightInfoSqlInfo(CommandGetSqlInfo.getDefaultInstance(),
                        callOf("metadata-token"), descriptor).getEndpoints().size());
                ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
                producer.getStreamSqlInfo(CommandGetSqlInfo.getDefaultInstance(), callOf("metadata-token"), listener);
                Mockito.verify(listener).completed();
                Mockito.verify(listener, Mockito.never()).error(Mockito.any(Throwable.class));
            });
            metadata.get(5, TimeUnit.SECONDS);
            Assertions.assertFalse(owner.isDone(), "static metadata waited for the running command");
            Mockito.verify(ctx).refreshStartTime();
            release.countDown();
            owner.get(5, TimeUnit.SECONDS);
            producer.getFlightInfoCatalogs(CommandGetCatalogs.getDefaultInstance(), callOf("metadata-token"),
                    FlightDescriptor.command(new byte[0]));
            Mockito.verify(ctx).refreshStartTime();
        } finally {
            release.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
            ctx.cleanup();
            producer.close();
        }
    }

    @Test
    public void testDrainingSessionRejectsNewMetadataTickets() throws Exception {
        ConnectContext ctx = ConnectContext.forFlight("metadata-token");
        ctx.getFlightSqlChannel().addOKResult("pending", "SELECT 1");
        ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx))) {
            FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
            FlightRuntimeException catalogs = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoCatalogs(CommandGetCatalogs.getDefaultInstance(),
                            callOf("metadata-token"), descriptor));
            FlightRuntimeException sqlInfo = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoSqlInfo(CommandGetSqlInfo.getDefaultInstance(),
                            callOf("metadata-token"), descriptor));
            ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
            FlightRuntimeException stream = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getStreamSqlInfo(CommandGetSqlInfo.getDefaultInstance(),
                            callOf("metadata-token"), listener));
            Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, catalogs.status().code());
            Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, sqlInfo.status().code());
            Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, stream.status().code());
            Mockito.verifyNoInteractions(listener);
            Assertions.assertEquals(1, ctx.getFlightSqlChannel().resultNum());
        } finally {
            ctx.cleanup();
        }
    }

    private static TicketStatementQuery ticket(String token, String queryId) {
        return TicketStatementQuery.newBuilder().setStatementHandle(ByteString.copyFromUtf8(token + ":" + queryId))
                .build();
    }

    // Arrow Flight SQL keeps a query's coordinator alive across GetFlightInfo -> DoGet (see #62259):
    // executeAndSendResult() registers it as a deferred executor on the ConnectContext right after
    // submitting it to the BE. GetFlightInfo then still has to fetch the Arrow schema from the BE.
    // If that fetch fails (timeout / non-OK / empty / mismatched schema / RPC error), no FlightInfo
    // is returned, so no DoGet will ever pull this query's results. The deferred coordinator must be
    // finalized on this error path; otherwise its external-table batch SplitSource, query queue slot
    // and query registration leak until the next query starts or the connection is torn down.
    @Test
    public void testGetFlightInfoFinalizesDeferredExecutorWhenSchemaFetchFails() throws Exception {
        ConnectContext ctx = Mockito.spy(ConnectContext.forFlight("token"));

        // Stands in for the just-planned external-table query whose results DoGet would pull from BE.
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);

        FlightSessionsManager sessionsManager = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessionsManager.getConnectContext(Mockito.anyString())).thenReturn(ctx);

        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("token");

        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsManager);
        try (MockedConstruction<FlightSqlConnectProcessor> mocked = Mockito.mockConstruction(
                FlightSqlConnectProcessor.class, (mock, context) -> {
                    // handleQuery plans + submits to BE and defers the coordinator (coordBase == coord),
                    // exactly as executeAndSendResult() does for an Arrow Flight external-table scan.
                    Mockito.doAnswer(invocation -> {
                        FlightProtocolAdapter.of(ctx).beforeQuery(ctx);
                        ctx.addFlightSqlDeferredExecutor(deferred);
                        ctx.getFlightSqlChannel().addOKResult("failed-result", "SELECT 1");
                        ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
                        return null;
                    }).when(mock).handleQuery(Mockito.anyString());
                    // The Arrow schema fetch fails after the coordinator was already deferred.
                    Mockito.doThrow(new RuntimeException("fetch arrow flight schema timeout"))
                            .when(mock).fetchArrowFlightSchema(Mockito.anyInt());
                })) {
            CommandStatementQuery request = CommandStatementQuery.newBuilder().setQuery("select 1").build();
            FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);

            Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoStatement(request, callContext, descriptor));
            Assertions.assertEquals(0, ctx.getFlightSqlChannel().getAllocatedMemory());
            Assertions.assertFalse(FlightProtocolAdapter.of(ctx).hasPendingResponse());

            // The deferred coordinator of the failed query is finalized on the error path instead of
            // leaking until the next query / connection teardown.
            Mockito.verify(deferred).finalizeArrowFlightQuery();

            // It is also removed from the deferred list, so a later teardown does not finalize it
            // again (no double-close, no retained reference).
            ctx.closeFlightSqlDeferredExecutors();
            Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
        } finally {
            producer.close();
        }
    }

    /** What a session action answered: the values it sent, whether it completed, what it failed with. */
    private static class Answer<T> implements StreamListener<T> {
        final List<T> values = new ArrayList<>();
        Throwable error;
        boolean completed;

        @Override
        public void onNext(T val) {
            values.add(val);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }

        T single() {
            Assertions.assertNull(error, "the action failed: " + error);
            Assertions.assertTrue(completed, "the action did not complete");
            Assertions.assertEquals(1, values.size(), "the action answered " + values);
            return values.get(0);
        }
    }

    private static FlightSessionsManager sessionsOf(ConnectContext ctx) {
        FlightSessionsManager sessionsManager = Mockito.mock(FlightSessionsManager.class);
        Mockito.when(sessionsManager.getConnectContext(Mockito.anyString())).thenReturn(ctx);
        return sessionsManager;
    }

    private static CallContext callOf(String peerIdentity) {
        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn(peerIdentity);
        return callContext;
    }

    // The session options are answered through the listener, one result and a completion; the
    // per-option outcomes are inside that result, see FlightSessionOptionsTest for what they are.
    @Test
    public void testSessionOptionsAreAnsweredThroughTheListener() throws Exception {
        ConnectContext ctx = ConnectContext.forFlight("token");
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx));
        try {
            Answer<GetSessionOptionsResult> got = new Answer<>();
            producer.getSessionOptions(new GetSessionOptionsRequest(), callOf("token"), got);
            GetSessionOptionsResult options = got.single();
            Assertions.assertEquals(SessionOptionValueFactory.makeSessionOptionValue("internal"),
                    options.getSessionOptions().get(FlightSessionOptions.CATALOG));
            Assertions.assertEquals(SessionOptionValueFactory.makeSessionOptionValue(""),
                    options.getSessionOptions().get(FlightSessionOptions.SCHEMA));
            Assertions.assertEquals(SessionOptionValueFactory.makeSessionOptionValue(
                    String.valueOf(ctx.getSessionVariable().getWaitTimeoutS())),
                    options.getSessionOptions().get("wait_timeout"));

            Answer<SetSessionOptionsResult> set = new Answer<>();
            producer.setSessionOptions(new SetSessionOptionsRequest(ImmutableMap.of(
                    "no_such_variable", SessionOptionValueFactory.makeSessionOptionValue("1"))), callOf("token"), set);
            Assertions.assertEquals(ImmutableMap.of("no_such_variable",
                    new SetSessionOptionsResult.Error(ErrorValue.INVALID_NAME)), set.single().getErrors());

            Answer<SetSessionOptionsResult> nothing = new Answer<>();
            producer.setSessionOptions(new SetSessionOptionsRequest(ImmutableMap.of()), callOf("token"), nothing);
            Assertions.assertFalse(nothing.single().hasErrors());
        } finally {
            producer.close();
        }
    }

    // A session action is a command of the session like any other: while another command holds the
    // session, it waits for it and then gives up with UNAVAILABLE rather than touching the context.
    @Test
    public void testSessionOptionsWaitForTheRunningCommand() throws Exception {
        ConnectContext ctx = ConnectContext.forFlight("token");
        ctx.getSessionVariable().setQueryTimeoutS(1);
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            try {
                adapter.runCommand(ctx, () -> {
                    started.countDown();
                    release.await();
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        holder.start();
        try {
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Answer<SetSessionOptionsResult> set = new Answer<>();
            producer.setSessionOptions(new SetSessionOptionsRequest(ImmutableMap.of(
                    "query_timeout", SessionOptionValueFactory.makeSessionOptionValue(5L))), callOf("token"), set);
            Assertions.assertTrue(set.values.isEmpty());
            Assertions.assertFalse(set.completed);
            Assertions.assertInstanceOf(FlightRuntimeException.class, set.error);
            Assertions.assertEquals(FlightStatusCode.UNAVAILABLE, ((FlightRuntimeException) set.error).status().code());
            Assertions.assertEquals(1, ctx.getSessionVariable().getQueryTimeoutS());

            Answer<GetSessionOptionsResult> got = new Answer<>();
            producer.getSessionOptions(new GetSessionOptionsRequest(), callOf("token"), got);
            Assertions.assertInstanceOf(FlightRuntimeException.class, got.error);
            Assertions.assertEquals(FlightStatusCode.UNAVAILABLE, ((FlightRuntimeException) got.error).status().code());
        } finally {
            release.countDown();
            holder.join(10_000);
            producer.close();
        }
    }

    // A session option action is activity of the session's client, whether or not it runs a
    // statement: reading the options, setting none, setting only ones that are refused before any
    // statement runs, all start wait_timeout over.
    @Test
    public void testSessionOptionActionsKeepTheSessionAlive() throws Exception {
        ConnectContext ctx = ConnectPoolTestSupport.flightSession(ConnectPoolTestSupport.envAllowing(100),
                UserIdentity.ROOT, "token");
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, ctx.registerClientSession(scheduler.getConnectPoolMgr()));
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setStartTime();
        long waitTimeoutMs = ctx.getSessionVariable().getWaitTimeoutS() * 1000L;
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsOf(ctx));
        try {
            List<Runnable> actions = ImmutableList.of(
                    () -> {
                        Answer<GetSessionOptionsResult> got = new Answer<>();
                        producer.getSessionOptions(new GetSessionOptionsRequest(), callOf("token"), got);
                        got.single();
                    },
                    () -> {
                        Answer<SetSessionOptionsResult> set = new Answer<>();
                        producer.setSessionOptions(new SetSessionOptionsRequest(ImmutableMap.of()), callOf("token"), set);
                        Assertions.assertFalse(set.single().hasErrors());
                    },
                    () -> {
                        Answer<SetSessionOptionsResult> set = new Answer<>();
                        producer.setSessionOptions(new SetSessionOptionsRequest(ImmutableMap.of(
                                "no_such_variable", SessionOptionValueFactory.makeSessionOptionValue("1"),
                                "net_buffer_length", SessionOptionValueFactory.makeSessionOptionValue("1"))),
                                callOf("token"), set);
                        Assertions.assertEquals(2, set.single().getErrors().size());
                    });
            for (Runnable action : actions) {
                long before = ctx.getStartTime();
                // One tick of the clock, so that the refresh is visible.
                while (System.currentTimeMillis() <= before) {
                    Thread.sleep(1);
                }
                action.run();
                long after = ctx.getStartTime();
                Assertions.assertTrue(after > before, "the action did not count as activity");
                Assertions.assertEquals(MysqlCommand.COM_SLEEP, ctx.getCommand());
                // Idle since the action, not since before it: alive at the boundary counted from
                // the action ...
                ctx.checkTimeout(before + waitTimeoutMs + 1);
                Assertions.assertFalse(ctx.isClosingOrClosed());
                ctx.checkTimeout(after + waitTimeoutMs);
                Assertions.assertFalse(ctx.isClosingOrClosed());
            }
            // ... and gone past it, as any idle session.
            ctx.checkTimeout(ctx.getStartTime() + waitTimeoutMs + 1);
            Assertions.assertTrue(ctx.isClosingOrClosed());
        } finally {
            producer.close();
        }
    }

    // ListActions answers what this producer implements, the session actions included, and nothing
    // it would answer UNIMPLEMENTED.
    @Test
    public void testListActionsNamesTheImplementedActions() throws Exception {
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), Mockito.mock(FlightSessionsManager.class));
        try {
            Answer<ActionType> listed = new Answer<>();
            producer.listActions(callOf("token"), listed);
            Assertions.assertNull(listed.error);
            Assertions.assertTrue(listed.completed);
            Assertions.assertEquals(DorisFlightSqlProducer.SUPPORTED_ACTIONS, listed.values);
            Set<String> types = listed.values.stream().map(ActionType::getType).collect(Collectors.toSet());
            Assertions.assertTrue(types.contains(FlightConstants.SET_SESSION_OPTIONS.getType()));
            Assertions.assertTrue(types.contains(FlightConstants.GET_SESSION_OPTIONS.getType()));
            Assertions.assertTrue(types.contains(FlightConstants.CLOSE_SESSION.getType()));
            Assertions.assertTrue(types.contains(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType()));
            Assertions.assertTrue(types.contains(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.getType()));
            Assertions.assertFalse(types.contains(FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION.getType()));
            Assertions.assertFalse(types.contains(FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY.getType()));
            Assertions.assertFalse(types.contains(FlightConstants.CANCEL_FLIGHT_INFO.getType()));
        } finally {
            producer.close();
        }
    }

    // CloseSession invalidates the session's bearer token (which unregisters its context) and
    // answers CLOSED; when that fails it answers the failure and nothing else.
    @Test
    public void testCloseSessionAnswersWhileTheCancelledCommandDrains() throws Exception {
        ConnectContext ctx = ConnectPoolTestSupport.flightSession(ConnectPoolTestSupport.envAllowing(100),
                UserIdentity.ROOT, "close-token");
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, ctx.registerClientSession(scheduler.getConnectPoolMgr()));
        FlightSessionsManager sessions = new FlightSessionsInConnectPool(scheduler);
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessions);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ExecutorService rpcWorker = Executors.newSingleThreadExecutor();
        try {
            FlightProtocolAdapter.of(ctx).runCommand(ctx, () -> {
                ctx.setExecutor(running);
                ctx.getFlightSqlChannel().addOKResult("result", "SELECT 1");
                Answer<CloseSessionResult> closed = new Answer<>();
                // Another RPC must finish without taking execution ownership from this thread.
                rpcWorker.submit(() -> producer.closeSession(new CloseSessionRequest(),
                        callOf("close-token"), closed)).get(10, TimeUnit.SECONDS);
                Assertions.assertEquals(CloseSessionResult.Status.CLOSED, closed.single().getStatus());
                Assertions.assertTrue(ctx.isClosingOrClosed());
                Assertions.assertFalse(ctx.isClosingAfterResponse());
                Assertions.assertTrue(ctx.getFlightSqlChannel().getAllocatedMemory() > 0);
                Mockito.verify(running).cancel(Mockito.any(org.apache.doris.common.Status.class), Mockito.eq(false));
                FlightRuntimeException error = Assertions.assertThrows(FlightRuntimeException.class,
                        () -> sessions.getConnectContext("close-token"));
                Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, error.status().code());
            });
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Assertions.assertEquals(0, ctx.getFlightSqlChannel().getAllocatedMemory());
        } finally {
            rpcWorker.shutdownNow();
            Assertions.assertTrue(rpcWorker.awaitTermination(10, TimeUnit.SECONDS));
            ctx.cleanup();
            producer.close();
        }
    }

    @Test
    public void testCloseSessionInvalidatesTheTokenAndAnswersOnce() throws Exception {
        FlightSessionsManager sessionsManager = Mockito.mock(FlightSessionsManager.class);
        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 9090), sessionsManager);
        try {
            Answer<CloseSessionResult> closed = new Answer<>();
            producer.closeSession(new CloseSessionRequest(), callOf("token"), closed);
            Assertions.assertEquals(CloseSessionResult.Status.CLOSED, closed.single().getStatus());
            Mockito.verify(sessionsManager).closeConnectContext("token");

            Mockito.doThrow(new IllegalStateException("pool is gone")).when(sessionsManager)
                    .closeConnectContext("other-token");
            Answer<CloseSessionResult> failed = new Answer<>();
            producer.closeSession(new CloseSessionRequest(), callOf("other-token"), failed);
            Assertions.assertTrue(failed.values.isEmpty(), "answered " + failed.values + " after failing");
            Assertions.assertFalse(failed.completed);
            Assertions.assertInstanceOf(FlightRuntimeException.class, failed.error);
            Assertions.assertEquals(FlightStatusCode.INTERNAL, ((FlightRuntimeException) failed.error).status().code());
        } finally {
            producer.close();
        }
    }
}
