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

import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.results.FlightSqlChannel;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
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
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisFlightSqlProducerTest {

    private boolean prevRunningUnitTest;

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
     * <p>Before the fix, each prepare allocated a {@link org.apache.arrow.vector.VectorSchemaRoot}
     * via {@code FlightSqlChannel.createOneOneSchemaRoot("ResultMeta", ...)} and read only its
     * {@code Schema}, never closing the root, leaking one off-heap {@code VarCharVector} buffer per
     * prepare. Because Arrow Flight has no parameter binding, every client query triggers a fresh
     * prepare, so the leak grows monotonically until {@code MaxDirectMemorySize} is exhausted.
     *
     * <p>This test drives {@code createPreparedStatement} against a real {@link FlightSqlChannel} many
     * times and asserts the channel's Arrow allocator holds zero bytes afterwards. It goes red if the
     * try-with-resources that closes the temporary roots is removed.
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
            public ConnectContext getConnectContext(String peerIdentity) {
                return connectContext;
            }

            @Override
            public ConnectContext createConnectContext(String peerIdentity) {
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
                        // discard the placeholder prepared-statement result
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
                        .setQuery("select * from t where id = " + i).build();
                producer.createPreparedStatement(request, callContext, listener);
                Assertions.assertTrue(finished.await(30, TimeUnit.SECONDS), "createPreparedStatement #" + i + " did not finish in time");
            }

            // Guard against a false pass: if a prepare failed before reaching the allocation, no buffer
            // would be leaked and the memory assertion below could not detect a regression.
            Assertions.assertEquals(0, errors.get(), "no createPreparedStatement call should fail");
            // Every temporary VectorSchemaRoot must have been closed, so the channel's Arrow allocator
            // is back to zero. Reverting the fix leaves `rounds` ResultMeta buffers allocated here.
            Assertions.assertEquals(0L, channel.getAllocatedMemory(), "createPreparedStatement leaked off-heap memory in the channel allocator");
        } finally {
            producer.close();
        }
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
                        return null;
                    }).when(mock).handleQuery(Mockito.anyString());
                    // The Arrow schema fetch fails after the coordinator was already deferred.
                    Mockito.doThrow(new RuntimeException("fetch arrow flight schema timeout"))
                            .when(mock).fetchArrowFlightSchema(Mockito.anyInt());
                })) {
            CommandStatementQuery request = CommandStatementQuery.newBuilder().setQuery("select 1").build();
            FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);

            try {
                producer.getFlightInfoStatement(request, callContext, descriptor);
                Assertions.fail("expected the schema fetch failure to propagate as a CallStatus");
            } catch (Throwable expected) {
                // GetFlightInfo is expected to fail; the point of the test is what happens to the
                // deferred coordinator, not the thrown status itself.
            }

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

    // CloseSession invalidates the session's bearer token (which unregisters its context) and
    // answers CLOSED; when that fails it answers the failure and nothing else.
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
