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

import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisFlightSqlProducerTest {

    private boolean prevRunningUnitTest;

    @Before
    public void setUp() {
        // FlightSqlConnectContext.init() only reaches Env when this is false; keep it true so the
        // context can be built without a running FE.
        prevRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @After
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
        FlightSqlConnectContext connectContext = new FlightSqlConnectContext("test-peer-identity");
        FlightSqlChannel channel = connectContext.getFlightSqlChannel();
        Assert.assertEquals("channel allocator should start empty", 0L, channel.getAllocatedMemory());

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
                Assert.assertTrue("createPreparedStatement #" + i + " did not finish in time",
                        finished.await(30, TimeUnit.SECONDS));
            }

            // Guard against a false pass: if a prepare failed before reaching the allocation, no buffer
            // would be leaked and the memory assertion below could not detect a regression.
            Assert.assertEquals("no createPreparedStatement call should fail", 0, errors.get());
            // Every temporary VectorSchemaRoot must have been closed, so the channel's Arrow allocator
            // is back to zero. Reverting the fix leaves `rounds` ResultMeta buffers allocated here.
            Assert.assertEquals("createPreparedStatement leaked off-heap memory in the channel allocator",
                    0L, channel.getAllocatedMemory());
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
        // A flight ConnectContext whose getFlightSqlChannel() works (the base context throws).
        ConnectContext ctx = Mockito.spy(new ConnectContext());
        Mockito.doReturn(Mockito.mock(FlightSqlChannel.class)).when(ctx).getFlightSqlChannel();

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
                        ctx.setReturnResultFromLocal(false);
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
                Assert.fail("expected the schema fetch failure to propagate as a CallStatus");
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
}
