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

package org.apache.doris.arrowflight.protocol;

import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An Arrow Flight SQL session is a ConnectContext bound to a FlightProtocolAdapter. The adapter
 * owns what only that protocol has (result cache, endpoints, deferred executors, the pool the
 * session is registered in) and serializes the session's commands, which gRPC does not do.
 */
public class FlightProtocolAdapterTest {
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
        ConnectContext.remove();
    }

    private static ConnectContext flightSession() {
        return ConnectContext.forFlight("test-peer-identity");
    }

    // A command that blocks on a latch, run from a plain thread.
    private static void holdSession(FlightProtocolAdapter adapter, ConnectContext ctx,
            FlightProtocolAdapter.SessionAction<InterruptedException> command) {
        try {
            adapter.runCommand(ctx, command);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Runs a trivial command of the session from another thread and returns what it answered, or
    // rethrows what it failed with (UNAVAILABLE if the session was still held).
    private static String callFromAnotherThread(FlightProtocolAdapter adapter, ConnectContext ctx)
            throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return executor.submit(() -> adapter.callCommand(ctx, () -> "ok")).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFlightSessionIsBoundToItsAdapter() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);

        Assertions.assertEquals(ConnectType.ARROW_FLIGHT_SQL, ctx.getConnectType());
        Assertions.assertEquals(TResultSinkType.ARROW_FLIGHT_PROTOCOL, ctx.getResultSinkType());
        Assertions.assertEquals("test-peer-identity", ctx.getPeerIdentity());
        Assertions.assertSame(adapter.getChannel(), ctx.getFlightSqlChannel());
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());

        // There is no MySQL side to such a session.
        Assertions.assertThrows(IllegalStateException.class, ctx::getMysqlChannel);
        Assertions.assertThrows(IllegalStateException.class, ctx::getCapability);
        Assertions.assertThrows(IllegalStateException.class, () -> MysqlProtocolAdapter.of(ctx));
    }

    @Test
    public void testSessionRegistersItsTraceIdInTheFlightPool() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ConnectContext ctx = flightSession();
        ctx.setConnectScheduler(scheduler);
        ctx.setTraceId("trace-1");
        TUniqueId queryId = new TUniqueId(1, 2);

        ctx.setQueryId(queryId);

        Assertions.assertEquals(DebugUtil.printId(queryId),
                scheduler.getFlightSqlConnectPoolMgr().getQueryIdByTraceId("trace-1"));
        Assertions.assertEquals("", scheduler.getConnectPoolMgr().getQueryIdByTraceId("trace-1"));
    }

    @Test
    public void testKillUnregistersTheSessionFromTheFlightPool() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ConnectContext ctx = flightSession();
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, scheduler.getFlightSqlConnectPoolMgr().registerConnection(ctx));
        Assertions.assertSame(ctx, scheduler.getContext(ctx.getConnectionId()));

        ctx.kill(true);

        Assertions.assertTrue(ctx.isKilled());
        Assertions.assertNull(scheduler.getContext(ctx.getConnectionId()));
    }

    @Test
    public void testCommandsOfOneSessionRunOneAtATime() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        Thread first = new Thread(() -> holdSession(adapter, ctx, () -> {
            order.add("first:start");
            firstStarted.countDown();
            releaseFirst.await();
            order.add("first:end");
        }));
        first.start();
        Assertions.assertTrue(firstStarted.await(10, TimeUnit.SECONDS));

        Thread second = new Thread(() -> adapter.runCommand(ctx, () -> order.add("second")));
        second.start();
        // The second command has to wait for the first one, however long it takes.
        second.join(500);
        Assertions.assertTrue(second.isAlive());
        Assertions.assertEquals(Collections.singletonList("first:start"), order);

        releaseFirst.countDown();
        first.join(10_000);
        second.join(10_000);
        Assertions.assertEquals(Arrays.asList("first:start", "first:end", "second"), order);
    }

    @Test
    public void testCommandRunsWithItsSessionAsTheCurrentContext() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);

        // With no current context, the thread is left clean afterwards ...
        ConnectContext.remove();
        adapter.runCommand(ctx, () -> Assertions.assertSame(ctx, ConnectContext.get()));
        Assertions.assertNull(ConnectContext.get());

        // ... and a previous one is put back.
        ConnectContext previous = new ConnectContext();
        previous.setThreadLocalInfo();
        Assertions.assertSame(ctx, adapter.callCommand(ctx, ConnectContext::get));
        Assertions.assertSame(previous, ConnectContext.get());
    }

    @Test
    public void testFailedCommandReleasesTheSession() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        ConnectContext.remove();

        // The command's own exception comes out unchanged, checked or not ...
        Assertions.assertThrows(IllegalStateException.class, () -> adapter.runCommand(ctx, () -> {
            throw new IllegalStateException("boom");
        }));
        Assertions.assertThrows(TException.class, () -> adapter.runCommand(ctx, () -> {
            throw new TException("boom");
        }));
        // ... the thread is clean, and the next command of the session is not blocked. That next
        // command runs on another thread: the lock is reentrant, so this thread would get through
        // even if the failed command had left the lock held.
        Assertions.assertNull(ConnectContext.get());
        ctx.getSessionVariable().setQueryTimeoutS(1);
        Assertions.assertEquals("ok", callFromAnotherThread(adapter, ctx));
    }

    @Test
    public void testAFlightSessionTakesNoResultProducedForAMysqlClient() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);

        // The SQL cache and the master answer with MySQL packets; a FE-side result is untyped
        // Utf8; a short circuit has no Arrow result at either end; a retry would leave the failed
        // attempt's endpoints behind.
        Assertions.assertFalse(adapter.supportsSqlCacheReplay());
        Assertions.assertFalse(adapter.canReplayForwardedQueryResult());
        Assertions.assertFalse(adapter.supportsFeSideResult());
        Assertions.assertFalse(ctx.supportHandleByFe());
        Assertions.assertFalse(adapter.supportsShortCircuitPointQuery());
        Assertions.assertFalse(adapter.canRetryQuery(ctx));

        // The master needs to know nothing about the client: its response is consumed here.
        TMasterOpRequest request = new TMasterOpRequest();
        adapter.fillForwardRequest(ctx, request);
        Assertions.assertFalse(request.isSetMysqlCapability());
        Assertions.assertFalse(request.isSetClientDeprecatedEOF());
        Assertions.assertFalse(request.isSetPrepareExecuteBuffer());
        Assertions.assertFalse(request.isSetCursorFetchRequested());
    }

    @Test
    public void testWhereTheResultIsFollowsTheStatementLifecycle() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        ShowResultSet resultSet = new ShowResultSet(
                ShowResultSetMetaData.builder().addColumn(new Column("c", ScalarType.createVarchar(20))).build(),
                Lists.<List<String>>newArrayList(Lists.newArrayList("v")));

        // A statement's result is on this frontend (a SHOW, a SET) ...
        adapter.beforeStatement(ctx);
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        // ... until a query is run for it on the backends: then the client pulls it from where
        // the coordinator registered it.
        adapter.beforeQuery(ctx);
        Assertions.assertFalse(ctx.isReturnResultFromLocal());
        ctx.addFlightSqlEndpointsLocation(new FlightSqlEndpointsLocation(new TUniqueId(1, 1),
                new TNetworkAddress("127.0.0.1", 8070), new TNetworkAddress("127.0.0.1", 8060), new ArrayList<>()));
        Assertions.assertEquals(1, ctx.getFlightSqlEndpointsLocations().size());
        // The next statement of the request starts on this frontend again.
        adapter.beforeStatement(ctx);
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        // An EXPLAIN is answered here without ever touching a backend, so it never leaves the
        // frontend: the sender does not decide where the result is.
        ctx.setQueryId(new TUniqueId(2, 2));
        ctx.getResultSender().sendResultSet(resultSet, null, false);
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        Assertions.assertEquals(1, adapter.getChannel().resultNum());

        // A new request drops everything the previous one left: its deferred coordinator, the
        // result nobody pulled, the endpoints, and the result is on this frontend again.
        adapter.beforeQuery(ctx);
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        ctx.addFlightSqlDeferredExecutor(deferred);
        adapter.beginRequest();
        Mockito.verify(deferred).finalizeArrowFlightQuery();
        Assertions.assertEquals(0, adapter.getChannel().resultNum());
        Assertions.assertTrue(ctx.getFlightSqlEndpointsLocations().isEmpty());
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
    }

    @Test
    public void testOnlyTheLastStatementOfARequestMayReturnAResult() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        ShowResultSet resultSet = new ShowResultSet(
                ShowResultSetMetaData.builder().addColumn(new Column("c", ScalarType.createVarchar(20))).build(),
                Lists.<List<String>>newArrayList(Lists.newArrayList("v")));

        // A statement without a result lets the request go on.
        Assertions.assertTrue(adapter.finishStatement(ctx, executor, 0, 2));

        // A result produced by the last statement is fine ...
        ctx.setQueryId(new TUniqueId(1, 1));
        ctx.getResultSender().sendResultSet(resultSet, null, false);
        Assertions.assertTrue(adapter.finishStatement(ctx, executor, 1, 2));
        Assertions.assertNotEquals(MysqlStateType.ERR, ctx.getState().getStateType());

        // ... one produced earlier stops the request with the error the client will see.
        Assertions.assertFalse(adapter.finishStatement(ctx, executor, 0, 2));
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertEquals(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT, ctx.getState().getErrorCode());
    }

    @Test
    public void testWaitingCommandGivesUpAfterTheQueryTimeout() throws Exception {
        ConnectContext ctx = flightSession();
        ctx.getSessionVariable().setQueryTimeoutS(1);
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread holder = new Thread(() -> holdSession(adapter, ctx, () -> {
            started.countDown();
            release.await();
        }));
        holder.start();
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        try {
            FlightRuntimeException e = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> adapter.runCommand(ctx, () -> Assertions.fail("must not run concurrently")));
            Assertions.assertEquals(FlightStatusCode.UNAVAILABLE, e.status().code());
            Assertions.assertTrue(e.status().description().contains("still running"), e.status().description());
        } finally {
            release.countDown();
            holder.join(10_000);
        }
        // Once the first command is done, the session takes commands again.
        Assertions.assertEquals("ok", adapter.callCommand(ctx, () -> "ok"));
    }
}
