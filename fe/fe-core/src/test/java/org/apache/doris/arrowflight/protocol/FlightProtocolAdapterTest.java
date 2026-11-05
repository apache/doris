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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolTestSupport;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An Arrow Flight SQL session is a ConnectContext bound to a FlightProtocolAdapter. The adapter
 * owns what only that protocol has (result cache, endpoints, deferred executors) and serializes
 * the session's commands, which gRPC does not do; the session is registered in the one connection
 * pool every protocol shares. ConnectContext asks the adapter to release those on teardown.
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
        return ConnectPoolTestSupport.flightSession(ConnectPoolTestSupport.envAllowing(100), UserIdentity.ROOT,
                "test-peer-identity");
    }

    // A command that blocks on a latch, run from a plain thread.
    private static void holdSession(FlightProtocolAdapter adapter, ConnectContext ctx,
            ConnectContext.ContextAction<InterruptedException> command) {
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

    private static void awaitCommandWait(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (thread.getState() == Thread.State.TIMED_WAITING) {
                return;
            }
            Assertions.assertTrue(thread.isAlive(), "command exited before waiting for its owner");
            Thread.sleep(1);
        }
        Assertions.fail("command did not wait for its owner");
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
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());

        // There is no MySQL side to such a session.
        Assertions.assertThrows(IllegalStateException.class, ctx::getMysqlChannel);
        Assertions.assertThrows(IllegalStateException.class, ctx::getCapability);
        Assertions.assertThrows(IllegalStateException.class, () -> MysqlProtocolAdapter.of(ctx));
    }

    @Test
    public void testSessionRegistersItsTraceIdInThePool() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ConnectContext ctx = flightSession();
        ctx.setConnectScheduler(scheduler);
        ctx.setTraceId("trace-1");
        TUniqueId queryId = new TUniqueId(1, 2);

        ctx.setQueryId(queryId);

        Assertions.assertEquals(DebugUtil.printId(queryId), scheduler.getQueryIdByTraceId("trace-1"));
    }

    @Test
    public void testKillUnregistersTheSessionFromThePool() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ConnectContext ctx = flightSession();
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, ctx.registerClientSession(scheduler.getConnectPoolMgr()));
        Assertions.assertSame(ctx, scheduler.getContext(ctx.getConnectionId()));
        Assertions.assertSame(ctx, scheduler.getContextWithPeerIdentity(ctx.getPeerIdentity()));

        ctx.kill(true);

        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertNull(scheduler.getContext(ctx.getConnectionId()));
        Assertions.assertNull(scheduler.getContextWithPeerIdentity(ctx.getPeerIdentity()));
    }

    // The release hook closes the channel-cached results and deferred executors even if the
    // session was never registered. ConnectContext invokes it after draining the running command.
    @Test
    public void testReleaseSessionContinuesAfterADeferredExecutorFails() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        StmtExecutor failing = Mockito.mock(StmtExecutor.class);
        Mockito.doThrow(new IllegalStateException("finalization failed")).when(failing).finalizeArrowFlightQuery();
        ctx.addFlightSqlDeferredExecutor(failing);
        ctx.addFlightSqlDeferredExecutor(deferred);
        // A result the client never pulled: off-heap Arrow buffers the channel holds.
        adapter.getChannel().addOKResult("query-1", "SELECT 1");
        Assertions.assertEquals(1, adapter.getChannel().resultNum());
        Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);

        ctx.releaseProtocolSession();

        // The channel's results are released with their buffers, and the deferred query finalized.
        Assertions.assertEquals(0, adapter.getChannel().resultNum());
        Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
        // Idempotent: the second release finds nothing to do, throws nothing, and finalizes nothing twice.
        Assertions.assertDoesNotThrow(ctx::releaseProtocolSession);
        Mockito.verify(failing, Mockito.times(1)).finalizeArrowFlightQuery();
        Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
    }


    @Test
    public void testCommandsOfOneSessionRunOneAtATime() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        FutureTask<Void> firstCommand = new FutureTask<>(() -> {
            adapter.runCommand(ctx, () -> {
                order.add("first:start");
                firstStarted.countDown();
                try {
                    Assertions.assertTrue(releaseFirst.await(10, TimeUnit.SECONDS));
                } finally {
                    order.add("first:finally");
                }
            });
            return null;
        });
        FutureTask<Void> secondCommand = new FutureTask<>(() -> {
            adapter.runCommand(ctx, () -> order.add("second"));
            return null;
        });
        Thread first = new Thread(firstCommand, "flight-first-command");
        Thread second = new Thread(secondCommand, "flight-second-command");
        try {
            first.start();
            Assertions.assertTrue(firstStarted.await(10, TimeUnit.SECONDS));
            second.start();
            // Observe the admission wait rather than assuming scheduling from elapsed time.
            awaitCommandWait(second);
            Assertions.assertEquals(Collections.singletonList("first:start"), order);

            releaseFirst.countDown();
            firstCommand.get(10, TimeUnit.SECONDS);
            secondCommand.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(Arrays.asList("first:start", "first:finally", "second"), order);
        } finally {
            releaseFirst.countDown();
            first.interrupt();
            second.interrupt();
            first.join(10_000);
            second.join(10_000);
            ctx.cleanup();
        }
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

    // A command of the session is activity of its client, whether or not it runs a statement:
    // wait_timeout starts over. What the last statement recorded is kept, though: a deferred query
    // of the session is finished later from that record.
    @Test
    public void testACommandIsActivityOfTheSession() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        ctx.setStartTime();
        ctx.updateReturnRows(7);
        ctx.getBackendSelectionProfile().recordQuerySelection(
                new BackendSelection.SelectionHint("group_a", BackendSelection.Mode.PREFER, "test"),
                BackendSelection.QuerySelectionResult.PREFERRED_HIT);
        long before = ctx.getStartTime();
        while (System.currentTimeMillis() <= before) {
            Thread.sleep(1);
        }

        adapter.runCommand(ctx, () -> { });

        Assertions.assertTrue(ctx.getStartTime() > before);
        Assertions.assertEquals(ctx.getStartTime(), ctx.getStartTimeInstant().toEpochMilli());
        Assertions.assertEquals(7, ctx.getReturnRows());
        Assertions.assertNotNull(ctx.getBackendSelectionProfile().getQuerySummary());
        // Starting a statement does drop the record, as it always did.
        ctx.setStartTime();
        Assertions.assertEquals(0, ctx.getReturnRows());
        Assertions.assertNull(ctx.getBackendSelectionProfile().getQuerySummary());
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
        // ... the thread is clean, and another thread can take ownership for the next command.
        Assertions.assertNull(ConnectContext.get());
        ctx.getSessionVariable().setQueryTimeoutS(1);
        Assertions.assertEquals("ok", callFromAnotherThread(adapter, ctx));
    }

    @Test
    public void testCommandExceptionsAreNotMistakenForAdmissionFailures() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        InterruptedException interrupted = new InterruptedException("command body interrupted");
        TimeoutException timedOut = new TimeoutException("command body timed out");

        Assertions.assertSame(interrupted, Assertions.assertThrows(InterruptedException.class,
                () -> adapter.runCommand(ctx, () -> {
                    throw interrupted;
                })));
        Assertions.assertSame(timedOut, Assertions.assertThrows(TimeoutException.class,
                () -> adapter.runCommand(ctx, () -> {
                    throw timedOut;
                })));
        Assertions.assertEquals("ok", adapter.callCommand(ctx, () -> "ok"));
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testNestedRequestCannotBorrowItsCallersExecutionOwnership() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);

        adapter.runCommand(ctx, () -> {
            Assertions.assertThrows(IllegalStateException.class, () -> adapter.runCommand(ctx,
                    () -> Assertions.fail("nested request must not run")));
            Assertions.assertSame(ctx, ConnectContext.get());
        });

        Assertions.assertEquals("ok", adapter.callCommand(ctx, () -> "ok"));
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testCloseWakesWaitingRpcBeforeTheRunningRpcExits() throws Exception {
        verifyCloseWakesWaitingRpc(ConnectContext.CloseMode.IMMEDIATE);
        verifyCloseWakesWaitingRpc(ConnectContext.CloseMode.AFTER_RESPONSE);
    }

    private void verifyCloseWakesWaitingRpc(ConnectContext.CloseMode mode) throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        FutureTask<FlightRuntimeException> waiting = new FutureTask<>(() -> Assertions.assertThrows(
                FlightRuntimeException.class, () -> adapter.runCommand(ctx,
                        () -> Assertions.fail("closed session must reject the waiting RPC"))));
        Thread waiter = new Thread(waiting, "flight-waiting-rpc");
        try {
            Future<?> command = worker.submit(() -> {
                adapter.runCommand(ctx, () -> {
                    adapter.getChannel().addOKResult("cached", "SELECT 1");
                    started.countDown();
                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                    Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);
                });
                return null;
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            waiter.start();
            awaitCommandWait(waiter);

            ctx.requestClose(mode);

            Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED,
                    waiting.get(10, TimeUnit.SECONDS).status().code());
            Assertions.assertFalse(command.isDone());
            Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);
            release.countDown();
            command.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
        } finally {
            release.countDown();
            waiter.interrupt();
            waiter.join(10_000);
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
            ctx.cleanup();
        }
    }

    @Test
    public void testInterruptedAdmissionLeavesTheRunningRpcAndSessionOpen() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        FutureTask<FlightStatusCode> waiting = new FutureTask<>(() -> {
            FlightRuntimeException failure = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> adapter.runCommand(ctx, () -> Assertions.fail("interrupted RPC must not run")));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
            return failure.status().code();
        });
        Thread waiter = new Thread(waiting, "flight-interrupted-rpc");
        try {
            Future<?> command = worker.submit(() -> {
                adapter.runCommand(ctx, () -> {
                    started.countDown();
                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                });
                return null;
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            waiter.start();
            awaitCommandWait(waiter);
            waiter.interrupt();
            Assertions.assertEquals(FlightStatusCode.CANCELLED, waiting.get(10, TimeUnit.SECONDS));
            Assertions.assertFalse(ctx.isClosingOrClosed());
            Assertions.assertFalse(command.isDone());
            release.countDown();
            command.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals("ok", adapter.callCommand(ctx, () -> "ok"));
        } finally {
            release.countDown();
            waiter.interrupt();
            waiter.join(10_000);
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
            ctx.cleanup();
        }
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
        Assertions.assertEquals(Lists.newArrayList(deferred), ctx.getFlightSqlDeferredExecutors());
        adapter.beginRequest();
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
        Mockito.verify(deferred).finalizeArrowFlightQuery();
        Assertions.assertEquals(0, adapter.getChannel().resultNum());
        Assertions.assertTrue(ctx.getFlightSqlEndpointsLocations().isEmpty());
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
    }

    // A replanned statement is run again without going through beforeStatement. The attempt that
    // failed had moved the result to the backends and registered endpoints there; the next one
    // starts as the statement did, so that a second failure before its query runs does not leave
    // the session believing a result is waiting on the backends, and that nothing the failed
    // attempt registered is delivered.
    @Test
    public void testAnAttemptStartsWhereTheStatementDid() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);

        adapter.beforeStatement(ctx);
        adapter.beforeAttempt(ctx);
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        Assertions.assertTrue(ctx.getFlightSqlEndpointsLocations().isEmpty());

        // The first attempt ran its query: result on the backends, one endpoint registered.
        adapter.beforeQuery(ctx);
        ctx.addFlightSqlEndpointsLocation(new FlightSqlEndpointsLocation(new TUniqueId(1, 1),
                new TNetworkAddress("127.0.0.1", 8070), new TNetworkAddress("127.0.0.1", 8060), new ArrayList<>()));
        Assertions.assertFalse(ctx.isReturnResultFromLocal());
        Assertions.assertEquals(1, ctx.getFlightSqlEndpointsLocations().size());

        // It failed and the statement is replanned: the second attempt starts afresh.
        adapter.beforeAttempt(ctx);
        Assertions.assertTrue(ctx.isReturnResultFromLocal());
        Assertions.assertTrue(ctx.getFlightSqlEndpointsLocations().isEmpty());

        // Only what the attempt that completes registers is delivered.
        adapter.beforeQuery(ctx);
        ctx.addFlightSqlEndpointsLocation(new FlightSqlEndpointsLocation(new TUniqueId(2, 2),
                new TNetworkAddress("127.0.0.1", 8070), new TNetworkAddress("127.0.0.1", 8060), new ArrayList<>()));
        Assertions.assertFalse(ctx.isReturnResultFromLocal());
        Assertions.assertEquals(1, ctx.getFlightSqlEndpointsLocations().size());
        Assertions.assertEquals(new TUniqueId(2, 2), ctx.getFlightSqlEndpointsLocations().get(0).getFinstId());

        // The next statement of the request leaves that endpoint as it was, whatever its own
        // attempts do: a retried attempt withdraws only what the failed one registered.
        adapter.beforeStatement(ctx);
        adapter.beforeAttempt(ctx);
        adapter.beforeQuery(ctx);
        ctx.addFlightSqlEndpointsLocation(new FlightSqlEndpointsLocation(new TUniqueId(3, 3),
                new TNetworkAddress("127.0.0.1", 8070), new TNetworkAddress("127.0.0.1", 8060), new ArrayList<>()));
        adapter.beforeAttempt(ctx);
        Assertions.assertEquals(1, ctx.getFlightSqlEndpointsLocations().size());
        Assertions.assertEquals(new TUniqueId(2, 2), ctx.getFlightSqlEndpointsLocations().get(0).getFinstId());
        adapter.beforeQuery(ctx);
        ctx.addFlightSqlEndpointsLocation(new FlightSqlEndpointsLocation(new TUniqueId(4, 4),
                new TNetworkAddress("127.0.0.1", 8070), new TNetworkAddress("127.0.0.1", 8060), new ArrayList<>()));
        Assertions.assertEquals(2, ctx.getFlightSqlEndpointsLocations().size());
        Assertions.assertEquals(new TUniqueId(4, 4), ctx.getFlightSqlEndpointsLocations().get(1).getFinstId());

        // A new request starts from nothing.
        adapter.beginRequest();
        adapter.beforeStatement(ctx);
        adapter.beforeAttempt(ctx);
        Assertions.assertTrue(ctx.getFlightSqlEndpointsLocations().isEmpty());
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
        adapter.beforeStatement(ctx);
        Assertions.assertTrue(adapter.finishStatement(ctx, executor, 0, 2));

        // A result produced by the last statement is fine ...
        ctx.setQueryId(new TUniqueId(1, 1));
        ctx.getResultSender().sendResultSet(resultSet, null, false);
        Assertions.assertTrue(adapter.finishStatement(ctx, executor, 1, 2));
        Assertions.assertNotEquals(MysqlStateType.ERR, ctx.getState().getStateType());

        // ... one produced earlier stops the request with the error the client will see. A result
        // cached on this frontend has nothing to cancel.
        Assertions.assertFalse(adapter.finishStatement(ctx, executor, 0, 2));
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertEquals(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT, ctx.getState().getErrorCode());
        Mockito.verify(executor, Mockito.never()).cancel(Mockito.any(Status.class));

        // A result left on the backends counts the same as one cached here: the FlightInfo of the
        // request describes exactly one result, so a query that is not the last statement stops the
        // request too, wherever its result is -- and the query, which nobody will pull from the
        // backends, is cancelled there rather than left to the BE's result buffer timer ...
        adapter.beginRequest();
        ctx.getState().reset();
        adapter.beforeStatement(ctx);
        adapter.beforeQuery(ctx);
        Assertions.assertEquals(0, adapter.getChannel().resultNum());
        Assertions.assertFalse(adapter.finishStatement(ctx, executor, 0, 2));
        Assertions.assertEquals(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT, ctx.getState().getErrorCode());
        ArgumentCaptor<Status> cancelReason = ArgumentCaptor.forClass(Status.class);
        Mockito.verify(executor).cancel(cancelReason.capture());
        Assertions.assertEquals(TStatusCode.CANCELLED, cancelReason.getValue().getErrorCode());
        // ... and as the last statement its result is the request's.
        ctx.getState().reset();
        adapter.beforeStatement(ctx);
        adapter.beforeQuery(ctx);
        Assertions.assertTrue(adapter.finishStatement(ctx, executor, 1, 2));
        Assertions.assertNotEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        // A query that failed produced no result, wherever it was headed: the request stops with
        // the query's own error, not with this one.
        ctx.getState().reset();
        adapter.beforeStatement(ctx);
        adapter.beforeQuery(ctx);
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "the query's own error");
        Assertions.assertTrue(adapter.finishStatement(ctx, executor, 0, 2));
        Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_ERROR, ctx.getState().getErrorCode());
    }

    // A request that fails after deferring its query (GetFlightInfo failed after planning) returns
    // no FlightInfo, so no DoGet will pull the result: the query is cancelled on the backends and
    // its executor finalized, once, in that order.
    @Test
    public void testAFailedRequestCancelsTheQueryItDeferred() {
        ConnectContext ctx = flightSession();
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        ctx.addFlightSqlDeferredExecutor(deferred);
        Status reason = new Status(TStatusCode.CANCELLED, "get flight info statement failed");

        ctx.cancelFlightSqlDeferredExecutors(reason);

        InOrder inOrder = Mockito.inOrder(deferred);
        inOrder.verify(deferred).cancel(reason);
        inOrder.verify(deferred).finalizeArrowFlightQuery();
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
        // Nothing left to take: a second call is a no-op.
        ctx.cancelFlightSqlDeferredExecutors(reason);
        Mockito.verify(deferred, Mockito.times(1)).cancel(reason);
        Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
    }

    @Test
    public void testDeferredCloseKeepsTheHandoffOpenUntilOperationExit() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        try {
            adapter.runCommand(ctx, () -> {
                ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
                Assertions.assertTrue(ctx.isClosingOrClosed());
                // Revoking admission does not prevent the admitted command from publishing its result.
                adapter.addDeferredExecutor(deferred);
                Assertions.assertEquals(Collections.singletonList(deferred), adapter.getDeferredExecutors());
                Mockito.verifyNoInteractions(deferred);
                adapter.getChannel().addOKResult("response", "KILL CONNECTION");
                Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);
            });
            Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
            Mockito.verify(deferred).finalizeArrowFlightQuery();
            Assertions.assertTrue(adapter.getDeferredExecutors().isEmpty());
        } finally {
            ctx.cleanup();
        }
    }

    // Immediate closure revokes admission and cancels execution, but deferred executors are
    // released with the other session resources after the admitted command has finished.
    @Test
    public void testImmediateCloseDrainsDeferredExecutorsAfterTheCommandExits() throws Exception {
        ConnectScheduler scheduler = new ConnectScheduler(10, 10);
        ConnectContext ctx = flightSession();
        ctx.setConnectScheduler(scheduler);
        scheduler.submit(ctx);
        Assertions.assertEquals(-1, ctx.registerClientSession(scheduler.getConnectPoolMgr()));
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor before = Mockito.mock(StmtExecutor.class);
        StmtExecutor late = Mockito.mock(StmtExecutor.class);
        CountDownLatch commandStarted = new CountDownLatch(1);
        CountDownLatch closeRequested = new CountDownLatch(1);
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch releaseCommand = new CountDownLatch(1);
        ctx.addFlightSqlDeferredExecutor(before);

        adapter.getChannel().addOKResult("cached", "SELECT 1");
        Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);

        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> command = worker.submit(() -> {
                adapter.runCommand(ctx, () -> {
                    commandStarted.countDown();
                    Assertions.assertTrue(closeRequested.await(10, TimeUnit.SECONDS));
                    ctx.addFlightSqlDeferredExecutor(late);
                    published.countDown();
                    Assertions.assertTrue(releaseCommand.await(10, TimeUnit.SECONDS));
                });
                return null;
            });
            Assertions.assertTrue(commandStarted.await(10, TimeUnit.SECONDS));

            ctx.kill(true);
            Assertions.assertNull(scheduler.getContext(ctx.getConnectionId()));
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verifyNoInteractions(before, late);
            Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);

            closeRequested.countDown();
            Assertions.assertTrue(published.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(Arrays.asList(before, late), adapter.getDeferredExecutors());
            Mockito.verifyNoInteractions(before, late);

            releaseCommand.countDown();
            command.get(10, TimeUnit.SECONDS);
            ctx.cleanup();
            Mockito.verify(before, Mockito.times(1)).finalizeArrowFlightQuery();
            Mockito.verify(late, Mockito.times(1)).finalizeArrowFlightQuery();
            Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
            Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
        } finally {
            closeRequested.countDown();
            releaseCommand.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
            ctx.cleanup();
        }

        // A command that gets its turn after teardown does not run on the session, and leaves the
        // thread as it found it.
        ConnectContext.remove();
        FlightRuntimeException e = Assertions.assertThrows(FlightRuntimeException.class,
                () -> adapter.runCommand(ctx, () -> Assertions.fail("must not run on a torn-down session")));
        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
        Assertions.assertTrue(e.status().description().contains("closed"), e.status().description());
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testFailedCommandStillReleasesExecutorsPublishedAfterClose() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        IllegalStateException failure = new IllegalStateException("command failed");

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> adapter.runCommand(ctx, () -> {
                    ctx.cleanup();
                    adapter.addDeferredExecutor(deferred);
                    adapter.getChannel().addOKResult("response", "SELECT 1");
                    Mockito.verifyNoInteractions(deferred);
                    throw failure;
                })));

        ctx.cleanup();
        Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
        Assertions.assertTrue(adapter.getDeferredExecutors().isEmpty());
        Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testCommandExitHandsCleanupToTheStillRunningCancellation() throws Exception {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        StmtExecutor deferred = Mockito.mock(StmtExecutor.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch releaseCommand = new CountDownLatch(1);
        CountDownLatch cancelling = new CountDownLatch(1);
        CountDownLatch releaseCancellation = new CountDownLatch(1);
        AtomicBoolean cancellationWaitCompleted = new AtomicBoolean();
        AtomicBoolean cancellationSawLiveMemory = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            // Closing again from a cancellation callback must not recurse or wait for itself.
            ctx.cleanup();
            cancelling.countDown();
            cancellationWaitCompleted.set(releaseCancellation.await(10, TimeUnit.SECONDS));
            cancellationSawLiveMemory.set(adapter.getChannel().getAllocatedMemory() > 0);
            return null;
        }).when(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> command = workers.submit(() -> {
                adapter.runCommand(ctx, () -> {
                    ctx.setExecutor(running);
                    adapter.addDeferredExecutor(deferred);
                    adapter.getChannel().addOKResult("cached", "SELECT 1");
                    started.countDown();
                    Assertions.assertTrue(releaseCommand.await(10, TimeUnit.SECONDS));
                });
                return null;
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Future<?> closing = workers.submit(ctx::cleanup);
            Assertions.assertTrue(cancelling.await(10, TimeUnit.SECONDS));

            releaseCommand.countDown();
            command.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(closing.isDone());
            Assertions.assertTrue(adapter.getChannel().getAllocatedMemory() > 0);
            Mockito.verifyNoInteractions(deferred);

            releaseCancellation.countDown();
            closing.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(cancellationWaitCompleted.get());
            Assertions.assertTrue(cancellationSawLiveMemory.get());
            Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
            Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
            Mockito.verify(running, Mockito.times(1)).cancel(Mockito.any(Status.class), Mockito.eq(false));
        } finally {
            releaseCommand.countDown();
            releaseCancellation.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
            ctx.cleanup();
        }
    }

    @Test
    public void testReentrantFailingFinalizerDoesNotRepeatCleanupOrSkipOtherResources() {
        ConnectContext ctx = flightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        StmtExecutor failing = Mockito.mock(StmtExecutor.class);
        StmtExecutor remaining = Mockito.mock(StmtExecutor.class);
        Mockito.doAnswer(invocation -> {
            ctx.cleanup();
            throw new IllegalStateException("deferred finalization failed");
        }).when(failing).finalizeArrowFlightQuery();
        adapter.addDeferredExecutor(failing);
        adapter.addDeferredExecutor(remaining);
        adapter.getChannel().addOKResult("cached", "SELECT 1");

        ctx.cleanup();
        ctx.cleanup();

        Assertions.assertEquals(0, adapter.getChannel().getAllocatedMemory());
        Assertions.assertTrue(adapter.getDeferredExecutors().isEmpty());
        Mockito.verify(failing, Mockito.times(1)).finalizeArrowFlightQuery();
        Mockito.verify(remaining, Mockito.times(1)).finalizeArrowFlightQuery();
        FlightRuntimeException closed = Assertions.assertThrows(FlightRuntimeException.class,
                () -> adapter.runCommand(ctx, () -> Assertions.fail("finalized session must reject RPCs")));
        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, closed.status().code());
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
