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

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TUniqueId;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
    public void testFailedCommandReleasesTheSession() {
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
        // ... the thread is clean, and the next command of the session is not blocked.
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertEquals("ok", adapter.callCommand(ctx, () -> "ok"));
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
