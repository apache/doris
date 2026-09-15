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

package org.apache.doris.arrowflight.sessions;

import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * The idle reaper for deferred Arrow Flight queries (#67503). A sleeping Flight session whose last
 * query kept its coordinator alive (an external-table scan in batch mode, see #62259) gets that
 * coordinator finalized by the connection timeout checker once
 * arrow_flight_deferred_query_idle_timeout_second, floored at the execution timeout the query ran
 * with, has passed since the query started -- each deferred query by its own deadline, taken out
 * of the session's list and finalized exactly once, whoever gets to it first (see
 * FlightProtocolAdapter). The session itself is not killed, wait_timeout still governs that, and a
 * MySQL session is untouched.
 */
public class FlightSqlDeferredQueryIdleTimeoutTest {
    private int savedIdleTimeout;
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedIdleTimeout = Config.arrow_flight_deferred_query_idle_timeout_second;
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        Config.arrow_flight_deferred_query_idle_timeout_second = savedIdleTimeout;
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    // A deferred query that started when the session's current statement did.
    private static StmtExecutor deferredExecutor(ConnectContext ctx, int execTimeoutS) {
        return deferredExecutor(ctx.getStartTime(), execTimeoutS);
    }

    private static StmtExecutor deferredExecutor(long startTimeMs, int execTimeoutS) {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getDeferredExecTimeoutS()).thenReturn(execTimeoutS);
        Mockito.when(executor.getDeferredStartTimeMs()).thenReturn(startTimeMs);
        return executor;
    }

    // A Flight session that ran a query and has been sleeping since; the client never closed it.
    private static ConnectContext sleepingFlightSession() {
        ConnectContext ctx = ConnectContext.forFlight("test-peer-identity");
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setStartTime();
        return ctx;
    }

    @Test
    public void testIdleSessionReleasesDeferredQueryButIsNotKilled() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        StmtExecutor deferred = deferredExecutor(ctx, 5);
        ctx.addFlightSqlDeferredExecutor(deferred);
        long start = ctx.getStartTime();
        Assertions.assertEquals(Lists.newArrayList(deferred), ctx.getFlightSqlDeferredExecutors());
        Assertions.assertEquals(7_000L, FlightProtocolAdapter.deferredBoundMs(deferred, 7));

        // not idle for long enough yet
        ctx.checkTimeout(start + 7_000L);
        Mockito.verify(deferred, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());

        // past the bound: the deferred coordinator is finalized and the session survives
        ctx.checkTimeout(start + 7_001L);
        Mockito.verify(deferred).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());

        // a later tick has nothing left to release
        ctx.checkTimeout(start + 60_000L);
        Mockito.verify(deferred, Mockito.times(1)).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
    }

    // The bound counts from when the deferred query started, not from the session's last command:
    // a session option or a metadata request that came since neither finished the query nor is a
    // reason to keep its coordinator alive for another bound.
    @Test
    public void testBoundCountsFromTheQuerysStartNotTheSessionsLastCommand() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        // The query started 100s ago; the session's last command was just now.
        long queryStart = ctx.getStartTime() - 100_000L;
        StmtExecutor deferred = deferredExecutor(queryStart, 5);
        ctx.addFlightSqlDeferredExecutor(deferred);

        ctx.checkTimeout(ctx.getStartTime() + 1L);
        Mockito.verify(deferred).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());

        // The other way round as well: the bound of a query that just started is where it is,
        // however many commands the session runs in the meantime.
        long youngStart = ctx.getStartTime();
        StmtExecutor young = deferredExecutor(youngStart, 5);
        ctx.addFlightSqlDeferredExecutor(young);
        ctx.refreshStartTime();
        ctx.checkTimeout(youngStart + 7_000L);
        Mockito.verify(young, Mockito.never()).finalizeArrowFlightQuery();
        ctx.checkTimeout(youngStart + 7_001L);
        Mockito.verify(young).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
    }

    // The bound is floored at the execution timeout the query ran with -- a client may still be
    // pulling its results from the BE -- and it is each deferred query's own: the statements of one
    // multi-statement request each ran with a timeout of their own (a SET_VAR hint), and the one
    // with the longer timeout does not keep the other alive.
    @Test
    public void testEachDeferredQueryIsBoundByItsOwnExecTimeout() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 3;
        ConnectContext ctx = sleepingFlightSession();
        StmtExecutor shortQuery = deferredExecutor(ctx, 5);
        StmtExecutor longQuery = deferredExecutor(ctx, 20);
        ctx.addFlightSqlDeferredExecutor(shortQuery);
        ctx.addFlightSqlDeferredExecutor(longQuery);
        long start = ctx.getStartTime();
        Assertions.assertEquals(5_000L, FlightProtocolAdapter.deferredBoundMs(shortQuery, 3));
        Assertions.assertEquals(20_000L, FlightProtocolAdapter.deferredBoundMs(longQuery, 3));

        ctx.checkTimeout(start + 5_000L);
        Mockito.verify(shortQuery, Mockito.never()).finalizeArrowFlightQuery();
        Mockito.verify(longQuery, Mockito.never()).finalizeArrowFlightQuery();

        ctx.checkTimeout(start + 5_001L);
        Mockito.verify(shortQuery).finalizeArrowFlightQuery();
        Mockito.verify(longQuery, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertEquals(Lists.newArrayList(longQuery), ctx.getFlightSqlDeferredExecutors());

        ctx.checkTimeout(start + 20_000L);
        Mockito.verify(longQuery, Mockito.never()).finalizeArrowFlightQuery();
        ctx.checkTimeout(start + 20_001L);
        Mockito.verify(longQuery).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
    }

    // And each is reaped by its own deadline, start included: of the statements of one request the
    // later ones started later (each statement sets the start time anew), and an overdue one does
    // not take a younger one with it -- the client may be pulling the younger one's results still.
    @Test
    public void testAnOverdueDeferredQueryDoesNotTakeAYoungerOneWithIt() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        long now = ctx.getStartTime();
        StmtExecutor old = deferredExecutor(now - 100_000L, 5);
        StmtExecutor young = deferredExecutor(now, 20);
        ctx.addFlightSqlDeferredExecutor(old);
        ctx.addFlightSqlDeferredExecutor(young);

        // The earliest start with the longest timeout would make both overdue at once.
        ctx.checkTimeout(now + 1L);
        Mockito.verify(old).finalizeArrowFlightQuery();
        Mockito.verify(young, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertEquals(Lists.newArrayList(young), ctx.getFlightSqlDeferredExecutors());

        ctx.checkTimeout(now + 20_000L);
        Mockito.verify(young, Mockito.never()).finalizeArrowFlightQuery();
        ctx.checkTimeout(now + 20_001L);
        Mockito.verify(young).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
    }

    // The timeout checker does not run as a command of the session: it may interleave with the
    // session's next request, which takes the deferred executors of the previous one
    // (beginRequest) and defers its own. Deciding and taking are one step, so an executor is
    // finalized exactly once, by whichever of the two took it, and one deferred after the checker
    // decided is not one it decided about.
    @Test
    public void testTheCheckerAndTheNextRequestFinalizeAnExecutorExactlyOnce() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        FlightProtocolAdapter adapter = FlightProtocolAdapter.of(ctx);
        long now = ctx.getStartTime();

        // The checker takes the overdue executor; the next request begins and defers a query of its
        // own before the checker gets to finalize what it took.
        StmtExecutor overdue = deferredExecutor(now - 100_000L, 5);
        ctx.addFlightSqlDeferredExecutor(overdue);
        List<StmtExecutor> taken = adapter.takeExpiredDeferredExecutors(now);
        Assertions.assertEquals(Lists.newArrayList(overdue), taken);
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
        adapter.beginRequest();
        StmtExecutor next = deferredExecutor(now, 5);
        ctx.addFlightSqlDeferredExecutor(next);
        FlightProtocolAdapter.finalizeDeferredExecutors(taken);
        Mockito.verify(overdue).finalizeArrowFlightQuery();
        Mockito.verify(next, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertEquals(Lists.newArrayList(next), ctx.getFlightSqlDeferredExecutors());
        // A later tick of the checker finds the young one within its bound, the old one gone.
        Assertions.assertTrue(adapter.takeExpiredDeferredExecutors(now + 1L).isEmpty());
        Mockito.verify(overdue, Mockito.times(1)).finalizeArrowFlightQuery();

        // The other way round: the next request took the overdue executor first; the checker's
        // decision, made after, finds nothing overdue and does not finalize it again.
        StmtExecutor overdueToo = deferredExecutor(now - 100_000L, 5);
        adapter.beginRequest();
        Mockito.verify(next).finalizeArrowFlightQuery();
        ctx.addFlightSqlDeferredExecutor(overdueToo);
        adapter.beginRequest();
        Mockito.verify(overdueToo).finalizeArrowFlightQuery();
        Assertions.assertTrue(adapter.takeExpiredDeferredExecutors(now + 1L).isEmpty());
        ctx.checkTimeout(now + 1L);
        Mockito.verify(overdueToo, Mockito.times(1)).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
    }

    // One executor failing to finalize does not keep the next from being finalized.
    @Test
    public void testAFailingFinalizationDoesNotStopTheOthers() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        long now = ctx.getStartTime();
        StmtExecutor failing = deferredExecutor(now - 100_000L, 5);
        Mockito.doThrow(new RuntimeException("boom")).when(failing).finalizeArrowFlightQuery();
        StmtExecutor other = deferredExecutor(now - 100_000L, 5);
        ctx.addFlightSqlDeferredExecutor(failing);
        ctx.addFlightSqlDeferredExecutor(other);

        ctx.checkTimeout(now + 1L);
        Mockito.verify(failing).finalizeArrowFlightQuery();
        Mockito.verify(other).finalizeArrowFlightQuery();
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testZeroDisablesTheReaper() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 0;
        ConnectContext ctx = sleepingFlightSession();
        StmtExecutor deferred = deferredExecutor(ctx, 5);
        ctx.addFlightSqlDeferredExecutor(deferred);

        // idle for almost the whole wait_timeout: nothing is released and the session is alive
        long waitTimeoutMs = ctx.getSessionVariable().getWaitTimeoutS() * 1000L;
        ctx.checkTimeout(ctx.getStartTime() + waitTimeoutMs - 1);
        Mockito.verify(deferred, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertEquals(Lists.newArrayList(deferred), ctx.getFlightSqlDeferredExecutors());
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testNothingDeferredMeansNothingToReap() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());
        Assertions.assertTrue(FlightProtocolAdapter.of(ctx).takeExpiredDeferredExecutors(Long.MAX_VALUE).isEmpty());

        ctx.checkTimeout(ctx.getStartTime() + 3_600_000L);
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testMysqlSessionIsUntouched() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 1;
        ConnectContext ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setStartTime();
        Assertions.assertTrue(ctx.getFlightSqlDeferredExecutors().isEmpty());

        // idle far beyond the Flight bound but within wait_timeout: still alive
        ctx.checkTimeout(ctx.getStartTime() + 3_600_000L);
        Assertions.assertFalse(ctx.isKilled());
    }
}
