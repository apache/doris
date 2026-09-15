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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The idle reaper for deferred Arrow Flight queries (#67503). A sleeping Flight session whose last
 * query kept its coordinator alive (an external-table scan in batch mode, see #62259) gets that
 * coordinator finalized by the connection timeout checker once
 * arrow_flight_deferred_query_idle_timeout_second, floored at the execution timeout the query ran
 * with, has passed since the query started. The session itself is not killed, wait_timeout still
 * governs that, and a MySQL session is untouched.
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
        Assertions.assertEquals(7L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());
        Assertions.assertEquals(start, ctx.getFlightSqlDeferredExecutorsStartTimeMs());

        // not idle for long enough yet
        ctx.checkTimeout(start + 7_000L);
        Mockito.verify(deferred, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());

        // past the bound: the deferred coordinator is finalized and the session survives
        ctx.checkTimeout(start + 7_001L);
        Mockito.verify(deferred).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());

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
        Assertions.assertEquals(queryStart, ctx.getFlightSqlDeferredExecutorsStartTimeMs());

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

    @Test
    public void testBoundIsFlooredAtTheExecTimeoutTheDeferredQueryRanWith() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 3;
        ConnectContext ctx = sleepingFlightSession();
        StmtExecutor shortQuery = deferredExecutor(ctx, 5);
        StmtExecutor longQuery = deferredExecutor(ctx, 20);
        ctx.addFlightSqlDeferredExecutor(shortQuery);
        ctx.addFlightSqlDeferredExecutor(longQuery);
        long start = ctx.getStartTime();
        // the longest deferred query wins: a client may still be pulling its results from the BE
        Assertions.assertEquals(20L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());

        ctx.checkTimeout(start + 19_999L);
        Mockito.verify(shortQuery, Mockito.never()).finalizeArrowFlightQuery();
        Mockito.verify(longQuery, Mockito.never()).finalizeArrowFlightQuery();

        ctx.checkTimeout(start + 20_001L);
        Mockito.verify(shortQuery).finalizeArrowFlightQuery();
        Mockito.verify(longQuery).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testZeroDisablesTheReaper() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 0;
        ConnectContext ctx = sleepingFlightSession();
        StmtExecutor deferred = deferredExecutor(ctx, 5);
        ctx.addFlightSqlDeferredExecutor(deferred);
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());

        // idle for almost the whole wait_timeout: nothing is released and the session is alive
        long waitTimeoutMs = ctx.getSessionVariable().getWaitTimeoutS() * 1000L;
        ctx.checkTimeout(ctx.getStartTime() + waitTimeoutMs - 1);
        Mockito.verify(deferred, Mockito.never()).finalizeArrowFlightQuery();
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testNothingDeferredMeansNoBound() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        ConnectContext ctx = sleepingFlightSession();
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsStartTimeMs());

        ctx.checkTimeout(ctx.getStartTime() + 3_600_000L);
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testMysqlSessionIsUntouched() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 1;
        ConnectContext ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setStartTime();
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsStartTimeMs());

        // idle far beyond the Flight bound but within wait_timeout: still alive
        ctx.checkTimeout(ctx.getStartTime() + 3_600_000L);
        Assertions.assertFalse(ctx.isKilled());
    }
}
