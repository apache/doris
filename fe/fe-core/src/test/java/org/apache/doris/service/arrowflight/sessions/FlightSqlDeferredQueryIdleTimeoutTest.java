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

package org.apache.doris.service.arrowflight.sessions;

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
 * coordinator finalized by the connection timeout checker once the session has been idle for
 * arrow_flight_deferred_query_idle_timeout_second, floored at the execution timeout the query ran
 * with. The session itself is not killed, wait_timeout still governs that, and a MySQL session is
 * untouched.
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

    private static StmtExecutor deferredExecutor(int execTimeoutS) {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getDeferredExecTimeoutS()).thenReturn(execTimeoutS);
        return executor;
    }

    // A Flight session that ran a query and has been sleeping since; the client never closed it.
    private static FlightSqlConnectContext sleepingFlightSession(StmtExecutor... deferred) {
        FlightSqlConnectContext ctx = new FlightSqlConnectContext("test-peer-identity");
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setStartTime();
        for (StmtExecutor executor : deferred) {
            ctx.addFlightSqlDeferredExecutor(executor);
        }
        return ctx;
    }

    @Test
    public void testIdleSessionReleasesDeferredQueryButIsNotKilled() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 7;
        StmtExecutor deferred = deferredExecutor(5);
        FlightSqlConnectContext ctx = sleepingFlightSession(deferred);
        long start = ctx.getStartTime();
        Assertions.assertEquals(7L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());

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

    @Test
    public void testBoundIsFlooredAtTheExecTimeoutTheDeferredQueryRanWith() {
        Config.arrow_flight_deferred_query_idle_timeout_second = 3;
        StmtExecutor shortQuery = deferredExecutor(5);
        StmtExecutor longQuery = deferredExecutor(20);
        FlightSqlConnectContext ctx = sleepingFlightSession(shortQuery, longQuery);
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
        StmtExecutor deferred = deferredExecutor(5);
        FlightSqlConnectContext ctx = sleepingFlightSession(deferred);
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
        FlightSqlConnectContext ctx = sleepingFlightSession();
        Assertions.assertEquals(-1L, ctx.getFlightSqlDeferredExecutorsIdleTimeoutS());

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

        // idle far beyond the Flight bound but within wait_timeout: still alive
        ctx.checkTimeout(ctx.getStartTime() + 3_600_000L);
        Assertions.assertFalse(ctx.isKilled());
    }
}
