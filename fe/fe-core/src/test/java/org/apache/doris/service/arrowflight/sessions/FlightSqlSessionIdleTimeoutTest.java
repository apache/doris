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
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The Flight-only idle bound: a Flight session's idle timeout is
 * min(wait_timeout, arrow_flight_session_idle_timeout_second); it never widens wait_timeout,
 * 0 disables it, and a MySQL-protocol context is untouched.
 */
public class FlightSqlSessionIdleTimeoutTest {

    @BeforeAll
    public static void setUp() {
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testFlightIdleBoundOnlyTightensWaitTimeout() {
        int saved = Config.arrow_flight_session_idle_timeout_second;
        try {
            FlightSqlConnectContext ctx = new FlightSqlConnectContext("test-peer-identity");
            long waitTimeoutS = ctx.getSessionVariable().getWaitTimeoutS();
            Assert.assertTrue(waitTimeoutS > 0);
            // the bound is floored at the query's exec timeout; pin it low so the bound is visible
            ctx.getSessionVariable().setQueryTimeoutS(5);

            // tighter than wait_timeout and above the exec timeout -> the Flight bound wins
            Config.arrow_flight_session_idle_timeout_second = 7;
            Assert.assertEquals(7L, ctx.getIdleTimeoutS());

            // below the exec timeout -> the exec timeout floors it (a result drain is never
            // killed before query_timeout would kill the query)
            Config.arrow_flight_session_idle_timeout_second = 3;
            Assert.assertEquals(5L, ctx.getIdleTimeoutS());
            ctx.getSessionVariable().setQueryTimeoutS(20);
            Assert.assertEquals(20L, ctx.getIdleTimeoutS());
            ctx.getSessionVariable().setQueryTimeoutS(5);

            // looser than wait_timeout -> wait_timeout still applies (the bound never widens it)
            Config.arrow_flight_session_idle_timeout_second = (int) Math.min(Integer.MAX_VALUE, waitTimeoutS + 1000);
            Assert.assertEquals(waitTimeoutS, ctx.getIdleTimeoutS());

            // 0 disables the Flight-specific bound
            Config.arrow_flight_session_idle_timeout_second = 0;
            Assert.assertEquals(waitTimeoutS, ctx.getIdleTimeoutS());
        } finally {
            Config.arrow_flight_session_idle_timeout_second = saved;
        }
    }

    @Test
    public void testMysqlContextKeepsWaitTimeout() {
        int saved = Config.arrow_flight_session_idle_timeout_second;
        try {
            Config.arrow_flight_session_idle_timeout_second = 7;
            ConnectContext ctx = new ConnectContext();
            Assert.assertEquals(ctx.getSessionVariable().getWaitTimeoutS(), ctx.getIdleTimeoutS());
        } finally {
            Config.arrow_flight_session_idle_timeout_second = saved;
        }
    }
}
