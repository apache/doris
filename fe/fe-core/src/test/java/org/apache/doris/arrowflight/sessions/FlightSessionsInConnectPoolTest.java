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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.auth2.FlightAuthResult;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.TokenMasker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolTestSupport;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.StmtExecutor;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * A bearer token is the name an Arrow Flight SQL session is registered in the connection pool
 * under, and nothing else: it is issued by opening the session and is invalid the moment the
 * session leaves the pool, by whichever path - CloseSession, KILL CONNECTION or wait_timeout.
 */
public class FlightSessionsInConnectPoolTest {
    private static final UserIdentity ALICE = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
    private static final FlightAuthResult ALICE_AUTH = FlightAuthResult.of("alice", ALICE, "10.26.20.3");

    private boolean savedRunningUnitTest;
    private MockedStatic<Env> mockedEnv;
    private Env env;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
        // The session takes its Env - and with it the user's limits - from Env.getCurrentEnv().
        env = ConnectPoolTestSupport.envAllowing(100);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    private static FlightStatusCode statusOf(Runnable call) {
        FlightRuntimeException e = Assertions.assertThrows(FlightRuntimeException.class, call::run);
        return e.status().code();
    }

    @Test
    public void testOpeningASessionIssuesTheTokenItIsRegisteredUnder() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 5);
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(scheduler);

        String token = sessions.openSession(ALICE_AUTH);
        ConnectContext ctx = sessions.getConnectContext(token);

        Assertions.assertSame(scheduler.getContextWithPeerIdentity(token), ctx);
        Assertions.assertSame(scheduler.getContext(ctx.getConnectionId()), ctx);
        Assertions.assertEquals(token, ctx.getPeerIdentity());
        Assertions.assertEquals(ConnectType.ARROW_FLIGHT_SQL, ctx.getConnectType());
        Assertions.assertEquals(ALICE, ctx.getCurrentUserIdentity());
        Assertions.assertEquals("10.26.20.3", ctx.getRemoteIP());
        Assertions.assertSame(scheduler, ctx.getConnectScheduler());
        Assertions.assertEquals(1, scheduler.getConnectionNum());
        Assertions.assertEquals(1, scheduler.getConnectPoolMgr().getFlightConnectionNum());

        // Every session has a token of its own, and each names only its own session.
        String other = sessions.openSession(ALICE_AUTH);
        Assertions.assertNotEquals(token, other);
        Assertions.assertNotSame(ctx, sessions.getConnectContext(other));
        Assertions.assertSame(ctx, sessions.getConnectContext(token));
        Assertions.assertEquals(2, scheduler.getConnectionNum());
    }

    @Test
    public void testATokenNoSessionIsRegisteredUnderIsUnauthenticated() {
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(new ConnectScheduler(10, 5));
        String token = sessions.newToken();

        FlightRuntimeException e = Assertions.assertThrows(FlightRuntimeException.class,
                () -> sessions.getConnectContext(token));

        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
        // The description names the token by its masked id, never by the token itself.
        Assertions.assertTrue(e.status().description().contains(TokenMasker.tokenId(token)),
                e.status().description());
        Assertions.assertFalse(e.status().description().contains(token));
    }

    @Test
    public void testCloseSessionEndsTheSessionAndWithItTheToken() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 5);
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(scheduler);
        String token = sessions.openSession(ALICE_AUTH);
        ConnectContext ctx = sessions.getConnectContext(token);
        // A statement may still be running on another thread when the client closes the session.
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);

        sessions.closeConnectContext(token);

        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertEquals(0, scheduler.getConnectPoolMgr().getFlightConnectionNum());
        Assertions.assertNull(scheduler.getContext(ctx.getConnectionId()));
        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, statusOf(() -> sessions.getConnectContext(token)));
        ArgumentCaptor<Status> cancelled = ArgumentCaptor.forClass(Status.class);
        Mockito.verify(running).cancel(cancelled.capture());
        Assertions.assertTrue(cancelled.getValue().getErrorMsg().contains("CloseSession"),
                cancelled.getValue().getErrorMsg());
        // Closing a session that has already ended is nothing to complain about.
        Assertions.assertDoesNotThrow(() -> sessions.closeConnectContext(token));
    }

    @Test
    public void testKillConnectionEndsTheToken() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 5);
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(scheduler);
        String token = sessions.openSession(ALICE_AUTH);

        sessions.getConnectContext(token).kill(true);

        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, statusOf(() -> sessions.getConnectContext(token)));
    }

    @Test
    public void testWaitTimeoutEndsTheToken() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 5);
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(scheduler);
        String token = sessions.openSession(ALICE_AUTH);
        ConnectContext ctx = sessions.getConnectContext(token);
        long waitTimeoutMs = ctx.getSessionVariable().getWaitTimeoutS() * 1000L;

        // Idle, but not yet past wait_timeout: the session and its token stay.
        scheduler.getConnectPoolMgr().timeoutChecker(ctx.getStartTime() + waitTimeoutMs);
        Assertions.assertSame(ctx, sessions.getConnectContext(token));

        scheduler.getConnectPoolMgr().timeoutChecker(ctx.getStartTime() + waitTimeoutMs + 1);

        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, statusOf(() -> sessions.getConnectContext(token)));
    }

    // The pool's quotas are the only admission control: a session that does not fit is refused at
    // the handshake, with the pool's own words, and neither a session nor a token is left of it -
    // no session already open is evicted to make room.
    @Test
    public void testASessionThatDoesNotFitIsRefusedAndLeavesNothing() {
        ConnectScheduler scheduler = new ConnectScheduler(10, 1);
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(scheduler);
        String first = sessions.openSession(ALICE_AUTH);

        FlightRuntimeException refused = Assertions.assertThrows(FlightRuntimeException.class,
                () -> sessions.openSession(ALICE_AUTH));

        Assertions.assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, refused.status().code());
        Assertions.assertEquals("Reach limit of connections. Total: 10, User: 100, Current: 1, Arrow Flight SQL: 1"
                + " (current: 1)", refused.status().description());
        Assertions.assertEquals(1, scheduler.getConnectionNum());
        Assertions.assertSame(scheduler.getContextWithPeerIdentity(first), sessions.getConnectContext(first));
    }

    @Test
    public void testTokensAreLongRandomBase32Strings() {
        FlightSessionsInConnectPool sessions = new FlightSessionsInConnectPool(new ConnectScheduler(10, 5));
        String a = sessions.newToken();
        String b = sessions.newToken();

        Assertions.assertNotEquals(a, b);
        // 130 random bits in base 32: 26 characters at most, from the digits and a-v.
        Assertions.assertTrue(a.matches("[0-9a-v]{20,26}"), a);
    }
}
