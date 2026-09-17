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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

/**
 * One pool for every protocol: MySQL connections and Arrow Flight SQL sessions share the pool's
 * limit and the user's limit, Flight sessions additionally their sub-quota, and every teardown
 * path meets in unregisterConnection.
 */
public class ConnectPoolMgrTest {

    private static final UserIdentity ALICE = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
    private static final UserIdentity BOB = UserIdentity.createAnalyzedUserIdentWithIp("bob", "%");

    private static ConnectContext registered(ConnectPoolMgr pool, ConnectContext ctx, int connectionId) {
        ctx.setConnectionId(connectionId);
        Assertions.assertEquals(-1, pool.registerConnection(ctx));
        return ctx;
    }

    @Test
    public void testBothProtocolsShareThePoolsLimit() {
        Env env = ConnectPoolTestSupport.envAllowing(100);
        ConnectPoolMgr pool = new ConnectPoolMgr(2);

        ConnectContext mysql = registered(pool, ConnectPoolTestSupport.mysqlConnection(env, ALICE), 1);
        ConnectContext flight = registered(pool, ConnectPoolTestSupport.flightSession(env, BOB, "token-2"), 2);
        Assertions.assertEquals(2, pool.getConnectionNum());
        Assertions.assertEquals(1, pool.getFlightConnectionNum());

        // The third connection is refused whichever protocol it speaks, with the count it was refused at,
        // and a refused registration changes no count: not the pool's, not the Flight share, not the user's.
        Assertions.assertEquals(2, pool.registerConnection(ConnectPoolTestSupport.mysqlConnection(env, ALICE)));
        Assertions.assertEquals(2,
                pool.registerConnection(ConnectPoolTestSupport.flightSession(env, ALICE, "token-3")));
        Assertions.assertEquals(2, pool.getConnectionNum());
        Assertions.assertEquals(1, pool.getFlightConnectionNum());
        Assertions.assertEquals(1, pool.getUserConnectionMap().get(ALICE.getQualifiedUser()).get());
        Assertions.assertEquals(1, pool.getUserConnectionMap().get(BOB.getQualifiedUser()).get());

        Assertions.assertSame(flight, pool.getContextWithPeerIdentity("token-2"));
        Assertions.assertNull(pool.getContextWithPeerIdentity("token-3"));
        Assertions.assertSame(mysql, pool.getContext(1));

        pool.unregisterConnection(mysql);
        Assertions.assertEquals(-1,
                pool.registerConnection(ConnectPoolTestSupport.flightSession(env, ALICE, "token-3")));
    }

    @Test
    public void testAUsersLimitCountsBothProtocols() {
        Env env = ConnectPoolTestSupport.envAllowing(1);
        ConnectPoolMgr pool = new ConnectPoolMgr(10);

        registered(pool, ConnectPoolTestSupport.mysqlConnection(env, ALICE), 1);
        // Alice's one connection is taken by MySQL: her Flight session is refused...
        Assertions.assertEquals(1, pool.registerConnection(ConnectPoolTestSupport.flightSession(env, ALICE, "a")));
        // ...and nothing of the refused registration lingers: not in the pool, not counted anywhere.
        Assertions.assertEquals(1, pool.getConnectionNum());
        Assertions.assertEquals(0, pool.getFlightConnectionNum());
        Assertions.assertEquals(1, pool.getUserConnectionMap().get(ALICE.getQualifiedUser()).get());
        Assertions.assertNull(pool.getContextWithPeerIdentity("a"));
        // Bob is not affected.
        registered(pool, ConnectPoolTestSupport.flightSession(env, BOB, "b"), 2);
    }

    @Test
    public void testTheFlightSubQuotaIsEnforcedWithinThePoolsLimit() {
        Env env = ConnectPoolTestSupport.envAllowing(100);
        ConnectPoolMgr pool = new ConnectPoolMgr(10, 1);
        Assertions.assertEquals(1, pool.getFlightMaxConnections());

        registered(pool, ConnectPoolTestSupport.flightSession(env, ALICE, "a"), 1);
        Assertions.assertEquals(1, pool.registerConnection(ConnectPoolTestSupport.flightSession(env, BOB, "b")));
        Assertions.assertEquals(1, pool.getConnectionNum());
        Assertions.assertEquals(1, pool.getFlightConnectionNum());
        // MySQL connections are not held to the Flight sub-quota.
        registered(pool, ConnectPoolTestSupport.mysqlConnection(env, BOB), 2);
        Assertions.assertEquals(2, pool.getConnectionNum());
    }

    @Test
    public void testTheFlightSubQuotaFollowsThePoolsLimitUnlessSetAndNeverExceedsIt() {
        Assertions.assertEquals(10, ConnectPoolMgr.effectiveFlightMaxConnections(10, -1));
        Assertions.assertEquals(10, new ConnectPoolMgr(10).getFlightMaxConnections());
        Assertions.assertEquals(3, ConnectPoolMgr.effectiveFlightMaxConnections(10, 3));
        Assertions.assertEquals(10, ConnectPoolMgr.effectiveFlightMaxConnections(10, 4096));
    }

    @Test
    public void testTheRefusalReadsTheSameForEveryProtocol() {
        Env env = ConnectPoolTestSupport.envAllowing(5);
        ConnectPoolMgr pool = new ConnectPoolMgr(10);
        Assertions.assertEquals("Reach limit of connections. Total: 10, User: 5, Current: 10",
                pool.limitReachedMessage(ConnectPoolTestSupport.mysqlConnection(env, ALICE), 10));
        Assertions.assertEquals("Reach limit of connections. Total: 10, User: 5, Current: 10",
                pool.limitReachedMessage(ConnectPoolTestSupport.flightSession(env, ALICE, "a"), 10));

        // The Flight sub-quota is named only when it is tighter than the pool's limit.
        ConnectPoolMgr quota = new ConnectPoolMgr(10, 2);
        registered(quota, ConnectPoolTestSupport.flightSession(env, BOB, "b"), 1);
        Assertions.assertEquals(
                "Reach limit of connections. Total: 10, User: 5, Current: 2, Arrow Flight SQL: 2 (current: 1)",
                quota.limitReachedMessage(ConnectPoolTestSupport.flightSession(env, ALICE, "a"), 2));
        Assertions.assertEquals("Reach limit of connections. Total: 10, User: 5, Current: 2",
                quota.limitReachedMessage(ConnectPoolTestSupport.mysqlConnection(env, ALICE), 2));
    }

    // Arrow Flight SQL keeps a query's coordinator alive across GetFlightInfo -> DoGet (see #62259).
    // unregisterConnection() is the catch-all teardown path: idle/query timeout, bearer token expiry
    // and explicit CloseSession all reach here. The protocol must release what it holds for the
    // session -- for Flight the channel-cached results and the deferred coordinators -- even for a
    // connection that was never registered (an abandoned connection is cleaned up, not leaked), and
    // before the bookkeeping, so that a failure there cannot strand the coordinators.
    @Test
    public void testUnregisterReleasesTheProtocolSessionFirstEvenWhenNotRegistered() {
        ConnectPoolMgr pool = new ConnectPoolMgr(100);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);

        pool.unregisterConnection(ctx);

        InOrder inOrder = Mockito.inOrder(ctx);
        inOrder.verify(ctx).releaseProtocolSession();
        inOrder.verify(ctx).closeTxn();
    }

    @Test
    public void testUnregisterRemovesAFlightSessionAndItsPeerIdentity() {
        Env env = ConnectPoolTestSupport.envAllowing(100);
        ConnectPoolMgr pool = new ConnectPoolMgr(100, 1);
        ConnectContext ctx = registered(pool, ConnectPoolTestSupport.flightSession(env, ALICE, "token-7"), 7);
        Assertions.assertSame(ctx, pool.getContextWithPeerIdentity("token-7"));

        pool.unregisterConnection(ctx);

        Assertions.assertNull(pool.getContext(7));
        Assertions.assertNull(pool.getContextWithPeerIdentity("token-7"));
        Assertions.assertEquals(0, pool.getConnectionNum());
        Assertions.assertEquals(0, pool.getFlightConnectionNum());
        Assertions.assertEquals(0, pool.getUserConnectionMap().get(ALICE.getQualifiedUser()).get());
        // The sub-quota slot is free again.
        registered(pool, ConnectPoolTestSupport.flightSession(env, ALICE, "token-8"), 8);
        // Unregistering twice is harmless.
        pool.unregisterConnection(ctx);
        Assertions.assertEquals(1, pool.getConnectionNum());
    }
}
