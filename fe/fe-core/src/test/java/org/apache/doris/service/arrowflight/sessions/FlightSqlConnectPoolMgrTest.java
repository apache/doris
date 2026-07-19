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

import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class FlightSqlConnectPoolMgrTest {

    // Arrow Flight SQL keeps a query's coordinator alive across GetFlightInfo -> DoGet (see #62259).
    // unregisterConnection() is the catch-all teardown path: idle/query timeout, bearer token expiry
    // and explicit CloseSession all reach here. It must finalize the deferred coordinators so an
    // abandoned connection cannot leak them (the external-table batch SplitSource and the query
    // queue slot the coordinator holds).
    @Test
    public void testUnregisterConnectionFinalizesDeferredExecutors() {
        FlightSqlConnectPoolMgr poolMgr = new FlightSqlConnectPoolMgr(100);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);

        poolMgr.unregisterConnection(ctx);

        // The deferred coordinators must be released on teardown even though this connection was
        // never registered in the pool (an abandoned connection is still cleaned up, not leaked).
        Mockito.verify(ctx).closeFlightSqlDeferredExecutors();
    }

    // Cleanup must run before the connection bookkeeping (closeTxn / map removal), so that a failure
    // there cannot strand the deferred coordinators. Verify the deferred executors are finalized
    // even when the context is the one stored in the pool.
    @Test
    public void testUnregisterRegisteredConnectionFinalizesDeferredExecutors() {
        FlightSqlConnectPoolMgr poolMgr = new FlightSqlConnectPoolMgr(100);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getConnectionId()).thenReturn(7);
        Mockito.when(ctx.getConnectType()).thenReturn(ConnectContext.ConnectType.ARROW_FLIGHT_SQL);
        Mockito.when(ctx.getPeerIdentity()).thenReturn("token-7");
        poolMgr.getConnectionMap().put(7, ctx);

        poolMgr.unregisterConnection(ctx);

        Mockito.verify(ctx).closeFlightSqlDeferredExecutors();
        Assert.assertNull(poolMgr.getConnectionMap().get(7));
    }
}
