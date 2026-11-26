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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;

import org.mockito.Mockito;

/**
 * Connections as the pool sees them, for tests that register real contexts: the pool asks the
 * connection's Env for the user's connection limit and files the connection under its user, so a
 * context needs both before it can be registered.
 */
public final class ConnectPoolTestSupport {

    private ConnectPoolTestSupport() {
    }

    /** An Env whose Auth allows every user {@code maxUserConnections} connections. */
    public static Env envAllowing(long maxUserConnections) {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(auth.getMaxConn(Mockito.anyString())).thenReturn(maxUserConnections);
        Mockito.when(env.getAuth()).thenReturn(auth);
        // ConnectContext.setEnv starts the connection in the internal catalog.
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        return env;
    }

    /** A MySQL connection of {@code user}, registrable in a pool. */
    public static ConnectContext mysqlConnection(Env env, UserIdentity user) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(user);
        return ctx;
    }

    /** An Arrow Flight SQL session of {@code user} under the bearer token {@code peerIdentity}, registrable in a pool. */
    public static ConnectContext flightSession(Env env, UserIdentity user, String peerIdentity) {
        ConnectContext ctx = ConnectContext.forFlight(peerIdentity);
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(user);
        return ctx;
    }
}
