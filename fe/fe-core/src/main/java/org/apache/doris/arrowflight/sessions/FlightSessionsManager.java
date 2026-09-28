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
// This file is copied from

package org.apache.doris.arrowflight.sessions;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.auth2.FlightAuthResult;
import org.apache.doris.catalog.Env;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;

/**
 * The Arrow Flight SQL sessions of this frontend and the bearer tokens they are known by.
 *
 * <p>A bearer token is the credential of exactly one session, and lives exactly as long as it:
 * the token is issued when the session is opened, at the handshake that authenticated the user's
 * password, and is invalid from the moment the session ends - whether the client closed it
 * (CloseSession), another connection killed it (KILL CONNECTION), the timeout checker ended it
 * (wait_timeout) or the frontend restarted. There is no second store of tokens that could outlive
 * a session or end one on its own: the connection pool's index of sessions by peer identity is
 * the only place a token exists.
 */
public interface FlightSessionsManager {

    /**
     * Opens a session for a user whose credentials were just authenticated, registering it in the
     * connection pool as a connection of the user's, and returns the bearer token the session is
     * known by from now on.
     *
     * @throws org.apache.arrow.flight.FlightRuntimeException with {@code RESOURCE_EXHAUSTED} when the
     *         pool refuses the session (the pool's limit, the Arrow Flight SQL sub-quota or the
     *         user's limit is reached), in the words a MySQL client is refused in; no session and
     *         no token exist then
     */
    String openSession(FlightAuthResult authResult);

    /**
     * The session known by this bearer token (the peer identity of an authenticated call).
     *
     * @throws org.apache.arrow.flight.FlightRuntimeException with {@code UNAUTHENTICATED} when no
     *         session is open under the token on this frontend: it was never issued here, or its
     *         session has ended
     */
    ConnectContext getConnectContext(String peerIdentity);

    /**
     * Ends the session known by this bearer token, the client's own CloseSession: nothing is held
     * for it afterwards and every later call under the token is {@code UNAUTHENTICATED}. Ending a
     * session that has already ended is not an error.
     */
    void closeConnectContext(String peerIdentity);

    static ConnectContext buildConnectContext(String peerIdentity, UserIdentity userIdentity, String remoteIP,
            ConnectScheduler connectScheduler) {
        ConnectContext connectContext = ConnectContext.forFlight(peerIdentity);
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setStartTime();
        connectContext.setCurrentUserIdentity(userIdentity);
        connectContext.setRemoteIP(remoteIP);
        connectContext.setUserQueryTimeout(
                connectContext.getEnv().getAuth().getQueryTimeout(connectContext.getQualifiedUser()));
        connectContext.setUserInsertTimeout(
                connectContext.getEnv().getAuth().getInsertTimeout(connectContext.getQualifiedUser()));

        connectContext.setConnectScheduler(connectScheduler);
        return connectContext;
    }
}
