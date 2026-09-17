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

import org.apache.doris.arrowflight.tokens.FlightTokenDetails;
import org.apache.doris.arrowflight.tokens.FlightTokenManager;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.TokenMasker;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.service.ExecuteEnv;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlightSessionsWithTokenManager implements FlightSessionsManager {
    private static final Logger LOG = LogManager.getLogger(FlightSessionsWithTokenManager.class);

    private final FlightTokenManager flightTokenManager;

    public FlightSessionsWithTokenManager(FlightTokenManager flightTokenManager) {
        this.flightTokenManager = flightTokenManager;
    }

    @Override
    public ConnectContext getConnectContext(String peerIdentity) {
        try {
            ConnectContext connectContext = ExecuteEnv.getInstance().getScheduler()
                    .getContextWithPeerIdentity(peerIdentity);
            if (null == connectContext) {
                connectContext = createConnectContext(peerIdentity);
                return connectContext;
            }
            return connectContext;
        } catch (FlightRuntimeException e) {
            // Already the status the client is meant to see (a connection refused for its limit).
            throw e;
        } catch (Exception e) {
            LOG.warn("get ConnectContext failed, " + e.getMessage(), e);
            throw CallStatus.INTERNAL.withDescription(Util.getRootCauseMessage(e)).withCause(e).toRuntimeException();
        }
    }

    @Override
    public ConnectContext createConnectContext(String peerIdentity) {
        final FlightTokenDetails flightTokenDetails = flightTokenManager.validateToken(peerIdentity);
        if (flightTokenDetails.getCreatedSession()) {
            flightTokenManager.invalidateToken(peerIdentity);
            throw new IllegalArgumentException("UserSession expire after access, try reconnect, bearer token id: "
                    + TokenMasker.tokenId(peerIdentity)
                    + ", a peerIdentity(bearer token) can only create a ConnectContext once. "
                    + "if ConnectContext is deleted without operation for a long time, it needs to be reconnected "
                    + "(at the same time obtain a new bearer token).");
        }
        flightTokenDetails.setCreatedSession(true);
        ConnectContext connectContext = FlightSessionsManager.buildConnectContext(peerIdentity,
                flightTokenDetails.getUserIdentity(), flightTokenDetails.getRemoteIp());
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        connectScheduler.submit(connectContext);
        // The one pool every protocol registers in: qe_max_connection, the user's
        // max_user_connections and the Arrow Flight SQL sub-quota, refused in the words a MySQL
        // client is refused in. The token goes with the refusal, so that the client does not keep a
        // credential that can never open a session.
        ConnectPoolMgr pool = connectScheduler.getConnectPoolMgr();
        int res = pool.registerConnection(connectContext);
        if (res >= 0) {
            String errMsg = pool.limitReachedMessage(connectContext, res);
            connectContext.getState().setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, errMsg);
            // The refused session never entered the pool, so nothing else releases what its
            // adapter allocated (the channel's allocator).
            connectContext.releaseProtocolSession();
            flightTokenManager.invalidateToken(peerIdentity);
            LOG.warn("refuse arrow flight sql session, bearer token id: {}, user: {}: {}",
                    TokenMasker.tokenId(peerIdentity), connectContext.getQualifiedUser(), errMsg);
            throw CallStatus.RESOURCE_EXHAUSTED.withDescription(errMsg).toRuntimeException();
        }
        return connectContext;
    }

    @Override
    public void closeConnectContext(String peerIdentity) {
        flightTokenManager.invalidateToken(peerIdentity);
    }
}
