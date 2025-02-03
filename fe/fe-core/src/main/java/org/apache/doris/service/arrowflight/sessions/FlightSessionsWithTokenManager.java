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

import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.arrowflight.tokens.FlightTokenDetails;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import org.apache.arrow.flight.CallStatus;
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
            ConnectContext connectContext = ExecuteEnv.getInstance().getScheduler().getContext(peerIdentity);
            if (null == connectContext) {
                connectContext = createConnectContext(peerIdentity);
                return connectContext;
            }
            return connectContext;
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
            throw new IllegalArgumentException("UserSession expire after access, try reconnect, bearer token: "
                    + peerIdentity + ", a peerIdentity(bearer token) can only create a ConnectContext once. "
                    + "if ConnectContext is deleted without operation for a long time, it needs to be reconnected "
                    + "(at the same time obtain a new bearer token).");
        }
        flightTokenDetails.setCreatedSession(true);
        ConnectContext connectContext = FlightSessionsManager.buildConnectContext(peerIdentity,
                flightTokenDetails.getUserIdentity(), flightTokenDetails.getRemoteIp());
        ExecuteEnv.getInstance().getScheduler().submit(connectContext);
        if (!ExecuteEnv.getInstance().getScheduler().registerConnection(connectContext)) {
            String err = "Reach limit of connections, increase `qe_max_connection` in fe.conf, or decrease "
                    + "`arrow_flight_token_cache_size` to evict unused bearer tokens and it connections faster";
            connectContext.getState().setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, err);
            throw new IllegalArgumentException(err);
        }
        return connectContext;
    }
}
