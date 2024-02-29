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

import java.util.concurrent.atomic.AtomicInteger;

public class FlightSessionsWithTokenManager implements FlightSessionsManager {
    private static final Logger LOG = LogManager.getLogger(FlightSessionsWithTokenManager.class);

    private final FlightTokenManager flightTokenManager;
    private final AtomicInteger nextConnectionId;

    public FlightSessionsWithTokenManager(FlightTokenManager flightTokenManager) {
        this.flightTokenManager = flightTokenManager;
        this.nextConnectionId = new AtomicInteger(0);
    }

    @Override
    public ConnectContext getConnectContext(String peerIdentity) {
        try {
            ConnectContext connectContext = ExecuteEnv.getInstance().getScheduler().getContext(peerIdentity);
            if (null == connectContext) {
                connectContext = createConnectContext(peerIdentity);
                if (null == connectContext) {
                    flightTokenManager.invalidateToken(peerIdentity);
                    String err = "UserSession expire after access, need reauthorize.";
                    LOG.error(err);
                    throw CallStatus.UNAUTHENTICATED.withDescription(err).toRuntimeException();
                }
                return connectContext;
            }
            return connectContext;
        } catch (Exception e) {
            LOG.warn("getConnectContext failed, " + e.getMessage(), e);
            throw CallStatus.INTERNAL.withDescription(Util.getRootCauseMessage(e)).withCause(e).toRuntimeException();
        }
    }

    @Override
    public ConnectContext createConnectContext(String peerIdentity) {
        try {
            final FlightTokenDetails flightTokenDetails = flightTokenManager.validateToken(peerIdentity);
            if (flightTokenDetails.getCreatedSession()) {
                return null;
            }
            flightTokenDetails.setCreatedSession(true);
            ConnectContext connectContext = FlightSessionsManager.buildConnectContext(peerIdentity,
                    flightTokenDetails.getUserIdentity(), flightTokenDetails.getRemoteIp());
            connectContext.setConnectionId(nextConnectionId.getAndAdd(1));
            connectContext.resetLoginTime();
            if (!ExecuteEnv.getInstance().getScheduler().registerConnection(connectContext)) {
                connectContext.getState()
                        .setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, "Reach limit of connections");
                throw CallStatus.UNAUTHENTICATED.withDescription("Reach limit of connections").toRuntimeException();
            }
            return connectContext;
        } catch (IllegalArgumentException e) {
            LOG.error("Bearer token validation failed.", e);
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }
}
