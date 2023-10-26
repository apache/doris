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

import org.apache.doris.analysis.UserIdentity;
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
    }

    @Override
    public ConnectContext createConnectContext(String peerIdentity) {
        try {
//            final FlightTokenDetails flightTokenDetails = flightTokenManager.validateToken(peerIdentity);
//            if (flightTokenDetails.getCreatedSession()) {
//                return null;
//            }
            UserIdentity root = new UserIdentity("root", "127.0.0.1");
            root.setIsAnalyzed();
            return FlightSessionsManager.buildConnectContext(peerIdentity, root,
                    "127.0.0.1");
        } catch (IllegalArgumentException e) {
            LOG.error("Bearer token validation failed.", e);
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }
}
