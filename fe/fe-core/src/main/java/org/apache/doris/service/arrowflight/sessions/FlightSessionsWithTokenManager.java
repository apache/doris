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
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/flight/TokenCacheFlightSessionManager.java
// and modified by Doris

package org.apache.doris.service.arrowflight.sessions;

import org.apache.doris.service.arrowflight.tokens.FlightTokenDetails;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class FlightSessionsWithTokenManager implements FlightSessionsManager {
    private static final Logger LOG = LogManager.getLogger(FlightSessionsWithTokenManager.class);

    private final Cache<String, FlightUserSession> userSessions;

    private final int cacheExpiration;

    private final FlightTokenManager flightTokenManager;

    public FlightSessionsWithTokenManager(FlightTokenManager flightTokenManager, final int cacheSize,
            final int cacheExpiration) {
        this.flightTokenManager = flightTokenManager;
        this.cacheExpiration = cacheExpiration;
        this.userSessions = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterAccess(cacheExpiration, TimeUnit.MINUTES)
                .build(new CacheLoader<String, FlightUserSession>() {
                    @Override
                    public FlightUserSession load(String key) {
                        return new FlightUserSession();
                    }
                });
    }

    @Override
    public FlightUserSession getUserSession(String peerIdentity) {
        FlightUserSession userSession = userSessions.getIfPresent(peerIdentity);
        if (null == userSession) {
            userSession = createUserSession(peerIdentity);
            if (null == userSession) {
                flightTokenManager.invalidateToken(peerIdentity);
                String err = "UserSession expire after access " + cacheExpiration + " minutes ago, reauthorize.";
                LOG.error(err);
                throw CallStatus.UNAUTHENTICATED.withDescription(err).toRuntimeException();
            }
            return userSession;
        }
        return userSession;
    }

    @Override
    public FlightUserSession createUserSession(String peerIdentity) {
        try {
            final FlightTokenDetails flightTokenDetails = flightTokenManager.validateToken(peerIdentity);
            if (flightTokenDetails.getCreatedSession()) {
                return null;
            }
            final FlightUserSession flightUserSession = new FlightUserSession(flightTokenDetails.getUsername(),
                    flightTokenDetails.getIssuedAt(), flightTokenDetails.getExpiresAt());
            flightUserSession.setAuthResult(flightTokenDetails.getUserIdentity(), flightTokenDetails.getRemoteIp());
            userSessions.put(peerIdentity, flightUserSession);
            return flightUserSession;
        } catch (IllegalArgumentException e) {
            LOG.error("Bearer token validation failed.", e);
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }

    @Override
    public void close() throws Exception {
        userSessions.invalidateAll();
    }
}
