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
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/flight/auth2/DremioCredentialValidator.java
// and modified by Doris

package org.apache.doris.arrowflight.auth2;

import org.apache.doris.arrowflight.sessions.FlightSessionsManager;

import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Authentication specialized CredentialValidator implementation.
 */
public class FlightCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
    private static final Logger LOG = LogManager.getLogger(FlightCredentialValidator.class);

    private final FlightSessionsManager flightSessionsManager;

    public FlightCredentialValidator(FlightSessionsManager flightSessionsManager) {
        this.flightSessionsManager = flightSessionsManager;
    }

    /**
     * Authenticates against with the provided username and password.
     *
     * @param username username.
     * @param password user password.
     * @return AuthResult with username as the peer identity.
     */
    @Override
    public AuthResult validate(String username, String password) {
        String remoteIp = FlightRemoteIpServerStreamTracer.getRemoteIp();
        FlightAuthResult flightAuthResult = FlightAuthUtils.authenticateCredentials(username, password, remoteIp, LOG);
        return getAuthResultWithBearerToken(flightAuthResult);
    }


    /**
     * Opens the session of the user just authenticated, with the client address the stream tracer
     * captured, and answers with the bearer token the session is known by: the token is the peer
     * identity of this and every later call of the session.
     *
     * @param flightAuthResult the FlightAuthResult from initial authentication, with peer identity captured.
     * @return an AuthResult whose peer identity is the bearer token.
     */
    AuthResult getAuthResultWithBearerToken(FlightAuthResult flightAuthResult) {
        final String token = flightSessionsManager.openSession(flightAuthResult);
        return () -> token;
    }
}
