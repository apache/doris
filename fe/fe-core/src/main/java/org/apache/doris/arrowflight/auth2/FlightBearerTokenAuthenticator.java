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
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/flight/auth2/DremioBearerTokenAuthenticator.java
// and modified by Doris

package org.apache.doris.arrowflight.auth2;

import org.apache.doris.arrowflight.sessions.FlightSessionsManager;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Doris's custom implementation of CallHeaderAuthenticator for bearer token authentication.
 * This class implements CallHeaderAuthenticator rather than BearerTokenAuthenticator: a bearer
 * token is the credential of the one session it was issued with, so validating it is asking the
 * sessions manager whether that session is still open, and issuing one is opening a session.
 */

public class FlightBearerTokenAuthenticator implements CallHeaderAuthenticator {
    private static final Logger LOG = LogManager.getLogger(FlightBearerTokenAuthenticator.class);

    private final CallHeaderAuthenticator initialAuthenticator;
    private final FlightSessionsManager flightSessionsManager;

    public FlightBearerTokenAuthenticator(FlightSessionsManager flightSessionsManager) {
        this.flightSessionsManager = flightSessionsManager;
        this.initialAuthenticator = new BasicCallHeaderAuthenticator(
                new FlightCredentialValidator(this.flightSessionsManager));
    }

    /**
     * If no bearer token is provided, the method initiates initial password and username
     * authentication. Once authenticated, the client's session is opened and the bearer token it
     * is known by is handed back in the outgoing headers.
     * <p>
     * If a bearer token is provided, the method validates the provided token: the session it
     * names must still be open on this frontend.
     *
     * @param incomingHeaders call headers to retrieve client properties and auth headers from.
     * @return an AuthResult with the bearer token and peer identity.
     */
    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        final String bearerToken = AuthUtilities.getValueFromAuthHeader(incomingHeaders,
                Auth2Constants.BEARER_PREFIX);

        if (bearerToken != null) {
            return validateBearer(bearerToken);
        } else {
            final AuthResult result = initialAuthenticator.authenticate(incomingHeaders);
            return createAuthResultWithBearerToken(result.getPeerIdentity());
        }
    }

    /**
     * Validates provided token.
     *
     * @param token the token to validate.
     * @return an AuthResult with the bearer token and peer identity.
     * @throws org.apache.arrow.flight.FlightRuntimeException {@code UNAUTHENTICATED} when no session
     *         is open under the token
     */
    AuthResult validateBearer(String token) {
        try {
            flightSessionsManager.getConnectContext(token);
        } catch (FlightRuntimeException e) {
            // Routine after a session ended - the client retries with the token it has and learns
            // to reconnect from the status - so no stack trace.
            LOG.warn("bearer token validation failed: {}", e.status().description());
            throw e;
        }
        return createAuthResultWithBearerToken(token);
    }


    /**
     * Helper method to create an AuthResult.
     *
     * @param token the bearer token of the session, its peer identity.
     * @return a new AuthResult with functionality to add given bearer token to the outgoing header.
     */
    private AuthResult createAuthResultWithBearerToken(String token) {
        return new AuthResult() {
            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                        Auth2Constants.BEARER_PREFIX + token);
            }

            @Override
            public String getPeerIdentity() {
                return token;
            }
        };
    }
}
