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

package org.apache.doris.service.arrowflight.auth2;

import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Doris's custom implementation of CallHeaderAuthenticator for bearer token authentication.
 * This class implements CallHeaderAuthenticator rather than BearerTokenAuthenticator. Doris
 * creates FlightTokenDetails objects when the bearer token is created and requires access to the CallHeaders
 * in getAuthResultWithBearerToken.
 */

public class FlightBearerTokenAuthenticator implements CallHeaderAuthenticator {
    private static final Logger LOG = LogManager.getLogger(FlightBearerTokenAuthenticator.class);

    private final CallHeaderAuthenticator initialAuthenticator;
    private final FlightTokenManager flightTokenManager;

    public FlightBearerTokenAuthenticator(FlightTokenManager flightTokenManager) {
        this.flightTokenManager = flightTokenManager;
        this.initialAuthenticator = new BasicCallHeaderAuthenticator(
                new FlightCredentialValidator(this.flightTokenManager));
    }

    /**
     * If no bearer token is provided, the method initiates initial password and username
     * authentication. Once authenticated, client properties are retrieved from incoming CallHeaders.
     * Then it generates a token and creates a FlightTokenDetails with the retrieved client properties.
     * associated with it.
     * <p>
     * If a bearer token is provided, the method validates the provided token.
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
     */
    AuthResult validateBearer(String token) {
        try {
            flightTokenManager.validateToken(token);
            return createAuthResultWithBearerToken(token);
        } catch (IllegalArgumentException e) {
            LOG.error("Bearer token validation failed.", e);
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }


    /**
     * Helper method to create an AuthResult.
     *
     * @param token the token to create a FlightTokenDetails for.
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
