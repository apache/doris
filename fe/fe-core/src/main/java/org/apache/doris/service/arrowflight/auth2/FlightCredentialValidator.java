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

package org.apache.doris.service.arrowflight.auth2;

import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Authentication specialized CredentialValidator implementation.
 */
public class FlightCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
    private static final Logger LOG = LogManager.getLogger(FlightCredentialValidator.class);

    private final FlightTokenManager flightTokenManager;

    public FlightCredentialValidator(FlightTokenManager flightTokenManager) {
        this.flightTokenManager = flightTokenManager;
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
        // TODO Add ClientAddress information while creating a Token
        String remoteIp = "0.0.0.0";
        FlightAuthResult flightAuthResult = FlightAuthUtils.authenticateCredentials(username, password, remoteIp, LOG);
        return getAuthResultWithBearerToken(flightAuthResult);
    }


    /**
     * Generates a bearer token, parses client properties from incoming headers, then creates a
     * FlightTokenDetails associated with the generated token and client properties.
     *
     * @param flightAuthResult the FlightAuthResult from initial authentication, with peer identity captured.
     * @return an FlightAuthResult with the bearer token and peer identity.
     */
    AuthResult getAuthResultWithBearerToken(FlightAuthResult flightAuthResult) {
        final String username = flightAuthResult.getUserName();
        final String token = FlightAuthUtils.createToken(flightTokenManager, username, flightAuthResult);
        return () -> token;
    }
}
