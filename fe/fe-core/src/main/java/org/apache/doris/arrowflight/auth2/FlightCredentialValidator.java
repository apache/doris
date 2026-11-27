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
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.AuthenticationFailureSummary;
import org.apache.doris.mysql.authenticate.AuthenticatorManager;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.qe.ConnectContext;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlightCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
    private static final Logger LOG = LogManager.getLogger(FlightCredentialValidator.class);

    private final AuthenticatorManager authenticator;
    private final FlightSessionsManager sessions;

    public FlightCredentialValidator(AuthenticatorManager authenticator, FlightSessionsManager sessions) {
        this.authenticator = authenticator;
        this.sessions = sessions;
    }

    @Override
    public AuthResult validate(String username, String password) {
        return validate(username, password, CredentialType.CLEAR_TEXT_PASSWORD);
    }

    AuthResult validate(String username, String secret, String credentialType) {
        String remoteIp = FlightRemoteIpServerStreamTracer.getRemoteIp();
        AuthenticateResponse response = authenticate(username, secret, credentialType, remoteIp);
        String token = sessions.openSession(response, remoteIp);
        return () -> token;
    }

    /**
     * Flight supplies a single credential and its explicit type. The current Flight listener is
     * plaintext, so the shared strategy refuses any method requiring a secure connection.
     */
    private AuthenticateResponse authenticate(String username, String secret, String credentialType, String remoteIp) {
        if (!CredentialType.CLEAR_TEXT_PASSWORD.equals(credentialType)
                && !CredentialType.OAUTH_TOKEN.equals(credentialType)
                && !CredentialType.OIDC_ID_TOKEN.equals(credentialType)
                && !CredentialType.JWT_TOKEN.equals(credentialType)) {
            throw CallStatus.UNAUTHENTICATED.withDescription("Unsupported Flight SQL credential type")
                    .toRuntimeException();
        }

        boolean password = CredentialType.CLEAR_TEXT_PASSWORD.equals(credentialType);
        // Some DBeaver configurations encode an empty password as the literal "null".
        String normalized = password && "null".equals(secret) ? null : secret;
        AuthenticateRequest request = AuthenticateRequest.builder()
                .userName(username)
                .remoteHost(remoteIp)
                .clientType("arrow-flight-sql")
                .credentialType(credentialType)
                .credential(normalized == null ? new byte[0] : normalized.getBytes(StandardCharsets.UTF_8))
                .password(password ? new ClearPassword(normalized) : null)
                .build();
        AuthenticateResponse response;
        // Legacy authenticators report MySQL errors through the current thread's QueryState.
        ConnectContext previous = ConnectContext.get();
        if (previous != null) {
            ConnectContext.remove();
        }
        try {
            response = authenticator.authenticate(request, false);
        } catch (IOException e) {
            LOG.warn("Flight SQL authentication could not complete: {}", e.getClass().getSimpleName());
            throw CallStatus.UNAVAILABLE.withDescription("Flight SQL authentication could not complete")
                    .toRuntimeException();
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
        if (!response.isSuccess()) {
            throw authenticationFailure(response.getFailureSummary()).toRuntimeException();
        }
        return response;
    }

    private CallStatus authenticationFailure(AuthenticationFailureSummary failure) {
        if (failure == null) {
            return CallStatus.UNAUTHENTICATED.withDescription("Flight SQL authentication failed");
        }
        if (failure.hasClientVisibleMessage()) {
            return CallStatus.UNAUTHENTICATED.withDescription(failure.getClientVisibleMessage());
        }
        if (failure.isOperationalFailure()) {
            CallStatus status = failure.getFailureType() == AuthenticationFailureType.SOURCE_UNAVAILABLE
                    ? CallStatus.UNAVAILABLE : CallStatus.INTERNAL;
            return status.withDescription("Flight SQL authentication could not complete");
        }
        return CallStatus.UNAUTHENTICATED.withDescription("Flight SQL authentication failed");
    }
}
