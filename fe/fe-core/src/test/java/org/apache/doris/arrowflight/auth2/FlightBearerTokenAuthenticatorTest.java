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

package org.apache.doris.arrowflight.auth2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.AuthenticationFailureSummary;
import org.apache.doris.mysql.authenticate.AuthenticatorManager;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

class FlightBearerTokenAuthenticatorTest {
    private final FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
    private final AuthenticatorManager authenticator = Mockito.mock(AuthenticatorManager.class);
    private final FlightBearerTokenAuthenticator headers = new FlightBearerTokenAuthenticator(sessions, authenticator);

    @Test
    void testBasicUsesSharedAuthenticatorAndBearerOnlyLooksUpSession() throws Exception {
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
        AuthenticateResponse accepted = new AuthenticateResponse(true, user);
        Mockito.when(authenticator.authenticate(Mockito.any(AuthenticateRequest.class), Mockito.eq(false)))
                .thenReturn(accepted);
        Mockito.when(sessions.openSession(Mockito.eq(accepted), Mockito.anyString())).thenReturn("session-token");

        FlightCallHeaders outgoing = new FlightCallHeaders();
        headers.authenticate(basic("null")).appendToOutgoingHeaders(outgoing);
        Assertions.assertEquals("Bearer session-token", outgoing.get(Auth2Constants.AUTHORIZATION_HEADER));

        ArgumentCaptor<AuthenticateRequest> request = ArgumentCaptor.forClass(AuthenticateRequest.class);
        Mockito.verify(authenticator).authenticate(request.capture(), Mockito.eq(false));
        Assertions.assertEquals("alice", request.getValue().getUserName());
        Assertions.assertEquals("arrow-flight-sql", request.getValue().getClientType());
        Assertions.assertEquals(CredentialType.CLEAR_TEXT_PASSWORD, request.getValue().getCredentialType());
        Assertions.assertArrayEquals(new byte[0], request.getValue().getCredential());

        Assertions.assertEquals("session-token", headers.authenticate(outgoing).getPeerIdentity());
        Mockito.verify(sessions).getConnectContext("session-token");
        Mockito.verifyNoMoreInteractions(authenticator);
    }

    @Test
    void testTypedCredentialReachesSharedAuthenticatorAndRejectionCreatesNoSession() throws Exception {
        FlightCallHeaders requestHeaders = basic("fixture-token");
        requestHeaders.insert(FlightBearerTokenAuthenticator.CREDENTIAL_TYPE_HEADER, CredentialType.OAUTH_TOKEN);
        Mockito.when(authenticator.authenticate(Mockito.any(AuthenticateRequest.class), Mockito.eq(false)))
                .thenReturn(AuthenticateResponse.failed(AuthenticationFailureSummary.forClientVisibleFailure(
                        AuthenticationFailureType.ACCESS_DENIED, "secure transport required",
                        "secure transport required")));

        FlightRuntimeException refusal = Assertions.assertThrows(FlightRuntimeException.class,
                () -> headers.authenticate(requestHeaders));
        Assertions.assertEquals(FlightStatusCode.UNAUTHENTICATED, refusal.status().code());
        Assertions.assertTrue(refusal.status().description().contains("secure transport"));

        ArgumentCaptor<AuthenticateRequest> request = ArgumentCaptor.forClass(AuthenticateRequest.class);
        Mockito.verify(authenticator).authenticate(request.capture(), Mockito.eq(false));
        Assertions.assertEquals(CredentialType.OAUTH_TOKEN, request.getValue().getCredentialType());
        Assertions.assertArrayEquals("fixture-token".getBytes(StandardCharsets.UTF_8),
                request.getValue().getCredential());
        Assertions.assertNull(request.getValue().getPassword());
        Mockito.verifyNoInteractions(sessions);
    }

    private static FlightCallHeaders basic(String credential) {
        FlightCallHeaders headers = new FlightCallHeaders();
        String encoded = Base64.getEncoder().encodeToString(("alice:" + credential).getBytes(StandardCharsets.UTF_8));
        headers.insert(Auth2Constants.AUTHORIZATION_HEADER, "Basic " + encoded);
        return headers;
    }
}
