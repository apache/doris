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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Unit tests for {@link AuthenticationOutcome}.
 */
@DisplayName("AuthenticationOutcome Unit Tests")
class AuthenticationOutcomeTest {

    @Test
    @DisplayName("UT-AO-001: Create outcome with success result")
    void testCreate_Success() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("test_integration")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result);

        Assertions.assertNotNull(outcome);
        Assertions.assertEquals(integration, outcome.getIntegration());
        Assertions.assertEquals(result, outcome.getAuthResult());
        Assertions.assertTrue(outcome.isSuccess());
        Assertions.assertFalse(outcome.isFailure());
        Assertions.assertFalse(outcome.isContinue());
    }

    @Test
    @DisplayName("UT-AO-002: Create outcome with failure result")
    void testCreate_Failure() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("password")
                .properties(new HashMap<>())
                .build();

        AuthenticationResult result = AuthenticationResult.failure("Invalid password");

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result);

        Assertions.assertNotNull(outcome);
        Assertions.assertTrue(outcome.isFailure());
        Assertions.assertFalse(outcome.isSuccess());
        Assertions.assertFalse(outcome.isContinue());
    }

    @Test
    @DisplayName("UT-AO-003: Get principal from result")
    void testGetPrincipal() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("charlie")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result);

        Assertions.assertTrue(outcome.getPrincipal().isPresent());
        Assertions.assertEquals(principal, outcome.getPrincipal().get());
    }

    @Test
    @DisplayName("UT-AO-004: Failure outcome has no principal")
    void testFailureOutcomeHasNoPrincipal() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .properties(new HashMap<>())
                .build();

        AuthenticationResult result = AuthenticationResult.failure("invalid credential");

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result);

        Assertions.assertFalse(outcome.getPrincipal().isPresent());
    }

    @Test
    @DisplayName("UT-AO-005: Null integration throws NPE")
    void testCreate_NullIntegration() {
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("eve")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationOutcome.of(null, result));
    }

    @Test
    @DisplayName("UT-AO-006: Null result throws NPE")
    void testCreate_NullResult() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationOutcome.of(integration, null));
    }

    @Test
    @DisplayName("UT-AO-007: toString includes key information")
    void testToString() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("my_integration")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("henry")
                .authenticator("my_integration")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result);

        String str = outcome.toString();
        Assertions.assertTrue(str.contains("my_integration"));
        Assertions.assertTrue(str.contains("success=true"));
    }

    @Test
    @DisplayName("UT-AO-008: Continue status")
    void testContinueStatus() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("saml")
                .properties(new HashMap<>())
                .build();

        byte[] challengeData = "challenge".getBytes();
        AuthenticationResult result = AuthenticationResult.continueWith("state", challengeData);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result);

        Assertions.assertTrue(outcome.isContinue());
        Assertions.assertFalse(outcome.isSuccess());
        Assertions.assertFalse(outcome.isFailure());
    }
}
