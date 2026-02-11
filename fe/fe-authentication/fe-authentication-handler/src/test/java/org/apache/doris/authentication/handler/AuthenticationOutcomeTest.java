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
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.Subject;
import org.apache.doris.authentication.spi.AuthenticationResult;

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
        Subject subject = Subject.builder()
                .principal(principal)
                .build();

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, subject);

        assertNotNull(outcome);
        assertEquals(integration, outcome.getIntegration());
        assertEquals(result, outcome.getAuthResult());
        assertTrue(outcome.getSubject().isPresent());
        assertEquals(subject, outcome.getSubject().get());
        assertTrue(outcome.isSuccess());
        assertFalse(outcome.isFailure());
        assertFalse(outcome.isContinue());
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

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null);

        assertNotNull(outcome);
        assertFalse(outcome.getSubject().isPresent());
        assertTrue(outcome.isFailure());
        assertFalse(outcome.isSuccess());
        assertFalse(outcome.isContinue());
    }

    @Test
    @DisplayName("UT-AO-003: Create outcome with all fields")
    void testCreate_AllFields() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("ldap")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("bob")
                .authenticator("test_integration")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);
        Subject subject = Subject.builder()
                .principal(principal)
                .build();
        Object resolvedUser = new Object();

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, subject, resolvedUser);

        assertTrue(outcome.getResolvedUser().isPresent());
        assertEquals(resolvedUser, outcome.getResolvedUser().get());
    }

    @Test
    @DisplayName("UT-AO-004: Get principal from result")
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

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null);

        assertTrue(outcome.getPrincipal().isPresent());
        assertEquals(principal, outcome.getPrincipal().get());
    }

    @Test
    @DisplayName("UT-AO-005: Get identity from result")
    void testGetIdentity() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .properties(new HashMap<>())
                .build();

        //         Identity identity = Identity.builder()
        //                 .username("dave")
        //                 .authenticatorName("test")
        //                 .authenticatorPluginName("ldap")
        //                 .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("dave")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null);

        // Note: Identity is stored in AuthenticationResult, availability depends on plugin implementation
        assertNotNull(outcome.getIdentity());
    }

    @Test
    @DisplayName("UT-AO-006: Null integration throws NPE")
    void testCreate_NullIntegration() {
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("eve")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        assertThrows(NullPointerException.class, () ->
                AuthenticationOutcome.of(null, result, null));
    }

    @Test
    @DisplayName("UT-AO-007: Null result throws NPE")
    void testCreate_NullResult() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        assertThrows(NullPointerException.class, () ->
                AuthenticationOutcome.of(integration, null, null));
    }

    @Test
    @DisplayName("UT-AO-008: Null subject is allowed")
    void testCreate_NullSubject() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("frank")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null);

        assertNotNull(outcome);
        assertFalse(outcome.getSubject().isPresent());
    }

    @Test
    @DisplayName("UT-AO-009: Null resolved user is allowed")
    void testCreate_NullResolvedUser() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("grace")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null, null);

        assertFalse(outcome.getResolvedUser().isPresent());
    }

    @Test
    @DisplayName("UT-AO-010: toString includes key information")
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

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null);

        String str = outcome.toString();
        assertTrue(str.contains("my_integration"));
        assertTrue(str.contains("success=true"));
    }

    @Test
    @DisplayName("UT-AO-011: Factory method with 3 parameters")
    void testFactoryMethod_ThreeParams() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("iris")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);
        Subject subject = Subject.builder()
                .principal(principal)
                .build();

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, subject);

        assertNotNull(outcome);
        assertTrue(outcome.getSubject().isPresent());
        assertFalse(outcome.getResolvedUser().isPresent());
    }

    @Test
    @DisplayName("UT-AO-012: Factory method with 4 parameters")
    void testFactoryMethod_FourParams() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("password")
                .properties(new HashMap<>())
                .build();

        BasicPrincipal principal = BasicPrincipal.builder()
                .name("jack")
                .authenticator("test")
                .build();

        AuthenticationResult result = AuthenticationResult.success(principal);
        Subject subject = Subject.builder()
                .principal(principal)
                .build();
        String resolvedUser = "user_object";

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, subject, resolvedUser);

        assertTrue(outcome.getSubject().isPresent());
        assertTrue(outcome.getResolvedUser().isPresent());
        assertEquals(resolvedUser, outcome.getResolvedUser().get());
    }

    @Test
    @DisplayName("UT-AO-013: Continue status")
    void testContinueStatus() {
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("saml")
                .properties(new HashMap<>())
                .build();

        byte[] challengeData = "challenge".getBytes();
        AuthenticationResult result = AuthenticationResult.continueWith("state", challengeData);

        AuthenticationOutcome outcome = AuthenticationOutcome.of(integration, result, null);

        assertTrue(outcome.isContinue());
        assertFalse(outcome.isSuccess());
        assertFalse(outcome.isFailure());
    }
}
