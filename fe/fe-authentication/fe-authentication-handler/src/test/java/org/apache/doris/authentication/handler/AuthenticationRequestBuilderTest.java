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

import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.CredentialType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link AuthenticationRequestBuilder}.
 */
@DisplayName("AuthenticationRequestBuilder Unit Tests")
class AuthenticationRequestBuilderTest {

    @Test
    @DisplayName("UT-ARB-001: Resolve integration from auth_integration property")
    void testResolve_AuthIntegration() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("auth_integration", "ldap_auth");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertTrue(result.isPresent());
        assertEquals("ldap_auth", result.get());
    }

    @Test
    @DisplayName("UT-ARB-002: Resolve integration from requested_integration property")
    void testResolve_RequestedIntegration() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("requested_integration", "oauth2_auth");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("bob")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertTrue(result.isPresent());
        assertEquals("oauth2_auth", result.get());
    }

    @Test
    @DisplayName("UT-ARB-003: Resolve integration from legacy auth_profile property")
    void testResolve_LegacyAuthProfile() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("auth_profile", "legacy_profile");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("charlie")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertTrue(result.isPresent());
        assertEquals("legacy_profile", result.get());
    }

    @Test
    @DisplayName("UT-ARB-004: Resolve integration from legacy requested_profile property")
    void testResolve_LegacyRequestedProfile() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("requested_profile", "legacy_requested");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("dave")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertTrue(result.isPresent());
        assertEquals("legacy_requested", result.get());
    }

    @Test
    @DisplayName("UT-ARB-005: Case-insensitive property key matching")
    void testResolve_CaseInsensitive() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("AUTH_INTEGRATION", "case_test");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("eve")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertTrue(result.isPresent());
        assertEquals("case_test", result.get());
    }

    @Test
    @DisplayName("UT-ARB-006: Priority order - auth_integration first")
    void testResolve_PriorityOrder() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("auth_integration", "first");
        properties.put("requested_integration", "second");
        properties.put("auth_profile", "third");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("frank")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertTrue(result.isPresent());
        assertEquals("first", result.get());
    }

    @Test
    @DisplayName("UT-ARB-007: Null request returns empty")
    void testResolve_NullRequest() {
        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(null);

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("UT-ARB-008: Empty properties returns empty")
    void testResolve_EmptyProperties() {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("grace")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("UT-ARB-009: Null properties returns empty")
    void testResolve_NullProperties() {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("henry")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(null)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("UT-ARB-010: Empty string value returns empty")
    void testResolve_EmptyStringValue() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("auth_integration", "");

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("iris")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("UT-ARB-011: Non-string value returns empty")
    void testResolve_NonStringValue() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("auth_integration", 123);

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("jack")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("pass".getBytes(StandardCharsets.UTF_8))
                .properties(properties)
                .build();

        Optional<String> result = AuthenticationRequestBuilder.resolveRequestedIntegration(request);

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("UT-ARB-012: Constants are defined correctly")
    void testConstants() {
        assertEquals("auth_integration", AuthenticationRequestBuilder.KEY_AUTH_INTEGRATION);
        assertEquals("requested_integration", AuthenticationRequestBuilder.KEY_REQUESTED_INTEGRATION);
        assertEquals("auth_profile", AuthenticationRequestBuilder.KEY_AUTH_PROFILE);
        assertEquals("requested_profile", AuthenticationRequestBuilder.KEY_REQUESTED_PROFILE);
    }
}
