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
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.spi.AuthenticationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for {@link BindingResolver}.
 */
@DisplayName("BindingResolver Unit Tests")
class BindingResolverTest {

    @Mock
    private IntegrationRegistry integrationRegistry;

    @Mock
    private BindingRegistry bindingRegistry;

    private BindingResolver resolver;
    private AuthenticationIntegration passwordIntegration;
    private AuthenticationIntegration ldapIntegration;
    private AuthenticationIntegration oauth2Integration;
    private AuthenticationRequest testRequest;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        passwordIntegration = AuthenticationIntegration.builder()
                .name("password_auth")
                .type("password")
                .properties(new HashMap<>())
                .build();

        ldapIntegration = AuthenticationIntegration.builder()
                .name("ldap_auth")
                .type("ldap")
                .properties(new HashMap<>())
                .build();

        oauth2Integration = AuthenticationIntegration.builder()
                .name("oauth2_auth")
                .type("oauth2")
                .properties(new HashMap<>())
                .build();

        testRequest = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .build();

        resolver = new BindingResolver(integrationRegistry, bindingRegistry);
    }

    // ==================== Constructor Tests ====================

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("UT-BR-C-001: Constructor with valid dependencies")
        void testConstructor_Valid() {
            BindingResolver resolver = new BindingResolver(integrationRegistry, bindingRegistry);
            assertNotNull(resolver);
        }

        @Test
        @DisplayName("UT-BR-C-002: Constructor with null integrationRegistry throws NPE")
        void testConstructor_NullIntegrationRegistry() {
            assertThrows(NullPointerException.class, () ->
                    new BindingResolver(null, bindingRegistry));
        }

        @Test
        @DisplayName("UT-BR-C-003: Constructor with null bindingRegistry throws NPE")
        void testConstructor_NullBindingRegistry() {
            assertThrows(NullPointerException.class, () ->
                    new BindingResolver(integrationRegistry, null));
        }
    }

    // ==================== ResolveIntegration Tests ====================

    @Nested
    @DisplayName("ResolveIntegration Tests")
    class ResolveIntegrationTests {

        @Test
        @DisplayName("UT-BR-R-001: Resolve integration with user binding")
        void testResolveIntegration_WithUserBinding() throws AuthenticationException {
            when(bindingRegistry.getIntegrationName("alice"))
                    .thenReturn(Optional.of("ldap_auth"));
            when(integrationRegistry.get("ldap_auth"))
                    .thenReturn(Optional.of(ldapIntegration));
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Collections.emptyList());
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            AuthenticationIntegration result = resolver.resolveIntegration("alice", testRequest);

            assertEquals(ldapIntegration, result);
            verify(bindingRegistry).getIntegrationName("alice");
        }

        @Test
        @DisplayName("UT-BR-R-002: Resolve integration from authentication chain")
        void testResolveIntegration_FromChain() throws AuthenticationException {
            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Arrays.asList(ldapIntegration, passwordIntegration));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            AuthenticationIntegration result = resolver.resolveIntegration("bob", testRequest);

            assertEquals(ldapIntegration, result);
        }

        @Test
        @DisplayName("UT-BR-R-003: Resolve falls back to default password integration")
        void testResolveIntegration_FallbackToDefault() throws AuthenticationException {
            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Collections.emptyList());
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(passwordIntegration);

            AuthenticationIntegration result = resolver.resolveIntegration("charlie", testRequest);

            assertEquals(passwordIntegration, result);
        }

        @Test
        @DisplayName("UT-BR-R-004: Resolve with no candidates throws exception")
        void testResolveIntegration_NoCandidates() {
            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Collections.emptyList());
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            AuthenticationException ex = assertThrows(AuthenticationException.class, () ->
                    resolver.resolveIntegration("dave", testRequest));

            assertTrue(ex.getMessage().contains("No authentication integration available"));
        }
    }

    // ==================== ResolveCandidates Tests ====================

    @Nested
    @DisplayName("ResolveCandidates Tests")
    class ResolveCandidatesTests {

        @Test
        @DisplayName("UT-BR-RC-001: User binding has highest priority")
        void testResolveCandidates_UserBindingFirst() {
            when(bindingRegistry.getIntegrationName("alice"))
                    .thenReturn(Optional.of("oauth2_auth"));
            when(integrationRegistry.get("oauth2_auth"))
                    .thenReturn(Optional.of(oauth2Integration));
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Arrays.asList(ldapIntegration, passwordIntegration));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(passwordIntegration);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("alice", testRequest);

            assertEquals(3, candidates.size());
            assertEquals(oauth2Integration, candidates.get(0)); // User binding first
            assertEquals(ldapIntegration, candidates.get(1));   // Chain second
            assertEquals(passwordIntegration, candidates.get(2)); // Default last (but not duplicated)
        }

        @Test
        @DisplayName("UT-BR-RC-002: Authentication chain in order")
        void testResolveCandidates_ChainOrder() {
            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Arrays.asList(ldapIntegration, oauth2Integration, passwordIntegration));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("bob", testRequest);

            assertEquals(3, candidates.size());
            assertEquals(ldapIntegration, candidates.get(0));
            assertEquals(oauth2Integration, candidates.get(1));
            assertEquals(passwordIntegration, candidates.get(2));
        }

        @Test
        @DisplayName("UT-BR-RC-003: No duplicates in candidates")
        void testResolveCandidates_NoDuplicates() {
            when(bindingRegistry.getIntegrationName("alice"))
                    .thenReturn(Optional.of("password_auth"));
            when(integrationRegistry.get("password_auth"))
                    .thenReturn(Optional.of(passwordIntegration));
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Arrays.asList(passwordIntegration, ldapIntegration));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(passwordIntegration);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("alice", testRequest);

            assertEquals(2, candidates.size()); // password only once
            assertEquals(passwordIntegration, candidates.get(0));
            assertEquals(ldapIntegration, candidates.get(1));
        }

        @Test
        @DisplayName("UT-BR-RC-004: Empty candidates when no integrations available")
        void testResolveCandidates_Empty() {
            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Collections.emptyList());
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("charlie", testRequest);

            assertTrue(candidates.isEmpty());
        }

        @Test
        @DisplayName("UT-BR-RC-005: User binding not found in registry skipped")
        void testResolveCandidates_UserBindingNotFound() {
            when(bindingRegistry.getIntegrationName("alice"))
                    .thenReturn(Optional.of("nonexistent"));
            when(integrationRegistry.get("nonexistent"))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Collections.singletonList(passwordIntegration));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("alice", testRequest);

            assertEquals(1, candidates.size());
            assertEquals(passwordIntegration, candidates.get(0));
        }

        @Test
        @DisplayName("UT-BR-RC-006: Null integrations filtered out")
        void testResolveCandidates_NullsFiltered() {
            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Arrays.asList(ldapIntegration, null, passwordIntegration));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("dave", testRequest);

            assertEquals(2, candidates.size());
            assertFalse(candidates.contains(null));
        }
    }

    // ==================== User Binding Query Tests ====================

    @Nested
    @DisplayName("User Binding Query Tests")
    class UserBindingQueryTests {

        @Test
        @DisplayName("UT-BR-UB-001: Has user binding returns true")
        void testHasUserBinding_True() {
            when(bindingRegistry.hasBinding("alice")).thenReturn(true);

            assertTrue(resolver.hasUserBinding("alice"));
            verify(bindingRegistry).hasBinding("alice");
        }

        @Test
        @DisplayName("UT-BR-UB-002: Has user binding returns false")
        void testHasUserBinding_False() {
            when(bindingRegistry.hasBinding("bob")).thenReturn(false);

            assertFalse(resolver.hasUserBinding("bob"));
        }

        @Test
        @DisplayName("UT-BR-UB-003: Get user bound integration present")
        void testGetUserBoundIntegration_Present() {
            when(bindingRegistry.getIntegrationName("alice"))
                    .thenReturn(Optional.of("ldap_auth"));
            when(integrationRegistry.get("ldap_auth"))
                    .thenReturn(Optional.of(ldapIntegration));

            Optional<AuthenticationIntegration> result = resolver.getUserBoundIntegration("alice");

            assertTrue(result.isPresent());
            assertEquals(ldapIntegration, result.get());
        }

        @Test
        @DisplayName("UT-BR-UB-004: Get user bound integration absent - no binding")
        void testGetUserBoundIntegration_NoBinding() {
            when(bindingRegistry.getIntegrationName("bob"))
                    .thenReturn(Optional.empty());

            Optional<AuthenticationIntegration> result = resolver.getUserBoundIntegration("bob");

            assertFalse(result.isPresent());
        }

        @Test
        @DisplayName("UT-BR-UB-005: Get user bound integration absent - integration not found")
        void testGetUserBoundIntegration_IntegrationNotFound() {
            when(bindingRegistry.getIntegrationName("charlie"))
                    .thenReturn(Optional.of("nonexistent"));
            when(integrationRegistry.get("nonexistent"))
                    .thenReturn(Optional.empty());

            Optional<AuthenticationIntegration> result = resolver.getUserBoundIntegration("charlie");

            assertFalse(result.isPresent());
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("UT-BR-E-001: Resolve with empty username")
        void testResolve_EmptyUsername() {
            when(bindingRegistry.getIntegrationName(""))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Collections.emptyList());
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(passwordIntegration);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("", testRequest);

            assertEquals(1, candidates.size());
            assertEquals(passwordIntegration, candidates.get(0));
        }

        @Test
        @DisplayName("UT-BR-E-002: Resolve with very long authentication chain")
        void testResolve_LongChain() {
            AuthenticationIntegration[] integrations = new AuthenticationIntegration[100];
            for (int i = 0; i < 100; i++) {
                integrations[i] = AuthenticationIntegration.builder()
                        .name("integration_" + i)
                        .type("type_" + i)
                        .properties(new HashMap<>())
                        .build();
            }

            when(bindingRegistry.getIntegrationName(anyString()))
                    .thenReturn(Optional.empty());
            when(integrationRegistry.getAuthenticationChain())
                    .thenReturn(Arrays.asList(integrations));
            when(integrationRegistry.getDefaultPasswordIntegration())
                    .thenReturn(null);

            List<AuthenticationIntegration> candidates = resolver.resolveCandidates("eve", testRequest);

            assertEquals(100, candidates.size());
        }
    }
}
