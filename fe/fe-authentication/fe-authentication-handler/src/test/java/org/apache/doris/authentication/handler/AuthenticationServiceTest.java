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
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Identity;
import org.apache.doris.authentication.spi.AuthenticationException;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationResult;

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
import java.util.HashSet;

/**
 * Unit tests for {@link AuthenticationService}.
 */
@DisplayName("AuthenticationService Unit Tests")
class AuthenticationServiceTest {

    @Mock
    private IntegrationRegistry integrationRegistry;

    @Mock
    private PluginManager pluginManager;

    @Mock
    private BindingResolver bindingResolver;

    @Mock
    private UserResolver userResolver;

    @Mock
    private RoleMapper roleMapper;

    @Mock
    private SubjectBuilder subjectBuilder;

    @Mock
    private RoleResolutionStage roleResolutionStage;

    @Mock
    private AuthenticationPlugin plugin;

    private AuthenticationService service;
    private AuthenticationIntegration testIntegration;
    private AuthenticationRequest testRequest;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        testIntegration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("password")
                .properties(new HashMap<>())
                .build();

        testRequest = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    // ==================== Constructor Tests ====================

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("UT-SVC-C-001: Minimal constructor with required dependencies")
        void testMinimalConstructor() {
            service = new AuthenticationService(integrationRegistry, pluginManager, bindingResolver);

            assertNotNull(service);
            assertEquals(integrationRegistry, service.getIntegrationRegistry());
            assertEquals(pluginManager, service.getPluginManager());
            assertEquals(bindingResolver, service.getBindingResolver());
        }

        @Test
        @DisplayName("UT-SVC-C-002: Full constructor with all dependencies")
        void testFullConstructor() {
            service = new AuthenticationService(
                    integrationRegistry, pluginManager, bindingResolver,
                    userResolver, roleMapper, subjectBuilder, roleResolutionStage);

            assertNotNull(service);
        }

        @Test
        @DisplayName("UT-SVC-C-003: Constructor with null integrationRegistry throws NPE")
        void testConstructor_NullIntegrationRegistry() {
            assertThrows(NullPointerException.class, () ->
                    new AuthenticationService(null, pluginManager, bindingResolver));
        }

        @Test
        @DisplayName("UT-SVC-C-004: Constructor with null pluginManager throws NPE")
        void testConstructor_NullPluginManager() {
            assertThrows(NullPointerException.class, () ->
                    new AuthenticationService(integrationRegistry, null, bindingResolver));
        }

        @Test
        @DisplayName("UT-SVC-C-005: Constructor with null bindingResolver throws NPE")
        void testConstructor_NullBindingResolver() {
            assertThrows(NullPointerException.class, () ->
                    new AuthenticationService(integrationRegistry, pluginManager, null));
        }

        @Test
        @DisplayName("UT-SVC-C-006: Constructor uses defaults for optional dependencies")
        void testConstructor_DefaultsForOptionalDeps() {
            service = new AuthenticationService(integrationRegistry, pluginManager, bindingResolver);

            // Service should work with default implementations
            assertNotNull(service);
        }
    }

    // ==================== Authentication Tests ====================

    @Nested
    @DisplayName("Authentication Flow Tests")
    class AuthenticationFlowTests {

        @BeforeEach
        void setUp() {
            service = new AuthenticationService(
                    integrationRegistry, pluginManager, bindingResolver,
                    userResolver, roleMapper, subjectBuilder, roleResolutionStage);
        }

        @Test
        @DisplayName("UT-SVC-A-001: Successful authentication flow")
        void testAuthenticate_Success() throws AuthenticationException {
            // Given
            Identity identity = Identity.builder()
                    .username("alice")
                    .authenticatorName("test_integration")
                    .authenticatorPluginName("password")
                    .build();

            AuthenticationResult successResult = AuthenticationResult.success(identity);

            when(bindingResolver.resolveCandidates(eq("alice"), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), eq(testIntegration))).thenReturn(successResult);
            when(userResolver.resolveUser(any(), any())).thenReturn("alice_user_object");
            when(roleResolutionStage.resolveRoles(any(), any()))
                    .thenReturn(new HashSet<>(Arrays.asList("user", "admin")));
            when(subjectBuilder.build(any(), any(), any())).thenReturn(null);

            // When
            AuthenticationResult result = service.authenticate(testRequest);

            // Then
            assertNotNull(result);
            assertTrue(result.isSuccess());
            verify(plugin).authenticate(any(), eq(testIntegration));
            verify(userResolver).resolveUser(any(), any());
            verify(roleResolutionStage).resolveRoles(any(), any());
        }

        @Test
        @DisplayName("UT-SVC-A-002: Authentication with null request throws exception")
        void testAuthenticate_NullRequest() {
            assertThrows(AuthenticationException.class, () ->
                    service.authenticate(null));
        }

        @Test
        @DisplayName("UT-SVC-A-003: No candidates available throws exception")
        void testAuthenticate_NoCandidates() {
            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.emptyList());

            AuthenticationException ex = assertThrows(AuthenticationException.class, () ->
                    service.authenticate(testRequest));

            assertTrue(ex.getMessage().contains("No authentication integration"));
        }

        @Test
        @DisplayName("UT-SVC-A-004: No plugin supports request throws exception")
        void testAuthenticate_NoSupportingPlugin() throws AuthenticationException {
            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(false);

            AuthenticationException ex = assertThrows(AuthenticationException.class, () ->
                    service.authenticate(testRequest));

            assertTrue(ex.getMessage().contains("No authentication integration supports"));
        }

        @Test
        @DisplayName("UT-SVC-A-005: Plugin authentication failure")
        void testAuthenticate_PluginFailure() throws AuthenticationException {
            AuthenticationResult failureResult = AuthenticationResult.failure("Invalid credentials");

            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), eq(testIntegration))).thenReturn(failureResult);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            assertNotNull(outcome);
            assertFalse(outcome.getAuthResult().isSuccess());
            verify(userResolver, never()).resolveUser(any(), any());
        }

        @Test
        @DisplayName("UT-SVC-A-006: Multiple candidates, first supporting plugin is used")
        void testAuthenticate_MultipleCandidatesFirstSupports() throws AuthenticationException {
            AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                    .name("integration2")
                    .type("ldap")
                    .properties(new HashMap<>())
                    .build();

            AuthenticationPlugin plugin2 = mock(AuthenticationPlugin.class);

            Identity identity = Identity.builder()
                    .username("alice")
                    .authenticatorName("test_integration")
                    .authenticatorPluginName("password")
                    .build();

            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), any())).thenReturn(AuthenticationResult.success(identity));

            service.authenticate(testRequest);

            verify(plugin).authenticate(any(), eq(testIntegration));
            verify(plugin2, never()).authenticate(any(), any());
        }
    }

    // ==================== Authentication Outcome Tests ====================

    @Nested
    @DisplayName("Authentication Outcome Tests")
    class AuthenticationOutcomeTests {

        @BeforeEach
        void setUp() {
            service = new AuthenticationService(
                    integrationRegistry, pluginManager, bindingResolver,
                    userResolver, roleMapper, subjectBuilder, roleResolutionStage);
        }

        @Test
        @DisplayName("UT-SVC-O-001: Outcome includes all components on success")
        void testAuthenticateWithOutcome_Success() throws AuthenticationException {
            //             Identity identity = Identity.builder()
            //                     .username("alice")
            //                     .authenticatorName("test_integration")
            //                     .authenticatorPluginName("password")
            //                     .build();

            BasicPrincipal principal = BasicPrincipal.builder()
                    .name("alice")
                    .authenticator("test_integration")
                    .build();

            AuthenticationResult successResult = AuthenticationResult.success(principal);

            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), eq(testIntegration))).thenReturn(successResult);
            when(userResolver.resolveUser(any(), any())).thenReturn("user_obj");
            when(roleResolutionStage.resolveRoles(any(), any()))
                    .thenReturn(new HashSet<>(Arrays.asList("admin")));
            when(subjectBuilder.build(any(), any(), any())).thenReturn(null);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            assertNotNull(outcome);
            assertTrue(outcome.getAuthResult().isSuccess());
            assertEquals(testIntegration, outcome.getIntegration());
            verify(plugin).authenticate(any(), eq(testIntegration));
            verify(subjectBuilder).build(eq(principal), any(), eq(testRequest));
        }

        @Test
        @DisplayName("UT-SVC-O-002: Outcome on failure does not resolve user or roles")
        void testAuthenticateWithOutcome_FailureShortCircuits() throws AuthenticationException {
            AuthenticationResult failureResult = AuthenticationResult.failure("Bad password");

            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), eq(testIntegration))).thenReturn(failureResult);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            assertFalse(outcome.getAuthResult().isSuccess());
            verify(userResolver, never()).resolveUser(any(), any());
            verify(roleResolutionStage, never()).resolveRoles(any(), any());
            verify(subjectBuilder, never()).build(any(), any(), any());
        }
    }

    // ==================== Component Integration Tests ====================

    @Nested
    @DisplayName("Component Integration Tests")
    class ComponentIntegrationTests {

        @Test
        @DisplayName("UT-SVC-I-001: Service works with minimal dependencies (defaults)")
        void testServiceWithMinimalDeps() throws AuthenticationException {
            service = new AuthenticationService(integrationRegistry, pluginManager, bindingResolver);

            Identity identity = Identity.builder()
                    .username("alice")
                    .authenticatorName("test_integration")
                    .authenticatorPluginName("password")
                    .build();

            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), any())).thenReturn(AuthenticationResult.success(identity));

            AuthenticationResult result = service.authenticate(testRequest);

            assertNotNull(result);
            assertTrue(result.isSuccess());
        }

        @Test
        @DisplayName("UT-SVC-I-002: Service handles authentication result properly")
        void testServiceWithValidResult() throws AuthenticationException {
            service = new AuthenticationService(
                    integrationRegistry, pluginManager, bindingResolver,
                    userResolver, roleMapper, subjectBuilder, roleResolutionStage);

            BasicPrincipal principal = BasicPrincipal.builder()
                    .name("alice")
                    .authenticator("test_integration")
                    .build();

            AuthenticationResult successResult = AuthenticationResult.success(principal);

            when(bindingResolver.resolveCandidates(any(), any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            when(plugin.supports(any())).thenReturn(true);
            when(plugin.authenticate(any(), any())).thenReturn(successResult);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            assertNotNull(outcome);
            assertTrue(outcome.getAuthResult().isSuccess());
        }
    }

    // ==================== Getter Tests ====================

    @Nested
    @DisplayName("Getter Tests")
    class GetterTests {

        @BeforeEach
        void setUp() {
            service = new AuthenticationService(integrationRegistry, pluginManager, bindingResolver);
        }

        @Test
        @DisplayName("UT-SVC-G-001: Get integration registry")
        void testGetIntegrationRegistry() {
            assertEquals(integrationRegistry, service.getIntegrationRegistry());
        }

        @Test
        @DisplayName("UT-SVC-G-002: Get plugin manager")
        void testGetPluginManager() {
            assertEquals(pluginManager, service.getPluginManager());
        }

        @Test
        @DisplayName("UT-SVC-G-003: Get binding resolver")
        void testGetBindingResolver() {
            assertEquals(bindingResolver, service.getBindingResolver());
        }
    }
}
