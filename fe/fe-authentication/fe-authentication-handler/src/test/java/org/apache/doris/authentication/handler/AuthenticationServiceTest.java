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

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Identity;
import org.apache.doris.authentication.spi.AuthenticationPlugin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
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
    private AuthenticationPluginManager pluginManager;

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

            Assertions.assertNotNull(service);
            Assertions.assertEquals(integrationRegistry, service.getIntegrationRegistry());
            Assertions.assertEquals(pluginManager, service.getPluginManager());
            Assertions.assertEquals(bindingResolver, service.getBindingResolver());
        }

        @Test
        @DisplayName("UT-SVC-C-002: Full constructor with all dependencies")
        void testFullConstructor() {
            service = new AuthenticationService(
                    integrationRegistry, pluginManager, bindingResolver,
                    userResolver, roleMapper, subjectBuilder, roleResolutionStage);

            Assertions.assertNotNull(service);
        }

        @Test
        @DisplayName("UT-SVC-C-003: Constructor with null integrationRegistry throws NPE")
        void testConstructor_NullIntegrationRegistry() {
            Assertions.assertThrows(NullPointerException.class, () ->
                    new AuthenticationService(null, pluginManager, bindingResolver));
        }

        @Test
        @DisplayName("UT-SVC-C-004: Constructor with null pluginManager throws NPE")
        void testConstructor_NullPluginManager() {
            Assertions.assertThrows(NullPointerException.class, () ->
                    new AuthenticationService(integrationRegistry, null, bindingResolver));
        }

        @Test
        @DisplayName("UT-SVC-C-005: Constructor with null bindingResolver throws NPE")
        void testConstructor_NullBindingResolver() {
            Assertions.assertThrows(NullPointerException.class, () ->
                    new AuthenticationService(integrationRegistry, pluginManager, null));
        }

        @Test
        @DisplayName("UT-SVC-C-006: Constructor uses defaults for optional dependencies")
        void testConstructor_DefaultsForOptionalDeps() {
            service = new AuthenticationService(integrationRegistry, pluginManager, bindingResolver);

            // Service should work with default implementations
            Assertions.assertNotNull(service);
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

            Mockito.when(bindingResolver.resolveCandidates(Mockito.eq("alice"), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(successResult);
            Mockito.when(userResolver.resolveUser(Mockito.any(), Mockito.any())).thenReturn("alice_user_object");
            Mockito.when(roleResolutionStage.resolveRoles(Mockito.any(), Mockito.any()))
                    .thenReturn(new HashSet<>(Arrays.asList("user", "admin")));
            Mockito.when(subjectBuilder.build(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(null);

            // When
            AuthenticationResult result = service.authenticate(testRequest);

            // Then
            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isSuccess());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(userResolver).resolveUser(Mockito.any(), Mockito.any());
            Mockito.verify(roleResolutionStage).resolveRoles(Mockito.any(), Mockito.any());
        }

        @Test
        @DisplayName("UT-SVC-A-002: Authentication with null request throws exception")
        void testAuthenticate_NullRequest() {
            Assertions.assertThrows(AuthenticationException.class, () ->
                    service.authenticate(null));
        }

        @Test
        @DisplayName("UT-SVC-A-003: No candidates available throws exception")
        void testAuthenticate_NoCandidates() {
            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.emptyList());

            AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () ->
                    service.authenticate(testRequest));

            Assertions.assertTrue(ex.getMessage().contains("No authentication integration"));
        }

        @Test
        @DisplayName("UT-SVC-A-004: No plugin supports request throws exception")
        void testAuthenticate_NoSupportingPlugin() throws AuthenticationException {
            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(false);

            AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () ->
                    service.authenticate(testRequest));

            Assertions.assertTrue(ex.getMessage().contains("No authentication integration supports"));
        }

        @Test
        @DisplayName("UT-SVC-A-005: Plugin authentication failure")
        void testAuthenticate_PluginFailure() throws AuthenticationException {
            AuthenticationResult failureResult = AuthenticationResult.failure("Invalid credentials");

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(failureResult);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            Assertions.assertNotNull(outcome);
            Assertions.assertFalse(outcome.getAuthResult().isSuccess());
            Mockito.verify(userResolver, Mockito.never()).resolveUser(Mockito.any(), Mockito.any());
        }

        @Test
        @DisplayName("UT-SVC-A-006: Multiple candidates, first supporting plugin is used")
        void testAuthenticate_MultipleCandidatesFirstSupports() throws AuthenticationException {
            AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                    .name("integration2")
                    .type("ldap")
                    .properties(new HashMap<>())
                    .build();

            AuthenticationPlugin plugin2 = Mockito.mock(AuthenticationPlugin.class);

            Identity identity = Identity.builder()
                    .username("alice")
                    .authenticatorName("test_integration")
                    .authenticatorPluginName("password")
                    .build();

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any())).thenReturn(AuthenticationResult.success(identity));

            service.authenticate(testRequest);

            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(plugin2, Mockito.never()).authenticate(Mockito.any(), Mockito.any());
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

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(successResult);
            Mockito.when(userResolver.resolveUser(Mockito.any(), Mockito.any())).thenReturn("user_obj");
            Mockito.when(roleResolutionStage.resolveRoles(Mockito.any(), Mockito.any()))
                    .thenReturn(new HashSet<>(Arrays.asList("admin")));
            Mockito.when(subjectBuilder.build(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(null);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            Assertions.assertNotNull(outcome);
            Assertions.assertTrue(outcome.getAuthResult().isSuccess());
            Assertions.assertEquals(testIntegration, outcome.getIntegration());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(subjectBuilder).build(Mockito.eq(principal), Mockito.any(), Mockito.eq(testRequest));
        }

        @Test
        @DisplayName("UT-SVC-O-002: Outcome on failure does not resolve user or roles")
        void testAuthenticateWithOutcome_FailureShortCircuits() throws AuthenticationException {
            AuthenticationResult failureResult = AuthenticationResult.failure("Bad password");

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(failureResult);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            Assertions.assertFalse(outcome.getAuthResult().isSuccess());
            Mockito.verify(userResolver, Mockito.never()).resolveUser(Mockito.any(), Mockito.any());
            Mockito.verify(roleResolutionStage, Mockito.never()).resolveRoles(Mockito.any(), Mockito.any());
            Mockito.verify(subjectBuilder, Mockito.never()).build(Mockito.any(), Mockito.any(), Mockito.any());
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

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any())).thenReturn(AuthenticationResult.success(identity));

            AuthenticationResult result = service.authenticate(testRequest);

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isSuccess());
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

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any())).thenReturn(successResult);

            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            Assertions.assertNotNull(outcome);
            Assertions.assertTrue(outcome.getAuthResult().isSuccess());
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
            Assertions.assertEquals(integrationRegistry, service.getIntegrationRegistry());
        }

        @Test
        @DisplayName("UT-SVC-G-002: Get plugin manager")
        void testGetPluginManager() {
            Assertions.assertEquals(pluginManager, service.getPluginManager());
        }

        @Test
        @DisplayName("UT-SVC-G-003: Get binding resolver")
        void testGetBindingResolver() {
            Assertions.assertEquals(bindingResolver, service.getBindingResolver());
        }
    }
}
