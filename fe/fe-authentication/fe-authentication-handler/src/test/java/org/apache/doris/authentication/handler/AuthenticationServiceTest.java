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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

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
                    integrationRegistry, pluginManager, bindingResolver);
        }

        @Test
        @DisplayName("UT-SVC-A-001: Successful authentication flow")
        void testAuthenticate_Success() throws AuthenticationException {
            // Given
            BasicPrincipal principal = BasicPrincipal.builder()
                    .name("alice")
                    .authenticator("test_integration")
                    .build();

            AuthenticationResult successResult = AuthenticationResult.success(principal);

            Mockito.when(bindingResolver.resolveCandidates(Mockito.eq("alice"), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(successResult);
            // When
            AuthenticationResult result = service.authenticate(testRequest);

            // Then
            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isSuccess());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
        }

        @Test
        @DisplayName("UT-SVC-A-002: Authentication with null request throws exception")
        void testAuthenticate_NullRequest() {
            Assertions.assertThrows(AuthenticationException.class, () ->
                    service.authenticate(null));
        }

        @Test
        @DisplayName("UT-SVC-A-003: No candidates available throws exception")
        void testAuthenticate_NoCandidates() throws AuthenticationException {
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

            BasicPrincipal principal = BasicPrincipal.builder()
                    .name("alice")
                    .authenticator("test_integration")
                    .build();

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any())).thenReturn(AuthenticationResult.success(principal));

            service.authenticate(testRequest);

            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(plugin2, Mockito.never()).authenticate(Mockito.any(), Mockito.any());
        }

        @Test
        @DisplayName("UT-SVC-A-007: Multiple candidates, failure falls through to next")
        void testAuthenticate_MultipleCandidatesFailureFallsThrough() throws AuthenticationException {
            AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                    .name("integration2")
                    .type("ldap")
                    .properties(new HashMap<>())
                    .build();

            AuthenticationPlugin plugin2 = Mockito.mock(AuthenticationPlugin.class);

            AuthenticationResult failureResult = AuthenticationResult.failure("Bad password");
            AuthenticationResult successResult = AuthenticationResult.success(
                    BasicPrincipal.builder()
                            .name("alice")
                            .authenticator("integration2")
                            .build()
            );

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            Mockito.when(bindingResolver.hasUserBinding(Mockito.anyString()))
                    .thenReturn(false);
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(failureResult);
            Mockito.when(plugin2.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin2.authenticate(Mockito.any(), Mockito.eq(integration2))).thenReturn(successResult);

            AuthenticationResult result = service.authenticate(testRequest);

            Assertions.assertTrue(result.isSuccess());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(plugin2).authenticate(Mockito.any(), Mockito.eq(integration2));
        }

        @Test
        @DisplayName("UT-SVC-A-008: No-binding chain continues when candidate throws exception")
        void testAuthenticate_NoBindingChainContinuesAfterException() throws AuthenticationException {
            AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                    .name("integration2")
                    .type("ldap")
                    .properties(new HashMap<>())
                    .build();
            AuthenticationPlugin plugin2 = Mockito.mock(AuthenticationPlugin.class);

            AuthenticationResult successResult = AuthenticationResult.success(
                    BasicPrincipal.builder()
                            .name("alice")
                            .authenticator("integration2")
                            .build()
            );

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            Mockito.when(bindingResolver.hasUserBinding(Mockito.anyString()))
                    .thenReturn(false);
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration)))
                    .thenThrow(new AuthenticationException("integration unavailable"));
            Mockito.when(plugin2.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin2.authenticate(Mockito.any(), Mockito.eq(integration2))).thenReturn(successResult);

            AuthenticationResult result = service.authenticate(testRequest);

            Assertions.assertTrue(result.isSuccess());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(plugin2).authenticate(Mockito.any(), Mockito.eq(integration2));
        }

        @Test
        @DisplayName("UT-SVC-A-009: User binding is fail-fast and does not fallback on failure result")
        void testAuthenticate_UserBindingFailFastOnFailureResult() throws AuthenticationException {
            AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                    .name("integration2")
                    .type("ldap")
                    .properties(new HashMap<>())
                    .build();
            AuthenticationPlugin plugin2 = Mockito.mock(AuthenticationPlugin.class);
            AuthenticationResult failureResult = AuthenticationResult.failure("Invalid credentials");

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            Mockito.when(bindingResolver.hasUserBinding(Mockito.anyString()))
                    .thenReturn(true);
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration))).thenReturn(failureResult);

            AuthenticationResult result = service.authenticate(testRequest);

            Assertions.assertTrue(result.isFailure());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
            Mockito.verify(plugin2, Mockito.never()).authenticate(Mockito.any(), Mockito.any());
        }

        @Test
        @DisplayName("UT-SVC-A-010: User binding is fail-fast and propagates plugin exception")
        void testAuthenticate_UserBindingFailFastOnPluginException() throws AuthenticationException {
            AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                    .name("integration2")
                    .type("ldap")
                    .properties(new HashMap<>())
                    .build();
            AuthenticationPlugin plugin2 = Mockito.mock(AuthenticationPlugin.class);

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Arrays.asList(testIntegration, integration2));
            Mockito.when(bindingResolver.hasUserBinding(Mockito.anyString()))
                    .thenReturn(true);
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(pluginManager.getPlugin(integration2)).thenReturn(plugin2);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.eq(testIntegration)))
                    .thenThrow(new AuthenticationException("bound integration internal error"));

            AuthenticationException ex = Assertions.assertThrows(AuthenticationException.class, () ->
                    service.authenticate(testRequest));

            Assertions.assertTrue(ex.getMessage().contains("bound integration internal error"));
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
                    integrationRegistry, pluginManager, bindingResolver);
        }

        @Test
        @DisplayName("UT-SVC-O-001: Outcome includes all components on success")
        void testAuthenticateWithOutcome_Success() throws AuthenticationException {
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
            AuthenticationOutcome outcome = service.authenticateWithOutcome(testRequest);

            Assertions.assertNotNull(outcome);
            Assertions.assertTrue(outcome.getAuthResult().isSuccess());
            Assertions.assertEquals(testIntegration, outcome.getIntegration());
            Mockito.verify(plugin).authenticate(Mockito.any(), Mockito.eq(testIntegration));
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

            BasicPrincipal principal = BasicPrincipal.builder()
                    .name("alice")
                    .authenticator("test_integration")
                    .build();

            Mockito.when(bindingResolver.resolveCandidates(Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.singletonList(testIntegration));
            Mockito.when(pluginManager.getPlugin(testIntegration)).thenReturn(plugin);
            Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
            Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any())).thenReturn(AuthenticationResult.success(principal));

            AuthenticationResult result = service.authenticate(testRequest);

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.isSuccess());
        }

        @Test
        @DisplayName("UT-SVC-I-002: Service handles authentication result properly")
        void testServiceWithValidResult() throws AuthenticationException {
            service = new AuthenticationService(
                    integrationRegistry, pluginManager, bindingResolver);

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

    // ==================== External Plugin Management Tests ====================

    @Nested
    @DisplayName("External Plugin Management Tests")
    class ExternalPluginManagementTests {

        @BeforeEach
        void setUp() {
            service = new AuthenticationService(integrationRegistry, pluginManager, bindingResolver);
        }

        @Test
        @DisplayName("UT-SVC-EPM-001: Load external plugins delegates to plugin manager")
        void testLoadExternalPlugins() throws AuthenticationException {
            // Given
            ClassLoader parent = Thread.currentThread().getContextClassLoader();

            // When
            service.loadExternalPlugins(Arrays.asList(Path.of("/tmp/plugins")), parent);

            // Then
            Mockito.verify(pluginManager).loadAll(Arrays.asList(Path.of("/tmp/plugins")), parent);
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
