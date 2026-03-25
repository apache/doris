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
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.plugin.oidc.TestOidcAuthenticationPluginFactory;

import com.nimbusds.jose.jwk.RSAKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DisplayName("OIDC AuthenticationService Tests")
class OidcAuthenticationServiceTest {

    private static final String ISSUER = "https://issuer.example.com";
    private static final List<String> AUDIENCES = List.of("doris");

    private IntegrationRegistry integrationRegistry;
    private BindingRegistry bindingRegistry;
    private AuthenticationPluginManager pluginManager;
    private AuthenticationService service;
    private RSAKey rsaKey;

    @BeforeEach
    void setUp() throws Exception {
        integrationRegistry = new IntegrationRegistry();
        bindingRegistry = new BindingRegistry();
        pluginManager = new AuthenticationPluginManager();
        rsaKey = TestOidcAuthenticationPluginFactory.newRsaJwk("service-test-key");
        pluginManager.registerFactory(new TestOidcAuthenticationPluginFactory(rsaKey));
        service = new AuthenticationService(
                integrationRegistry,
                pluginManager,
                new BindingResolver(integrationRegistry, bindingRegistry)
        );
    }

    @AfterEach
    void tearDown() {
        pluginManager.clearCache();
        integrationRegistry.clear();
        bindingRegistry.clear();
    }

    @Test
    @DisplayName("UT-SVC-OIDC-001: no-binding chain selects oidc plugin for jwt request")
    void testAuthenticate_NoBindingChainUsesOidcPlugin() throws Exception {
        AuthenticationIntegration passwordIntegration = AuthenticationIntegration.builder()
                .name("local_password")
                .type("password")
                .build();
        AuthenticationIntegration oidcIntegration = createOidcIntegration("oidc_primary", Map.of());

        integrationRegistry.register(passwordIntegration);
        integrationRegistry.register(oidcIntegration);
        integrationRegistry.setAuthenticationChain(List.of(passwordIntegration, oidcIntegration));

        String token = TestOidcAuthenticationPluginFactory.createSignedToken(
                rsaKey,
                ISSUER,
                AUDIENCES,
                "subject-alice",
                "alice",
                List.of("team-a", "sre"),
                Map.of("email", "alice@example.com")
        );

        AuthenticationResult result = service.authenticate(
                createRequest("alice", CredentialType.JWT_TOKEN, token)
        );

        Assertions.assertTrue(result.isSuccess());
        Principal principal = result.getPrincipal();
        Assertions.assertEquals("alice", principal.getName());
        Assertions.assertEquals("oidc_primary", principal.getAuthenticator());
        Assertions.assertEquals("subject-alice", principal.getExternalPrincipal().orElseThrow());
        Assertions.assertEquals(Set.of("team-a", "sre"), principal.getExternalGroups());
        Assertions.assertEquals(Map.of("email", "alice@example.com"), principal.getAttributes());
    }

    @Test
    @DisplayName("UT-SVC-OIDC-002: misconfigured oidc candidate does not silently fall through")
    void testAuthenticate_MisconfiguredOidcDoesNotFallThrough() throws Exception {
        AuthenticationIntegration misconfiguredIntegration = createOidcIntegration(
                "oidc_misconfigured",
                Map.of("oidc.allowed_algorithms", "HS256")
        );
        AuthenticationIntegration fallbackIntegration = createOidcIntegration("oidc_fallback", Map.of());

        integrationRegistry.register(misconfiguredIntegration);
        integrationRegistry.register(fallbackIntegration);
        integrationRegistry.setAuthenticationChain(List.of(misconfiguredIntegration, fallbackIntegration));

        String token = TestOidcAuthenticationPluginFactory.createSignedToken(
                rsaKey,
                ISSUER,
                AUDIENCES,
                "subject-alice",
                "alice",
                List.of("team-a"),
                Map.of()
        );

        AuthenticationException exception = Assertions.assertThrows(
                AuthenticationException.class,
                () -> service.authenticate(createRequest("alice", CredentialType.OIDC_ID_TOKEN, token))
        );

        Assertions.assertEquals(AuthenticationFailureType.MISCONFIGURED, exception.getFailureType());
        Assertions.assertTrue(exception.getMessage().contains("Unsupported OIDC algorithm"));
        Assertions.assertEquals(0, pluginManager.getCachedPluginCount());
    }

    private AuthenticationIntegration createOidcIntegration(String name, Map<String, String> overrides) {
        java.util.HashMap<String, String> properties = new java.util.HashMap<>();
        properties.put("oidc.issuer", ISSUER);
        properties.put("oidc.jwks_uri", ISSUER + "/keys");
        properties.put("oidc.allowed_audiences", String.join(",", AUDIENCES));
        properties.put("oidc.extra_claims", "email");
        properties.putAll(overrides);
        return AuthenticationIntegration.builder()
                .name(name)
                .type("oidc")
                .properties(properties)
                .build();
    }

    private AuthenticationRequest createRequest(String username, String credentialType, String token) {
        return AuthenticationRequest.builder()
                .username(username)
                .credentialType(credentialType)
                .credential(token.getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
