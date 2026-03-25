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

package org.apache.doris.authentication.plugin.oidc;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.RSAKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DisplayName("OIDC Authentication Plugin Tests")
class OidcAuthenticationPluginTest {

    private RSAKey rsaKey;
    private TestableOidcAuthenticationPlugin plugin;
    private AuthenticationIntegration integration;

    @BeforeEach
    void setUp() throws Exception {
        rsaKey = OidcTestUtils.newRsaJwk("key-1");
        plugin = new TestableOidcAuthenticationPlugin(rsaKey);
        integration = createIntegration("test_oidc", Map.of());
    }

    @AfterEach
    void tearDown() {
        plugin.close();
    }

    @Nested
    @DisplayName("Plugin Metadata")
    class PluginMetadataTests {

        @Test
        @DisplayName("UT-OIDC-PLUGIN-001: plugin name should be oidc")
        void testName() {
            Assertions.assertEquals("oidc", plugin.name());
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-002: description should mention oidc")
        void testDescription() {
            Assertions.assertTrue(plugin.description().toLowerCase().contains("oidc"));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-003: plugin does not require clear password")
        void testRequiresClearPassword() {
            Assertions.assertFalse(plugin.requiresClearPassword());
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-004: plugin is single-step authentication")
        void testSupportsMultiStep() {
            Assertions.assertFalse(plugin.supportsMultiStep());
        }
    }

    @Nested
    @DisplayName("Request Support")
    class SupportsTests {

        @Test
        @DisplayName("UT-OIDC-PLUGIN-005: supports OIDC_ID_TOKEN")
        void testSupportsOidcIdToken() {
            Assertions.assertTrue(plugin.supports(createRequest("alice", CredentialType.OIDC_ID_TOKEN, "token")));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-006: supports JWT_TOKEN")
        void testSupportsJwtToken() {
            Assertions.assertTrue(plugin.supports(createRequest("alice", CredentialType.JWT_TOKEN, "token")));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-007: rejects unsupported credential type")
        void testRejectsUnsupportedType() {
            Assertions.assertFalse(plugin.supports(createRequest("alice", CredentialType.OAUTH_TOKEN, "token")));
        }
    }

    @Nested
    @DisplayName("Configuration Validation")
    class ValidationTests {

        @Test
        @DisplayName("UT-OIDC-PLUGIN-008: validate accepts valid configuration")
        void testValidateAcceptsValidConfiguration() {
            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-009: validate rejects missing issuer")
        void testValidateRejectsMissingIssuer() {
            AuthenticationIntegration invalidIntegration = createIntegration("test_oidc", Map.of("oidc.issuer", ""));

            AuthenticationException exception = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> plugin.validate(invalidIntegration)
            );

            Assertions.assertEquals(AuthenticationFailureType.MISCONFIGURED, exception.getFailureType());
            Assertions.assertTrue(exception.getMessage().contains("oidc.issuer"));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-010: validate rejects unsupported algorithm")
        void testValidateRejectsUnsupportedAlgorithm() {
            AuthenticationIntegration invalidIntegration = createIntegration(
                    "test_oidc",
                    Map.of("oidc.allowed_algorithms", "HS256")
            );

            AuthenticationException exception = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> plugin.validate(invalidIntegration)
            );

            Assertions.assertEquals(AuthenticationFailureType.MISCONFIGURED, exception.getFailureType());
            Assertions.assertTrue(exception.getMessage().contains("Unsupported OIDC algorithm"));
        }
    }

    @Nested
    @DisplayName("Authentication")
    class AuthenticationTests {

        @Test
        @DisplayName("UT-OIDC-PLUGIN-011: authenticate lazily initializes and returns principal")
        void testAuthenticateSuccess() throws Exception {
            String token = createToken(
                    rsaKey,
                    "subject-alice",
                    "alice",
                    List.of("team-a", "sre"),
                    Map.of("email", "alice@example.com")
            );

            AuthenticationResult result = plugin.authenticate(
                    createRequest("alice", CredentialType.OIDC_ID_TOKEN, token),
                    integration
            );

            Assertions.assertTrue(result.isSuccess());
            Principal principal = result.getPrincipal();
            Assertions.assertEquals("alice", principal.getName());
            Assertions.assertEquals("test_oidc", principal.getAuthenticator());
            // Assertions.assertEquals("subject-alice", principal.getExternalPrincipal().orElseThrow());
            Assertions.assertEquals(Set.of("team-a", "sre"), principal.getExternalGroups());
            Assertions.assertEquals(Map.of("email", "alice@example.com"), principal.getAttributes());
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-012: authenticate fails when token is missing")
        void testAuthenticateFailsWhenTokenMissing() throws Exception {
            AuthenticationResult result = plugin.authenticate(
                    AuthenticationRequest.builder()
                            .username("alice")
                            .credentialType(CredentialType.OIDC_ID_TOKEN)
                            .credential(null)
                            .build(),
                    integration
            );

            Assertions.assertTrue(result.isFailure());
            Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, result.getException().getFailureType());
            Assertions.assertTrue(result.getException().getMessage().contains("token"));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-013: authenticate fails when username does not match token")
        void testAuthenticateFailsOnUsernameMismatch() throws Exception {
            String token = createToken(
                    rsaKey,
                    "subject-alice",
                    "bob",
                    List.of("team-a"),
                    Map.of()
            );

            AuthenticationResult result = plugin.authenticate(
                    createRequest("alice", CredentialType.JWT_TOKEN, token),
                    integration
            );

            Assertions.assertTrue(result.isFailure());
            Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, result.getException().getFailureType());
            Assertions.assertTrue(result.getException().getMessage().contains("username"));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-014: initialize rejects malformed jwks uri")
        void testInitializeRejectsMalformedJwksUri() {
            OidcAuthenticationPlugin realPlugin = new OidcAuthenticationPlugin();
            AuthenticationIntegration invalidIntegration = createIntegration(
                    "invalid_oidc",
                    Map.of("oidc.jwks_uri", "::not-a-valid-url::")
            );

            AuthenticationException exception = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> realPlugin.initialize(invalidIntegration)
            );

            Assertions.assertEquals(AuthenticationFailureType.MISCONFIGURED, exception.getFailureType());
            Assertions.assertTrue(exception.getMessage().contains("oidc.jwks_uri"));
        }

        @Test
        @DisplayName("UT-OIDC-PLUGIN-015: reload replaces validator state")
        void testReloadReplacesValidatorState() throws Exception {
            plugin.initialize(integration);

            RSAKey newKey = OidcTestUtils.newRsaJwk("key-2");
            plugin.setRsaKey(newKey);
            AuthenticationIntegration updatedIntegration = createIntegration(
                    "test_oidc",
                    Map.of("oidc.allowed_audiences", "doris,updated-client")
            );
            plugin.reload(updatedIntegration);

            String token = createToken(
                    newKey,
                    "subject-alice",
                    "alice",
                    List.of("team-a"),
                    Map.of("email", "alice@example.com"),
                    List.of("updated-client")
            );

            AuthenticationResult result = plugin.authenticate(
                    createRequest("alice", CredentialType.OIDC_ID_TOKEN, token),
                    updatedIntegration
            );

            Assertions.assertTrue(result.isSuccess());
        }
    }

    private AuthenticationIntegration createIntegration(String name, Map<String, String> overrides) {
        Map<String, String> properties = new java.util.HashMap<>();
        properties.put("oidc.issuer", "https://issuer.example.com");
        properties.put("oidc.jwks_uri", "https://issuer.example.com/keys");
        properties.put("oidc.allowed_audiences", "doris,grafana");
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

    private String createToken(
            RSAKey key,
            String subject,
            String username,
            Object groups,
            Map<String, Object> extraClaims
    ) throws Exception {
        return createToken(key, subject, username, groups, extraClaims, List.of("doris"));
    }

    private String createToken(
            RSAKey key,
            String subject,
            String username,
            Object groups,
            Map<String, Object> extraClaims,
            List<String> audiences
    ) throws Exception {
        return OidcTestUtils.createSignedToken(
                key,
                JWSAlgorithm.RS256,
                "https://issuer.example.com",
                audiences,
                subject,
                username,
                groups,
                extraClaims,
                Instant.now().minusSeconds(30),
                Instant.now().plusSeconds(300)
        );
    }

    private static final class TestableOidcAuthenticationPlugin extends OidcAuthenticationPlugin {
        private RSAKey rsaKey;

        private TestableOidcAuthenticationPlugin(RSAKey rsaKey) {
            this.rsaKey = rsaKey;
        }

        @Override
        protected OidcTokenValidator createValidator(OidcIntegrationConfig config) {
            return new OidcTokenValidator(config, OidcTestUtils.fixedJwkSource(rsaKey));
        }

        private void setRsaKey(RSAKey rsaKey) {
            this.rsaKey = rsaKey;
        }
    }
}
