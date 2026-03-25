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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.RSAKey;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DisplayName("OIDC Token Validator Tests")
class OidcTokenValidatorTest {

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-001: valid signed JWT passes")
    void testValidSignedJwtPasses() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-1");
        OidcTokenValidator validator = new OidcTokenValidator(createConfig(), OidcTestUtils.fixedJwkSource(rsaKey));
        String token = OidcTestUtils.createSignedToken(
                rsaKey,
                JWSAlgorithm.RS256,
                "https://issuer.example.com",
                List.of("doris", "grafana"),
                "subject-alice",
                "alice",
                List.of("team-a", "sre"),
                Map.of("email", "alice@example.com"),
                Instant.now().minusSeconds(30),
                Instant.now().plusSeconds(300)
        );

        OidcIdentity identity = validator.validate(token, "alice");

        Assertions.assertEquals("alice", identity.getUsername());
        Assertions.assertEquals("subject-alice", identity.getSubject());
        Assertions.assertEquals(Set.of("team-a", "sre"), identity.getGroups());
        Assertions.assertEquals(Map.of("email", "alice@example.com"), identity.getAttributes());
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-002: issuer mismatch fails")
    void testIssuerMismatchFails() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-1");
        OidcTokenValidator validator = new OidcTokenValidator(createConfig(), OidcTestUtils.fixedJwkSource(rsaKey));
        String token = OidcTestUtils.createSignedToken(
                rsaKey,
                JWSAlgorithm.RS256,
                "https://another-issuer.example.com",
                List.of("doris"),
                "subject-alice",
                "alice",
                List.of("team-a"),
                Map.of(),
                Instant.now().minusSeconds(30),
                Instant.now().plusSeconds(300)
        );

        AuthenticationException exception = Assertions.assertThrows(
                AuthenticationException.class,
                () -> validator.validate(token, "alice")
        );

        Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, exception.getFailureType());
        Assertions.assertTrue(exception.getMessage().contains("issuer"));
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-003: audience mismatch fails")
    void testAudienceMismatchFails() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-1");
        OidcTokenValidator validator = new OidcTokenValidator(createConfig(), OidcTestUtils.fixedJwkSource(rsaKey));
        String token = OidcTestUtils.createSignedToken(
                rsaKey,
                JWSAlgorithm.RS256,
                "https://issuer.example.com",
                List.of("other-service"),
                "subject-alice",
                "alice",
                List.of("team-a"),
                Map.of(),
                Instant.now().minusSeconds(30),
                Instant.now().plusSeconds(300)
        );

        AuthenticationException exception = Assertions.assertThrows(
                AuthenticationException.class,
                () -> validator.validate(token, "alice")
        );

        Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, exception.getFailureType());
        Assertions.assertTrue(exception.getMessage().contains("audience"));
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-004: expired token fails")
    void testExpiredTokenFails() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-1");
        OidcTokenValidator validator = new OidcTokenValidator(createConfig(), OidcTestUtils.fixedJwkSource(rsaKey));
        String token = OidcTestUtils.createSignedToken(
                rsaKey,
                JWSAlgorithm.RS256,
                "https://issuer.example.com",
                List.of("doris"),
                "subject-alice",
                "alice",
                List.of("team-a"),
                Map.of(),
                Instant.now().minusSeconds(300),
                Instant.now().minusSeconds(61)
        );

        AuthenticationException exception = Assertions.assertThrows(
                AuthenticationException.class,
                () -> validator.validate(token, "alice")
        );

        Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, exception.getFailureType());
        Assertions.assertTrue(exception.getMessage().contains("expired"));
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-005: disallowed algorithm fails")
    void testDisallowedAlgorithmFails() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-1");
        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(Map.of(
                "oidc.issuer", "https://issuer.example.com",
                "oidc.jwks_uri", "https://issuer.example.com/keys",
                "oidc.allowed_audiences", "doris",
                "oidc.allowed_algorithms", "RS512"
        ));
        config.validate();
        OidcTokenValidator validator = new OidcTokenValidator(config, OidcTestUtils.fixedJwkSource(rsaKey));
        String token = OidcTestUtils.createSignedToken(
                rsaKey,
                JWSAlgorithm.RS256,
                "https://issuer.example.com",
                List.of("doris"),
                "subject-alice",
                "alice",
                List.of("team-a"),
                Map.of(),
                Instant.now().minusSeconds(30),
                Instant.now().plusSeconds(300)
        );

        AuthenticationException exception = Assertions.assertThrows(
                AuthenticationException.class,
                () -> validator.validate(token, "alice")
        );

        Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, exception.getFailureType());
        Assertions.assertTrue(exception.getMessage().contains("algorithm"));
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-006: single string groups claim is accepted")
    void testSingleStringGroupsClaimIsAccepted() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-1");
        OidcTokenValidator validator = new OidcTokenValidator(createConfig(), OidcTestUtils.fixedJwkSource(rsaKey));
        String token = OidcTestUtils.createSignedToken(
                rsaKey,
                JWSAlgorithm.RS256,
                "https://issuer.example.com",
                List.of("doris"),
                "subject-alice",
                "alice",
                "team-a",
                Map.of(),
                Instant.now().minusSeconds(30),
                Instant.now().plusSeconds(300)
        );

        OidcIdentity identity = validator.validate(token, "alice");

        Assertions.assertEquals(Set.of("team-a"), identity.getGroups());
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-007: remote jwks endpoint validates signed JWT")
    void testRemoteJwksEndpointValidatesSignedJwt() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-remote");
        try (EmbeddedJwksServer server = EmbeddedJwksServer.withJwks(rsaKey.toPublicJWK().toJSONString())) {
            OidcTokenValidator validator = new OidcTokenValidator(createConfig(server.getJwksUri()));
            String token = OidcTestUtils.createSignedToken(
                    rsaKey,
                    JWSAlgorithm.RS256,
                    "https://issuer.example.com",
                    List.of("doris"),
                    "subject-alice",
                    "alice",
                    List.of("team-a", "sre"),
                    Map.of("email", "alice@example.com"),
                    Instant.now().minusSeconds(30),
                    Instant.now().plusSeconds(300)
            );

            OidcIdentity identity = validator.validate(token, "alice");

            Assertions.assertEquals("alice", identity.getUsername());
            Assertions.assertEquals("subject-alice", identity.getSubject());
            Assertions.assertEquals(Set.of("team-a", "sre"), identity.getGroups());
            Assertions.assertEquals(Map.of("email", "alice@example.com"), identity.getAttributes());
        }
    }

    @Test
    @DisplayName("UT-OIDC-VALIDATOR-008: malformed remote jwks payload maps to source unavailable")
    void testMalformedRemoteJwksPayloadFailsAsSourceUnavailable() throws Exception {
        RSAKey rsaKey = OidcTestUtils.newRsaJwk("key-remote");
        try (EmbeddedJwksServer server = EmbeddedJwksServer.withJwks("{not-json")) {
            OidcTokenValidator validator = new OidcTokenValidator(createConfig(server.getJwksUri()));
            String token = OidcTestUtils.createSignedToken(
                    rsaKey,
                    JWSAlgorithm.RS256,
                    "https://issuer.example.com",
                    List.of("doris"),
                    "subject-alice",
                    "alice",
                    List.of("team-a"),
                    Map.of(),
                    Instant.now().minusSeconds(30),
                    Instant.now().plusSeconds(300)
            );

            AuthenticationException exception = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> validator.validate(token, "alice")
            );

            Assertions.assertEquals(AuthenticationFailureType.SOURCE_UNAVAILABLE, exception.getFailureType());
            Assertions.assertTrue(exception.getMessage().contains("Failed to load or process OIDC JWKS"));
        }
    }

    private OidcIntegrationConfig createConfig() {
        return createConfig("https://issuer.example.com/keys");
    }

    private OidcIntegrationConfig createConfig(String jwksUri) {
        OidcIntegrationConfig config = OidcIntegrationConfig.fromProperties(Map.of(
                "oidc.issuer", "https://issuer.example.com",
                "oidc.jwks_uri", jwksUri,
                "oidc.allowed_audiences", "doris,grafana",
                "oidc.extra_claims", "email"
        ));
        config.validate();
        return config;
    }

    private static final class EmbeddedJwksServer implements AutoCloseable {
        private final HttpServer server;

        private EmbeddedJwksServer(String payload) throws IOException {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/keys", new JwksHandler(payload));
            server.setExecutor(null);
            server.start();
        }

        private static EmbeddedJwksServer withJwks(String payload) throws IOException {
            String body = "{\"keys\":[" + payload + "]}";
            return new EmbeddedJwksServer(body);
        }

        private String getJwksUri() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/keys";
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static final class JwksHandler implements HttpHandler {
        private final byte[] responseBody;

        private JwksHandler(String payload) {
            this.responseBody = payload.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBody.length);
            try (OutputStream responseStream = exchange.getResponseBody()) {
                responseStream.write(responseBody);
            }
        }
    }
}
