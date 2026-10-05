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

package org.apache.doris.authentication.spi;

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for {@link AuthenticationPlugin} implementations.
 * These tests define the expected behavior that all plugin implementations must follow.
 */
@DisplayName("AuthenticationPlugin Contract Tests")
class AuthenticationPluginContractTest {

    private TestAuthenticationPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = new TestAuthenticationPlugin();
    }

    @Test
    @DisplayName("UT-SPI-PC-001: Plugin should return non-null name")
    void testPluginName() {
        // When
        String name = plugin.name();

        // Then
        Assertions.assertNotNull(name, "Plugin name must not be null");
        Assertions.assertFalse(name.trim().isEmpty(), "Plugin name must not be empty");
    }

    @Test
    @DisplayName("UT-SPI-PC-002: Plugin should handle null request gracefully")
    void testAuthenticateWithNullRequest() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("test")
                .build();

        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                plugin.authenticate(null, integration)
        );
    }

    @Test
    @DisplayName("UT-SPI-PC-003: Plugin should handle null integration gracefully")
    void testAuthenticateWithNullIntegration() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("test")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .build();

        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                plugin.authenticate(request, null)
        );
    }

    @Test
    @DisplayName("UT-SPI-PC-004: Plugin lifecycle - initialize should be called before authenticate")
    void testPluginLifecycle_Initialize() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("test")
                .build();

        // When
        plugin.initialize(integration);

        // Then
        Assertions.assertTrue(plugin.isInitialized());
    }

    @Test
    @DisplayName("UT-SPI-PC-005: Plugin lifecycle - close should clean up resources")
    void testPluginLifecycle_Close() {
        // Given
        plugin.initialize(AuthenticationIntegration.builder()
                .name("test")
                .type("test")
                .build());

        // When
        plugin.close();

        // Then
        Assertions.assertTrue(plugin.isClosed());
    }

    @Test
    @DisplayName("UT-SPI-PC-007: Plugin should validate configuration")
    void testValidateConfiguration() {
        // Given
        AuthenticationIntegration validConfig = AuthenticationIntegration.builder()
                .name("valid")
                .type("test")
                .property("key", "value")
                .build();

        AuthenticationIntegration invalidConfig = AuthenticationIntegration.builder()
                .name("invalid")
                .type("test")
                .property("invalid_key", "value")
                .build();

        // When & Then
        Assertions.assertDoesNotThrow(() -> plugin.validate(validConfig));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                plugin.validate(invalidConfig)
        );
    }

    /**
     * Test implementation of AuthenticationPlugin for contract testing.
     */
    private static class TestAuthenticationPlugin implements AuthenticationPlugin {
        private boolean initialized = false;
        private boolean closed = false;

        @Override
        public String name() {
            return "test-plugin";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return true;
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request,
                                                  AuthenticationIntegration integration) {
            if (request == null || integration == null) {
                throw new NullPointerException("Request and integration must not be null");
            }
            return AuthenticationResult.success(
                    org.apache.doris.authentication.BasicPrincipal.builder()
                            .name(request.getUsername())
                            .authenticator(name())
                            .build()
            );
        }

        @Override
        public void initialize(AuthenticationIntegration integration) {
            if (integration == null) {
                throw new NullPointerException("Integration must not be null");
            }
            initialized = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void validate(AuthenticationIntegration integration) {
            if (integration.getProperties().containsKey("invalid_key")) {
                throw new IllegalArgumentException("Invalid configuration key");
            }
        }

        public boolean isInitialized() {
            return initialized;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
