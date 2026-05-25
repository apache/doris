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
import org.apache.doris.authentication.BasicPrincipal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for {@link AuthenticationPluginFactory} implementations.
 * These tests define the expected behavior that all factory implementations must follow.
 */
@DisplayName("AuthenticationPluginFactory Contract Tests")
class AuthenticationPluginFactoryContractTest {

    @Test
    @DisplayName("UT-SPI-FC-001: Factory should return non-null plugin name")
    void testFactoryName() {
        // Given
        TestAuthenticationPluginFactory factory = new TestAuthenticationPluginFactory();

        // When
        String name = factory.name();

        // Then
        Assertions.assertNotNull(name, "Factory name must not be null");
        Assertions.assertFalse(name.trim().isEmpty(), "Factory name must not be empty");
    }

    @Test
    @DisplayName("UT-SPI-FC-002: Factory should create non-null plugin instance")
    void testFactoryCreate() {
        // Given
        TestAuthenticationPluginFactory factory = new TestAuthenticationPluginFactory();

        // When
        AuthenticationPlugin plugin = factory.create();

        // Then
        Assertions.assertNotNull(plugin, "Factory must return non-null plugin instance");
    }

    @Test
    @DisplayName("UT-SPI-FC-003: Factory name should match created plugin name")
    void testFactoryNameMatchesPluginName() {
        // Given
        TestAuthenticationPluginFactory factory = new TestAuthenticationPluginFactory();

        // When
        AuthenticationPlugin plugin = factory.create();

        // Then
        Assertions.assertEquals(factory.name(), plugin.name(),
                "Factory name should match the plugin name it creates");
    }

    @Test
    @DisplayName("UT-SPI-FC-004: Factory should provide description")
    void testFactoryDescription() {
        // Given
        TestAuthenticationPluginFactory factory = new TestAuthenticationPluginFactory();

        // When
        String description = factory.description();

        // Then
        Assertions.assertNotNull(description, "Factory description must not be null");
    }

    /**
     * Test implementation of AuthenticationPluginFactory for contract testing.
     */
    private static class TestAuthenticationPluginFactory implements AuthenticationPluginFactory {
        public String name() {
            return "test-factory";
        }

        public AuthenticationPlugin create() {
            return new TestPlugin();
        }

        public String description() {
            return "Test factory for contract testing";
        }
    }

    /**
     * Test implementation of AuthenticationPlugin.
     */
    private static class TestPlugin implements AuthenticationPlugin {
        @Override
        public String name() {
            return "test-factory";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return true;
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request,
                                                  AuthenticationIntegration integration) {
            return AuthenticationResult.success(
                    BasicPrincipal.builder()
                            .name(request.getUsername())
                            .authenticator(name())
                            .build()
            );
        }

        @Override
        public void initialize(AuthenticationIntegration integration) {
        }

        @Override
        public void close() {
        }

        @Override
        public void validate(AuthenticationIntegration integration) {
        }
    }
}
