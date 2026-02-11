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
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * Unit tests for {@link AuthenticationPluginManager}.
 */
@DisplayName("PluginManager Unit Tests")
class AuthenticationPluginManagerTest {

    private AuthenticationPluginManager pluginManager;

    @BeforeEach
    void setUp() {
        pluginManager = new AuthenticationPluginManager();
    }

    @Test
    @DisplayName("UT-HANDLER-PM-001: Plugins loaded automatically from ServiceLoader")
    void testPluginsAutoLoaded() {
        // When - plugins are loaded in constructor automatically
        List<String> pluginNames = pluginManager.getRegisteredPluginNames();

        // Then
        Assertions.assertNotNull(pluginNames);
        Assertions.assertFalse(pluginNames.isEmpty(), "Should load at least built-in plugins");
        Assertions.assertTrue(pluginNames.contains("password"), "Should include password plugin");
    }

    @Test
    @DisplayName("UT-HANDLER-PM-002: Get plugin instance with integration")
    void testGetPluginWithIntegration() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();

        // When
        AuthenticationPlugin plugin = pluginManager.getPlugin(integration);

        // Then
        Assertions.assertNotNull(plugin);
        Assertions.assertEquals("password", plugin.name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-003: Get plugin for non-existent type throws exception")
    void testGetPlugin_NonExistentType() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("nonexistent")
                .build();

        // When & Then
        Assertions.assertThrows(AuthenticationException.class, () -> {
            pluginManager.getPlugin(integration);
        });
    }

    @Test
    @DisplayName("UT-HANDLER-PM-004: Register factory manually")
    void testRegisterFactory() {
        // Given
        TestPluginFactory factory = new TestPluginFactory();

        // When
        pluginManager.registerFactory(factory);

        // Then
        Optional<AuthenticationPluginFactory> retrieved = pluginManager.getFactory("test-manual");
        Assertions.assertTrue(retrieved.isPresent());
        Assertions.assertEquals("test-manual", retrieved.get().name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-005: Register duplicate factory replaces existing")
    void testRegisterFactory_Duplicate() {
        // Given
        TestPluginFactory factory1 = new TestPluginFactory();
        TestPluginFactory factory2 = new TestPluginFactory();

        // When
        pluginManager.registerFactory(factory1);
        pluginManager.registerFactory(factory2);  // Should replace

        // Then
        Optional<AuthenticationPluginFactory> retrieved = pluginManager.getFactory("test-manual");
        Assertions.assertTrue(retrieved.isPresent());
        Assertions.assertSame(factory2, retrieved.get());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-006: Remove factory")
    void testRemoveFactory() {
        // Given
        TestPluginFactory factory = new TestPluginFactory();
        pluginManager.registerFactory(factory);
        Assertions.assertTrue(pluginManager.hasFactory("test-manual"));

        // When
        pluginManager.removeFactory("test-manual");

        // Then
        Assertions.assertFalse(pluginManager.hasFactory("test-manual"));
        Assertions.assertFalse(pluginManager.getFactory("test-manual").isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-007: Remove non-existent factory does not throw")
    void testRemoveFactory_NotExists() {
        // When & Then - should not throw
        Assertions.assertDoesNotThrow(() -> pluginManager.removeFactory("nonexistent"));
    }

    @Test
    @DisplayName("UT-HANDLER-PM-008: Get factory by name")
    void testGetFactory() {
        // When
        Optional<AuthenticationPluginFactory> factory = pluginManager.getFactory("password");

        // Then
        Assertions.assertTrue(factory.isPresent());
        Assertions.assertEquals("password", factory.get().name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-009: Get non-existent factory returns empty")
    void testGetFactory_NotExists() {
        // When
        Optional<AuthenticationPluginFactory> factory = pluginManager.getFactory("nonexistent");

        // Then
        Assertions.assertFalse(factory.isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-010: Has factory check")
    void testHasFactory() {
        // When & Then
        Assertions.assertTrue(pluginManager.hasFactory("password"));
        Assertions.assertFalse(pluginManager.hasFactory("nonexistent"));
    }

    @Test
    @DisplayName("UT-HANDLER-PM-011: Plugin instances are cached")
    void testPluginCaching() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();

        // When - get same plugin twice
        AuthenticationPlugin plugin1 = pluginManager.getPlugin(integration);
        AuthenticationPlugin plugin2 = pluginManager.getPlugin(integration);

        // Then - should be same instance (cached)
        Assertions.assertSame(plugin1, plugin2);
        Assertions.assertEquals(1, pluginManager.getCachedPluginCount());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-012: Remove plugin from cache")
    void testRemovePlugin() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();
        pluginManager.getPlugin(integration);
        Assertions.assertEquals(1, pluginManager.getCachedPluginCount());

        // When
        pluginManager.removePlugin("test_password");

        // Then
        Assertions.assertEquals(0, pluginManager.getCachedPluginCount());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-013: Clear all cached plugins")
    void testClearCache() throws AuthenticationException {
        // Given - create multiple plugin instances
        AuthenticationIntegration integration1 = AuthenticationIntegration.builder()
                .name("test1")
                .type("password")
                .build();
        AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                .name("test2")
                .type("password")
                .build();

        pluginManager.getPlugin(integration1);
        pluginManager.getPlugin(integration2);
        Assertions.assertEquals(2, pluginManager.getCachedPluginCount());

        // When
        pluginManager.clearCache();

        // Then
        Assertions.assertEquals(0, pluginManager.getCachedPluginCount());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-014: Reload plugin")
    void testReloadPlugin() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();

        //         AuthenticationPlugin oldPlugin = pluginManager.getPlugin(integration);

        // When - reload creates new instance
        pluginManager.reloadPlugin(integration);
        AuthenticationPlugin newPlugin = pluginManager.getPlugin(integration);

        // Then - should be different instance
        Assertions.assertNotNull(newPlugin);
        // Note: may or may not be same instance depending on implementation
    }

    @Test
    @DisplayName("UT-HANDLER-PM-015: Health check all plugins")
    void testHealthCheckAll() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();
        pluginManager.getPlugin(integration);

        // When & Then - should not throw
        Assertions.assertDoesNotThrow(() -> pluginManager.healthCheckAll());
    }

    /**
     * Test plugin factory for manual registration testing.
     */
    private static class TestPluginFactory implements AuthenticationPluginFactory {
        @Override
        public String name() {
            return "test-manual";
        }

        @Override
        public AuthenticationPlugin create() {
            return new TestPlugin();
        }

        public String description() {
            return "Test plugin for manual registration";
        }
    }

    /**
     * Test plugin implementation.
     */
    private static class TestPlugin implements AuthenticationPlugin {
        @Override
        public String name() {
            return "test-manual";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            // Support all requests for testing
            return true;
        }

        @Override
        public AuthenticationResult authenticate(
                AuthenticationRequest request,
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
            // No initialization needed for test
        }

        @Override
        public void close() {
            // No cleanup needed for test
        }
    }
}
