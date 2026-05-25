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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link IntegrationRegistry}.
 */
@DisplayName("IntegrationRegistry Unit Tests")
class IntegrationRegistryTest {

    private IntegrationRegistry integrationRegistry;

    @BeforeEach
    void setUp() {
        integrationRegistry = new IntegrationRegistry();
    }

    @Test
    @DisplayName("UT-HANDLER-IR-001: Register integration")
    void testRegisterIntegration() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("corp_ldap")
                .type("ldap")
                .property("server", "ldap://corp.example.com")
                .build();

        // When
        integrationRegistry.register(integration);

        // Then
        Optional<AuthenticationIntegration> retrieved = integrationRegistry.get("corp_ldap");
        Assertions.assertTrue(retrieved.isPresent());
        Assertions.assertEquals("corp_ldap", retrieved.get().getName());
        Assertions.assertEquals("ldap", retrieved.get().getType());
    }

    @Test
    @DisplayName("UT-HANDLER-IR-003: Get integration by name")
    void testGetIntegration() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_ldap")
                .type("ldap")
                .build();
        integrationRegistry.register(integration);

        // When
        Optional<AuthenticationIntegration> retrieved = integrationRegistry.get("test_ldap");

        // Then
        Assertions.assertTrue(retrieved.isPresent());
        Assertions.assertEquals("test_ldap", retrieved.get().getName());
    }

    @Test
    @DisplayName("UT-HANDLER-IR-004: Get non-existent integration returns empty")
    void testGetIntegration_NotExists() {
        // When
        Optional<AuthenticationIntegration> retrieved = integrationRegistry.get("nonexistent");

        // Then
        Assertions.assertFalse(retrieved.isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-IR-007: Unregister integration")
    void testUnregisterIntegration() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("temp_ldap")
                .type("ldap")
                .build();
        integrationRegistry.register(integration);

        // When
        integrationRegistry.unregister("temp_ldap");

        // Then
        Assertions.assertFalse(integrationRegistry.get("temp_ldap").isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-IR-009: Get all integrations")
    void testGetAllIntegrations() {
        // Given
        integrationRegistry.register(AuthenticationIntegration.builder()
                .name("ldap1")
                .type("ldap")
                .build());
        integrationRegistry.register(AuthenticationIntegration.builder()
                .name("ldap2")
                .type("ldap")
                .build());

        // When
        Map<String, AuthenticationIntegration> integrations = integrationRegistry.getAll();

        // Then
        Assertions.assertNotNull(integrations);
        Assertions.assertEquals(2, integrations.size());
    }

    @Test
    @DisplayName("UT-HANDLER-IR-010: Manage authentication chain")
    void testAuthenticationChain() {
        // Given
        AuthenticationIntegration i1 = AuthenticationIntegration.builder().name("i1").type("t").build();
        AuthenticationIntegration i2 = AuthenticationIntegration.builder().name("i2").type("t").build();
        List<AuthenticationIntegration> chain = List.of(i1, i2);

        // When
        integrationRegistry.setAuthenticationChain(chain);

        // Then
        Assertions.assertEquals(chain, integrationRegistry.getAuthenticationChain());

        // Unregistering an item should remove it from chain
        integrationRegistry.unregister("i1");
        Assertions.assertEquals(1, integrationRegistry.getAuthenticationChain().size());
        Assertions.assertEquals("i2", integrationRegistry.getAuthenticationChain().get(0).getName());
    }

    @Test
    @DisplayName("UT-HANDLER-IR-011: Manage default integration")
    void testDefaultIntegration() {
        // Given
        AuthenticationIntegration def = AuthenticationIntegration.builder().name("def").type("pwd").build();

        // When
        integrationRegistry.setDefaultPasswordIntegration(def);

        // Then
        Assertions.assertEquals(def, integrationRegistry.getDefaultPasswordIntegration());

        // Unregistering should clear default
        integrationRegistry.unregister("def");
        Assertions.assertNull(integrationRegistry.getDefaultPasswordIntegration());
    }
}
