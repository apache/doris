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

import org.apache.doris.authentication.AuthenticationBinding;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for {@link BindingRegistry}.
 */
@DisplayName("BindingRegistry Unit Tests")
class BindingRegistryTest {

    private BindingRegistry bindingRegistry;

    @BeforeEach
    void setUp() {
        bindingRegistry = new BindingRegistry();
    }

    @Test
    @DisplayName("UT-HANDLER-BR-001: Register user binding to integration")
    void testRegisterBinding() {
        // Given
        String username = "alice";
        String integrationName = "corp_ldap";

        // When
        bindingRegistry.register(username, integrationName);

        // Then
        Optional<String> bound = bindingRegistry.getIntegrationName(username);
        Assertions.assertTrue(bound.isPresent());
        Assertions.assertEquals(integrationName, bound.get());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-002: Re-register user to different integration")
    void testReRegisterBinding() {
        // Given
        String username = "alice";
        bindingRegistry.register(username, "ldap1");

        // When - rebind to different integration
        bindingRegistry.register(username, "ldap2");

        // Then
        Optional<String> bound = bindingRegistry.getIntegrationName(username);
        Assertions.assertTrue(bound.isPresent());
        Assertions.assertEquals("ldap2", bound.get());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-003: Get integration for unbound user")
    void testGetIntegration_NotBound() {
        // Given
        String username = "bob";

        // When
        Optional<String> bound = bindingRegistry.getIntegrationName(username);

        // Then
        Assertions.assertFalse(bound.isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-004: Unregister user binding")
    void testUnregisterBinding() {
        // Given
        String username = "alice";
        String integrationName = "corp_ldap";
        bindingRegistry.register(username, integrationName);

        // When
        Optional<AuthenticationBinding> removed = bindingRegistry.unregister(username);

        // Then
        Assertions.assertTrue(removed.isPresent());
        Assertions.assertEquals(username, removed.get().getUsername());
        Assertions.assertEquals(integrationName, removed.get().getIntegrationName());
        Assertions.assertFalse(bindingRegistry.getIntegrationName(username).isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-005: Unregister non-existent user returns empty")
    void testUnregisterBinding_NotBound() {
        // Given
        String username = "nonexistent";

        // When
        Optional<AuthenticationBinding> removed = bindingRegistry.unregister(username);

        // Then
        Assertions.assertFalse(removed.isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-006: Check if user has binding")
    void testHasBinding() {
        // Given
        String boundUser = "alice";
        String unboundUser = "bob";
        bindingRegistry.register(boundUser, "corp_ldap");

        // When & Then
        Assertions.assertTrue(bindingRegistry.hasBinding(boundUser));
        Assertions.assertFalse(bindingRegistry.hasBinding(unboundUser));
    }

    @Test
    @DisplayName("UT-HANDLER-BR-007: Get all bindings")
    void testGetAllBindings() {
        // Given
        bindingRegistry.register("alice", "corp_ldap");
        bindingRegistry.register("bob", "local_password");

        // When
        Collection<AuthenticationBinding> bindings = bindingRegistry.getAllBindings();

        // Then
        Assertions.assertNotNull(bindings);
        Assertions.assertEquals(2, bindings.size());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-008: Get bound usernames")
    void testGetBoundUsernames() {
        // Given
        bindingRegistry.register("alice", "corp_ldap");
        bindingRegistry.register("bob", "local_password");
        bindingRegistry.register("charlie", "corp_ldap");

        // When
        List<String> usernames = bindingRegistry.getBoundUsernames();

        // Then
        Assertions.assertNotNull(usernames);
        Assertions.assertEquals(3, usernames.size());
        Assertions.assertTrue(usernames.contains("alice"));
        Assertions.assertTrue(usernames.contains("bob"));
        Assertions.assertTrue(usernames.contains("charlie"));
    }

    @Test
    @DisplayName("UT-HANDLER-BR-009: Registry size tracking")
    void testRegistrySize() {
        // Given
        Assertions.assertEquals(0, bindingRegistry.size());

        // When
        bindingRegistry.register("alice", "corp_ldap");
        bindingRegistry.register("bob", "local_password");

        // Then
        Assertions.assertEquals(2, bindingRegistry.size());

        // When - unregister one
        bindingRegistry.unregister("alice");

        // Then
        Assertions.assertEquals(1, bindingRegistry.size());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-010: Clear all bindings")
    void testClearBindings() {
        // Given
        bindingRegistry.register("alice", "corp_ldap");
        bindingRegistry.register("bob", "local_password");
        Assertions.assertEquals(2, bindingRegistry.size());

        // When
        bindingRegistry.clear();

        // Then
        Assertions.assertEquals(0, bindingRegistry.size());
        Assertions.assertFalse(bindingRegistry.hasBinding("alice"));
        Assertions.assertFalse(bindingRegistry.hasBinding("bob"));
    }

    @Test
    @DisplayName("UT-HANDLER-BR-011: Register with AuthenticationBinding object")
    void testRegisterWithBindingObject() {
        // Given
        AuthenticationBinding binding = AuthenticationBinding.forUser("alice", "corp_ldap");

        // When
        bindingRegistry.register(binding);

        // Then
        Assertions.assertTrue(bindingRegistry.hasBinding("alice"));
        Assertions.assertEquals("corp_ldap", bindingRegistry.getIntegrationName("alice").get());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-012: Get binding object")
    void testGetBindingObject() {
        // Given
        String username = "alice";
        String integrationName = "corp_ldap";
        bindingRegistry.register(username, integrationName);

        // When
        Optional<AuthenticationBinding> binding = bindingRegistry.getBinding(username);

        // Then
        Assertions.assertTrue(binding.isPresent());
        Assertions.assertEquals(username, binding.get().getUsername());
        Assertions.assertEquals(integrationName, binding.get().getIntegrationName());
        Assertions.assertTrue(binding.get().isUserBinding());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-013: Null username returns empty")
    void testNullUsername() {
        // When & Then
        Assertions.assertFalse(bindingRegistry.getIntegrationName(null).isPresent());
        Assertions.assertFalse(bindingRegistry.getBinding(null).isPresent());
        Assertions.assertFalse(bindingRegistry.hasBinding(null));
        Assertions.assertFalse(bindingRegistry.unregister(null).isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-BR-014: Register null binding throws exception")
    void testRegisterNullBinding() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () -> {
            bindingRegistry.register((AuthenticationBinding) null);
        });
    }

    @Test
    @DisplayName("UT-HANDLER-BR-015: Register non-user binding throws exception")
    void testRegisterNonUserBinding() {
        // Given - create a binding with null username (not a user binding)
        AuthenticationBinding nonUserBinding = AuthenticationBinding.of(null, "some_integration");

        // When & Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            bindingRegistry.register(nonUserBinding);
        });
    }
}
