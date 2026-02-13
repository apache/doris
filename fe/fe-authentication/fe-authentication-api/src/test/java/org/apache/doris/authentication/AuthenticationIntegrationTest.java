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

package org.apache.doris.authentication;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Unit tests for {@link AuthenticationIntegration}.
 */
@DisplayName("AuthenticationIntegration Unit Tests")
class AuthenticationIntegrationTest {

    @Test
    @DisplayName("UT-API-AI-001: Create AuthenticationIntegration with required fields")
    void testCreateIntegration_RequiredFields() {
        // Given
        String name = "corp_ldap";
        String type = "ldap";

        // When
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name(name)
                .type(type)
                .build();

        // Then
        Assertions.assertNotNull(integration);
        Assertions.assertEquals(name, integration.getName());
        Assertions.assertEquals(type, integration.getType());
        Assertions.assertTrue(integration.getProperties().isEmpty());
        Assertions.assertFalse(integration.getComment().isPresent());
    }

    @Test
    @DisplayName("UT-API-AI-002: Create AuthenticationIntegration with all fields")
    void testCreateIntegration_AllFields() {
        // Given
        String name = "partner_ldap";
        String type = "ldap";
        Map<String, String> properties = Map.of(
                "server", "ldap://partner.example.com:389",
                "base_dn", "dc=partner,dc=com"
        );
        String comment = "Partner LDAP authentication";

        // When
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name(name)
                .type(type)
                .properties(properties)
                .comment(comment)
                .build();

        // Then
        Assertions.assertEquals(name, integration.getName());
        Assertions.assertEquals(type, integration.getType());
        Assertions.assertEquals(properties, integration.getProperties());
        Assertions.assertTrue(integration.getComment().isPresent());
        Assertions.assertEquals(comment, integration.getComment().get());
    }

    @Test
    @DisplayName("UT-API-AI-003: Create AuthenticationIntegration with null name should throw exception")
    void testCreateIntegration_NullName() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationIntegration.builder()
                        .name(null)
                        .type("ldap")
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-AI-004: Create AuthenticationIntegration with null type should throw exception")
    void testCreateIntegration_NullType() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationIntegration.builder()
                        .name("test")
                        .type(null)
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-AI-005: AuthenticationIntegration properties should be immutable")
    void testIntegration_PropertiesImmutability() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .property("key1", "value1")
                .build();

        // When & Then
        Map<String, String> properties = integration.getProperties();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                properties.put("key2", "value2")
        );
    }

    @Test
    @DisplayName("UT-API-AI-006: Get property value - property exists")
    void testIntegration_GetProperty_Exists() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .property("server", "ldap://example.com")
                .build();

        // When
        String serverValue = integration.getProperty("server").get();

        // Then
        Assertions.assertEquals("ldap://example.com", serverValue);
    }

    @Test
    @DisplayName("UT-API-AI-007: Get property value - property does not exist")
    void testIntegration_GetProperty_NotExists() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .build();

        // When
        boolean present = integration.getProperty("nonexistent").isPresent();

        // Then
        Assertions.assertFalse(present);
    }

    @Test
    @DisplayName("UT-API-AI-008: Get property with default value")
    void testIntegration_GetPropertyWithDefault() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .property("timeout", "30")
                .build();

        // When
        String timeout = integration.getProperty("timeout", "60");
        String port = integration.getProperty("port", "389");

        // Then
        Assertions.assertEquals("30", timeout);
        Assertions.assertEquals("389", port);
    }

    @Test
    @DisplayName("UT-API-AI-009: AuthenticationIntegration equals and hashCode - same name")
    void testIntegration_EqualsAndHashCode() {
        // Given
        AuthenticationIntegration integration1 = AuthenticationIntegration.builder()
                .name("test_ldap")
                .type("ldap")
                .property("server", "ldap1.example.com")
                .build();

        AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                .name("test_ldap")
                .type("oidc") // Different type
                .property("server", "ldap2.example.com") // Different properties
                .build();

        // When & Then
        Assertions.assertEquals(integration1, integration2, "Integrations with same name should be equal");
        Assertions.assertEquals(integration1.hashCode(), integration2.hashCode());
    }

    @Test
    @DisplayName("UT-API-AI-010: AuthenticationIntegration equals - different name")
    void testIntegration_NotEquals_DifferentName() {
        // Given
        AuthenticationIntegration integration1 = AuthenticationIntegration.builder()
                .name("ldap1")
                .type("ldap")
                .build();

        AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                .name("ldap2")
                .type("ldap")
                .build();

        // When & Then
        Assertions.assertNotEquals(integration1, integration2);
    }

    @Test
    @DisplayName("UT-API-AI-011: AuthenticationIntegration toString contains key information")
    void testIntegration_ToString() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_ldap")
                .type("ldap")
                .property("key1", "value1")
                .property("key2", "value2")
                .build();

        // When
        String toString = integration.toString();

        // Then
        Assertions.assertTrue(toString.contains("test_ldap"));
        Assertions.assertTrue(toString.contains("ldap"));
        Assertions.assertTrue(toString.contains("2 entries"));
    }

    @Test
    @DisplayName("UT-API-AI-012: AuthenticationIntegration toBuilder creates copy")
    void testIntegration_ToBuilder() {
        // Given
        AuthenticationIntegration original = AuthenticationIntegration.builder()
                .name("original")
                .type("ldap")
                .property("server", "ldap.example.com")
                .comment("Original comment")
                .build();

        // When
        AuthenticationIntegration modified = original.toBuilder()
                .name("modified")
                .build();

        // Then
        Assertions.assertEquals("modified", modified.getName());
        Assertions.assertEquals("ldap", modified.getType());
        Assertions.assertEquals("ldap.example.com", modified.getProperty("server").get());
        Assertions.assertEquals("Original comment", modified.getComment().get());
    }
}
