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
import java.util.Set;

/**
 * Unit tests for {@link BasicPrincipal}.
 */
@DisplayName("BasicPrincipal Unit Tests")
class BasicPrincipalTest {

    @Test
    @DisplayName("UT-API-BP-001: Create BasicPrincipal with required fields")
    void testCreateBasicPrincipal_RequiredFields() {
        // Given
        String name = "alice";
        String authenticator = "password";

        // When
        BasicPrincipal principal = BasicPrincipal.builder()
                .name(name)
                .authenticator(authenticator)
                .build();

        // Then
        Assertions.assertNotNull(principal);
        Assertions.assertEquals(name, principal.getName());
        Assertions.assertEquals(authenticator, principal.getAuthenticator());
        Assertions.assertTrue(principal.getExternalGroups().isEmpty());
        Assertions.assertTrue(principal.getAttributes().isEmpty());
        Assertions.assertFalse(principal.isServicePrincipal());
    }

    @Test
    @DisplayName("UT-API-BP-002: Create BasicPrincipal with all fields")
    void testCreateBasicPrincipal_AllFields() {
        // Given
        String name = "bob";
        String authenticator = "ldap";
        String externalPrincipal = "cn=bob,ou=users,dc=corp,dc=com";
        Set<String> externalGroups = Set.of("developers", "admins");
        Map<String, String> attributes = Map.of("email", "bob@corp.com", "department", "engineering");

        // When
        BasicPrincipal principal = BasicPrincipal.builder()
                .name(name)
                .authenticator(authenticator)
                .externalPrincipal(externalPrincipal)
                .externalGroups(externalGroups)
                .attributes(attributes)
                .servicePrincipal(true)
                .build();

        // Then
        Assertions.assertEquals(name, principal.getName());
        Assertions.assertEquals(authenticator, principal.getAuthenticator());
        Assertions.assertTrue(principal.getExternalPrincipal().isPresent());
        Assertions.assertEquals(externalPrincipal, principal.getExternalPrincipal().get());
        Assertions.assertEquals(externalGroups, principal.getExternalGroups());
        Assertions.assertEquals(attributes, principal.getAttributes());
        Assertions.assertTrue(principal.isServicePrincipal());
    }

    @Test
    @DisplayName("UT-API-BP-003: Create BasicPrincipal with null name should throw exception")
    void testCreateBasicPrincipal_NullName() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                BasicPrincipal.builder()
                        .name(null)
                        .authenticator("test")
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-BP-004: Create BasicPrincipal with null authenticator should throw exception")
    void testCreateBasicPrincipal_NullAuthenticator() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                BasicPrincipal.builder()
                        .name("test")
                        .authenticator(null)
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-BP-005: BasicPrincipal external groups should be immutable")
    void testBasicPrincipal_ExternalGroupsImmutability() {
        // Given
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .addExternalGroup("group1")
                .build();

        // When & Then
        Set<String> groups = principal.getExternalGroups();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                groups.add("group2")
        );
    }

    @Test
    @DisplayName("UT-API-BP-006: BasicPrincipal attributes should be immutable")
    void testBasicPrincipal_AttributesImmutability() {
        // Given
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .attribute("email", "alice@example.com")
                .build();

        // When & Then
        Map<String, String> attributes = principal.getAttributes();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                attributes.put("newKey", "newValue")
        );
    }

    @Test
    @DisplayName("UT-API-BP-007: BasicPrincipal equals and hashCode - same name and authenticator")
    void testBasicPrincipal_EqualsAndHashCode_Same() {
        // Given
        BasicPrincipal principal1 = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .attribute("email", "alice@example.com")
                .build();

        BasicPrincipal principal2 = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .attribute("department", "engineering") // Different attributes
                .build();

        // When & Then
        Assertions.assertEquals(principal1, principal2, "Principals with same name and authenticator should be equal");
        Assertions.assertEquals(principal1.hashCode(), principal2.hashCode());
    }

    @Test
    @DisplayName("UT-API-BP-008: BasicPrincipal equals - different name")
    void testBasicPrincipal_NotEquals_DifferentName() {
        // Given
        BasicPrincipal principal1 = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .build();

        BasicPrincipal principal2 = BasicPrincipal.builder()
                .name("bob")
                .authenticator("ldap")
                .build();

        // When & Then
        Assertions.assertNotEquals(principal1, principal2);
    }

    @Test
    @DisplayName("UT-API-BP-009: BasicPrincipal equals - different authenticator")
    void testBasicPrincipal_NotEquals_DifferentAuthenticator() {
        // Given
        BasicPrincipal principal1 = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap1")
                .build();

        BasicPrincipal principal2 = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap2")
                .build();

        // When & Then
        Assertions.assertNotEquals(principal1, principal2);
    }

    @Test
    @DisplayName("UT-API-BP-010: BasicPrincipal toString contains key information")
    void testBasicPrincipal_ToString() {
        // Given
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .externalPrincipal("cn=alice,ou=users,dc=corp,dc=com")
                .build();

        // When
        String toString = principal.toString();

        // Then
        Assertions.assertTrue(toString.contains("alice"));
        Assertions.assertTrue(toString.contains("ldap"));
        Assertions.assertTrue(toString.contains("cn=alice"));
    }

    @Test
    @DisplayName("UT-API-BP-011: BasicPrincipal builder from existing principal")
    void testBasicPrincipal_BuilderFromExisting() {
        // Given
        Principal existingPrincipal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .externalPrincipal("cn=alice")
                .addExternalGroup("group1")
                .attribute("email", "alice@example.com")
                .servicePrincipal(true)
                .build();

        // When
        BasicPrincipal newPrincipal = BasicPrincipal.builder(existingPrincipal).build();

        // Then
        Assertions.assertEquals(existingPrincipal.getName(), newPrincipal.getName());
        Assertions.assertEquals(existingPrincipal.getAuthenticator(), newPrincipal.getAuthenticator());
        Assertions.assertEquals(existingPrincipal.getExternalPrincipal(), newPrincipal.getExternalPrincipal());
        Assertions.assertEquals(existingPrincipal.getExternalGroups(), newPrincipal.getExternalGroups());
        Assertions.assertEquals(existingPrincipal.getAttributes(), newPrincipal.getAttributes());
        Assertions.assertEquals(existingPrincipal.isServicePrincipal(), newPrincipal.isServicePrincipal());
    }

    @Test
    @DisplayName("UT-API-BP-012: BasicPrincipal builder chained calls")
    void testBasicPrincipal_BuilderChainedCalls() {
        // When
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("ldap")
                .externalPrincipal("cn=alice")
                .addExternalGroup("group1")
                .addExternalGroup("group2")
                .attribute("email", "alice@example.com")
                .attribute("department", "engineering")
                .servicePrincipal(false)
                .build();

        // Then
        Assertions.assertEquals("alice", principal.getName());
        Assertions.assertEquals("ldap", principal.getAuthenticator());
        Assertions.assertEquals(2, principal.getExternalGroups().size());
        Assertions.assertEquals(2, principal.getAttributes().size());
        Assertions.assertFalse(principal.isServicePrincipal());
    }
}
