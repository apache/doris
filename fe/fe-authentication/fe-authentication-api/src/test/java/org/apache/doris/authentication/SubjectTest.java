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

import java.util.Set;

/**
 * Unit tests for {@link Subject}.
 */
@DisplayName("Subject Unit Tests")
class SubjectTest {

    @Test
    @DisplayName("UT-API-S-001: Create Subject with minimal fields")
    void testCreateSubject_MinimalFields() {
        // Given
        Principal principal = Identity.builder()
                .username("alice")
                .authenticatorName("password")
                .build();

        // When
        Subject subject = Subject.builder()
                .principal(principal)
                .build();

        // Then
        Assertions.assertNotNull(subject);
        Assertions.assertEquals("alice", subject.getUser());
        Assertions.assertEquals(principal, subject.getPrincipal());
        Assertions.assertTrue(subject.getActiveRoles().isEmpty());
        Assertions.assertTrue(subject.getAvailableRoles().isEmpty());
        Assertions.assertNull(subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-API-S-002: Create Subject with all fields")
    void testCreateSubject_AllFields() {
        // Given
        Principal principal = Identity.builder()
                .username("bob")
                .authenticatorName("ldap")
                .build();
        Set<String> activeRoles = Set.of("admin", "developer");
        Set<String> availableRoles = Set.of("admin", "developer", "analyst");
        String sourceIp = "192.168.1.100";

        // When
        Subject subject = Subject.builder()
                .principal(principal)
                .activeRoles(activeRoles)
                .availableRoles(availableRoles)
                .sourceIp(sourceIp)
                .build();

        // Then
        Assertions.assertEquals("bob", subject.getUser());
        Assertions.assertEquals(activeRoles, subject.getActiveRoles());
        Assertions.assertEquals(availableRoles, subject.getAvailableRoles());
        Assertions.assertEquals(sourceIp, subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-API-S-003: Create Subject with null principal should throw exception")
    void testCreateSubject_NullPrincipal() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                Subject.builder()
                        .principal(null)
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-S-004: Subject active roles should be immutable")
    void testSubject_ActiveRolesImmutability() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .activeRoles(Set.of("admin"))
                .availableRoles(Set.of("admin", "user"))
                .build();

        // When & Then
        Set<String> activeRoles = subject.getActiveRoles();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                activeRoles.add("newRole")
        );
    }

    @Test
    @DisplayName("UT-API-S-005: Subject available roles should be immutable")
    void testSubject_AvailableRolesImmutability() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .availableRoles(Set.of("admin", "user"))
                .build();

        // When & Then
        Set<String> availableRoles = subject.getAvailableRoles();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                availableRoles.add("newRole")
        );
    }

    @Test
    @DisplayName("UT-API-S-006: Set active roles - valid roles")
    void testSubject_SetActiveRoles_ValidRoles() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .availableRoles(Set.of("admin", "developer", "analyst"))
                .activeRoles(Set.of("admin"))
                .build();

        // When
        subject.setActiveRoles(Set.of("developer", "analyst"));

        // Then
        Assertions.assertEquals(Set.of("developer", "analyst"), subject.getActiveRoles());
    }

    @Test
    @DisplayName("UT-API-S-007: Set active roles - role not available should throw exception")
    void testSubject_SetActiveRoles_RoleNotAvailable() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .availableRoles(Set.of("admin", "developer"))
                .build();

        // When & Then
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                subject.setActiveRoles(Set.of("superadmin"))
        );
    }

    @Test
    @DisplayName("UT-API-S-008: Check if user has role - has role")
    void testSubject_HasRole_True() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .activeRoles(Set.of("admin", "developer"))
                .availableRoles(Set.of("admin", "developer"))
                .build();

        // When
        boolean hasAdmin = subject.hasRole("admin");
        boolean hasDeveloper = subject.hasRole("developer");

        // Then
        Assertions.assertTrue(hasAdmin);
        Assertions.assertTrue(hasDeveloper);
    }

    @Test
    @DisplayName("UT-API-S-009: Check if user has role - does not have role")
    void testSubject_HasRole_False() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .activeRoles(Set.of("developer"))
                .availableRoles(Set.of("admin", "developer"))
                .build();

        // When
        boolean hasAdmin = subject.hasRole("admin");

        // Then
        Assertions.assertFalse(hasAdmin);
    }

    @Test
    @DisplayName("UT-API-S-010: Check if user has any role - has at least one")
    void testSubject_HasAnyRole_True() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .activeRoles(Set.of("developer"))
                .availableRoles(Set.of("admin", "developer"))
                .build();

        // When
        boolean hasAny = subject.hasAnyRole(Set.of("admin", "developer", "analyst"));

        // Then
        Assertions.assertTrue(hasAny, "Should return true if user has at least one of the specified roles");
    }

    @Test
    @DisplayName("UT-API-S-011: Check if user has any role - has none")
    void testSubject_HasAnyRole_False() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .activeRoles(Set.of("developer"))
                .availableRoles(Set.of("admin", "developer"))
                .build();

        // When
        boolean hasAny = subject.hasAnyRole(Set.of("admin", "analyst"));

        // Then
        Assertions.assertFalse(hasAny, "Should return false if user has none of the specified roles");
    }

    @Test
    @DisplayName("UT-API-S-012: Get Identity from Subject")
    void testSubject_GetIdentity() {
        // Given
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .externalPrincipal("cn=alice,ou=users,dc=corp,dc=com")
                .build();

        Subject subject = Subject.builder()
                .principal(identity)
                .build();

        // When
        Identity retrievedIdentity = subject.getIdentity();

        // Then
        Assertions.assertNotNull(retrievedIdentity);
        Assertions.assertEquals(identity, retrievedIdentity);
        Assertions.assertEquals("cn=alice,ou=users,dc=corp,dc=com", retrievedIdentity.getExternalPrincipal().get());
    }

    @Test
    @DisplayName("UT-API-S-013: Subject equals and hashCode")
    void testSubject_EqualsAndHashCode() {
        // Given
        Principal principal = createTestPrincipal("alice");
        String sourceIp = "192.168.1.100";

        Subject subject1 = Subject.builder()
                .principal(principal)
                .sourceIp(sourceIp)
                .activeRoles(Set.of("admin"))
                .build();

        Subject subject2 = Subject.builder()
                .principal(principal)
                .sourceIp(sourceIp)
                .activeRoles(Set.of("developer")) // Different active roles
                .build();

        // When & Then
        Assertions.assertEquals(subject1, subject2, "Subjects with same principal and sourceIp should be equal");
        Assertions.assertEquals(subject1.hashCode(), subject2.hashCode());
    }

    @Test
    @DisplayName("UT-API-S-014: Subject toString contains user and roles")
    void testSubject_ToString() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .activeRoles(Set.of("admin", "developer"))
                .sourceIp("192.168.1.100")
                .build();

        // When
        String toString = subject.toString();

        // Then
        Assertions.assertTrue(toString.contains("alice"));
        Assertions.assertTrue(toString.contains("192.168.1.100"));
    }

    @Test
    @DisplayName("UT-API-S-015: Set active roles to null should clear roles")
    void testSubject_SetActiveRoles_Null() {
        // Given
        Subject subject = Subject.builder()
                .principal(createTestPrincipal("alice"))
                .availableRoles(Set.of("admin", "developer"))
                .activeRoles(Set.of("admin"))
                .build();

        // When
        subject.setActiveRoles(null);

        // Then
        Assertions.assertTrue(subject.getActiveRoles().isEmpty());
    }

    // Helper method
    private Principal createTestPrincipal(String username) {
        return Identity.builder()
                .username(username)
                .authenticatorName("test")
                .build();
    }
}
