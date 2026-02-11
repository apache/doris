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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link Identity}.
 */
@DisplayName("Identity Unit Tests")
class IdentityTest {

    @Test
    @DisplayName("UT-API-I-001: Create valid Identity with required fields")
    void testCreateIdentity_ValidRequiredFields() {
        // Given
        String username = "alice";
        String authenticatorName = "local_password";

        // When
        Identity identity = Identity.builder()
                .username(username)
                .authenticatorName(authenticatorName)
                .build();

        // Then
        Assertions.assertNotNull(identity);
        Assertions.assertEquals(username, identity.getUsername());
        Assertions.assertEquals(username, identity.getName());
        Assertions.assertEquals(authenticatorName, identity.getAuthenticatorName());
        Assertions.assertEquals(authenticatorName, identity.getAuthenticator());
    }

    @Test
    @DisplayName("UT-API-I-002: Create Identity with all fields")
    void testCreateIdentity_AllFields() {
        // Given
        String username = "bob";
        String authenticatorName = "corp_ldap";
        String pluginName = "ldap";
        String externalPrincipal = "cn=bob,ou=users,dc=corp,dc=com";
        Set<String> externalGroups = Set.of("developers", "engineers");
        Instant expiresAt = Instant.now().plus(1, ChronoUnit.HOURS);
        Map<String, String> attributes = Map.of("email", "bob@corp.com", "department", "engineering");

        // When
        Identity identity = Identity.builder()
                .username(username)
                .authenticatorName(authenticatorName)
                .authenticatorPluginName(pluginName)
                .externalPrincipal(externalPrincipal)
                .externalGroups(externalGroups)
                .expiresAt(expiresAt)
                .attributes(attributes)
                .build();

        // Then
        Assertions.assertEquals(username, identity.getUsername());
        Assertions.assertEquals(authenticatorName, identity.getAuthenticatorName());
        Assertions.assertEquals(pluginName, identity.getAuthenticatorPluginName());
        Assertions.assertTrue(identity.getExternalPrincipal().isPresent());
        Assertions.assertEquals(externalPrincipal, identity.getExternalPrincipal().get());
        Assertions.assertEquals(externalGroups, identity.getExternalGroups());
        Assertions.assertTrue(identity.getExpiresAt().isPresent());
        Assertions.assertEquals(expiresAt, identity.getExpiresAt().get());
        Assertions.assertEquals(attributes, identity.getAttributes());
    }

    @Test
    @DisplayName("UT-API-I-003: Create Identity with null username should throw exception")
    void testCreateIdentity_NullUsername() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                Identity.builder()
                        .username(null)
                        .authenticatorName("test")
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-I-004: Create Identity with null authenticatorName should throw exception")
    void testCreateIdentity_NullAuthenticatorName() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                Identity.builder()
                        .username("test")
                        .authenticatorName(null)
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-I-005: Identity external groups should be immutable")
    void testIdentity_ExternalGroupsImmutability() {
        // Given
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .addExternalGroup("group1")
                .build();

        // When & Then
        Set<String> groups = identity.getExternalGroups();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                groups.add("group2")
        );
    }

    @Test
    @DisplayName("UT-API-I-006: Identity attributes should be immutable")
    void testIdentity_AttributesImmutability() {
        // Given
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .attribute("email", "alice@example.com")
                .build();

        // When & Then
        Map<String, String> attributes = identity.getAttributes();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                attributes.put("newKey", "newValue")
        );
    }

    @Test
    @DisplayName("UT-API-I-007: Check if Identity is expired - not expired")
    void testIdentity_IsNotExpired() {
        // Given
        Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("oidc")
                .expiresAt(futureTime)
                .build();

        // When
        boolean expired = identity.isExpired();

        // Then
        Assertions.assertFalse(expired, "Identity with future expiration should not be expired");
    }

    @Test
    @DisplayName("UT-API-I-008: Check if Identity is expired - expired")
    void testIdentity_IsExpired() {
        // Given
        Instant pastTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("oidc")
                .expiresAt(pastTime)
                .build();

        // When
        boolean expired = identity.isExpired();

        // Then
        Assertions.assertTrue(expired, "Identity with past expiration should be expired");
    }

    @Test
    @DisplayName("UT-API-I-009: Identity without expiration should not be expired")
    void testIdentity_NoExpiration() {
        // Given
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("password")
                .build();

        // When
        boolean expired = identity.isExpired();

        // Then
        Assertions.assertFalse(expired, "Identity without expiration should never be expired");
        Assertions.assertFalse(identity.getExpiresAt().isPresent());
    }

    @Test
    @DisplayName("UT-API-I-010: Identity equals and hashCode - same username and authenticator")
    void testIdentity_EqualsAndHashCode_SameIdentity() {
        // Given
        Identity identity1 = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .attribute("email", "alice@example.com")
                .build();

        Identity identity2 = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .attribute("department", "engineering") // Different attributes
                .build();

        // When & Then
        Assertions.assertEquals(identity1, identity2, "Identities with same username and authenticator should be equal");
        Assertions.assertEquals(identity1.hashCode(), identity2.hashCode());
    }

    @Test
    @DisplayName("UT-API-I-011: Identity equals - different username")
    void testIdentity_NotEquals_DifferentUsername() {
        // Given
        Identity identity1 = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .build();

        Identity identity2 = Identity.builder()
                .username("bob")
                .authenticatorName("ldap")
                .build();

        // When & Then
        Assertions.assertNotEquals(identity1, identity2);
    }

    @Test
    @DisplayName("UT-API-I-012: Identity equals - different authenticator")
    void testIdentity_NotEquals_DifferentAuthenticator() {
        // Given
        Identity identity1 = Identity.builder()
                .username("alice")
                .authenticatorName("ldap1")
                .build();

        Identity identity2 = Identity.builder()
                .username("alice")
                .authenticatorName("ldap2")
                .build();

        // When & Then
        Assertions.assertNotEquals(identity1, identity2);
    }

    @Test
    @DisplayName("UT-API-I-013: Identity toString should not expose sensitive data")
    void testIdentity_ToString_NoSensitiveData() {
        // Given
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .externalPrincipal("cn=alice,ou=users,dc=corp,dc=com")
                .attribute("password", "should-not-appear")
                .build();

        // When
        String toString = identity.toString();

        // Then
        Assertions.assertTrue(toString.contains("alice"));
        Assertions.assertTrue(toString.contains("ldap"));
        Assertions.assertFalse(toString.contains("should-not-appear"), "toString should not expose sensitive attributes");
    }

    @Test
    @DisplayName("UT-API-I-014: Identity builder - chained calls")
    void testIdentity_BuilderChainedCalls() {
        // When
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .authenticatorPluginName("ldap-plugin")
                .externalPrincipal("cn=alice")
                .addExternalGroup("group1")
                .addExternalGroup("group2")
                .attribute("email", "alice@example.com")
                .attribute("department", "engineering")
                .build();

        // Then
        Assertions.assertEquals("alice", identity.getUsername());
        Assertions.assertEquals("ldap", identity.getAuthenticatorName());
        Assertions.assertEquals("ldap-plugin", identity.getAuthenticatorPluginName());
        Assertions.assertEquals(2, identity.getExternalGroups().size());
        Assertions.assertEquals(2, identity.getAttributes().size());
    }

    @Test
    @DisplayName("UT-API-I-015: Identity implements Principal interface")
    void testIdentity_ImplementsPrincipal() {
        // Given
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .build();

        // When & Then
        Assertions.assertInstanceOf(Principal.class, identity);
        Assertions.assertEquals(identity.getUsername(), identity.getName());
        Assertions.assertEquals(identity.getAuthenticatorName(), identity.getAuthenticator());
    }
}
