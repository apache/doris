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

package org.apache.doris.authentication.plugin.ldap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link LdapClient}.
 *
 * Note: These tests validate configuration and API contracts.
 * Integration tests with real LDAP server are in LdapAuthenticationPluginIntegrationTest.
 */
@DisplayName("LdapClient Unit Tests")
class LdapClientTest {

    private LdapClient ldapClient;
    private Map<String, String> baseConfig;

    @BeforeEach
    void setUp() {
        baseConfig = new HashMap<>();
        baseConfig.put("server", "ldap://localhost:389");
        baseConfig.put("base_dn", "dc=example,dc=com");
        baseConfig.put("user_base_dn", "ou=users");
        baseConfig.put("user_filter", "(uid={login})");
        baseConfig.put("group_base_dn", "ou=groups");
    }

    @AfterEach
    void tearDown() {
        if (ldapClient != null) {
            ldapClient.close();
        }
    }

    @Test
    @DisplayName("UT-LDAP-C-001: Create LdapClient with valid configuration")
    void testCreateClient() {
        // When
        ldapClient = new LdapClient(baseConfig);

        // Then
        Assertions.assertNotNull(ldapClient);
    }

    @Test
    @DisplayName("UT-LDAP-C-002: Create LdapClient with bind credentials")
    void testCreateClientWithBindCredentials() {
        // Given
        baseConfig.put("bind_dn", "cn=admin,dc=example,dc=com");
        baseConfig.put("bind_password", "admin123");

        // When
        ldapClient = new LdapClient(baseConfig);

        // Then
        Assertions.assertNotNull(ldapClient);
    }

    @Test
    @DisplayName("UT-LDAP-C-003: Create LdapClient should validate server URL")
    void testCreateClientValidatesServerUrl() {
        // Given
        baseConfig.put("server", "");

        // When & Then
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                new LdapClient(baseConfig)
        );
    }

    @Test
    @DisplayName("UT-LDAP-C-004: Create LdapClient should validate base DN")
    void testCreateClientValidatesBaseDn() {
        // Given
        baseConfig.put("base_dn", "");

        // When & Then
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                new LdapClient(baseConfig)
        );
    }

    @Test
    @DisplayName("UT-LDAP-C-005: Create LdapClient with LDAPS URL")
    void testSupportLdapsConnection() {
        // Given
        baseConfig.put("server", "ldaps://secure.example.com:636");

        // When
        ldapClient = new LdapClient(baseConfig);

        // Then
        Assertions.assertNotNull(ldapClient);
    }

    @Test
    @DisplayName("UT-LDAP-C-006: Create LdapClient with custom user filter")
    void testCreateClientWithCustomUserFilter() {
        // Given
        baseConfig.put("user_filter", "(cn={login})");

        // When
        ldapClient = new LdapClient(baseConfig);

        // Then
        Assertions.assertNotNull(ldapClient);
    }

    @Test
    @DisplayName("UT-LDAP-C-007: Create LdapClient with custom group filter")
    void testCreateClientWithCustomGroupFilter() {
        // Given
        baseConfig.put("group_filter", "(memberUid={login})");

        // When
        ldapClient = new LdapClient(baseConfig);

        // Then
        Assertions.assertNotNull(ldapClient);
    }

    @Test
    @DisplayName("UT-LDAP-C-008: Close client should be idempotent")
    void testCloseClient() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When & Then
        Assertions.assertDoesNotThrow(() -> ldapClient.close());

        // Multiple close calls should be safe (idempotent)
        Assertions.assertDoesNotThrow(() -> ldapClient.close());
    }

    @Test
    @DisplayName("UT-LDAP-C-010: GetUserDn with null username returns null")
    void testGetUserDnWithNullUsername() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        String userDn = ldapClient.getUserDn(null);

        // Then
        Assertions.assertNull(userDn);
    }

    @Test
    @DisplayName("UT-LDAP-C-011: GetUserDn with empty username returns null")
    void testGetUserDnWithEmptyUsername() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        String userDn = ldapClient.getUserDn("");

        // Then
        Assertions.assertNull(userDn);
    }

    @Test
    @DisplayName("UT-LDAP-C-012: CheckPassword with null username returns false")
    void testCheckPasswordWithNullUsername() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        boolean valid = ldapClient.checkPassword(null, "password");

        // Then
        Assertions.assertFalse(valid);
    }

    @Test
    @DisplayName("UT-LDAP-C-013: CheckPassword with empty username returns false")
    void testCheckPasswordWithEmptyUsername() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        boolean valid = ldapClient.checkPassword("", "password");

        // Then
        Assertions.assertFalse(valid);
    }

    @Test
    @DisplayName("UT-LDAP-C-014: CheckPassword with null password returns false")
    void testCheckPasswordWithNullPassword() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        boolean valid = ldapClient.checkPassword("alice", null);

        // Then
        Assertions.assertFalse(valid);
    }

    @Test
    @DisplayName("UT-LDAP-C-015: CheckPassword with empty password returns false")
    void testCheckPasswordWithEmptyPassword() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        boolean valid = ldapClient.checkPassword("alice", "");

        // Then
        Assertions.assertFalse(valid);
    }

    @Test
    @DisplayName("UT-LDAP-C-016: GetGroups with null username returns empty list")
    void testGetGroupsWithNullUsername() {
        // Given
        ldapClient = new LdapClient(baseConfig);

        // When
        List<String> groups = ldapClient.getGroups(null);

        // Then
        Assertions.assertNotNull(groups);
        Assertions.assertTrue(groups.isEmpty());
    }

    @Test
    @DisplayName("UT-LDAP-C-017: GetGroups with no group base DN returns empty list")
    void testGetGroupsWithNoGroupBaseDn() {
        // Given
        baseConfig.put("group_base_dn", "");
        ldapClient = new LdapClient(baseConfig);

        // When
        List<String> groups = ldapClient.getGroups("alice");

        // Then
        Assertions.assertNotNull(groups);
        Assertions.assertTrue(groups.isEmpty());
    }
}
