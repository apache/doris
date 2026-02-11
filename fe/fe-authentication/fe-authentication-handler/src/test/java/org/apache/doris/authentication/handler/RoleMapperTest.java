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
import org.apache.doris.authentication.Identity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link RoleMapper}.
 */
@DisplayName("RoleMapper Unit Tests")
class RoleMapperTest {

    private RoleMapper roleMapper;
    private AuthenticationIntegration integration;

    @BeforeEach
    void setUp() {
        roleMapper = new RoleMapper();

        Map<String, String> properties = new HashMap<>();
        properties.put("role.mapping.admins", "admin");
        properties.put("role.mapping.developers", "developer");
        properties.put("role.mapping.users", "user");
        properties.put("role.mapping.cn=admins,ou=groups,dc=example,dc=com", "admin");

        integration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("ldap")
                .properties(properties)
                .build();
    }

    @Test
    @DisplayName("UT-RM-001: Map single external group to role")
    void testMapRoles_SingleGroup() {
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.singleton("admins"))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(1, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
    }

    @Test
    @DisplayName("UT-RM-002: Map multiple external groups to roles")
    void testMapRoles_MultipleGroups() {
        Identity identity = Identity.builder()
                .username("bob")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(new HashSet<>(Arrays.asList("admins", "developers", "users")))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(3, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
        Assertions.assertTrue(roles.contains("developer"));
        Assertions.assertTrue(roles.contains("user"));
    }

    @Test
    @DisplayName("UT-RM-003: Unmapped groups return empty set")
    void testMapRoles_UnmappedGroups() {
        Identity identity = Identity.builder()
                .username("charlie")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.singleton("nonexistent"))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertTrue(roles.isEmpty());
    }

    @Test
    @DisplayName("UT-RM-004: Null identity returns empty set")
    void testMapRoles_NullIdentity() {
        Set<String> roles = roleMapper.mapRoles(null, integration);

        Assertions.assertTrue(roles.isEmpty());
    }

    @Test
    @DisplayName("UT-RM-005: Null integration returns empty set")
    void testMapRoles_NullIntegration() {
        Identity identity = Identity.builder()
                .username("dave")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.singleton("admins"))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, null);

        Assertions.assertTrue(roles.isEmpty());
    }

    @Test
    @DisplayName("UT-RM-006: Empty external groups return empty set")
    void testMapRoles_EmptyGroups() {
        Identity identity = Identity.builder()
                .username("eve")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.emptySet())
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertTrue(roles.isEmpty());
    }

    @Test
    @DisplayName("UT-RM-007: Null groups in list are skipped")
    void testMapRoles_NullGroupsSkipped() {
        Identity identity = Identity.builder()
                .username("frank")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(new HashSet<>(Arrays.asList("admins", null, "developers")))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(2, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
        Assertions.assertTrue(roles.contains("developer"));
    }

    @Test
    @DisplayName("UT-RM-008: LDAP DN format mapping works")
    void testMapRoles_LdapDnFormat() {
        Identity identity = Identity.builder()
                .username("grace")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.singleton("cn=admins,ou=groups,dc=example,dc=com"))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(1, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
    }

    @Test
    @DisplayName("UT-RM-009: Mixed mapped and unmapped groups")
    void testMapRoles_MixedGroups() {
        Identity identity = Identity.builder()
                .username("henry")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(new HashSet<>(Arrays.asList("admins", "unknown1", "developers", "unknown2")))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(2, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
        Assertions.assertTrue(roles.contains("developer"));
    }

    @Test
    @DisplayName("UT-RM-010: No duplicate roles in result")
    void testMapRoles_NoDuplicates() {
        // Multiple groups mapping to same role
        Map<String, String> props = new HashMap<>();
        props.put("role.mapping.group1", "admin");
        props.put("role.mapping.group2", "admin");

        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .properties(props)
                .build();

        Identity identity = Identity.builder()
                .username("iris")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(new HashSet<>(Arrays.asList("group1", "group2")))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(1, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
    }

    @Test
    @DisplayName("UT-RM-011: Empty role mapping value is ignored")
    void testMapRoles_EmptyMappingValue() {
        Map<String, String> props = new HashMap<>();
        props.put("role.mapping.group1", "");
        props.put("role.mapping.group2", "admin");

        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("ldap")
                .properties(props)
                .build();

        Identity identity = Identity.builder()
                .username("jack")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(new HashSet<>(Arrays.asList("group1", "group2")))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertEquals(1, roles.size());
        Assertions.assertTrue(roles.contains("admin"));
        Assertions.assertFalse(roles.contains(""));
    }

    @Test
    @DisplayName("UT-RM-012: Integration with no mappings returns empty")
    void testMapRoles_NoMappingsInIntegration() {
        AuthenticationIntegration emptyIntegration = AuthenticationIntegration.builder()
                .name("empty")
                .type("ldap")
                .properties(new HashMap<>())
                .build();

        Identity identity = Identity.builder()
                .username("kate")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.singleton("admins"))
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, emptyIntegration);

        Assertions.assertTrue(roles.isEmpty());
    }

    @Test
    @DisplayName("UT-RM-013: Case-sensitive group matching")
    void testMapRoles_CaseSensitive() {
        Identity identity = Identity.builder()
                .username("luke")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .externalGroups(Collections.singleton("ADMINS")) // uppercase
                .build();

        Set<String> roles = roleMapper.mapRoles(identity, integration);

        Assertions.assertTrue(roles.isEmpty()); // Should not match "admins"
    }

    @Test
    @DisplayName("UT-RM-014: Role mapping prefix constant is correct")
    void testRoleMappingPrefix() {
        Assertions.assertEquals("role.mapping.", RoleMapper.ROLE_MAPPING_PREFIX);
    }
}
