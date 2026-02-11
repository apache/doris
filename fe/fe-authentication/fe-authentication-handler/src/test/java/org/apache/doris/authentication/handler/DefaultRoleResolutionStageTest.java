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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


/**
 * Unit tests for {@link DefaultRoleResolutionStage}.
 */
@DisplayName("DefaultRoleResolutionStage Unit Tests")
class DefaultRoleResolutionStageTest {

    @Mock
    private RoleMapper roleMapper;

    private DefaultRoleResolutionStage stage;
    private AuthenticationIntegration integration;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        stage = new DefaultRoleResolutionStage(roleMapper);

        integration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("ldap")
                .properties(new HashMap<>())
                .build();
    }

    @Test
    @DisplayName("UT-DRRS-001: Resolve roles successfully")
    void testResolveRoles_Success() {
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .build();

        Set<String> expectedRoles = new HashSet<>(Arrays.asList("admin", "developer"));
        Mockito.when(roleMapper.mapRoles(identity, integration)).thenReturn(expectedRoles);

        Set<String> result = stage.resolveRoles(identity, integration);

        Assertions.assertEquals(expectedRoles, result);
        Mockito.verify(roleMapper).mapRoles(identity, integration);
    }

    @Test
    @DisplayName("UT-DRRS-002: Null identity returns empty set")
    void testResolveRoles_NullIdentity() {
        Set<String> result = stage.resolveRoles(null, integration);

        Assertions.assertTrue(result.isEmpty());
        Mockito.verify(roleMapper, Mockito.never()).mapRoles(Mockito.any(), Mockito.any());
    }

    @Test
    @DisplayName("UT-DRRS-003: Null integration returns empty set")
    void testResolveRoles_NullIntegration() {
        Identity identity = Identity.builder()
                .username("bob")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .build();

        Set<String> result = stage.resolveRoles(identity, null);

        Assertions.assertTrue(result.isEmpty());
        Mockito.verify(roleMapper, Mockito.never()).mapRoles(Mockito.any(), Mockito.any());
    }

    @Test
    @DisplayName("UT-DRRS-004: Both null returns empty set")
    void testResolveRoles_BothNull() {
        Set<String> result = stage.resolveRoles(null, null);

        Assertions.assertTrue(result.isEmpty());
        Mockito.verify(roleMapper, Mockito.never()).mapRoles(Mockito.any(), Mockito.any());
    }

    @Test
    @DisplayName("UT-DRRS-005: RoleMapper returns empty set")
    void testResolveRoles_EmptyResult() {
        Identity identity = Identity.builder()
                .username("charlie")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .build();

        Mockito.when(roleMapper.mapRoles(identity, integration)).thenReturn(Collections.emptySet());

        Set<String> result = stage.resolveRoles(identity, integration);

        Assertions.assertTrue(result.isEmpty());
        Mockito.verify(roleMapper).mapRoles(identity, integration);
    }

    @Test
    @DisplayName("UT-DRRS-006: Constructor with null RoleMapper throws NPE")
    void testConstructor_NullRoleMapper() {
        Assertions.assertThrows(NullPointerException.class, () ->
                new DefaultRoleResolutionStage(null));
    }

    @Test
    @DisplayName("UT-DRRS-007: Multiple calls delegate to RoleMapper")
    void testResolveRoles_MultipleCalls() {
        Identity identity1 = Identity.builder()
                .username("dave")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .build();

        Identity identity2 = Identity.builder()
                .username("eve")
                .authenticatorName("test")
                .authenticatorPluginName("ldap")
                .build();

        Mockito.Mockito.when(roleMapper.mapRoles(identity1, integration))
                .thenReturn(Collections.singleton("role1"));
        Mockito.Mockito.when(roleMapper.mapRoles(identity2, integration))
                .thenReturn(new HashSet<>(Arrays.asList("role2", "role3")));

        Set<String> result1 = stage.resolveRoles(identity1, integration);
        Set<String> result2 = stage.resolveRoles(identity2, integration);

        Assertions.assertEquals(1, result1.size());
        Assertions.assertEquals(2, result2.size());
        Mockito.verify(roleMapper, Mockito.times(2)).mapRoles(Mockito.any(), Mockito.eq(integration));
    }
}
