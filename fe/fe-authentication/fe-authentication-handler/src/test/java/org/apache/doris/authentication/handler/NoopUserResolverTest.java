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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Unit tests for {@link NoopUserResolver}.
 */
@DisplayName("NoopUserResolver Unit Tests")
class NoopUserResolverTest {

    private NoopUserResolver resolver;
    private AuthenticationIntegration integration;

    @BeforeEach
    void setUp() {
        resolver = new NoopUserResolver();
        integration = AuthenticationIntegration.builder()
                .name("test_integration")
                .type("password")
                .properties(new HashMap<>())
                .build();
    }

    @Test
    @DisplayName("UT-NUR-001: Resolve user returns null")
    void testResolveUser_ReturnsNull() {
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("test")
                .authenticatorPluginName("password")
                .build();

        Object result = resolver.resolveUser(identity, integration);

        assertNull(result);
    }

    @Test
    @DisplayName("UT-NUR-002: Resolve with null identity returns null")
    void testResolveUser_NullIdentity() {
        Object result = resolver.resolveUser(null, integration);

        assertNull(result);
    }

    @Test
    @DisplayName("UT-NUR-003: Resolve with null integration returns null")
    void testResolveUser_NullIntegration() {
        Identity identity = Identity.builder()
                .username("bob")
                .authenticatorName("test")
                .authenticatorPluginName("password")
                .build();

        Object result = resolver.resolveUser(identity, null);

        assertNull(result);
    }

    @Test
    @DisplayName("UT-NUR-004: Multiple calls always return null")
    void testResolveUser_MultipleCalls() {
        Identity identity1 = Identity.builder()
                .username("charlie")
                .authenticatorName("test1")
                .authenticatorPluginName("password")
                .build();

        Identity identity2 = Identity.builder()
                .username("dave")
                .authenticatorName("test2")
                .authenticatorPluginName("ldap")
                .build();

        Object result1 = resolver.resolveUser(identity1, integration);
        Object result2 = resolver.resolveUser(identity2, integration);

        assertNull(result1);
        assertNull(result2);
    }

    @Test
    @DisplayName("UT-NUR-005: Implements UserResolver interface")
    void testImplementsInterface() {
        assertTrue(resolver instanceof UserResolver);
    }
}
