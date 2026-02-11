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

import org.apache.doris.authentication.spi.AuthenticationPlugin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LdapAuthenticationPluginFactory}.
 */
@DisplayName("LdapAuthenticationPluginFactory Unit Tests")
class LdapAuthenticationPluginFactoryTest {

    @Test
    @DisplayName("UT-LDAP-F-001: Factory should return plugin name 'ldap'")
    void testFactoryName() {
        // Given
        LdapAuthenticationPluginFactory factory = new LdapAuthenticationPluginFactory();

        // When
        String name = factory.name();

        // Then
        assertEquals("ldap", name, "Factory name should be 'ldap'");
    }

    @Test
    @DisplayName("UT-LDAP-F-002: Factory should create plugin instance")
    void testFactoryCreate() {
        // Given
        LdapAuthenticationPluginFactory factory = new LdapAuthenticationPluginFactory();

        // When
        AuthenticationPlugin plugin = factory.create();

        // Then
        assertNotNull(plugin, "Factory should return non-null plugin");
        assertInstanceOf(LdapAuthenticationPlugin.class, plugin);
        assertEquals("ldap", plugin.name());
    }

    @Test
    @DisplayName("UT-LDAP-F-003: Factory returns singleton instance")
    void testFactoryReturnsSingleton() {
        // Given
        LdapAuthenticationPluginFactory factory = new LdapAuthenticationPluginFactory();

        // When
        AuthenticationPlugin plugin1 = factory.create();
        AuthenticationPlugin plugin2 = factory.create();

        // Then
        assertNotNull(plugin1);
        assertNotNull(plugin2);
        assertSame(plugin1, plugin2,
                "Factory should return singleton instance for stateless LDAP plugin");
    }
}
