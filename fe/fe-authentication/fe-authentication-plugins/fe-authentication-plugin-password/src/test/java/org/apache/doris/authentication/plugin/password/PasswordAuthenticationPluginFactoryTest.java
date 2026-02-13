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

package org.apache.doris.authentication.plugin.password;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PasswordAuthenticationPluginFactory}.
 */
@DisplayName("PasswordAuthenticationPluginFactory Unit Tests")
class PasswordAuthenticationPluginFactoryTest {

    @Test
    @DisplayName("UT-PWD-F-001: Factory should return plugin name 'password'")
    void testFactoryName() {
        // Given
        PasswordAuthenticationPluginFactory factory = new PasswordAuthenticationPluginFactory();

        // When
        String name = factory.name();

        // Then
        Assertions.assertEquals("password", name, "Factory name should be 'password'");
    }

    @Test
    @DisplayName("UT-PWD-F-002: Factory should create plugin instance")
    void testFactoryCreate() {
        // Given
        PasswordAuthenticationPluginFactory factory = new PasswordAuthenticationPluginFactory();

        // When
        PasswordAuthenticationPlugin plugin = (PasswordAuthenticationPlugin) factory.create();

        // Then
        Assertions.assertNotNull(plugin, "Factory should return non-null plugin");
        Assertions.assertInstanceOf(PasswordAuthenticationPlugin.class, plugin);
    }

    @Test
    @DisplayName("UT-PWD-F-003: Factory should create new instance per integration")
    void testFactoryCreatesNewInstance() {
        // Given
        PasswordAuthenticationPluginFactory factory = new PasswordAuthenticationPluginFactory();

        // When
        PasswordAuthenticationPlugin plugin1 = (PasswordAuthenticationPlugin) factory.create();
        PasswordAuthenticationPlugin plugin2 = (PasswordAuthenticationPlugin) factory.create();

        // Then
        Assertions.assertNotSame(plugin1, plugin2, "Factory should return distinct instances");
    }
}
