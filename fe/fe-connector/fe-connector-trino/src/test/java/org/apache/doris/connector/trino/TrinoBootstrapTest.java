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

package org.apache.doris.connector.trino;

import org.apache.doris.common.Config;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Unit tests for {@link TrinoBootstrap#resolvePluginDir}.
 *
 * <p>Guards the plugin-directory resolution the regression environment depends on: the
 * connectors are dispatched to a custom directory and {@code fe.conf} points
 * {@code trino_connector_plugin_dir} at it. A refactoring once dropped the FE-config read,
 * which made every {@code trino-connector} catalog fail with "Cannot find Trino
 * ConnectorFactory". These tests ensure the override paths stay honored.
 */
public class TrinoBootstrapTest {

    // Config.trino_connector_plugin_dir is process-global static state; capture it per
    // instance (JUnit 5 creates one instance per method) and restore it after each test.
    private final String savedPluginDir = Config.trino_connector_plugin_dir;

    @AfterEach
    public void restoreConfig() {
        Config.trino_connector_plugin_dir = savedPluginDir;
    }

    @Test
    public void perCatalogPropertyTakesPrecedence() {
        Config.trino_connector_plugin_dir = "/etc/should-be-ignored";
        String resolved = TrinoBootstrap.resolvePluginDir(
                ImmutableMap.of("trino.plugin.dir", "/custom/catalog/dir"));
        Assertions.assertEquals("/custom/catalog/dir", resolved);
    }

    @Test
    public void feConfigOverrideIsHonored() {
        // Exactly what the regression environment sets in fe.conf.
        String configured = "/tmp/trino_connector/connectors";
        Config.trino_connector_plugin_dir = configured;
        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap());
        Assertions.assertEquals(configured, resolved);
    }
}
