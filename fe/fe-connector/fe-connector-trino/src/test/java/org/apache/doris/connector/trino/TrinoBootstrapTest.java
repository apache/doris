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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Unit tests for {@link TrinoBootstrap#resolvePluginDir}.
 *
 * <p>Guards the plugin-directory resolution the regression environment depends on: the
 * connectors are dispatched to a custom directory and {@code fe.conf} points
 * {@code trino_connector_plugin_dir} at it. Because this plugin runs in an isolated
 * classloader, it cannot read FE {@code Config}; the value must arrive through the engine
 * environment map. A regression once made every {@code trino-connector} catalog fail with
 * "Cannot find Trino ConnectorFactory" because that override was not honored.
 */
public class TrinoBootstrapTest {

    @Test
    public void perCatalogPropertyTakesPrecedence() {
        Map<String, String> env = ImmutableMap.of(
                "doris_home", "/opt/doris",
                "trino_connector_plugin_dir", "/should/be/ignored");
        String resolved = TrinoBootstrap.resolvePluginDir(
                ImmutableMap.of("trino.plugin.dir", "/custom/catalog/dir"), env);
        Assertions.assertEquals("/custom/catalog/dir", resolved);
    }

    @Test
    public void feConfigFromEnvironmentIsHonored() {
        // Exactly what the regression environment sets in fe.conf, delivered via the
        // engine environment because the plugin classloader cannot read FE Config.
        Map<String, String> env = ImmutableMap.of(
                "doris_home", "/opt/doris",
                "trino_connector_plugin_dir", "/tmp/trino_connector/connectors");
        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), env);
        Assertions.assertEquals("/tmp/trino_connector/connectors", resolved);
    }

    @Test
    public void defaultsToDorisHomeWhenConfigIsDefault() {
        // Config left at its default value (DORIS_HOME/plugins/connectors). With no
        // pre-2.1.8 dir present under the fake home, the default dir is returned.
        Map<String, String> env = ImmutableMap.of(
                "doris_home", "/opt/doris",
                "trino_connector_plugin_dir", "/opt/doris/plugins/connectors");
        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), env);
        Assertions.assertEquals("/opt/doris/plugins/connectors", resolved);
    }
}
