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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 *
 * <p>Resolution is deliberately only two steps deep — per-catalog property, else the config
 * verbatim. The legacy-dir fallbacks that used to sit under them were dropped, so the cases here
 * are as much about what is <em>not</em> consulted as about what is.
 */
public class TrinoBootstrapTest {

    /** A Trino plugin is a directory of jars; a probe would only care that the dir is non-empty. */
    private static void installPluginIn(Path dorisHome, String subdir) throws IOException {
        Files.createDirectories(dorisHome.resolve(subdir).resolve("trino-hive"));
    }

    private static Map<String, String> envAtDefault(Path dorisHome) {
        return ImmutableMap.of(
                "doris_home", dorisHome.toString(),
                "trino_connector_plugin_dir", dorisHome + "/plugins/trino_plugins");
    }

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
    public void legacyPluginDirsAreNotConsultedEvenWhenTheyHoldPlugins(@TempDir Path dorisHome) throws IOException {
        // The config names the dir; nothing probes the filesystem behind it. Both dirs that earlier
        // versions fell back to are populated here and both must be ignored -- an upgrade only picks
        // them up if the operator points the config at one. Asserted because the alternative is
        // invisible: resolving to a legacy dir silently loads a different set of plugins, and
        // re-introducing the fallback would otherwise pass every other case in this file.
        installPluginIn(dorisHome, "connectors");
        installPluginIn(dorisHome, "plugins/connectors");

        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), envAtDefault(dorisHome));
        Assertions.assertEquals(dorisHome + "/plugins/trino_plugins", resolved);
    }

    @Test
    public void missingConfigInTheEnvironmentFailsLoud() {
        // DefaultConnectorContext always passes the FE config, so an absent key means the engine
        // failed to deliver it. Guessing a dir would surface far away as "catalog creates fine but
        // every query fails"; throwing keeps the cause at the point of breakage.
        Assertions.assertThrows(IllegalStateException.class,
                () -> TrinoBootstrap.resolvePluginDir(
                        Collections.emptyMap(), ImmutableMap.of("doris_home", "/opt/doris")));
    }
}
