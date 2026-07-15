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
 * <p>Also guards backward compatibility across the two moves of the default dir
 * ({@code DORIS_HOME/connectors} -> {@code DORIS_HOME/plugins/connectors} in 2.1.8 ->
 * {@code DORIS_HOME/plugins/trino_plugins}). A deployment that upgrades without moving its
 * plugins must keep loading them; the failure mode is silent (the load is caught and logged),
 * surfacing only as "catalog creates fine but every query fails".
 *
 * <p>Each test that reaches the legacy probe uses its own {@link TempDir} as {@code doris_home}:
 * the probe is memoized per home, so a shared home would leak results between tests.
 */
public class TrinoBootstrapTest {

    /** A Trino plugin is a directory of jars; the probe only cares that the dir is non-empty. */
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
    public void freshInstallGetsTheCurrentDefault(@TempDir Path dorisHome) {
        // Nothing legacy on disk: build.sh no longer creates plugins/connectors/, so a fresh
        // install has no legacy dir at all and must land on the current default.
        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), envAtDefault(dorisHome));
        Assertions.assertEquals(dorisHome + "/plugins/trino_plugins", resolved);
    }

    @Test
    public void upgradeFromTwoOneEightKeepsLoadingItsExistingPlugins(@TempDir Path dorisHome) throws IOException {
        // The compatibility case that motivates the fallback: an existing deployment has its Trino
        // plugins in plugins/connectors/ and upgrades without touching them. Resolving to the new
        // default here would silently break every trino-connector catalog.
        installPluginIn(dorisHome, "plugins/connectors");

        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), envAtDefault(dorisHome));
        Assertions.assertEquals(dorisHome + "/plugins/connectors", resolved);
    }

    @Test
    public void preTwoOneEightDirStillWinsOverTheTwoOneEightDir(@TempDir Path dorisHome) throws IOException {
        // Precedence is oldest-first, unchanged from what 2.1.8 shipped. Asserted explicitly because
        // adding plugins/connectors/ to the chain must not reorder the existing two entries: a
        // deployment that never migrated off DORIS_HOME/connectors keeps resolving there.
        installPluginIn(dorisHome, "connectors");
        installPluginIn(dorisHome, "plugins/connectors");

        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), envAtDefault(dorisHome));
        Assertions.assertEquals(dorisHome + "/connectors", resolved);
    }

    @Test
    public void emptyLegacyDirsDoNotDivertFromTheDefault(@TempDir Path dorisHome) throws IOException {
        // An empty legacy dir means "nothing to be compatible with". This is what an operator is left
        // with after moving their plugins out, and what older build.sh versions created on install.
        Files.createDirectories(dorisHome.resolve("connectors"));
        Files.createDirectories(dorisHome.resolve("plugins/connectors"));

        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), envAtDefault(dorisHome));
        Assertions.assertEquals(dorisHome + "/plugins/trino_plugins", resolved);
    }

    @Test
    public void spellingTheOldDefaultInFeConfCountsAsAnExplicitOverride(@TempDir Path dorisHome) {
        // An operator who pinned the pre-move default in fe.conf gets exactly what they asked for:
        // it no longer equals the built-in default, so it is honored as an explicit override and the
        // legacy probe is skipped entirely (note nothing is on disk here).
        Map<String, String> env = ImmutableMap.of(
                "doris_home", dorisHome.toString(),
                "trino_connector_plugin_dir", dorisHome + "/plugins/connectors");

        String resolved = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), env);
        Assertions.assertEquals(dorisHome + "/plugins/connectors", resolved);
    }

    @Test
    public void legacyProbeIsMemoizedSoEveryCatalogInAProcessAgrees(@TempDir Path dorisHome) throws IOException {
        // TrinoBootstrap.getInstance() throws when a later catalog asks for a different plugin dir than
        // the singleton was built with, and the plugins are loaded exactly once per process. So the
        // probe must not change its answer mid-life: a user dropping plugins into a legacy dir after the
        // first catalog exists would otherwise break every later catalog with an IllegalStateException,
        // pointing at plugins that could never be loaded without an FE restart anyway.
        Map<String, String> env = envAtDefault(dorisHome);
        String firstCatalog = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), env);
        Assertions.assertEquals(dorisHome + "/plugins/trino_plugins", firstCatalog);

        installPluginIn(dorisHome, "plugins/connectors");

        String laterCatalog = TrinoBootstrap.resolvePluginDir(Collections.emptyMap(), env);
        Assertions.assertEquals(firstCatalog, laterCatalog);
    }
}
