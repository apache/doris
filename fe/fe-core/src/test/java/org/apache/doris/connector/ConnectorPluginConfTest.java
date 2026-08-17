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

package org.apache.doris.connector;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connectorconf.testplugins.AlterValidationHelper;
import org.apache.doris.connectorconf.testplugins.ConfProbeConnectorProviderA;
import org.apache.doris.connectorconf.testplugins.ConfProbeConnectorProviderB;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.PluginRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * A connector plugin's own {@code <name>.conf} really reaches the connector, on real plugin directories.
 *
 * <p>Everything here goes through {@code loadPlugins} + {@code createConnector} rather than the
 * {@code registerDiscovered} seam the rest of {@link ConnectorPluginManagerTest} uses, because the two
 * things worth proving — that the file is found next to the jars, and that each provider gets its own
 * file — only exist on that path.
 */
public class ConnectorPluginConfTest {

    @TempDir
    Path tempDir;

    private ConnectorPluginManager manager;

    @BeforeEach
    @AfterEach
    void reset() {
        // loadPlugins writes inventory rows into a process-wide singleton; leaving them behind would make
        // information_schema.extensions assertions in other tests depend on execution order.
        PluginRegistry.getInstance().clearForTest();
        ConfProbeSink.reset();
        manager = new ConnectorPluginManager();
    }

    @Test
    public void connectorReceivesTheConfShippedNextToItsJars() throws IOException {
        Path root = pluginRoot();
        deployPlugin(root, ConfProbeConnectorProviderA.class, ConfProbeConnectorProviderA.TYPE,
                "drivers_dir=/opt/drivers\ntimeout=30\n");

        manager.loadPlugins(Collections.singletonList(root));
        manager.createConnector(ConfProbeConnectorProviderA.TYPE, Collections.emptyMap(), context());

        Map<String, String> seen = ConfProbeSink.seen(ConfProbeConnectorProviderA.TYPE);
        Assertions.assertNotNull(seen, "the probe provider was never asked to create a connector");
        Assertions.assertEquals("/opt/drivers", seen.get("drivers_dir"));
        Assertions.assertEquals("30", seen.get("timeout"));
    }

    @Test
    public void alterValidationUsesTheDirectoryProviderClassLoader() throws IOException {
        Path root = pluginRoot();
        deployPlugin(root, ConfProbeConnectorProviderA.class, ConfProbeConnectorProviderA.TYPE,
                null, AlterValidationHelper.class);
        manager.loadPlugins(Collections.singletonList(root));
        ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();

        Assertions.assertDoesNotThrow(() -> manager.validatePropertiesForUpdate(
                ConfProbeConnectorProviderA.TYPE, Collections.emptyMap(), Collections.emptyMap()));
        Assertions.assertSame(callerLoader, Thread.currentThread().getContextClassLoader(),
                "ALTER validation must restore the FE caller's context classloader");
    }

    @Test
    public void confIsNamedAfterTheProviderNotAfterTheDirectory() throws IOException {
        // The plugin directory name is the deployer's choice; the conf file name is the plugin's own
        // identity. hive/hms.conf ships exactly this way, so a file named after the directory must NOT be
        // picked up -- otherwise renaming a plugin directory would silently change which file is read.
        Path root = pluginRoot();
        Path dir = deployPlugin(root, ConfProbeConnectorProviderA.class,
                ConfProbeConnectorProviderA.TYPE, null);
        Files.write(dir.resolve("some_plugin_dir.conf"),
                "drivers_dir=/wrong\n".getBytes(StandardCharsets.UTF_8));

        manager.loadPlugins(Collections.singletonList(root));
        manager.createConnector(ConfProbeConnectorProviderA.TYPE, Collections.emptyMap(), context());

        Assertions.assertEquals(Collections.emptyMap(),
                ConfProbeSink.seen(ConfProbeConnectorProviderA.TYPE),
                "only <providerName>.conf may be read");
    }

    @Test
    public void missingConfLeavesTheProviderRegisteredWithAnEmptyMap() throws IOException {
        // Shipping no conf at all is the normal case for a connector whose settings have defaults or a
        // fe.conf fallback. It must cost the plugin nothing.
        Path root = pluginRoot();
        deployPlugin(root, ConfProbeConnectorProviderA.class, ConfProbeConnectorProviderA.TYPE, null);

        manager.loadPlugins(Collections.singletonList(root));

        Assertions.assertTrue(manager.getRegisteredTypes().contains(ConfProbeConnectorProviderA.TYPE),
                manager.getRegisteredTypes().toString());
        Assertions.assertNotNull(
                manager.createConnector(ConfProbeConnectorProviderA.TYPE, Collections.emptyMap(), context()));
        Assertions.assertEquals(Collections.emptyMap(),
                ConfProbeSink.seen(ConfProbeConnectorProviderA.TYPE));
    }

    @Test
    public void unreadableConfLeavesTheProviderRegisteredWithAnEmptyMap() throws IOException {
        // A broken file must not make the catalog type disappear: CREATE CATALOG would then answer "no
        // provider supports type", which points nowhere near the real cause. The connector proceeds
        // without the file and falls back to fe.conf.
        Path root = pluginRoot();
        deployPlugin(root, ConfProbeConnectorProviderA.class, ConfProbeConnectorProviderA.TYPE,
                "k=\\uZZZZ\n");

        manager.loadPlugins(Collections.singletonList(root));

        Assertions.assertTrue(manager.getRegisteredTypes().contains(ConfProbeConnectorProviderA.TYPE),
                "a bad conf file must not cost the deployment its catalog type");
        manager.createConnector(ConfProbeConnectorProviderA.TYPE, Collections.emptyMap(), context());
        Assertions.assertEquals(Collections.emptyMap(),
                ConfProbeSink.seen(ConfProbeConnectorProviderA.TYPE));
    }

    @Test
    public void onePluginsConfNeverReachesAnother() throws IOException {
        // The map is keyed by provider instance, and this is the assertion that keeps it that way. It is
        // also what makes a sibling connector correct: createSiblingConnector comes back through
        // createConnector, so a gateway's sibling is handed its OWN plugin's conf, not the gateway's.
        Path root = pluginRoot();
        deployPlugin(root, ConfProbeConnectorProviderA.class, ConfProbeConnectorProviderA.TYPE,
                "shared_key=from_a\nonly_in_a=yes\n");
        deployPlugin(root, ConfProbeConnectorProviderB.class, ConfProbeConnectorProviderB.TYPE,
                "shared_key=from_b\n");

        manager.loadPlugins(Collections.singletonList(root));
        manager.createConnector(ConfProbeConnectorProviderA.TYPE, Collections.emptyMap(), context());
        manager.createConnector(ConfProbeConnectorProviderB.TYPE, Collections.emptyMap(), context());

        Map<String, String> seenA = ConfProbeSink.seen(ConfProbeConnectorProviderA.TYPE);
        Map<String, String> seenB = ConfProbeSink.seen(ConfProbeConnectorProviderB.TYPE);
        Assertions.assertEquals("from_a", seenA.get("shared_key"));
        Assertions.assertEquals("from_b", seenB.get("shared_key"));
        Assertions.assertNull(seenB.get("only_in_a"), "B must not see a key only A's conf declares");
    }

    @Test
    public void providerWithNoPluginDirectoryGetsAnEmptyMap() {
        // Classpath built-ins and providers a test registers directly have no plugin directory, so there
        // is no file to read. They must fall through to the interface default rather than to a null map.
        manager.registerProvider(new ConnectorProvider() {
            @Override
            public String getType() {
                return "no_plugin_dir";
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                Assertions.assertEquals(Collections.emptyMap(), context.getConnectorConfig());
                return new Connector() {
                    @Override
                    public ConnectorMetadata getMetadata(ConnectorSession session) {
                        return null;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        });

        Assertions.assertNotNull(
                manager.createConnector("no_plugin_dir", Collections.emptyMap(), context()));
    }

    private Path pluginRoot() throws IOException {
        Path root = tempDir.resolve("connector-plugins");
        Files.createDirectories(root);
        return root;
    }

    private static ConnectorContext context() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    /**
     * Lays out one plugin the way the assembly and build.sh really do: {@code <root>/<dir>/<dir>.jar} with
     * the provider's class bytes, its ServiceLoader registration and the served API version in the
     * MANIFEST — plus, when {@code confContent} is given, {@code <name>.conf} beside the jar.
     *
     * <p>The directory is deliberately NOT named after the provider, so that nothing here can pass by
     * accidentally reading a file named after the directory.
     */
    private Path deployPlugin(Path root, Class<?> providerClass, String providerName, String confContent,
            Class<?>... additionalClasses)
            throws IOException {
        Path dir = root.resolve("some_plugin_dir_" + providerName);
        Files.createDirectories(dir);
        Path jarPath = dir.resolve("plugin.jar");

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Doris-Connector-Plugin-Api-Version",
                ApiVersionGate.forFamily("connector", ConnectorProvider.class).getExpectedVersion());
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            writeClass(jar, providerClass);
            for (Class<?> additionalClass : additionalClasses) {
                writeClass(jar, additionalClass);
            }
            jar.putNextEntry(new JarEntry("META-INF/services/" + ConnectorProvider.class.getName()));
            jar.write((providerClass.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
        if (confContent != null) {
            Files.write(dir.resolve(providerName + ".conf"), confContent.getBytes(StandardCharsets.UTF_8));
        }
        return dir;
    }

    private static void writeClass(JarOutputStream jar, Class<?> clazz) throws IOException {
        String classEntry = clazz.getName().replace('.', '/') + ".class";
        jar.putNextEntry(new JarEntry(classEntry));
        try (InputStream classBytes = clazz.getClassLoader().getResourceAsStream(classEntry)) {
            Assertions.assertNotNull(classBytes, "class bytes not found: " + classEntry);
            byte[] buffer = new byte[8192];
            int read;
            while ((read = classBytes.read(buffer)) != -1) {
                jar.write(buffer, 0, read);
            }
        }
        jar.closeEntry();
    }
}
