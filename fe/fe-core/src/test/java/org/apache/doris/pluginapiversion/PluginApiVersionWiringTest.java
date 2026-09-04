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

package org.apache.doris.pluginapiversion;

import org.apache.doris.authentication.spi.AuthenticationPluginFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.PluginRegistry;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.fs.FileSystemPluginManager;
import org.apache.doris.nereids.lineage.LineagePluginFactory;
import org.apache.doris.pluginapiversion.testplugins.VersionProbeConnectorProvider;
import org.apache.doris.pluginapiversion.testplugins.VersionProbeFileSystemProvider;

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
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Each fe-core plugin family really enforces its own plugin API version, on real plugin jars.
 *
 * <p>The decision rule itself is proved family-neutrally in fe-extension-loader
 * ({@code ApiVersionGateTest}, {@code DirectoryPluginRuntimeManagerApiVersionTest}). What is only true of a
 * family, and is checked here, is the wiring: that its manager passes a gate at all, that the gate is built
 * from <em>its own</em> kernel resource, and that its version is independent of the other families'. The
 * AUTHENTICATION half of the same contract lives in {@code AuthenticationPluginManagerTest}, next to the
 * lazy-load path that has to carry the rejection reason into its exception.
 *
 * <p>What no unit test can check: that the {@code <manifestEntries>} element name written in
 * {@code fe/fe-connector/pom.xml} and {@code fe/fe-filesystem/pom.xml} equals the attribute name derived
 * here. That name appears once as XML and once as a derivation rule, and nothing links them at build time —
 * so the derived names are pinned literally below, to be read against the poms in review.
 */
public class PluginApiVersionWiringTest {

    @TempDir
    Path tempDir;

    @BeforeEach
    @AfterEach
    void resetProcessWideRegistry() {
        // loadPlugins writes inventory rows into a process-wide singleton; leaving them behind would make
        // information_schema.extensions assertions in other tests depend on execution order.
        PluginRegistry.getInstance().clearForTest();
    }

    @Test
    public void connectorPluginDeclaringTheServedVersionIsAdmitted() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("connector", ConnectorProvider.class);
        ConnectorPluginManager manager = new ConnectorPluginManager();

        manager.loadPlugins(Collections.singletonList(connectorPluginRoot(gate.getExpectedVersion())));

        Assertions.assertTrue(manager.getRegisteredTypes().contains("version_probe"),
                "a plugin built against the version this FE serves must load: "
                        + manager.getRegisteredTypes());
    }

    @Test
    public void connectorPluginDeclaringAnotherMajorIsRefused() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("connector", ConnectorProvider.class);
        ConnectorPluginManager manager = new ConnectorPluginManager();

        manager.loadPlugins(Collections.singletonList(
                connectorPluginRoot((gate.getExpectedMajor() + 1) + ".0")));

        Assertions.assertFalse(manager.getRegisteredTypes().contains("version_probe"),
                "a plugin built against another major must not become a routable catalog type");
    }

    @Test
    public void connectorPluginDeclaringNothingIsRefused() throws IOException {
        // The regression this whole change exists for: before, a plugin that said nothing about its API
        // version inherited the kernel's own default and was always admitted.
        ConnectorPluginManager manager = new ConnectorPluginManager();

        manager.loadPlugins(Collections.singletonList(connectorPluginRoot(null)));

        Assertions.assertFalse(manager.getRegisteredTypes().contains("version_probe"),
                "an undeclared plugin API version must fail closed");
    }

    @Test
    public void filesystemPluginDeclaringTheServedVersionIsAdmitted() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("filesystem", FileSystemProvider.class);
        FileSystemPluginManager manager = new FileSystemPluginManager();

        manager.loadPlugins(Collections.singletonList(filesystemPluginRoot(gate.getExpectedVersion())));

        Assertions.assertTrue(providerNames(manager).contains("version_probe_fs"), providerNames(manager)
                .toString());
    }

    @Test
    public void filesystemPluginDeclaringAnotherMajorIsRefused() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("filesystem", FileSystemProvider.class);
        FileSystemPluginManager manager = new FileSystemPluginManager();

        manager.loadPlugins(Collections.singletonList(
                filesystemPluginRoot((gate.getExpectedMajor() + 1) + ".0")));

        Assertions.assertFalse(providerNames(manager).contains("version_probe_fs"),
                "an incompatible filesystem plugin must not join the storage routing table");
    }

    @Test
    public void filesystemPluginDeclaringNothingIsRefused() throws IOException {
        FileSystemPluginManager manager = new FileSystemPluginManager();

        manager.loadPlugins(Collections.singletonList(filesystemPluginRoot(null)));

        Assertions.assertFalse(providerNames(manager).contains("version_probe_fs"),
                "an undeclared plugin API version must fail closed");
    }

    @Test
    public void everyFamilyDeclaresItsOwnIndependentContract() {
        // Four properties, four resources, four attributes. The point of keeping them separate is that
        // changing one family's SPI must not force plugins of the other three to be rebuilt (design 3.3);
        // a shared attribute name or a shared resource would quietly undo that.
        ApiVersionGate connector = ApiVersionGate.forFamily("connector", ConnectorProvider.class);
        ApiVersionGate filesystem = ApiVersionGate.forFamily("filesystem", FileSystemProvider.class);
        ApiVersionGate authentication =
                ApiVersionGate.forFamily("authentication", AuthenticationPluginFactory.class);
        ApiVersionGate lineage = ApiVersionGate.forFamily("lineage", LineagePluginFactory.class);

        Assertions.assertEquals("Doris-Connector-Plugin-Api-Version", connector.getManifestAttribute());
        Assertions.assertEquals("Doris-Filesystem-Plugin-Api-Version", filesystem.getManifestAttribute());
        Assertions.assertEquals("Doris-Authentication-Plugin-Api-Version",
                authentication.getManifestAttribute());
        Assertions.assertEquals("Doris-Lineage-Plugin-Api-Version", lineage.getManifestAttribute());

        Set<String> attributes = new HashSet<>();
        for (ApiVersionGate gate : new ApiVersionGate[] {connector, filesystem, authentication, lineage}) {
            Assertions.assertTrue(gate.getExpectedMajor() >= 1,
                    "a family's major starts at 1; 0 means the resource was read but never set");
            Assertions.assertTrue(attributes.add(gate.getManifestAttribute()),
                    "two families share a MANIFEST attribute: " + gate.getManifestAttribute());
        }
    }

    private static Set<String> providerNames(FileSystemPluginManager manager) {
        Set<String> names = new HashSet<>();
        manager.getProviders().forEach(provider -> names.add(provider.name()));
        return names;
    }

    private Path connectorPluginRoot(String declaredApiVersion) throws IOException {
        return pluginRoot("connector-root-" + declaredApiVersion, "version-probe",
                VersionProbeConnectorProvider.class, ConnectorProvider.class,
                "Doris-Connector-Plugin-Api-Version", declaredApiVersion);
    }

    private Path filesystemPluginRoot(String declaredApiVersion) throws IOException {
        return pluginRoot("filesystem-root-" + declaredApiVersion, "version-probe-fs",
                VersionProbeFileSystemProvider.class, FileSystemProvider.class,
                "Doris-Filesystem-Plugin-Api-Version", declaredApiVersion);
    }

    /**
     * Writes {@code <root>/<pluginDirName>/<pluginDirName>.jar} the way the assembly really lays a plugin
     * out: the provider's class bytes, its ServiceLoader registration, and a MANIFEST declaring the plugin
     * API version. A null {@code declaredApiVersion} omits the attribute entirely.
     */
    private Path pluginRoot(String rootName, String pluginDirName, Class<?> providerClass,
            Class<?> spiInterface, String manifestAttribute, String declaredApiVersion) throws IOException {
        Path root = tempDir.resolve(rootName);
        Path jarPath = root.resolve(pluginDirName).resolve(pluginDirName + ".jar");
        Files.createDirectories(jarPath.getParent());

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (declaredApiVersion != null) {
            manifest.getMainAttributes().putValue(manifestAttribute, declaredApiVersion);
        }
        String classEntry = providerClass.getName().replace('.', '/') + ".class";
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            jar.putNextEntry(new JarEntry(classEntry));
            try (InputStream classBytes = providerClass.getClassLoader().getResourceAsStream(classEntry)) {
                Assertions.assertNotNull(classBytes, "class bytes not found: " + classEntry);
                byte[] buffer = new byte[8192];
                int read;
                while ((read = classBytes.read(buffer)) != -1) {
                    jar.write(buffer, 0, read);
                }
            }
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("META-INF/services/" + spiInterface.getName()));
            jar.write((providerClass.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
        return root;
    }
}
