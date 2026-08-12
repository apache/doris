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
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.PluginRegistry;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.fs.FileSystemPluginManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.lineage.LineagePluginFactory;
import org.apache.doris.pluginapiversion.testplugins.ShadowingAuthorizationPluginFactory;
import org.apache.doris.pluginapiversion.testplugins.VersionProbeAuthorizationPluginFactory;
import org.apache.doris.pluginapiversion.testplugins.VersionProbeConnectorProvider;
import org.apache.doris.pluginapiversion.testplugins.VersionProbeFileSystemProvider;
import org.apache.doris.utframe.PluginJarWriter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
        // Five properties, five resources, five attributes. The point of keeping them separate is that
        // changing one family's SPI must not force plugins of the other four to be rebuilt (design 3.3);
        // a shared attribute name or a shared resource would quietly undo that.
        ApiVersionGate connector = ApiVersionGate.forFamily("connector", ConnectorProvider.class);
        ApiVersionGate filesystem = ApiVersionGate.forFamily("filesystem", FileSystemProvider.class);
        ApiVersionGate authentication =
                ApiVersionGate.forFamily("authentication", AuthenticationPluginFactory.class);
        ApiVersionGate authorization =
                ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);
        ApiVersionGate lineage = ApiVersionGate.forFamily("lineage", LineagePluginFactory.class);

        Assertions.assertEquals("Doris-Connector-Plugin-Api-Version", connector.getManifestAttribute());
        Assertions.assertEquals("Doris-Filesystem-Plugin-Api-Version", filesystem.getManifestAttribute());
        Assertions.assertEquals("Doris-Authentication-Plugin-Api-Version",
                authentication.getManifestAttribute());
        Assertions.assertEquals("Doris-Authorization-Plugin-Api-Version",
                authorization.getManifestAttribute());
        Assertions.assertEquals("Doris-Lineage-Plugin-Api-Version", lineage.getManifestAttribute());

        Set<String> attributes = new HashSet<>();
        for (ApiVersionGate gate : new ApiVersionGate[] {
                connector, filesystem, authentication, authorization, lineage}) {
            Assertions.assertTrue(gate.getExpectedMajor() >= 1,
                    "a family's major starts at 1; 0 means the resource was read but never set");
            Assertions.assertTrue(attributes.add(gate.getManifestAttribute()),
                    "two families share a MANIFEST attribute: " + gate.getManifestAttribute());
        }
    }

    @Test
    public void authorizationPluginDeclaringTheServedVersionGovernsTheInstance() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);

        AccessControllerManager manager = managerLoadingProbeFrom(gate.getExpectedVersion());

        // The observable end of the whole channel: the source named in fe.conf is the one deciding.
        Assertions.assertEquals(VersionProbeAuthorizationPluginFactory.NAME,
                manager.getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME).name());
    }

    @Test
    public void authorizationPluginDeclaringAnotherMajorIsRefusedWithBothVersionsNamed() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);
        int otherMajor = gate.getExpectedMajor() + 1;

        String failure = refusalOf(otherMajor + ".0");

        // Refusing is not enough: an operator has to be able to tell "installed but incompatible" from
        // "never installed", which is the same message with nothing appended.
        Assertions.assertTrue(failure.contains("refused on their declared API version"), failure);
        Assertions.assertTrue(failure.contains(otherMajor + ".0"), failure);
        Assertions.assertTrue(failure.contains(gate.getExpectedVersion()), failure);
    }

    @Test
    public void authorizationPluginDeclaringNothingIsRefused() throws IOException {
        // The regression the version gate exists for: a plugin that says nothing about the API it was built
        // against must not inherit the kernel's own answer and be admitted.
        String failure = refusalOf(null);

        Assertions.assertTrue(failure.contains("refused on their declared API version"), failure);
        Assertions.assertTrue(failure.contains("Doris-Authorization-Plugin-Api-Version"), failure);
    }

    @Test
    public void authorizationPluginFromADirectoryCannotTakeTheNameOfAShippedSource() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);
        // Declares the version this FE serves, so the version gate is not what refuses it - the name is.
        Path root = pluginRoot("authorization-shadow-root", "shadow-authz",
                ShadowingAuthorizationPluginFactory.class, AuthorizationPluginFactory.class,
                "Doris-Authorization-Plugin-Api-Version", gate.getExpectedVersion());
        String originalDir = Config.authorization_plugins_dir;
        AccessControllerManager manager;
        try {
            Config.authorization_plugins_dir = root.toString();
            manager = new AccessControllerManager(new Auth());
        } finally {
            Config.authorization_plugins_dir = originalDir;
        }

        // The factory answering to that name must still be the FE's own, not the one from the directory.
        ConcurrentHashMap<String, AuthorizationPluginFactory> factories =
                Deencapsulation.getField(manager, "authorizationPluginFactories");
        AuthorizationPluginFactory installed = factories.get(
                ShadowingAuthorizationPluginFactory.SHADOWED_NAME);
        Assertions.assertNotNull(installed, "the shipped ranger-doris source disappeared from the test setup,"
                + " so this test would pass without proving anything");
        // By class NAME, not by Class object. A directory plugin is loaded through its own classloader, so
        // its factory class is never the same object as the one this test holds - even when it did displace
        // the shipped source. Written as an identity comparison first, this assertion could not fail at all:
        // a mutation deleting the guard below left the whole test green.
        Assertions.assertNotEquals(ShadowingAuthorizationPluginFactory.class.getName(),
                installed.getClass().getName(),
                "a jar dropped into the plugin directory displaced a source shipped with the FE; an"
                        + " allow-everything plugin under a real source's name is the whole point of"
                        + " refusing this");
        // Refusing it has to release it too: a plugin kept in the directory runtime keeps its classloader,
        // and every jar it opened, for the lifetime of the FE.
        DirectoryPluginRuntimeManager<AuthorizationPluginFactory> directoryRuntime =
                Deencapsulation.getField(manager, "pluginDirectoryRuntime");
        Assertions.assertFalse(
                directoryRuntime.get(ShadowingAuthorizationPluginFactory.SHADOWED_NAME).isPresent(),
                "the refused plugin is still held by the directory runtime, and so is its classloader");
    }

    @Test
    public void theDecisionVocabularyIsSharedWithADirectoryPlugin() throws IOException {
        ApiVersionGate gate = ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);
        // The jar carries its OWN copy of a vocabulary class. Without org.apache.doris.authorization. being
        // parent-first, the plugin's classloader would prefer that copy, and a resource kind it returned
        // would not be the resource kind the engine switches on - a ClassCastException with no plugin bug
        // behind it. With an empty extras list this assertion would hold trivially, which is why the copy
        // is deliberately planted.
        Path root = pluginRoot("authorization-shared-types-root", "shared-types-authz",
                VersionProbeAuthorizationPluginFactory.class, AuthorizationPluginFactory.class,
                "Doris-Authorization-Plugin-Api-Version", gate.getExpectedVersion(),
                Collections.singletonList(ResourceKind.class));
        String originalDir = Config.authorization_plugins_dir;
        String originalType = Config.access_controller_type;
        AccessControllerManager manager;
        try {
            Config.authorization_plugins_dir = root.toString();
            Config.access_controller_type = VersionProbeAuthorizationPluginFactory.NAME;
            manager = new AccessControllerManager(new Auth());
        } finally {
            Config.authorization_plugins_dir = originalDir;
            Config.access_controller_type = originalType;
        }

        ClassLoader pluginClassLoader = manager
                .getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME)
                .getClass().getClassLoader();
        Assertions.assertNotSame(getClass().getClassLoader(), pluginClassLoader,
                "the plugin was not loaded through its own classloader, so nothing about class sharing is"
                        + " being tested here");
        try {
            Assertions.assertSame(ResourceKind.class,
                    pluginClassLoader.loadClass(ResourceKind.class.getName()),
                    "the plugin resolved its own copy of the decision vocabulary");
        } catch (ClassNotFoundException e) {
            Assertions.fail("the plugin classloader cannot see the decision vocabulary at all", e);
        }
    }

    /**
     * Builds an {@link AccessControllerManager} whose plugin directory holds one probe plugin declaring
     * {@code declaredApiVersion}, with {@code access_controller_type} naming it. Construction is where the
     * whole channel runs: directory sweep, version gate, factory registration, and installing the named
     * source.
     */
    private AccessControllerManager managerLoadingProbeFrom(String declaredApiVersion) throws IOException {
        Path root = pluginRoot("authorization-root-" + declaredApiVersion, "version-probe-authz",
                VersionProbeAuthorizationPluginFactory.class, AuthorizationPluginFactory.class,
                "Doris-Authorization-Plugin-Api-Version", declaredApiVersion);
        String originalDir = Config.authorization_plugins_dir;
        String originalType = Config.access_controller_type;
        try {
            Config.authorization_plugins_dir = root.toString();
            Config.access_controller_type = VersionProbeAuthorizationPluginFactory.NAME;
            return new AccessControllerManager(new Auth());
        } finally {
            Config.authorization_plugins_dir = originalDir;
            Config.access_controller_type = originalType;
        }
    }

    private String refusalOf(String declaredApiVersion) throws IOException {
        try {
            managerLoadingProbeFrom(declaredApiVersion);
        } catch (RuntimeException e) {
            return e.getMessage();
        }
        return Assertions.fail("a plugin declaring " + declaredApiVersion
                + " must not become the source governing this instance");
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
     * Writes one plugin under {@code tempDir/rootName} and returns that root, the way the assembly really
     * lays a plugin out. A null {@code declaredApiVersion} omits the attribute entirely.
     */
    private Path pluginRoot(String rootName, String pluginDirName, Class<?> providerClass,
            Class<?> spiInterface, String manifestAttribute, String declaredApiVersion) throws IOException {
        return pluginRoot(rootName, pluginDirName, providerClass, spiInterface, manifestAttribute,
                declaredApiVersion, Collections.emptyList());
    }

    /**
     * @param alsoBundled classes whose bytes are copied into the jar as well, to stand for a plugin that
     *         ships its own copy of something the FE also has
     */
    private Path pluginRoot(String rootName, String pluginDirName, Class<?> providerClass,
            Class<?> spiInterface, String manifestAttribute, String declaredApiVersion,
            List<Class<?>> alsoBundled) throws IOException {
        return PluginJarWriter.writePluginRoot(tempDir.resolve(rootName), pluginDirName, providerClass,
                spiInterface, manifestAttribute, declaredApiVersion, alsoBundled);
    }
}
