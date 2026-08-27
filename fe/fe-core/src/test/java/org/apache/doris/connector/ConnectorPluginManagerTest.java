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
import org.apache.doris.datasource.CatalogFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Tests for {@link ConnectorPluginManager}: provider selection, the type-name contract enforced when a
 * provider is discovered, and the split between sibling lookup and building a standalone catalog.
 *
 * <p>Plugin API compatibility is deliberately absent here. It used to be decided by {@code
 * ConnectorProvider.apiVersion()}, which could never reject anything (the SPI interface — and therefore its
 * default body — always came from the FE's own classloader). It is now decided at load time from a MANIFEST
 * attribute of the plugin jar, and is covered where that decision lives: {@code ApiVersionGateTest} and
 * {@code DirectoryPluginRuntimeManagerApiVersionTest} in fe-extension-loader, plus this family's wiring in
 * {@code org.apache.doris.pluginapiversion.PluginApiVersionWiringTest}.
 */
public class ConnectorPluginManagerTest {

    private static final String CLASSPATH_PROVIDER_CONSTRUCTED =
            "doris.test.connector.classpath-provider-constructed";

    private ConnectorPluginManager manager;
    private ConnectorContext testContext;

    @BeforeEach
    void setUp() {
        manager = new ConnectorPluginManager();
        testContext = new ConnectorContext() {
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

    @Test
    void testRegisteredProviderCreatesConnector() {
        manager.registerProvider(createProvider("test_type"));

        Connector connector = manager.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNotNull(connector,
                "A registered provider that supports the type should create the connector");
    }

    @Test
    void testValidatePropertiesDelegatesToTheMatchingProvider() {
        manager.registerProvider(new ConnectorProvider() {
            @Override
            public String getType() {
                return "validating_type";
            }

            @Override
            public void validateProperties(Map<String, String> properties) {
                throw new IllegalArgumentException("rejected by provider");
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new TaggedConnector("validating");
            }
        });

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> manager.validateProperties("validating_type", Collections.emptyMap()));
        Assertions.assertEquals("rejected by provider", e.getMessage(),
                "CREATE CATALOG must surface the provider's own reason, not one invented by the engine");
        Assertions.assertDoesNotThrow(() -> manager.validateProperties("unknown_type", Collections.emptyMap()),
                "an unmatched type validates to nothing here; CatalogFactory decides how to fail");
    }

    @Test
    void testNoMatchingProviderReturnsNull() {
        Connector connector = manager.createConnector("nonexistent",
                Collections.emptyMap(), testContext);
        Assertions.assertNull(connector,
                "No matching provider should return null");
    }

    @Test
    void siblingOnlyTypeIsReachableForSiblingLookupButNeverAsACatalog() {
        // A sibling-only connector serves a table format parasitic on another connector's metastore (hudi on an
        // HMS catalog): the gateway builds it through createConnector, and that is its ONLY way in. If the
        // standalone filter leaked into createConnector, every such table would stop being readable; if the
        // filter were missing from createStandaloneCatalogConnector, CREATE CATALOG could build a catalog with
        // no engine-side semantics behind it. Both directions must hold at once.
        manager.registerProvider(createProvider("sibling_only", false, "sib"));

        Assertions.assertNotNull(manager.createConnector("sibling_only", Collections.emptyMap(), testContext),
                "sibling lookup must still reach a sibling-only connector — it has no other entry point");
        Assertions.assertNull(
                manager.createStandaloneCatalogConnector("sibling_only", Collections.emptyMap(), testContext),
                "a sibling-only connector must never back a standalone catalog");
    }

    @Test
    void anyRegisteredTypeCanBackACatalog_noEngineSideList() {
        // The point of removing the catalog type allow-list: a type name the engine has never heard of becomes
        // usable purely by registering a provider for it. This cannot pass while an engine-side list of
        // accepted types exists.
        manager.registerProvider(createProvider("acme-lake", true, "acme"));

        Assertions.assertNotNull(
                manager.createStandaloneCatalogConnector("acme-lake", Collections.emptyMap(), testContext),
                "a third-party type must be routed to its provider without any engine-side registration");
        Assertions.assertEquals(Collections.singletonList("acme-lake"), manager.getStandaloneCatalogTypes(),
                "a standalone type must be listed as creatable");
    }

    @Test
    void siblingOnlyTypeIsNotListedAsCreatable() {
        // The list feeds the CREATE CATALOG diagnostic; naming a type that can never be created would send the
        // user chasing a value the engine will always reject.
        manager.registerProvider(createProvider("sibling_only", false, "sib"));

        Assertions.assertTrue(manager.getStandaloneCatalogTypes().isEmpty(),
                "sibling-only types must not appear among the creatable catalog types");
        Assertions.assertEquals(Collections.singletonList("sibling_only"), manager.getRegisteredTypes(),
                "it is still a registered provider — only its eligibility for a catalog differs");
    }

    @Test
    void duplicateTypeNameOnClasspathFailsLoud() {
        // Type names are the identity CREATE CATALOG routes on and the anchor of source-prefixed namespaces.
        // Two providers claiming one name on the classpath is a build error, and the winner would be decided by
        // ServiceLoader order — silently, differently per build.
        Assertions.assertTrue(manager.registerDiscovered(createProvider("dup", true, "first"), true));

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.registerDiscovered(createProvider("DUP", true, "second"), true),
                "a classpath duplicate must fail loud, and case must not be a way around it");
        Assertions.assertTrue(e.getMessage().contains("already claimed"), e.getMessage());
    }

    @Test
    void duplicateTypeNameInPluginDirectoryIsSkippedButDoesNotStopFe() {
        // Same conflict, different blame: two plugin directories shipping one type name is a deployment
        // accident. loadPlugins promises partial success — one bad plugin dir must not keep FE from starting —
        // so the offender is skipped and the incumbent keeps serving.
        Assertions.assertTrue(manager.registerDiscovered(createProvider("dup", true, "first"), false));
        Assertions.assertFalse(manager.registerDiscovered(createProvider("dup", true, "second"), false),
                "the second claimant must be refused, not silently appended");

        Assertions.assertEquals(Collections.singletonList("dup"), manager.getRegisteredTypes(),
                "the type must be claimed exactly once");
        Connector connector = manager.createConnector("dup", Collections.emptyMap(), testContext);
        Assertions.assertEquals("first", ((TaggedConnector) connector).tag,
                "the provider that claimed the name first must keep serving it");
    }

    @Test
    void providerClaimingAnEngineBuiltinCatalogTypeIsRefused() {
        // Reserving the engine's own catalog type names is what makes the plugin-first routing order safe: a
        // plugin declaring itself "doris" could otherwise quietly take over every remote-Doris catalog a user
        // creates. Refusing it at registration means the shadowing case cannot arise at all.
        for (String builtin : new String[] {"doris", "test", "lakesoul"}) {
            Assertions.assertTrue(CatalogFactory.isBuiltinCatalogType(builtin), builtin);
            Assertions.assertFalse(manager.registerDiscovered(createProvider(builtin, true, "x"), false),
                    "a plugin must not be allowed to claim engine built-in type '" + builtin + "'");
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.registerDiscovered(createProvider(builtin.toUpperCase(), true, "x"),
                            true),
                    "on the classpath the same violation must fail loud, case-insensitively");
        }
        Assertions.assertTrue(manager.getRegisteredTypes().isEmpty(),
                "no refused provider may end up in the registry");
    }

    @Test
    void blankTypeNameIsRefused() {
        // getType() is now the sole admission ticket for third-party code; a blank name would otherwise sit in
        // the registry and match nothing, or match everything the moment someone compares loosely.
        Assertions.assertFalse(manager.registerDiscovered(createProvider("  ", true, "x"), false),
                "a blank type name must be refused");
        Assertions.assertTrue(manager.getRegisteredTypes().isEmpty());
    }

    @Test
    void registerProviderStillShadowsADiscoveredType() {
        // registerProvider exists to stand in for a real plugin in tests (several rely on shadowing a real
        // type name). The uniqueness check must not reach it, or those tests lose their only seam.
        Assertions.assertTrue(manager.registerDiscovered(createProvider("iceberg", true, "real"), false));
        manager.registerProvider(createProvider("iceberg", true, "override"));

        Connector connector = manager.createConnector("iceberg", Collections.emptyMap(), testContext);
        Assertions.assertEquals("override", ((TaggedConnector) connector).tag,
                "the explicitly registered provider must win over the discovered one");
    }

    @Test
    void classpathProviderMustDeclareTheKernelApiMajor(@TempDir Path tempDir) throws Exception {
        // Both majors are derived from the kernel's own declared version rather than written as literals.
        // Bumping connector.plugin.api.version is a routine part of any SPI surface change, and what this
        // test asserts is the GATE — stale major refused, current major admitted — not which number happens
        // to be current. ConnectorPluginSurfaceTest is where the number is deliberately pinned; pinning it
        // here as well only produced a second, uninformative failure on every bump.
        int kernelMajor = kernelApiMajor();
        Path staleJar = createClasspathProviderJar(tempDir.resolve("api-stale.jar"), (kernelMajor - 1) + ".0");
        ConnectorPluginManager incompatible = new ConnectorPluginManager();
        try (URLClassLoader loader = providerClassLoader(staleJar)) {
            incompatible.loadBuiltins(loader);
        }
        Assertions.assertFalse(incompatible.getRegisteredTypes().contains("classpath-test"),
                "a provider one major behind the kernel must not enter through classpath discovery");

        Path futureJar = createClasspathProviderJar(tempDir.resolve("api-future.jar"), (kernelMajor + 1) + ".0");
        ConnectorPluginManager newer = new ConnectorPluginManager();
        try (URLClassLoader loader = providerClassLoader(futureJar)) {
            newer.loadBuiltins(loader);
        }
        Assertions.assertFalse(newer.getRegisteredTypes().contains("classpath-test"),
                "a provider one major ahead of the kernel must not enter through classpath discovery");

        Path currentJar = createClasspathProviderJar(tempDir.resolve("api-current.jar"), kernelMajor + ".0");
        ConnectorPluginManager compatible = new ConnectorPluginManager();
        try (URLClassLoader loader = providerClassLoader(currentJar)) {
            compatible.loadBuiltins(loader);
        }
        Assertions.assertTrue(compatible.getRegisteredTypes().contains("classpath-test"));
    }

    /** The connector plugin API major this FE build serves — the same value {@code ApiVersionGate} compares against. */
    private static int kernelApiMajor() throws IOException {
        Properties version = new Properties();
        try (InputStream in = ConnectorProvider.class.getResourceAsStream(
                "/META-INF/doris/connector-plugin-api-version.properties")) {
            Assertions.assertNotNull(in, "missing connector plugin API version resource");
            version.load(in);
        }
        String declared = version.getProperty("api.version");
        Assertions.assertNotNull(declared, "api.version must be declared by the kernel resource");
        return Integer.parseInt(declared.substring(0, declared.indexOf('.')));
    }

    @Test
    void incompatibleClasspathProviderIsRejectedBeforeConstruction(@TempDir Path tempDir) throws Exception {
        System.clearProperty(CLASSPATH_PROVIDER_CONSTRUCTED);
        try {
            Path apiOneJar = createClasspathProviderJar(tempDir.resolve("api-one-constructor.jar"), "1.0");
            try (URLClassLoader loader = providerClassLoader(apiOneJar)) {
                new ConnectorPluginManager().loadBuiltins(loader);
            }
            Assertions.assertNull(System.getProperty(CLASSPATH_PROVIDER_CONSTRUCTED),
                    "the API-major gate must run before untrusted provider construction");
        } finally {
            System.clearProperty(CLASSPATH_PROVIDER_CONSTRUCTED);
        }
    }

    @Test
    void duplicateCreateTableEngineNameIsRefused() {
        // Engine names route CREATE TABLE ... ENGINE= the same way type names route CREATE CATALOG. Two
        // plugins answering to one engine name would make the statement mean whichever registered first, so
        // the conflict is refused where it can still be refused: at registration.
        Assertions.assertTrue(manager.registerDiscovered(
                createProviderWithEngines("first_type", "shared_engine"), false));
        Assertions.assertFalse(manager.registerDiscovered(
                        createProviderWithEngines("second_type", "SHARED_ENGINE"), false),
                "a second claimant of the same engine name must be refused, case-insensitively");

        Assertions.assertEquals(Collections.singletonList("first_type"), manager.getRegisteredTypes(),
                "the refused provider must not end up in the registry");

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.registerDiscovered(createProviderWithEngines("third_type", "shared_engine"), true),
                "on the classpath the same conflict must fail loud");
        Assertions.assertTrue(e.getMessage().contains("already claimed"), e.getMessage());
    }

    @Test
    void providerClaimingAnEngineReservedEngineNameIsRefused() {
        // olap is the internal catalog's own engine, and odbc/mysql/broker are retired table types that still
        // owe the user a specific "use X instead" message from InternalCatalog. A plugin claiming one would
        // silently take over a statement the engine answers for.
        for (String reserved : new String[] {"olap", "mysql", "odbc", "broker"}) {
            Assertions.assertFalse(manager.registerDiscovered(
                            createProviderWithEngines("t_" + reserved, reserved.toUpperCase()), false),
                    "a plugin must not be allowed to claim reserved engine name '" + reserved + "'");
        }
        Assertions.assertTrue(manager.getRegisteredTypes().isEmpty(),
                "no refused provider may end up in the registry");
    }

    @Test
    void refusedProviderDoesNotLeaveItsTypeOrEngineNameClaimed() {
        // The checks run before anything is claimed, so a provider rejected for one reason must not poison the
        // name it never got. Otherwise a bad plugin directory could permanently disable a good one.
        Assertions.assertFalse(manager.registerDiscovered(createProviderWithEngines("good_type", "olap"), false),
                "rejected for the reserved engine name");

        Assertions.assertTrue(manager.registerDiscovered(createProviderWithEngines("good_type", "good_engine"),
                        false),
                "the type name must still be free after the earlier provider was refused");
        Assertions.assertEquals(Collections.singletonList("good_type"), manager.getRegisteredTypes());
    }

    private static ConnectorProvider createProviderWithEngines(String type, String... engineNames) {
        Set<String> engines = new HashSet<>(Arrays.asList(engineNames));
        return new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public Set<String> acceptedCreateTableEngineNames() {
                return engines;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new TaggedConnector(type);
            }
        };
    }

    public static class ClasspathProvider implements ConnectorProvider {
        public ClasspathProvider() {
            System.setProperty(CLASSPATH_PROVIDER_CONSTRUCTED, "true");
        }

        @Override
        public String getType() {
            return "classpath-test";
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            return null;
        }
    }

    private static Path createClasspathProviderJar(Path jarPath, String apiVersion) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Doris-Connector-Plugin-Api-Version", apiVersion);
        String classEntry = ClasspathProvider.class.getName().replace('.', '/') + ".class";
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            jar.putNextEntry(new JarEntry(classEntry));
            try (InputStream bytes = ClasspathProvider.class.getClassLoader().getResourceAsStream(classEntry)) {
                Assertions.assertNotNull(bytes, "provider class bytes");
                byte[] buffer = new byte[8192];
                int read;
                while ((read = bytes.read(buffer)) != -1) {
                    jar.write(buffer, 0, read);
                }
            }
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("META-INF/services/" + ConnectorProvider.class.getName()));
            jar.write((ClasspathProvider.class.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
        return jarPath;
    }

    private static URLClassLoader providerClassLoader(Path jarPath) throws IOException {
        return new URLClassLoader(new URL[] {jarPath.toUri().toURL()}, ConnectorProvider.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals(ClasspathProvider.class.getName())) {
                    synchronized (getClassLoadingLock(name)) {
                        Class<?> loaded = findLoadedClass(name);
                        if (loaded == null) {
                            loaded = findClass(name);
                        }
                        if (resolve) {
                            resolveClass(loaded);
                        }
                        return loaded;
                    }
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    private static ConnectorProvider createProvider(String type) {
        return createProvider(type, true, "");
    }

    private static ConnectorProvider createProvider(String type, boolean standalone, String tag) {
        return new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public boolean isStandaloneCatalogType() {
                return standalone;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new TaggedConnector(tag);
            }
        };
    }

    /** A connector that remembers which provider made it, so selection can be asserted. */
    private static final class TaggedConnector implements Connector {
        private final String tag;

        private TaggedConnector(String tag) {
            this.tag = tag;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
