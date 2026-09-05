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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Unit tests for {@link AuthenticationPluginManager}.
 */
@DisplayName("PluginManager Unit Tests")
public class AuthenticationPluginManagerTest {

    /**
     * The same contract {@link AuthenticationPluginManager} loads plugins under. Building it here through the
     * public {@code forFamily} path also asserts, by construction, that fe-authentication-spi really ships
     * the filtered kernel resource — a build that dropped it fails every test in this class.
     */
    private static final ApiVersionGate AUTH_GATE =
            ApiVersionGate.forFamily("authentication", AuthenticationPluginFactory.class);

    private AuthenticationPluginManager pluginManager;

    @BeforeEach
    void setUp() {
        pluginManager = new AuthenticationPluginManager();
    }

    @Test
    @DisplayName("UT-HANDLER-PM-001: Plugins loaded automatically from ServiceLoader")
    void testPluginsAutoLoaded() {
        // When - plugins are loaded in constructor automatically
        List<String> pluginNames = pluginManager.getRegisteredPluginNames();

        // Then
        Assertions.assertNotNull(pluginNames);
        Assertions.assertFalse(pluginNames.isEmpty(), "Should load at least built-in plugins");
        Assertions.assertTrue(pluginNames.contains("password"), "Should include password plugin");
    }

    @Test
    @DisplayName("UT-HANDLER-PM-002: Get plugin instance with integration")
    void testGetPluginWithIntegration() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();

        // When
        AuthenticationPlugin plugin = pluginManager.getPlugin(integration);

        // Then
        Assertions.assertNotNull(plugin);
        Assertions.assertEquals("password", plugin.name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-003: Get plugin for non-existent type throws exception")
    void testGetPlugin_NonExistentType() {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test")
                .type("nonexistent")
                .build();

        // When & Then
        Assertions.assertThrows(AuthenticationException.class, () -> {
            pluginManager.getPlugin(integration);
        });
    }

    @Test
    @DisplayName("UT-HANDLER-PM-004: Register factory manually")
    void testRegisterFactory() {
        // Given
        TestPluginFactory factory = new TestPluginFactory();

        // When
        pluginManager.registerFactory(factory);

        // Then
        Optional<AuthenticationPluginFactory> retrieved = pluginManager.getFactory("test-manual");
        Assertions.assertTrue(retrieved.isPresent());
        Assertions.assertEquals("test-manual", retrieved.get().name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-005: Register duplicate factory replaces existing")
    void testRegisterFactory_Duplicate() {
        // Given
        TestPluginFactory factory1 = new TestPluginFactory();
        TestPluginFactory factory2 = new TestPluginFactory();

        // When
        pluginManager.registerFactory(factory1);
        pluginManager.registerFactory(factory2);  // Should replace

        // Then
        Optional<AuthenticationPluginFactory> retrieved = pluginManager.getFactory("test-manual");
        Assertions.assertTrue(retrieved.isPresent());
        Assertions.assertSame(factory2, retrieved.get());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-008: Get factory by name")
    void testGetFactory() {
        // When
        Optional<AuthenticationPluginFactory> factory = pluginManager.getFactory("password");

        // Then
        Assertions.assertTrue(factory.isPresent());
        Assertions.assertEquals("password", factory.get().name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-009: Get non-existent factory returns empty")
    void testGetFactory_NotExists() {
        // When
        Optional<AuthenticationPluginFactory> factory = pluginManager.getFactory("nonexistent");

        // Then
        Assertions.assertFalse(factory.isPresent());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-010: Has factory check")
    void testHasFactory() {
        // When & Then
        Assertions.assertTrue(pluginManager.hasFactory("password"));
        Assertions.assertFalse(pluginManager.hasFactory("nonexistent"));
    }

    @Test
    @DisplayName("UT-HANDLER-PM-011: Plugin instances are cached")
    void testPluginCaching() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();

        // When - get same plugin twice
        AuthenticationPlugin plugin1 = pluginManager.getPlugin(integration);
        AuthenticationPlugin plugin2 = pluginManager.getPlugin(integration);

        // Then - should be same instance (cached)
        Assertions.assertSame(plugin1, plugin2);
        Assertions.assertEquals(1, pluginManager.getCachedPluginCount());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-012: Remove plugin from cache")
    void testRemovePlugin() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();
        pluginManager.getPlugin(integration);
        Assertions.assertEquals(1, pluginManager.getCachedPluginCount());

        // When
        pluginManager.removePlugin("test_password");

        // Then
        Assertions.assertEquals(0, pluginManager.getCachedPluginCount());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-013: Clear all cached plugins")
    void testClearCache() throws AuthenticationException {
        // Given - create multiple plugin instances
        AuthenticationIntegration integration1 = AuthenticationIntegration.builder()
                .name("test1")
                .type("password")
                .build();
        AuthenticationIntegration integration2 = AuthenticationIntegration.builder()
                .name("test2")
                .type("password")
                .build();

        pluginManager.getPlugin(integration1);
        pluginManager.getPlugin(integration2);
        Assertions.assertEquals(2, pluginManager.getCachedPluginCount());

        // When
        pluginManager.clearCache();

        // Then
        Assertions.assertEquals(0, pluginManager.getCachedPluginCount());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-014: Reload plugin")
    void testReloadPlugin() throws AuthenticationException {
        // Given
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .build();

        //         AuthenticationPlugin oldPlugin = pluginManager.getPlugin(integration);

        // When - reload creates new instance
        pluginManager.reloadPlugin(integration);
        AuthenticationPlugin newPlugin = pluginManager.getPlugin(integration);

        // Then - should be different instance
        Assertions.assertNotNull(newPlugin);
        // Note: may or may not be same instance depending on implementation
    }

    @Test
    @DisplayName("UT-HANDLER-PM-016: Load external plugin from plugin roots")
    void testLoadAllFromPluginRoots() throws Exception {
        // Given
        Path root = Files.createTempDirectory("plugin-root");
        Path pluginDir = Files.createDirectories(root.resolve("external-dir-test"));
        createServiceOnlyJar(
                pluginDir.resolve("external-dir-test.jar"),
                DirectoryPluginFactory.class.getName());

        // When
        pluginManager.loadAll(
                Arrays.asList(root),
                Thread.currentThread().getContextClassLoader());

        // Then
        Assertions.assertTrue(pluginManager.hasFactory("external-dir-test"));
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
                .name("external_integration")
                .type("external-dir-test")
                .build();
        AuthenticationPlugin plugin = pluginManager.getPlugin(integration);
        Assertions.assertEquals("external-dir-test", plugin.name());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-017: Duplicate plugin name from external directory is rejected")
    void testLoadAll_DuplicateNameConflict() throws Exception {
        // Given
        Optional<AuthenticationPluginFactory> original = pluginManager.getFactory("password");
        Assertions.assertTrue(original.isPresent());

        Path root = Files.createTempDirectory("plugin-root-conflict");
        Path pluginDir = Files.createDirectories(root.resolve("password"));
        createServiceOnlyJar(
                pluginDir.resolve("password.jar"),
                "org.apache.doris.authentication.plugin.password.PasswordAuthenticationPluginFactory");

        // When
        pluginManager.loadAll(
                Arrays.asList(root),
                Thread.currentThread().getContextClassLoader());

        // Then - keep original factory, do not overwrite
        Optional<AuthenticationPluginFactory> after = pluginManager.getFactory("password");
        Assertions.assertTrue(after.isPresent());
        Assertions.assertSame(original.get(), after.get());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-018: All external directories failed to load")
    void testLoadAll_AllDiscoveredDirectoriesFailed() throws Exception {
        // Given
        Path root = Files.createTempDirectory("plugin-root-all-failed");
        Path pluginDir = Files.createDirectories(root.resolve("external-broken"));
        createServiceOnlyJar(pluginDir.resolve("external-broken.jar"), "not.exist.Factory");

        // When
        AuthenticationException ex = Assertions.assertThrows(
                AuthenticationException.class,
                () -> pluginManager.loadAll(
                        Arrays.asList(root),
                        Thread.currentThread().getContextClassLoader()));

        // Then
        Assertions.assertTrue(ex.getMessage().contains("Failed to load any external plugin"));
    }

    @Test
    @DisplayName("UT-HANDLER-PM-019: Duplicate external factory closes discarded classloader")
    void testLoadAll_DuplicateNameClosesDiscardedClassLoader() throws Exception {
        // Given
        TrackingClassLoader duplicatedClassLoader = new TrackingClassLoader();
        AuthenticationPluginFactory duplicatedFactory = new DuplicatePasswordFactory();
        PluginHandle<AuthenticationPluginFactory> duplicatedHandle = new PluginHandle<>(
                "password",
                Paths.get("ignored-plugin-dir"),
                Collections.emptyList(),
                duplicatedClassLoader,
                duplicatedFactory,
                Instant.now());
        LoadReport<AuthenticationPluginFactory> report = new LoadReport<>(
                Collections.singletonList(duplicatedHandle),
                Collections.emptyList(),
                1,
                1);
        AuthenticationPluginManager manager = new AuthenticationPluginManager(new StaticRuntimeManager(report));

        // When
        manager.loadAll(
                Collections.singletonList(Paths.get("ignored-root")),
                Thread.currentThread().getContextClassLoader());

        // Then
        Assertions.assertTrue(duplicatedClassLoader.isClosed(), "Discarded classloader should be closed");
        Optional<AuthenticationPluginFactory> passwordFactory = manager.getFactory("password");
        Assertions.assertTrue(passwordFactory.isPresent());
        Assertions.assertNotSame(duplicatedFactory, passwordFactory.get(), "Built-in password factory should stay");
    }

    @Test
    @DisplayName("UT-HANDLER-PM-020: Conflict-only repeated loadAll is idempotent")
    void testLoadAll_ConflictOnlyRepeatedInvocationIsIdempotent() throws Exception {
        // Given
        Path root = Files.createTempDirectory("plugin-root-conflict-only");
        Path pluginDir = Files.createDirectories(root.resolve("external-dir-test"));
        createServiceOnlyJar(
                pluginDir.resolve("external-dir-test.jar"),
                DirectoryPluginFactory.class.getName());

        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        pluginManager.loadAll(Arrays.asList(root), parent);
        Assertions.assertTrue(pluginManager.hasFactory("external-dir-test"));

        // When & Then
        Assertions.assertDoesNotThrow(() -> pluginManager.loadAll(Arrays.asList(root), parent));
        Assertions.assertTrue(pluginManager.hasFactory("external-dir-test"));
    }

    @Test
    @DisplayName("UT-HANDLER-PM-021: Plugin declaring an incompatible API version is refused, with a reason")
    void testLoadAll_IncompatibleApiVersionIsRefusedWithDiagnosableReason() throws Exception {
        // Given: one plugin dir that loads fine, and one whose jar declares a different major. The healthy
        // one matters: it keeps loadAll from throwing, which is exactly the case where the refusal would
        // otherwise reach the operator as a bare "no factory found for type".
        Path root = Files.createTempDirectory("plugin-root-api-version");
        createServiceOnlyJar(
                Files.createDirectories(root.resolve("external-dir-test")).resolve("external-dir-test.jar"),
                DirectoryPluginFactory.class.getName());
        createPluginJar(
                Files.createDirectories(root.resolve("stale-plugin")).resolve("stale-plugin.jar"),
                DirectoryPluginFactory.class.getName(),
                (AUTH_GATE.getExpectedMajor() + 1) + ".0");

        // When
        pluginManager.loadAll(Arrays.asList(root), Thread.currentThread().getContextClassLoader());

        // Then: the healthy plugin is in, and the refusal is retrievable rather than only in the FE log.
        Assertions.assertTrue(pluginManager.hasFactory("external-dir-test"));
        String hint = pluginManager.apiVersionRejectionHint();
        Assertions.assertTrue(hint.contains("stale-plugin"), hint);
        Assertions.assertTrue(hint.contains(AUTH_GATE.getManifestAttribute()), hint);
        Assertions.assertTrue(hint.contains(AUTH_GATE.getExpectedVersion()), hint);
    }

    @Test
    @DisplayName("UT-HANDLER-PM-022: Legacy plugin declaring no API version remains loadable")
    void testLoadAll_UndeclaredLegacyApiVersionIsAccepted() throws Exception {
        // Given: an authentication plugin built before the manifest version contract existed.
        Path root = Files.createTempDirectory("plugin-root-no-api-version");
        createPluginJar(
                Files.createDirectories(root.resolve("external-dir-test")).resolve("external-dir-test.jar"),
                DirectoryPluginFactory.class.getName(),
                null);

        // When
        pluginManager.loadAll(Arrays.asList(root), Thread.currentThread().getContextClassLoader());

        // Then: absence maps to the documented legacy API 1.0, while explicitly incompatible versions are
        // still covered by UT-HANDLER-PM-021.
        Assertions.assertTrue(pluginManager.hasFactory("external-dir-test"));
        Assertions.assertEquals("", pluginManager.apiVersionRejectionHint());
    }

    @Test
    @DisplayName("UT-HANDLER-PM-023: A clean load leaves no stale rejection hint behind")
    void testLoadAll_RejectionHintIsResetPerRun() throws Exception {
        Path bad = Files.createTempDirectory("plugin-root-hint-reset-bad");
        createPluginJar(
                Files.createDirectories(bad.resolve("stale-plugin")).resolve("stale-plugin.jar"),
                DirectoryPluginFactory.class.getName(),
                (AUTH_GATE.getExpectedMajor() + 1) + ".0");
        Path good = Files.createTempDirectory("plugin-root-hint-reset-good");
        createServiceOnlyJar(
                Files.createDirectories(good.resolve("external-dir-test")).resolve("external-dir-test.jar"),
                DirectoryPluginFactory.class.getName());

        Assertions.assertThrows(AuthenticationException.class, () -> pluginManager.loadAll(
                Arrays.asList(bad), Thread.currentThread().getContextClassLoader()));
        Assertions.assertFalse(pluginManager.apiVersionRejectionHint().isEmpty());

        // A later successful load must not keep advertising the earlier run's rejection: the hint is
        // appended to "no factory found" messages, and a stale one would misdiagnose an unrelated failure.
        pluginManager.loadAll(Arrays.asList(good), Thread.currentThread().getContextClassLoader());
        Assertions.assertEquals("", pluginManager.apiVersionRejectionHint());
    }

    private static void createServiceOnlyJar(Path jarPath, String providerClassName) throws IOException {
        createPluginJar(jarPath, providerClassName, AUTH_GATE.getExpectedVersion());
    }

    /**
     * Builds a plugin jar the way the build really produces one: a MANIFEST declaring the authentication
     * plugin API version, the provider's class bytes, and the ServiceLoader registration.
     *
     * <p>The class bytes matter for the version check, not just for classloading: the loader reads the
     * declared version from the jar that <em>defines</em> the factory class, so a jar carrying only a service
     * descriptor (with the implementation coming from the FE's own classpath) declares nothing and is
     * refused. Pass a null {@code declaredApiVersion} to reproduce exactly that.
     */
    private static void createPluginJar(Path jarPath, String providerClassName, String declaredApiVersion)
            throws IOException {
        Files.createDirectories(jarPath.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (declaredApiVersion != null) {
            manifest.getMainAttributes().putValue(AUTH_GATE.getManifestAttribute(), declaredApiVersion);
        }
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            String classEntry = providerClassName.replace('.', '/') + ".class";
            try (InputStream classBytes = AuthenticationPluginManagerTest.class.getClassLoader()
                    .getResourceAsStream(classEntry)) {
                if (classBytes != null) {
                    jar.putNextEntry(new JarEntry(classEntry));
                    byte[] buffer = new byte[8192];
                    int read;
                    while ((read = classBytes.read(buffer)) != -1) {
                        jar.write(buffer, 0, read);
                    }
                    jar.closeEntry();
                }
            }
            JarEntry serviceEntry = new JarEntry(
                    "META-INF/services/org.apache.doris.authentication.spi.AuthenticationPluginFactory");
            jar.putNextEntry(serviceEntry);
            jar.write((providerClassName + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }

    /**
     * Test plugin factory for manual registration testing.
     */
    private static class TestPluginFactory implements AuthenticationPluginFactory {
        @Override
        public String name() {
            return "test-manual";
        }

        @Override
        public AuthenticationPlugin create() {
            return new TestPlugin();
        }

        public String description() {
            return "Test plugin for manual registration";
        }
    }

    /**
     * Test plugin implementation.
     */
    private static class TestPlugin implements AuthenticationPlugin {
        @Override
        public String name() {
            return "test-manual";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            // Support all requests for testing
            return true;
        }

        @Override
        public AuthenticationResult authenticate(
                AuthenticationRequest request,
                AuthenticationIntegration integration) {
            return AuthenticationResult.success(
                    BasicPrincipal.builder()
                            .name(request.getUsername())
                            .authenticator(name())
                            .build()
            );
        }

        @Override
        public void initialize(AuthenticationIntegration integration) {
            // No initialization needed for test
        }

        @Override
        public void close() {
            // No cleanup needed for test
        }
    }

    /**
     * Directory-discovered plugin factory used by UT-HANDLER-PM-016.
     */
    public static class DirectoryPluginFactory implements AuthenticationPluginFactory {
        @Override
        public String name() {
            return "external-dir-test";
        }

        @Override
        public AuthenticationPlugin create() {
            return new DirectoryPlugin();
        }
    }

    /**
     * Directory-discovered plugin implementation used by UT-HANDLER-PM-016.
     */
    public static class DirectoryPlugin implements AuthenticationPlugin {
        @Override
        public String name() {
            return "external-dir-test";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return true;
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationIntegration integration) {
            return AuthenticationResult.success(
                    BasicPrincipal.builder()
                            .name(request.getUsername())
                            .authenticator(name())
                            .build()
            );
        }
    }

    private static class DuplicatePasswordFactory implements AuthenticationPluginFactory {
        @Override
        public String name() {
            return "password";
        }

        @Override
        public AuthenticationPlugin create() {
            return new DirectoryPlugin();
        }
    }

    private static class TrackingClassLoader extends ClassLoader implements Closeable {
        private boolean closed;

        @Override
        public void close() {
            this.closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    private static class StaticRuntimeManager extends DirectoryPluginRuntimeManager<AuthenticationPluginFactory> {
        private final LoadReport<AuthenticationPluginFactory> report;

        private StaticRuntimeManager(LoadReport<AuthenticationPluginFactory> report) {
            this.report = report;
        }

        @Override
        public LoadReport<AuthenticationPluginFactory> loadAll(List<Path> pluginRoots, ClassLoader parent,
                Class<AuthenticationPluginFactory> factoryType, ClassLoadingPolicy policy,
                ApiVersionGate apiVersionGate) {
            return report;
        }

        @Override
        public Optional<PluginHandle<AuthenticationPluginFactory>> get(String pluginName) {
            return Optional.empty();
        }

        @Override
        public void discard(String pluginName) {
            // This stub bypasses the real loadAll bookkeeping, so honor the
            // contract that discarding a reported success closes its classloader.
            for (PluginHandle<AuthenticationPluginFactory> handle : report.getSuccesses()) {
                if (handle.getPluginName().equals(pluginName)
                        && handle.getClassLoader() instanceof Closeable) {
                    try {
                        ((Closeable) handle.getClassLoader()).close();
                    } catch (IOException ignored) {
                        // test stub: never expected
                    }
                }
            }
        }

        @Override
        public List<PluginHandle<AuthenticationPluginFactory>> list() {
            return Collections.emptyList();
        }
    }

}
