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

package org.apache.doris.extension.loader;

import org.apache.doris.extension.spi.PluginFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Directory-driven plugin runtime manager for Doris FE.
 *
 * <p>This class is the generic runtime entry point used by FE business modules
 * (authentication, authorization, protocol extensions, etc.) to load external
 * plugin factories from one or more plugin root directories.
 *
 * <h2>Responsibilities</h2>
 *
 * <p>The {@link #loadAll(List, ClassLoader, Class, ClassLoadingPolicy)} flow:
 * <ol>
 *   <li>Scans each root in {@code pluginRoots} and treats its direct
 *       subdirectories as plugin directories.</li>
 *   <li>Resolves plugin jars using the convention:
 *       {@code pluginDir/*.jar + pluginDir/lib/*.jar}.</li>
 *   <li>Creates a per-plugin classloader (child-first by default) with a
 *       configurable parent-first package-prefix policy.</li>
 *   <li>Discovers exactly one typed factory via {@link java.util.ServiceLoader}
 *       (for example, {@code AuthenticationPluginFactory}).</li>
 *   <li>Validates and records load outcomes and returns a {@link LoadReport}
 *       with successes and failures.</li>
 * </ol>
 *
 * <p>The {@link #get(String)} and {@link #list()} methods provide read-only
 * access to successfully loaded plugin handles.
 *
 * <h2>Non-Goals / Out of Scope</h2>
 *
 * <p>This manager intentionally does not:
 * <ol>
 *   <li>Instantiate business plugins (it loads factories, not plugin instances).</li>
 *   <li>Provide runtime {@code reload}/{@code unload} semantics.</li>
 *   <li>Watch directories for changes or download plugins from remote repositories.</li>
 * </ol>
 *
 * <h2>Failure Semantics</h2>
 *
 * <p>Failures are staged for observability and troubleshooting:
 * {@code scan}, {@code resolve}, {@code createClassLoader}, {@code discover},
 * {@code instantiate}, and {@code conflict}. Per-directory failures do not stop
 * other directories from loading.
 *
 * <h2>Conflict Strategy</h2>
 *
 * <p>If multiple plugin directories yield the same {@code factory.name()}:
 * the first successfully loaded one is kept, later ones are recorded as
 * {@code conflict} and their classloaders are closed to avoid resource leakage.
 *
 * <h2>Classloading Notes</h2>
 *
 * <p>Child-first classloading isolates plugin dependencies from FE's process
 * classpath. Parent-first prefixes ensure core API/SPI types are loaded from
 * a single source to avoid type-isolation issues such as {@link ClassCastException}.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>The manager stores loaded handles in a concurrent map. The load lifecycle
 * is guarded by a lock to prevent concurrent {@code loadAll} invocations from
 * interleaving and producing inconsistent outcomes.
 */
public class DirectoryPluginRuntimeManager<F extends PluginFactory> {

    private final ConcurrentMap<String, PluginHandle<F>> handlesByName = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();

    public LoadReport<F> loadAll(List<Path> pluginRoots, ClassLoader parent, Class<F> factoryType,
            ClassLoadingPolicy policy) {
        Objects.requireNonNull(pluginRoots, "pluginRoots");
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(factoryType, "factoryType");
        ClassLoadingPolicy effectivePolicy = policy != null ? policy : ClassLoadingPolicy.defaultPolicy();

        List<Path> pluginDirs = new ArrayList<>();
        List<LoadFailure> failures = new ArrayList<>();

        int rootsScanned = 0;
        for (Path root : pluginRoots) {
            if (root == null) {
                continue;
            }
            rootsScanned++;
            collectPluginDirs(root, pluginDirs, failures);
        }

        List<PluginHandle<F>> successes = new ArrayList<>();
        synchronized (lifecycleLock) {
            for (Path pluginDir : pluginDirs) {
                try {
                    PluginHandle<F> handle = loadFromPluginDir(pluginDir, parent, factoryType, effectivePolicy);
                    if (handlesByName.containsKey(handle.getPluginName())) {
                        closeClassLoader(handle.getClassLoader());
                        failures.add(new LoadFailure(
                                pluginDir,
                                LoadFailure.STAGE_CONFLICT,
                                "Duplicate plugin name: " + handle.getPluginName(),
                                null));
                        continue;
                    }
                    handlesByName.put(handle.getPluginName(), handle);
                    successes.add(handle);
                } catch (PluginLoadException e) {
                    failures.add(e.toLoadFailure());
                }
            }
        }
        return new LoadReport<>(successes, failures, rootsScanned, pluginDirs.size());
    }

    public Optional<PluginHandle<F>> get(String pluginName) {
        return Optional.ofNullable(handlesByName.get(pluginName));
    }

    public List<PluginHandle<F>> list() {
        Collection<PluginHandle<F>> handles = handlesByName.values();
        List<PluginHandle<F>> results = new ArrayList<>(handles);
        Collections.sort(results, Comparator.comparing(PluginHandle::getPluginName));
        return results;
    }

    private void collectPluginDirs(Path root, List<Path> pluginDirs, List<LoadFailure> failures) {
        Path normalized = normalize(root);
        if (!Files.exists(normalized)) {
            failures.add(new LoadFailure(
                    normalized,
                    LoadFailure.STAGE_SCAN,
                    "Plugin root does not exist: " + normalized,
                    null));
            return;
        }
        if (!Files.isDirectory(normalized)) {
            failures.add(new LoadFailure(
                    normalized,
                    LoadFailure.STAGE_SCAN,
                    "Plugin root is not a directory: " + normalized,
                    null));
            return;
        }

        try (Stream<Path> stream = Files.list(normalized)) {
            pluginDirs.addAll(stream.filter(Files::isDirectory)
                    .map(this::normalize)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            failures.add(new LoadFailure(
                    normalized,
                    LoadFailure.STAGE_SCAN,
                    "Failed to list plugin root: " + normalized,
                    e));
        }
    }

    private PluginHandle<F> loadFromPluginDir(Path pluginDir, ClassLoader parent, Class<F> factoryType,
            ClassLoadingPolicy policy) throws PluginLoadException {
        Path normalizedDir = normalize(pluginDir);
        List<Path> resolvedJars = resolveJars(normalizedDir);
        URL[] urls = toUrls(resolvedJars, normalizedDir);
        ClassLoader filteredParent = new ServiceResourceFilteringParentClassLoader(parent, factoryType);

        ClassLoader classLoader;
        try {
            classLoader = new PluginLoader(policy.toParentFirstPackages()).createClassLoader(urls, filteredParent);
        } catch (RuntimeException e) {
            throw new PluginLoadException(
                    normalizedDir,
                    LoadFailure.STAGE_CREATE_CLASSLOADER,
                    "Failed to create classloader for " + normalizedDir,
                    e);
        }

        F factory;
        try {
            factory = discoverSingleFactory(factoryType, classLoader, normalizedDir);
        } catch (PluginLoadException e) {
            closeClassLoader(classLoader);
            throw e;
        }

        String pluginName;
        try {
            pluginName = factory.name();
        } catch (RuntimeException e) {
            closeClassLoader(classLoader);
            throw new PluginLoadException(
                    normalizedDir,
                    LoadFailure.STAGE_INSTANTIATE,
                    "Failed to get plugin name from discovered factory in " + normalizedDir,
                    e);
        }
        if (pluginName == null || pluginName.trim().isEmpty()) {
            closeClassLoader(classLoader);
            throw new PluginLoadException(
                    normalizedDir,
                    LoadFailure.STAGE_INSTANTIATE,
                    "Plugin name is empty for directory: " + normalizedDir,
                    null);
        }

        return new PluginHandle<>(
                pluginName.trim(),
                normalizedDir,
                resolvedJars,
                classLoader,
                factory,
                Instant.now());
    }

    private List<Path> resolveJars(Path pluginDir) throws PluginLoadException {
        if (!Files.exists(pluginDir) || !Files.isDirectory(pluginDir)) {
            throw new PluginLoadException(
                    pluginDir,
                    LoadFailure.STAGE_RESOLVE,
                    "Plugin directory is missing or not a directory: " + pluginDir,
                    null);
        }

        List<Path> jars = new ArrayList<>();
        collectJars(pluginDir, jars);

        Path libDir = pluginDir.resolve("lib");
        if (Files.isDirectory(libDir)) {
            collectJars(libDir, jars);
        }

        if (jars.isEmpty()) {
            throw new PluginLoadException(
                    pluginDir,
                    LoadFailure.STAGE_RESOLVE,
                    "No jar found under plugin directory: " + pluginDir,
                    null);
        }
        return jars;
    }

    private void collectJars(Path directory, List<Path> target) throws PluginLoadException {
        try (Stream<Path> stream = Files.list(directory)) {
            target.addAll(stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .map(this::normalize)
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            throw new PluginLoadException(
                    directory,
                    LoadFailure.STAGE_RESOLVE,
                    "Failed to resolve jars under " + directory,
                    e);
        }
    }

    private URL[] toUrls(List<Path> jars, Path pluginDir) throws PluginLoadException {
        URL[] urls = new URL[jars.size()];
        for (int i = 0; i < jars.size(); i++) {
            try {
                urls[i] = jars.get(i).toUri().toURL();
            } catch (MalformedURLException e) {
                throw new PluginLoadException(
                        pluginDir,
                        LoadFailure.STAGE_RESOLVE,
                        "Invalid jar path: " + jars.get(i),
                        e);
            }
        }
        return urls;
    }

    private F discoverSingleFactory(Class<F> factoryType, ClassLoader classLoader, Path pluginDir)
            throws PluginLoadException {
        List<F> discovered = new ArrayList<>();
        try {
            ServiceLoader.load(factoryType, classLoader).forEach(discovered::add);
        } catch (Throwable t) {
            throw new PluginLoadException(
                    pluginDir,
                    LoadFailure.STAGE_DISCOVER,
                    "Failed to discover " + factoryType.getName() + " in " + pluginDir,
                    t);
        }

        if (discovered.isEmpty()) {
            throw new PluginLoadException(
                    pluginDir,
                    LoadFailure.STAGE_DISCOVER,
                    "No " + factoryType.getName() + " found in " + pluginDir,
                    null);
        }

        if (discovered.size() > 1) {
            throw new PluginLoadException(
                    pluginDir,
                    LoadFailure.STAGE_DISCOVER,
                    "Multiple " + factoryType.getName() + " found in " + pluginDir + ": " + discovered.size(),
                    null);
        }
        return discovered.get(0);
    }

    private static void closeClassLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        try {
            if (classLoader instanceof Closeable) {
                ((Closeable) classLoader).close();
            }
        } catch (IOException ignored) {
            // Best effort close.
        }
    }

    private Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }

    private static final class PluginLoadException extends Exception {

        private final Path pluginDir;
        private final String stage;

        private PluginLoadException(Path pluginDir, String stage, String message, Throwable cause) {
            super(message, cause);
            this.pluginDir = pluginDir;
            this.stage = stage;
        }

        private Path getPluginDir() {
            return pluginDir;
        }

        private LoadFailure toLoadFailure() {
            return new LoadFailure(getPluginDir(), stage, getMessage(), getCause());
        }
    }

    /**
     * Hide parent service descriptor resources for the target factory type to prevent
     * parent classpath providers from polluting per-plugin discovery.
     */
    private static final class ServiceResourceFilteringParentClassLoader extends ClassLoader {

        private final String blockedServiceResource;

        private ServiceResourceFilteringParentClassLoader(ClassLoader parent, Class<?> factoryType) {
            super(parent);
            this.blockedServiceResource = "META-INF/services/" + factoryType.getName();
        }

        @Override
        public URL getResource(String name) {
            if (blockedServiceResource.equals(name)) {
                return null;
            }
            return super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (blockedServiceResource.equals(name)) {
                return Collections.enumeration(Collections.<URL>emptyList());
            }
            return super.getResources(name);
        }
    }
}
