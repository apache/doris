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
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for authentication plugins.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Discover built-in and external {@link AuthenticationPluginFactory}</li>
 *   <li>Create and cache {@link AuthenticationPlugin} instances by integration name</li>
 *   <li>Expose plugin lookup for {@link AuthenticationService}</li>
 * </ul>
 *
 * <p>V1 scope:
 * <ul>
 *   <li>Supports external plugin loading via plugin roots</li>
 *   <li>Does not support plugin reload/unload</li>
 * </ul>
 */
public class AuthenticationPluginManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticationPluginManager.class);
    private static final List<String> AUTH_PARENT_FIRST_PREFIXES =
            Collections.singletonList("org.apache.doris.authentication.");

    /** Factories by plugin name (e.g., "ldap", "oidc", "password") */
    private final Map<String, AuthenticationPluginFactory> factories = new ConcurrentHashMap<>();

    /** Plugin instances by integration name */
    private final Map<String, AuthenticationPlugin> pluginByIntegration = new ConcurrentHashMap<>();

    private final DirectoryPluginRuntimeManager<AuthenticationPluginFactory> runtimeManager;
    private final ClassLoadingPolicy classLoadingPolicy;

    public AuthenticationPluginManager() {
        this(new DirectoryPluginRuntimeManager<AuthenticationPluginFactory>());
    }

    public AuthenticationPluginManager(DirectoryPluginRuntimeManager<AuthenticationPluginFactory> runtimeManager) {
        this(runtimeManager, new ClassLoadingPolicy(AUTH_PARENT_FIRST_PREFIXES));
    }

    public AuthenticationPluginManager(DirectoryPluginRuntimeManager<AuthenticationPluginFactory> runtimeManager,
            ClassLoadingPolicy classLoadingPolicy) {
        this.runtimeManager = Objects.requireNonNull(runtimeManager, "runtimeManager");
        this.classLoadingPolicy = classLoadingPolicy != null
                ? classLoadingPolicy
                : ClassLoadingPolicy.defaultPolicy();

        ServiceLoader.load(AuthenticationPluginFactory.class)
                .forEach(factory -> factories.put(factory.name(), factory));
    }

    /**
     * Register/override a factory programmatically (useful for tests).
     *
     * @param factory the factory to register
     */
    public void registerFactory(AuthenticationPluginFactory factory) {
        Objects.requireNonNull(factory, "factory");
        factories.put(factory.name(), factory);
    }

    /**
     * Load external authentication plugins from plugin roots using directory convention:
     * pluginDir/*.jar + pluginDir/lib/*.jar.
     *
     * <p>Behavior:
     * <ul>
     *   <li>Only direct subdirectories under each root are treated as plugin directories.</li>
     *   <li>Single directory failure does not stop other directories from loading.</li>
     *   <li>If no external plugin is loaded and all discovered directories fail, throws exception.</li>
     * </ul>
     *
     * @param pluginRoots plugin root directories
     * @param parent parent classloader
     * @throws AuthenticationException when all discovered plugin directories fail to load
     */
    public void loadAll(List<Path> pluginRoots, ClassLoader parent) throws AuthenticationException {
        Objects.requireNonNull(pluginRoots, "pluginRoots");
        Objects.requireNonNull(parent, "parent");

        LoadReport<AuthenticationPluginFactory> report = runtimeManager.loadAll(
                pluginRoots,
                parent,
                AuthenticationPluginFactory.class,
                classLoadingPolicy);

        for (LoadFailure failure : report.getFailures()) {
            LOG.warn("Skip plugin directory due to load failure: pluginDir={}, stage={}, message={}",
                    failure.getPluginDir(), failure.getStage(), failure.getMessage(), failure.getCause());
        }

        int loadedPlugins = 0;
        for (PluginHandle<AuthenticationPluginFactory> handle : report.getSuccesses()) {
            String pluginName = handle.getPluginName();
            AuthenticationPluginFactory existing = factories.putIfAbsent(pluginName, handle.getFactory());
            if (existing != null) {
                closeClassLoaderQuietly(handle.getClassLoader());
                LOG.warn("Skip duplicated plugin name: {} from directory {}", pluginName, handle.getPluginDir());
                continue;
            }
            loadedPlugins++;
            LOG.info("Loaded external authentication plugin: name={}, pluginDir={}, jarCount={}",
                    pluginName, handle.getPluginDir(), handle.getResolvedJars().size());
        }

        LoadFailure firstNonConflictFailure = firstNonConflictFailure(report.getFailures());
        if (report.getDirsScanned() > 0 && loadedPlugins == 0 && firstNonConflictFailure != null) {
            throw new AuthenticationException(
                    "Failed to load any external plugin from pluginRoots: stage="
                            + firstNonConflictFailure.getStage()
                            + ", pluginDir=" + firstNonConflictFailure.getPluginDir()
                            + ", message=" + firstNonConflictFailure.getMessage(),
                    firstNonConflictFailure.getCause());
        }
    }

    private static LoadFailure firstNonConflictFailure(List<LoadFailure> failures) {
        for (LoadFailure failure : failures) {
            if (!LoadFailure.STAGE_CONFLICT.equals(failure.getStage())) {
                return failure;
            }
        }
        return null;
    }

    private static void closeClassLoaderQuietly(ClassLoader classLoader) {
        if (!(classLoader instanceof Closeable)) {
            return;
        }
        try {
            ((Closeable) classLoader).close();
        } catch (IOException ignored) {
            // Best effort close.
        }
    }

    /**
     * Get a factory by plugin name.
     *
     * @param pluginName the plugin name
     * @return the factory, or empty if not found
     */
    public Optional<AuthenticationPluginFactory> getFactory(String pluginName) {
        return Optional.ofNullable(factories.get(pluginName));
    }

    /**
     * Check if a factory exists for the given plugin name.
     *
     * @param pluginName the plugin name
     * @return true if factory exists
     */
    public boolean hasFactory(String pluginName) {
        return factories.containsKey(pluginName);
    }

    /**
     * Get or create plugin instance for the given integration.
     *
     * @param integration authentication integration
     * @return plugin instance
     * @throws AuthenticationException if plugin creation fails
     */
    public AuthenticationPlugin getPlugin(AuthenticationIntegration integration) throws AuthenticationException {
        Objects.requireNonNull(integration, "integration");
        AuthenticationPlugin existing = pluginByIntegration.get(integration.getName());
        if (existing != null) {
            return existing;
        }
        synchronized (pluginByIntegration) {
            AuthenticationPlugin cached = pluginByIntegration.get(integration.getName());
            if (cached != null) {
                return cached;
            }
            AuthenticationPlugin plugin = createAndInit(integration);
            pluginByIntegration.put(integration.getName(), plugin);
            return plugin;
        }
    }

    private AuthenticationPlugin createAndInit(AuthenticationIntegration integration) throws AuthenticationException {
        AuthenticationPluginFactory factory = factories.get(integration.getType());
        if (factory == null) {
            throw new AuthenticationException(
                    "No AuthenticationPluginFactory found for plugin: " + integration.getType());
        }
        AuthenticationPlugin plugin = factory.create();
        plugin.validate(integration);
        plugin.initialize(integration);
        return plugin;
    }

    /**
     * Refresh plugin instance for one integration by evicting cache and recreating.
     *
     * @param integration authentication integration
     * @throws AuthenticationException if plugin creation fails
     */
    public void reloadPlugin(AuthenticationIntegration integration) throws AuthenticationException {
        removePlugin(integration.getName());
        getPlugin(integration);
    }

    /**
     * Remove plugin instance.
     *
     * @param integrationName integration name
     */
    public void removePlugin(String integrationName) {
        AuthenticationPlugin plugin = pluginByIntegration.remove(integrationName);
        if (plugin != null) {
            plugin.close();
        }
    }

    /**
     * Get all registered plugin names.
     *
     * @return list of plugin names
     */
    public List<String> getRegisteredPluginNames() {
        return new ArrayList<>(factories.keySet());
    }

    /**
     * Get number of cached plugin instances.
     *
     * @return count of cached plugins
     */
    public int getCachedPluginCount() {
        return pluginByIntegration.size();
    }

    /**
     * Clear all cached plugins.
     */
    public void clearCache() {
        pluginByIntegration.values().forEach(AuthenticationPlugin::close);
        pluginByIntegration.clear();
    }
}
