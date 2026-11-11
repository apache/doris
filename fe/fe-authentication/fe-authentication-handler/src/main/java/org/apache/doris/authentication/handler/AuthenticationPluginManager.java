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
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;
import org.apache.doris.extension.loader.PluginRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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

    /** Family label in the process-wide {@link PluginRegistry}. */
    private static final String PLUGIN_FAMILY = "AUTHENTICATION";
    private static final String LEGACY_UNVERSIONED_API_VERSION = "1.0";

    /**
     * The authentication plugin API contract this FE serves. Built from the version filtered into
     * fe-authentication-spi at build time, anchored on {@link AuthenticationPluginFactory} so that it is read
     * from the very artifact carrying the SPI. A missing or malformed resource is a build defect and fails
     * class initialization loudly rather than degrading into a check that admits everything. Authentication
     * plugins shipped before the manifest contract are treated as API 1.0 for upgrade compatibility; explicit
     * malformed or incompatible declarations remain rejected.
     */
    private static final ApiVersionGate API_VERSION_GATE =
            ApiVersionGate.forFamilyAllowingUnversioned(
                    "authentication", AuthenticationPluginFactory.class, LEGACY_UNVERSIONED_API_VERSION);

    /** Factories by plugin name (e.g., "ldap", "oidc", "password") */
    private final Map<String, AuthenticationPluginFactory> factories = new ConcurrentHashMap<>();

    /**
     * Plugins the last {@link #loadAll} refused on their declared API version, newest run only.
     *
     * <p>Authentication is the one family that loads lazily and reports "no factory for this type" from a
     * different place than the load itself. Without this, an operator whose plugin was refused on its version
     * sees only "not found", with the actual reason buried in an FE log line — so the reason is kept here and
     * appended to that exception (see {@link #apiVersionRejectionHint()}).
     */
    private final List<String> apiVersionRejections = new CopyOnWriteArrayList<>();

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
                .forEach(factory -> {
                    try {
                        // Snapshot self-reported metadata before publishing the factory so
                        // one throwing implementation is rejected cleanly. The registry is
                        // first-wins on (family, name), so re-registration from another
                        // manager instance is a quiet no-op. First registration also wins
                        // here, keeping the executable factory and the inventory row the
                        // same plugin.
                        PluginRegistry.getInstance().registerBuiltin(PLUGIN_FAMILY, factory);
                        if (factories.putIfAbsent(factory.name(), factory) != null) {
                            LOG.warn("Skip duplicated built-in authentication plugin name: {}", factory.name());
                        }
                    } catch (RuntimeException e) {
                        LOG.warn("Skip built-in authentication plugin factory {}: self-reported metadata failed",
                                factory.getClass().getName(), e);
                    }
                });
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
                classLoadingPolicy,
                API_VERSION_GATE);

        apiVersionRejections.clear();
        for (LoadFailure failure : report.getFailures()) {
            LOG.warn("Skip plugin directory due to load failure: pluginDir={}, stage={}, message={}",
                    failure.getPluginDir(), failure.getStage(), failure.getMessage(), failure.getCause());
            if (LoadFailure.STAGE_API_VERSION.equals(failure.getStage())) {
                apiVersionRejections.add(failure.getMessage());
            }
        }

        int loadedPlugins = 0;
        for (PluginHandle<AuthenticationPluginFactory> handle : report.getSuccesses()) {
            String pluginName = handle.getPluginName();
            AuthenticationPluginFactory existing = factories.putIfAbsent(pluginName, handle.getFactory());
            if (existing != null) {
                // Remove the rejected handle from the runtime manager too, so its
                // classloader is not retained for the FE lifetime.
                runtimeManager.discard(pluginName);
                LOG.warn("Skip duplicated plugin name: {} from directory {}", pluginName, handle.getPluginDir());
                continue;
            }
            loadedPlugins++;
            PluginRegistry.getInstance().registerExternal(PLUGIN_FAMILY, handle);
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

    /**
     * A clause naming any plugin the last {@link #loadAll} refused on its declared API version, or the empty
     * string when there was none.
     *
     * <p>Callers append this to their "no factory for type X" exception. A plugin directory that loaded some
     * other plugin successfully does not throw from {@code loadAll} at all, so without this the version
     * rejection would never reach the user — exactly the undiagnosable case this exists to prevent.
     */
    public String apiVersionRejectionHint() {
        if (apiVersionRejections.isEmpty()) {
            return "";
        }
        return ". Note that " + apiVersionRejections.size()
                + " plugin(s) were refused on their declared API version: "
                + String.join("; ", apiVersionRejections);
    }

    private static LoadFailure firstNonConflictFailure(List<LoadFailure> failures) {
        for (LoadFailure failure : failures) {
            if (!LoadFailure.STAGE_CONFLICT.equals(failure.getStage())) {
                return failure;
            }
        }
        return null;
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

    /**
     * Create and initialize a plugin instance without caching it.
     *
     * @param integration authentication integration
     * @return initialized plugin instance
     * @throws AuthenticationException if plugin creation fails
     */
    public AuthenticationPlugin createPlugin(AuthenticationIntegration integration) throws AuthenticationException {
        Objects.requireNonNull(integration, "integration");
        return createAndInit(integration);
    }

    /**
     * Install a prepared plugin instance into the cache, replacing the old one atomically.
     *
     * @param integration authentication integration
     * @param plugin initialized plugin instance
     */
    public void installPlugin(AuthenticationIntegration integration, AuthenticationPlugin plugin) {
        Objects.requireNonNull(integration, "integration");
        Objects.requireNonNull(plugin, "plugin");
        synchronized (pluginByIntegration) {
            AuthenticationPlugin oldPlugin = pluginByIntegration.put(integration.getName(), plugin);
            if (oldPlugin != null && oldPlugin != plugin) {
                oldPlugin.close();
            }
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
