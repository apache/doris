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

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.spi.AuthenticationException;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;
import org.apache.doris.extension.loader.ChildFirstClassLoader;
import org.apache.doris.extension.loader.PluginLoader;
import org.apache.doris.extension.spi.PluginDescriptor;
import org.apache.doris.extension.spi.PluginException;
import org.apache.doris.extension.spi.PluginFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for authentication plugins.
 *
 * <p>Handles plugin instance lifecycle: creation, caching, health checks, reloading.
 * Uses {@link ServiceLoader} for classpath plugins and {@link PluginLoader}
 * for external plugins.</p>
 *
 * <p>Design per auth.md:
 * <ul>
 *   <li>Plugins are identified by string name (not enum)</li>
 *   <li>Plugin instances are cached per integration name</li>
 *   <li>Factories are discovered via ServiceLoader</li>
 * </ul>
 */
public class PluginManager {

    /** Factories by plugin name (e.g., "ldap", "oidc", "password") */
    private final Map<String, AuthenticationPluginFactory> factories = new ConcurrentHashMap<>();

    /** Plugin instances by integration name */
    private final Map<String, AuthenticationPlugin> pluginByIntegration = new ConcurrentHashMap<>();

    /** Integration snapshots for reload detection */
    private final Map<String, AuthenticationIntegration> integrationSnapshot = new ConcurrentHashMap<>();

    /** External classloaders by plugin name */
    private final Map<String, ClassLoader> externalClassLoaders = new ConcurrentHashMap<>();

    private final PluginLoader pluginLoader;

    public PluginManager() {
        this(new PluginLoader(defaultParentFirstPackages()));
    }

    public PluginManager(PluginLoader pluginLoader) {
        this.pluginLoader = Objects.requireNonNull(pluginLoader, "pluginLoader");
        // Discover factories via ServiceLoader
        ServiceLoader.load(AuthenticationPluginFactory.class).forEach(factory ->
                factories.put(factory.name(), factory));
    }

    private static List<String> defaultParentFirstPackages() {
        List<String> packages = new ArrayList<>(ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES);
        packages.add("org.apache.doris.authentication.");
        return packages;
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
     * Register an external plugin factory by descriptor and classloader.
     *
     * @param descriptor plugin descriptor
     * @param classLoader classloader for the plugin
     * @throws AuthenticationException if registration fails
     */
    public void registerExternalFactory(PluginDescriptor descriptor,
            ClassLoader classLoader) throws AuthenticationException {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(classLoader, "classLoader");
        PluginFactory factory;
        try {
            factory = pluginLoader.loadFactory(descriptor, classLoader);
        } catch (PluginException e) {
            throw new AuthenticationException(
                    "Failed to load plugin factory: " + descriptor.getFactoryClass(), e);
        }
        if (!(factory instanceof AuthenticationPluginFactory)) {
            throw new AuthenticationException(
                    "Factory is not AuthenticationPluginFactory: " + factory.getClass().getName());
        }
        AuthenticationPluginFactory authFactory = (AuthenticationPluginFactory) factory;
        factories.put(authFactory.name(), authFactory);
        externalClassLoaders.put(authFactory.name(), classLoader);
    }

    /**
     * Convenience method to register external factory using plugin URLs.
     *
     * @param descriptor plugin descriptor
     * @param urls URLs for the plugin classloader
     * @param parent parent classloader
     * @throws AuthenticationException if registration fails
     */
    public void registerExternalFactory(PluginDescriptor descriptor, URL[] urls, ClassLoader parent)
            throws AuthenticationException {
        ClassLoader classLoader = pluginLoader.createClassLoader(urls, parent);
        registerExternalFactory(descriptor, classLoader);
    }

    /**
     * Remove a factory by plugin name and close its classloader if external.
     *
     * @param pluginName the plugin name
     */
    public void removeFactory(String pluginName) {
        factories.remove(pluginName);
        ClassLoader classLoader = externalClassLoaders.remove(pluginName);
        if (classLoader instanceof java.net.URLClassLoader) {
            try {
                ((java.net.URLClassLoader) classLoader).close();
            } catch (Exception ignored) {
                // best-effort close
            }
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
            integrationSnapshot.put(integration.getName(), integration);
            return plugin;
        }
    }

    private AuthenticationPlugin createAndInit(AuthenticationIntegration integration) throws AuthenticationException {
        AuthenticationPluginFactory factory = factories.get(integration.getPluginName());
        if (factory == null) {
            throw new AuthenticationException(
                    "No AuthenticationPluginFactory found for plugin: " + integration.getPluginName());
        }
        AuthenticationPlugin plugin = factory.create();
        plugin.validate(integration);
        plugin.initialize(integration);
        return plugin;
    }

    /**
     * Reload plugin for the given integration.
     *
     * @param integration authentication integration
     * @throws AuthenticationException if reload fails
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
        integrationSnapshot.remove(integrationName);
    }

    /**
     * Perform health check on all plugins.
     */
    public void healthCheckAll() {
        pluginByIntegration.forEach((integrationName, plugin) -> {
            AuthenticationIntegration integration = integrationSnapshot.get(integrationName);
            if (integration != null) {
                plugin.healthCheck(integration);
            }
        });
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
        integrationSnapshot.clear();
    }
}
