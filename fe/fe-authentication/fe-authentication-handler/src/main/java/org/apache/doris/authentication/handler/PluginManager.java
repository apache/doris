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

import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
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
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for authentication plugins.
 *
 * <p>Handles plugin instance lifecycle: creation, caching, health checks, reloading.
 * Uses {@link ServiceLoader} for classpath plugins and {@link PluginLoader}
 * for external plugins.</p>
 */
public class PluginManager {

    private final Map<AuthenticationPluginType, AuthenticationPluginFactory> factories = new ConcurrentHashMap<>();
    private final Map<String, AuthenticationPlugin> pluginByProfile = new ConcurrentHashMap<>();
    private final Map<String, AuthenticationProfile> profileSnapshot = new ConcurrentHashMap<>();
    private final Map<AuthenticationPluginType, ClassLoader> externalClassLoaders = new ConcurrentHashMap<>();
    private final PluginLoader pluginLoader;

    public PluginManager() {
        this(new PluginLoader(defaultParentFirstPackages()));
    }

    public PluginManager(PluginLoader pluginLoader) {
        this.pluginLoader = Objects.requireNonNull(pluginLoader, "pluginLoader");
        ServiceLoader.load(AuthenticationPluginFactory.class).forEach(factory ->
                factories.put(factory.pluginType(), factory));
    }

    private static List<String> defaultParentFirstPackages() {
        List<String> packages = new ArrayList<>(ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES);
        packages.add("org.apache.doris.authentication.");
        return packages;
    }

    /** Register/override a factory programmatically (useful for tests). */
    public void registerFactory(AuthenticationPluginFactory factory) {
        Objects.requireNonNull(factory, "factory");
        factories.put(factory.pluginType(), factory);
    }

    /**
     * Register an external plugin factory by descriptor and classloader.
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
        factories.put(authFactory.pluginType(), authFactory);
        externalClassLoaders.put(authFactory.pluginType(), classLoader);
    }

    /**
     * Convenience method to register external factory using plugin URLs.
     */
    public void registerExternalFactory(PluginDescriptor descriptor, URL[] urls, ClassLoader parent)
            throws AuthenticationException {
        ClassLoader classLoader = pluginLoader.createClassLoader(urls, parent);
        registerExternalFactory(descriptor, classLoader);
    }

    /**
     * Remove a factory and close its classloader if it is external.
     */
    public void removeFactory(AuthenticationPluginType type) {
        factories.remove(type);
        ClassLoader classLoader = externalClassLoaders.remove(type);
        if (classLoader instanceof java.net.URLClassLoader) {
            try {
                ((java.net.URLClassLoader) classLoader).close();
            } catch (Exception ignored) {
                // best-effort close
            }
        }
    }

    /**
     * Get or create plugin instance for the given profile.
     *
     * @param profile authentication profile
     * @return plugin instance
     * @throws AuthenticationException if plugin creation fails
     */
    public AuthenticationPlugin getPlugin(AuthenticationProfile profile) throws AuthenticationException {
        Objects.requireNonNull(profile, "profile");
        AuthenticationPlugin existing = pluginByProfile.get(profile.getName());
        if (existing != null) {
            return existing;
        }
        synchronized (pluginByProfile) {
            AuthenticationPlugin cached = pluginByProfile.get(profile.getName());
            if (cached != null) {
                return cached;
            }
            AuthenticationPlugin plugin = createAndInit(profile);
            pluginByProfile.put(profile.getName(), plugin);
            profileSnapshot.put(profile.getName(), profile);
            return plugin;
        }
    }

    private AuthenticationPlugin createAndInit(AuthenticationProfile profile) throws AuthenticationException {
        AuthenticationPluginFactory factory = factories.get(profile.getPluginType());
        if (factory == null) {
            throw new AuthenticationException(
                    "No AuthenticationPluginFactory found for type: " + profile.getPluginType());
        }
        AuthenticationPlugin plugin = factory.create();
        plugin.validate(profile);
        plugin.initialize(profile);
        return plugin;
    }

    /**
     * Reload plugin for the given profile.
     *
     * @param profile authentication profile
     * @throws AuthenticationException if reload fails
     */
    public void reloadPlugin(AuthenticationProfile profile) throws AuthenticationException {
        removePlugin(profile.getName());
        getPlugin(profile);
    }

    /**
     * Remove plugin instance.
     *
     * @param profileName profile name
     */
    public void removePlugin(String profileName) {
        AuthenticationPlugin plugin = pluginByProfile.remove(profileName);
        if (plugin != null) {
            plugin.close();
        }
        profileSnapshot.remove(profileName);
    }

    /**
     * Perform health check on all plugins.
     */
    public void healthCheckAll() {
        pluginByProfile.forEach((profileName, plugin) -> {
            AuthenticationProfile profile = profileSnapshot.get(profileName);
            if (profile != null) {
                plugin.healthCheck(profile);
            }
        });
    }
}
