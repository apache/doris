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
import org.apache.doris.authentication.spi.AuthenticationException;
import org.apache.doris.extension.loader.PluginLoader;
import org.apache.doris.extension.spi.PluginDescriptor;

import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for external authentication plugins.
 *
 * <p>This class stores plugin descriptors and provides a single entry point
 * to register them into {@link PluginManager}. IO and directory scanning are
 * intentionally out of scope.</p>
 */
public class ExternalPluginRegistry {

    private final Map<AuthenticationPluginType, ExternalPluginEntry> entries = new ConcurrentHashMap<>();
    private final PluginManager pluginManager;
    private final PluginLoader pluginLoader;

    public ExternalPluginRegistry(PluginManager pluginManager, PluginLoader pluginLoader) {
        this.pluginManager = Objects.requireNonNull(pluginManager, "pluginManager");
        this.pluginLoader = Objects.requireNonNull(pluginLoader, "pluginLoader");
    }

    /**
     * Register or replace an external plugin for the given type.
     *
     * <p>Existing plugin (if any) will be removed before the new one is registered.</p>
     */
    public void register(AuthenticationPluginType type, PluginDescriptor descriptor, URL[] urls, ClassLoader parent)
            throws AuthenticationException {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(urls, "urls");

        synchronized (entries) {
            ExternalPluginEntry existing = entries.get(type);
            if (existing != null) {
                pluginManager.removeFactory(type);
            }

            ClassLoader classLoader = pluginLoader.createClassLoader(urls, parent);
            pluginManager.registerExternalFactory(descriptor, classLoader);
            entries.put(type, new ExternalPluginEntry(descriptor, urls, classLoader));
        }
    }

    public void unregister(AuthenticationPluginType type) {
        if (type == null) {
            return;
        }
        synchronized (entries) {
            pluginManager.removeFactory(type);
            entries.remove(type);
        }
    }

    public ExternalPluginEntry get(AuthenticationPluginType type) {
        return type == null ? null : entries.get(type);
    }

    public static final class ExternalPluginEntry {
        private final PluginDescriptor descriptor;
        private final URL[] urls;
        private final ClassLoader classLoader;

        private ExternalPluginEntry(PluginDescriptor descriptor, URL[] urls, ClassLoader classLoader) {
            this.descriptor = descriptor;
            this.urls = urls;
            this.classLoader = classLoader;
        }

        public PluginDescriptor getDescriptor() {
            return descriptor;
        }

        public URL[] getUrls() {
            return urls;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
