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

package org.apache.doris.trinoconnector;

import com.google.common.util.concurrent.MoreExecutors;
import io.trino.FeaturesConfig;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.TypeRegistry;
import io.trino.server.ServerPluginsProvider;
import io.trino.server.ServerPluginsProviderConfig;
import io.trino.spi.type.TypeOperators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Loads the Trino connectors installed under {@code plugins/trino_plugins}, once per process.
 *
 * <h2>A second classloader inside this one</h2>
 *
 * <p>Each installed connector gets a Trino {@code PluginClassLoader} of its own, so a Trino
 * connector is isolated from this plugin exactly as this plugin is isolated from BE - and by the
 * same construction, since BE's plugin loader was modelled on this one. Two consequences are worth
 * spelling out because they are what makes the nesting work at all:
 *
 * <ul>
 *   <li>Its parent is the platform classloader, not this plugin's loader. An installed connector
 *       therefore brings its own hadoop, its own filesystems and its own everything; nothing this
 *       plugin's directory holds is visible to it, and nothing it holds leaks back.
 *   <li>The exception is the handful of packages Trino calls its SPI - {@code io.trino.spi.} and
 *       friends - which are delegated to the classloader of this class, so a connector's handle
 *       classes and this plugin's Trino engine agree on the interfaces between them.
 * </ul>
 *
 * <p>That is why isolating this module needed no filesystem implementations of its own, unlike
 * every other plugin that reads files: this one never touches a file. It deserializes a split and
 * hands it to a connector that reads on its own terms.
 */
// Noninstancetiable utility class
public class TrinoConnectorPluginLoader {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorPluginLoader.class);

    // Set from the scan parameter BE sends before the first scan opens - see
    // TrinoConnectorScannerFactory. Read exactly once, when the holder below initializes.
    private static volatile String pluginsDir = "";

    // Suppress default constructor for noninstantiability
    private TrinoConnectorPluginLoader() {
        throw new AssertionError();
    }

    private static class TrinoConnectorPluginLoad {
        private static FeaturesConfig featuresConfig = new FeaturesConfig();
        private static TrinoConnectorPluginManager trinoConnectorPluginManager;

        static {
            String dir = pluginsDir;
            try {
                // Allow self-attachment for Java agents, which is how jol measures object sizes -
                // Trino uses it to account for the memory a page holds.
                System.setProperty("jdk.attach.allowAttachSelf", "true");
                String osName = System.getProperty("os.name").toLowerCase();
                // Skip HotSpot SAAttach for Mac/Darwin systems to avoid potential issues
                if (osName.contains("mac") || osName.contains("darwin")) {
                    System.setProperty("jol.skipHotspotSAAttach", "true");
                }

                LOG.info("TrinoConnectorPluginLoader starting to load plugins...");

                TypeOperators typeOperators = new TypeOperators();
                featuresConfig = new FeaturesConfig();
                TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);

                ServerPluginsProviderConfig serverPluginsProviderConfig = new ServerPluginsProviderConfig()
                        .setInstalledPluginsDir(new File(dir));
                ServerPluginsProvider serverPluginsProvider = new ServerPluginsProvider(serverPluginsProviderConfig,
                        MoreExecutors.directExecutor());
                HandleResolver handleResolver = new HandleResolver();
                trinoConnectorPluginManager = new TrinoConnectorPluginManager(serverPluginsProvider,
                        typeRegistry, handleResolver);
                trinoConnectorPluginManager.loadPlugins();

                LOG.info("TrinoConnectorPluginLoader successfully loaded plugins from: {}", dir);
            } catch (Exception e) {
                LOG.warn("Failed load trino-connector plugins from " + dir
                        + ", Exception:" + e.getMessage(), e);
            }
        }
    }

    /**
     * Must be called before the first scan opens; the connectors are loaded once and a later call
     * has no effect. BE sends the directory on every scan, so this is a repeated write of the same
     * value in practice.
     */
    static void setPluginsDir(String pluginsDir) {
        TrinoConnectorPluginLoader.pluginsDir = pluginsDir;
    }

    public static TrinoConnectorPluginManager getTrinoConnectorPluginManager() {
        return TrinoConnectorPluginLoad.trinoConnectorPluginManager;
    }

    public static FeaturesConfig getFeaturesConfig() {
        return TrinoConnectorPluginLoad.featuresConfig;
    }
}
