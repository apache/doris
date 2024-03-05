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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class PluginLoader {
    private static final Logger LOG = LogManager.getLogger(PluginLoader.class);
    private FeaturesConfig featuresConfig;
    private TrinoConnectorPluginManager trinoConnectorPluginManager;

    public void loadSpiPlugins(String pluginsDir) {
        File trinoConnectorPluginsDir = new File(pluginsDir);
        if (!trinoConnectorPluginsDir.exists()) {
            LOG.warn("trino_connector_plugin_dir=" + pluginsDir + " is not found.");
            return;
        } else if (trinoConnectorPluginsDir.isFile()) {
            LOG.warn("trino_connector_plugin_dir must be a directory, not a file.");
            return;
        }

        TypeOperators typeOperators = new TypeOperators();
        this.featuresConfig = new FeaturesConfig();
        TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);

        ServerPluginsProviderConfig serverPluginsProviderConfig = new ServerPluginsProviderConfig()
                .setInstalledPluginsDir(trinoConnectorPluginsDir);
        ServerPluginsProvider serverPluginsProvider = new ServerPluginsProvider(serverPluginsProviderConfig,
                MoreExecutors.directExecutor());
        HandleResolver handleResolver = new HandleResolver();
        this.trinoConnectorPluginManager = new TrinoConnectorPluginManager(serverPluginsProvider,
                typeRegistry, handleResolver);
        trinoConnectorPluginManager.loadPlugins();
    }

    public TrinoConnectorPluginManager getTrinoConnectorPluginManager() {
        return trinoConnectorPluginManager;
    }

    public FeaturesConfig getFeaturesConfig() {
        return featuresConfig;
    }
}
