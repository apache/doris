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

import org.apache.doris.common.EnvUtils;

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

public class TrinoConnectorPluginLoader {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorPluginLoader.class);

    private static String pluginsDir = EnvUtils.getDorisHome() + "/connectors";

    private static class TrinoConnectorPluginLoad {
        private static FeaturesConfig featuresConfig = new FeaturesConfig();
        private static TrinoConnectorPluginManager trinoConnectorPluginManager;

        static {
            try {
                TypeOperators typeOperators = new TypeOperators();
                featuresConfig = new FeaturesConfig();
                TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);

                ServerPluginsProviderConfig serverPluginsProviderConfig = new ServerPluginsProviderConfig()
                        .setInstalledPluginsDir(new File(pluginsDir));
                ServerPluginsProvider serverPluginsProvider = new ServerPluginsProvider(serverPluginsProviderConfig,
                        MoreExecutors.directExecutor());
                HandleResolver handleResolver = new HandleResolver();
                trinoConnectorPluginManager = new TrinoConnectorPluginManager(serverPluginsProvider,
                        typeRegistry, handleResolver);
                trinoConnectorPluginManager.loadPlugins();
            } catch (Exception e) {
                LOG.warn("Failed load trino-connector plugins from  " + pluginsDir + ", Exception:" + e.getMessage());
            }
        }
    }

    // called by c++
    public static void setPluginsDir(String pluginsDir) {
        TrinoConnectorPluginLoader.pluginsDir = pluginsDir;
    }

    public static TrinoConnectorPluginManager getTrinoConnectorPluginManager() {
        return TrinoConnectorPluginLoad.trinoConnectorPluginManager;
    }

    public static FeaturesConfig getFeaturesConfig() {
        return TrinoConnectorPluginLoad.featuresConfig;
    }
}
