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
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

// Noninstancetiable utility class
public class TrinoConnectorPluginLoader {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorPluginLoader.class);

    private static String pluginsDir = EnvUtils.getDorisHome() + "/connectors";

    // Suppress default constructor for noninstantiability
    private TrinoConnectorPluginLoader() {
        throw new AssertionError();
    }

    private static class TrinoConnectorPluginLoad {
        private static FeaturesConfig featuresConfig = new FeaturesConfig();
        private static TrinoConnectorPluginManager trinoConnectorPluginManager;

        static {
            try {
                // Trino uses jul as its own log system, so the attributes of JUL are configured here
                System.setProperty("java.util.logging.SimpleFormatter.format",
                        "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s: %5$s%6$s%n");
                java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
                logger.setUseParentHandlers(false);
                Arrays.stream(logger.getHandlers())
                        .filter(handler -> handler instanceof ConsoleHandler)
                        .forEach(handler -> handler.setLevel(Level.OFF));
                FileHandler fileHandler = new FileHandler(EnvUtils.getDorisHome() + "/log/trinoconnector%g.log",
                        500000000, 10, true);
                fileHandler.setLevel(Level.INFO);
                fileHandler.setFormatter(new SimpleFormatter());
                logger.addHandler(fileHandler);

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
