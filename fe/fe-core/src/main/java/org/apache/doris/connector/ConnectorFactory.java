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

package org.apache.doris.connector;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Static factory providing access to the {@link ConnectorPluginManager}.
 *
 * <p>Call {@link #initPluginManager(ConnectorPluginManager)} at FE startup before any
 * {@code createConnector()} call. In production, providers are loaded from the plugin
 * directory configured via {@code connector_plugin_root}. In unit tests, providers are
 * discovered from the test classpath via ServiceLoader.
 *
 * <p>Returns {@code null} from {@link #createConnector} when no provider matches,
 * allowing fe-core to gracefully fall back to the existing hardcoded CatalogFactory
 * switch-case during the migration period.
 */
public final class ConnectorFactory {

    private static final Logger LOG = LogManager.getLogger(ConnectorFactory.class);

    private static volatile ConnectorPluginManager pluginManager;

    private ConnectorFactory() {}

    /**
     * Sets the plugin manager singleton. Called once at FE startup.
     *
     * @param manager the initialized ConnectorPluginManager
     */
    public static void initPluginManager(ConnectorPluginManager manager) {
        pluginManager = manager;
    }

    /**
     * Creates a connector for the given catalog type.
     * Returns {@code null} if no provider matches (fe-core can fall back to built-in logic).
     *
     * @param catalogType the catalog type (e.g. "hive", "iceberg", "es")
     * @param properties  catalog configuration properties
     * @param context     runtime context provided by fe-core
     * @return a ready-to-use Connector, or {@code null} if no provider matches
     */
    public static Connector createConnector(
            String catalogType, Map<String, String> properties, ConnectorContext context) {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr == null) {
            LOG.debug("ConnectorPluginManager not initialized, returning null for type: {}",
                    catalogType);
            return null;
        }
        return mgr.createConnector(catalogType, properties, context);
    }

    /** Returns true if the plugin manager has been initialized. */
    public static boolean isInitialized() {
        return pluginManager != null;
    }

    /** Returns the set of connector types registered in the plugin manager. */
    public static java.util.List<String> getRegisteredTypes() {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr == null) {
            return java.util.Collections.emptyList();
        }
        return mgr.getRegisteredTypes();
    }

    /**
     * Validates catalog properties using the matching provider.
     * Does nothing if no provider matches or plugin manager is not initialized.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateProperties(
            String catalogType, Map<String, String> properties) {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr != null) {
            mgr.validateProperties(catalogType, properties);
        }
    }

    /** For testing only. */
    static void clearPluginManager() {
        pluginManager = null;
    }
}
