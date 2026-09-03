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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    /**
     * Creates a connector to back a standalone catalog. Same as {@link #createConnector} except that a
     * sibling-only connector (one declaring {@code isStandaloneCatalogType() == false}) is not eligible.
     * Use this on every path that builds a catalog; use {@link #createConnector} for sibling lookup.
     *
     * @return a ready-to-use Connector, or {@code null} if no provider claims the type as a standalone catalog
     */
    public static Connector createStandaloneCatalogConnector(
            String catalogType, Map<String, String> properties, ConnectorContext context) {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr == null) {
            LOG.debug("ConnectorPluginManager not initialized, returning null for type: {}",
                    catalogType);
            return null;
        }
        return mgr.createStandaloneCatalogConnector(catalogType, properties, context);
    }

    /**
     * Finds the provider that would back a catalog of this type, without creating (and therefore without
     * initializing) a connector. Empty when the plugin manager is not initialized yet or no provider matches.
     *
     * @see ConnectorPluginManager#findProvider
     */
    public static Optional<ConnectorProvider> findProvider(
            String catalogType, Map<String, String> properties) {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr == null) {
            return Optional.empty();
        }
        return mgr.findProvider(catalogType, properties);
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

    /** Returns the registered types that can be named by {@code CREATE CATALOG}, sorted. */
    public static java.util.List<String> getStandaloneCatalogTypes() {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr == null) {
            return java.util.Collections.emptyList();
        }
        return mgr.getStandaloneCatalogTypes();
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

    /** Validates an ALTER candidate through the matching connector provider. */
    public static void validatePropertiesForUpdate(String catalogType,
            Map<String, String> currentProperties, Map<String, String> updatedProperties) {
        ConnectorPluginManager mgr = pluginManager;
        if (mgr != null) {
            mgr.validatePropertiesForUpdate(catalogType, currentProperties, updatedProperties);
        }
    }

    /**
     * The driver jar URLs the matching connector would load into the FE JVM for these properties, so the
     * caller can apply the operator's {@code jdbc_driver_secure_path} / {@code jdbc_driver_url_white_list}
     * gate to them. Empty when no provider matches or the connector loads no driver jar.
     */
    public static List<String> driverUrlsToValidate(String catalogType, Map<String, String> properties) {
        ConnectorPluginManager mgr = pluginManager;
        return mgr == null ? Collections.emptyList()
                : mgr.driverUrlsToValidate(catalogType, properties);
    }

    /** For testing only. */
    static void clearPluginManager() {
        pluginManager = null;
    }
}
