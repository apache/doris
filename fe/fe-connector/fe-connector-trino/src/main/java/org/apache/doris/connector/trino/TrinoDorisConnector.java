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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Doris Connector SPI implementation that bridges to a Trino Connector.
 *
 * <p>Created once per Doris catalog lifecycle. Bootstraps a Trino Connector
 * instance and exposes its metadata through the Doris connector API.</p>
 */
public class TrinoDorisConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(TrinoDorisConnector.class);
    private static final String TRINO_PROPERTIES_PREFIX = "trino.";

    private final Map<String, String> properties;
    private final ConnectorContext context;

    private volatile io.trino.spi.connector.Connector trinoConnector;
    private volatile Session trinoSession;
    private volatile CatalogHandle trinoCatalogHandle;
    private volatile ConnectorName trinoConnectorName;
    private volatile ImmutableMap<String, String> trinoProperties;

    public TrinoDorisConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        ensureInitialized();
        return new TrinoConnectorDorisMetadata(
                trinoConnector, trinoSession, trinoCatalogHandle);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        ensureInitialized();
        return new TrinoScanPlanProvider(this);
    }

    @Override
    public org.apache.doris.connector.api.ConnectorTestResult testConnection(ConnectorSession session) {
        ensureInitialized();
        if (trinoConnector != null) {
            return org.apache.doris.connector.api.ConnectorTestResult.success();
        }
        return org.apache.doris.connector.api.ConnectorTestResult.failure("Trino connector not initialized");
    }

    @Override
    public void close() throws IOException {
        if (trinoConnector != null) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                    trinoConnector.getClass().getClassLoader())) {
                trinoConnector.shutdown();
            }
        }
    }

    /**
     * Returns all Trino-specific properties (with "trino." prefix stripped).
     * Used by fe-core for backward compatibility (e.g., BE needs create_time key).
     */
    public Map<String, String> getTrinoProperties() {
        ensureInitialized();
        return trinoProperties;
    }

    public io.trino.spi.connector.Connector getTrinoConnector() {
        ensureInitialized();
        return trinoConnector;
    }

    public Session getTrinoSession() {
        ensureInitialized();
        return trinoSession;
    }

    public CatalogHandle getTrinoCatalogHandle() {
        ensureInitialized();
        return trinoCatalogHandle;
    }

    public ConnectorName getTrinoConnectorName() {
        ensureInitialized();
        return trinoConnectorName;
    }

    private void ensureInitialized() {
        if (trinoConnector == null) {
            synchronized (this) {
                if (trinoConnector == null) {
                    doInitialize();
                }
            }
        }
    }

    private void doInitialize() {
        // 1. Extract Trino properties (strip "trino." prefix)
        trinoProperties = ImmutableMap.copyOf(
                properties.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(TRINO_PROPERTIES_PREFIX))
                        .collect(Collectors.toMap(
                                e -> e.getKey().substring(TRINO_PROPERTIES_PREFIX.length()),
                                Map.Entry::getValue)));

        String connectorNameStr = trinoProperties.get("connector.name");
        if (connectorNameStr == null || connectorNameStr.isEmpty()) {
            throw new RuntimeException(
                    "Cannot find trino.connector.name property. "
                    + "Please specify a connector name in catalog properties.");
        }

        if (connectorNameStr.indexOf('-') >= 0) {
            String deprecated = connectorNameStr;
            connectorNameStr = connectorNameStr.replace('-', '_');
            LOG.warn("Using deprecated connector name '{}', corrected to '{}'",
                    deprecated, connectorNameStr);
        }

        // 2. Initialize Trino plugin infrastructure (singleton)
        String pluginDir = TrinoBootstrap.resolvePluginDir(properties);
        TrinoBootstrap bootstrap = TrinoBootstrap.getInstance(pluginDir);

        // 3. Create Trino Connector + Session for this catalog
        TrinoBootstrap.TrinoConnectionResult result = bootstrap.createConnection(
                context.getCatalogName(), connectorNameStr, trinoProperties);

        this.trinoConnector = result.getConnector();
        this.trinoSession = result.getSession();
        this.trinoCatalogHandle = result.getCatalogHandle();
        this.trinoConnectorName = result.getConnectorName();

        LOG.info("Trino connector initialized for catalog '{}', connector: {}",
                context.getCatalogName(), connectorNameStr);
    }
}
