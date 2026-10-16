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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorValidationContext;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Doris Connector SPI implementation that bridges to a Trino Connector.
 *
 * <p>Created once per Doris catalog lifecycle. Bootstraps a Trino Connector
 * instance and exposes its metadata through the Doris connector API.</p>
 */
public class TrinoDorisConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(TrinoDorisConnector.class);

    // Bound + validated once here: an existing TrinoDorisConnector always has usable catalog
    // properties. The heavyweight half (plugin loading, connector factory) stays in doInitialize().
    private final TrinoCatalogProperties props;
    private final ConnectorContext context;

    private volatile io.trino.spi.connector.Connector trinoConnector;
    private volatile Session trinoSession;
    private volatile CatalogHandle trinoCatalogHandle;
    private volatile ConnectorName trinoConnectorName;

    public TrinoDorisConnector(Map<String, String> properties, ConnectorContext context) {
        this.props = TrinoCatalogProperties.of(properties);
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
    public void preCreateValidation(ConnectorValidationContext context) {
        // Lift plugin loading + connector-factory resolution from first-query
        // to CREATE CATALOG time, so misconfigured plugin dir / connector name
        // surfaces immediately instead of on the first SELECT.
        ensureInitialized();
    }

    @Override
    public org.apache.doris.connector.spi.ConnectorTestResult testConnection(ConnectorSession session) {
        ensureInitialized();
        if (trinoConnector != null) {
            return org.apache.doris.connector.spi.ConnectorTestResult.success();
        }
        return org.apache.doris.connector.spi.ConnectorTestResult.failure("Trino connector not initialized");
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
     * All Trino-specific properties, with the {@code trino.} prefix stripped. Consumed by
     * {@link TrinoScanPlanProvider}, which serializes them into the BE scan payload.
     */
    public Map<String, String> getTrinoProperties() {
        return props.getTrinoProperties();
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
        // The stripped trino.* map, the connector name and its deprecated-spelling correction are all
        // the holder's, computed once when this connector was built.
        String connectorNameStr = props.getConnectorName();

        // 1. Initialize Trino plugin infrastructure (singleton).
        // The plugin dir is a deployment-level setting: this plugin's classloader cannot see FE Config
        // directly, so it arrives either in this plugin's own trino-connector.conf or, for a deployment
        // that has not moved to that file, from fe.conf through the engine environment. A catalog may
        // override it for itself.
        String pluginDir = TrinoBootstrap.resolvePluginDir(props.getPluginDirOverride(),
                TrinoConf.pluginDir(context));
        TrinoBootstrap bootstrap = TrinoBootstrap.getInstance(pluginDir);

        // 2. Create Trino Connector + Session for this catalog
        TrinoBootstrap.TrinoConnectionResult result = bootstrap.createConnection(
                context.getCatalogName(), connectorNameStr, props.getTrinoProperties());

        // Publish the guard field (trinoConnector) LAST. ensureInitialized() and the other readers use
        // `trinoConnector != null` as the initialized flag and then read trinoSession/trinoCatalogHandle.
        // Assigning the guard after its dependencies means a concurrent reader that sees it non-null is
        // guaranteed (via the volatile write/read happens-before) to also see the fully-published
        // session / catalog handle / name — closing the transient half-initialized NPE window.
        this.trinoSession = result.getSession();
        this.trinoCatalogHandle = result.getCatalogHandle();
        this.trinoConnectorName = result.getConnectorName();
        this.trinoConnector = result.getConnector();

        LOG.info("Trino connector initialized for catalog '{}', connector: {}",
                context.getCatalogName(), connectorNameStr);
    }
}
