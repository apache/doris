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

package org.apache.doris.datasource;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorDatabaseMetadata;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;

/**
 * Generic {@link ExternalDatabase} for plugin-driven catalogs.
 *
 * <p>Provides minimal implementation that delegates table construction
 * to {@link PluginDrivenExternalTable}.</p>
 */
public class PluginDrivenExternalDatabase extends ExternalDatabase<PluginDrivenExternalTable> {

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenExternalDatabase() {
        super(null, 0, null, null, InitDatabaseLog.Type.PLUGIN);
    }

    public PluginDrivenExternalDatabase(ExternalCatalog extCatalog, long id,
            String name, String remoteName) {
        super(extCatalog, id, name, remoteName, InitDatabaseLog.Type.PLUGIN);
    }

    @Override
    protected PluginDrivenExternalTable buildTableInternal(String remoteTableName,
            String localTableName, long tblId, ExternalCatalog catalog, ExternalDatabase db) {
        // Capability gate: connectors that expose a point-in-time snapshot (e.g. Paimon) declare
        // SUPPORTS_MVCC_SNAPSHOT and get the MVCC/MTMV-capable subclass. The plain plugin connectors
        // (jdbc/es/max_compute/trino-connector) do NOT declare it and keep the base class, which has
        // no MTMV/MvccTable behavior. getConnector() forces init (makeSureInitialized) and returns the
        // built connector; the null check is a defensive fallback to the base class for a not-yet-built
        // or failed connector (post-init it is normally non-null — initLocalObjectsImpl throws on null).
        if (catalog instanceof PluginDrivenExternalCatalog) {
            Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
            if (connector != null
                    && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT)) {
                return new PluginDrivenMvccExternalTable(tblId, localTableName, remoteTableName, catalog, db);
            }
        }
        return new PluginDrivenExternalTable(tblId, localTableName, remoteTableName, catalog, db);
    }

    /**
     * The database (namespace) base location for the SHOW CREATE DATABASE {@code LOCATION '...'} clause,
     * fetched through the connector's {@code getDatabase} SPI (Trino-aligned properties-map, the
     * {@code location} key). Returns "" when the connector exposes no namespace location (the default
     * {@code getDatabase} returns an empty property map), so SHOW CREATE DATABASE renders no LOCATION for
     * connectors without a database-level location — matching their pre-flip behavior.
     */
    public String getLocation() {
        if (!(extCatalog instanceof PluginDrivenExternalCatalog)) {
            return "";
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) extCatalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return "";
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorDatabaseMetadata dbMetadata = metadata.getDatabase(session, getRemoteName());
        return dbMetadata.getProperties().getOrDefault(ConnectorDatabaseMetadata.LOCATION_PROPERTY, "");
    }
}
