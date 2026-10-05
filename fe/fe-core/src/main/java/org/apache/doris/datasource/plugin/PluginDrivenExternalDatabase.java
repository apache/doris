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

package org.apache.doris.datasource.plugin;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorDatabaseMetadata;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;

/**
 * Generic {@link ExternalDatabase} for plugin-driven catalogs.
 *
 * <p>Provides minimal implementation that delegates table construction
 * to {@link PluginDrivenExternalTable}.</p>
 */
public class PluginDrivenExternalDatabase extends ExternalDatabase<PluginDrivenExternalTable> {

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenExternalDatabase() {
        super(null, 0, null, null);
    }

    public PluginDrivenExternalDatabase(ExternalCatalog extCatalog, long id,
            String name, String remoteName) {
        super(extCatalog, id, name, remoteName);
    }

    @Override
    protected PluginDrivenExternalTable buildTableInternal(String remoteTableName,
            String localTableName, long tblId, ExternalCatalog catalog, ExternalDatabase db) {
        // Capability gate: connectors that expose a point-in-time snapshot (e.g. Paimon) declare
        // SUPPORTS_MVCC_SNAPSHOT and get the MVCC/MTMV-capable subclass. The plain plugin connectors
        // (jdbc/es/max_compute/trino-connector) do NOT declare it and keep the base class, which has
        // no MTMV/MvccTable behavior. hasConnectorCapability forces init (makeSureInitialized) and degrades to
        // false for a not-yet-built or failed connector, falling back to the base class (post-init the
        // connector is normally non-null — initLocalObjectsImpl throws on null).
        if (catalog instanceof PluginDrivenExternalCatalog
                && ((PluginDrivenExternalCatalog) catalog)
                        .hasConnectorCapability(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT)) {
            return new PluginDrivenMvccExternalTable(tblId, localTableName, remoteTableName, catalog, db);
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
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorDatabaseMetadata dbMetadata = metadata.getDatabase(session, getRemoteName());
        return dbMetadata.getProperties().getOrDefault(ConnectorDatabaseMetadata.LOCATION_PROPERTY, "");
    }
}
