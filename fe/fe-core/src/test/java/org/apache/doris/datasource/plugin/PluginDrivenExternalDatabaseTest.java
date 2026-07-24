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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorDatabaseMetadata;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.datasource.ExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Pins {@link PluginDrivenExternalDatabase#getLocation()}, the SHOW CREATE DATABASE LOCATION source for a
 * flipped iceberg (and any plugin-driven) catalog. It reads the namespace location through the connector's
 * {@code getDatabase} SPI (Trino-aligned properties-map, the {@code location} key) keyed off the
 * <b>remote</b> db name, degrading to "" (no LOCATION clause) when the connector exposes none.
 */
public class PluginDrivenExternalDatabaseTest {

    private static PluginDrivenExternalCatalog catalogReturning(ConnectorDatabaseMetadata dbMetadata) {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getDatabase(Mockito.any(), Mockito.anyString())).thenReturn(dbMetadata);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.buildCrossStatementSession()).thenReturn(session);
        return catalog;
    }

    @Test
    public void getLocationSurfacesConnectorNamespaceLocationByRemoteName() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorDatabaseMetadata.LOCATION_PROPERTY, "s3://bucket/remote_db");
        PluginDrivenExternalCatalog catalog =
                catalogReturning(new ConnectorDatabaseMetadata("remote_db", props));
        PluginDrivenExternalDatabase db =
                new PluginDrivenExternalDatabase(catalog, 1L, "db1", "remote_db");

        Assertions.assertEquals("s3://bucket/remote_db", db.getLocation(),
                "getLocation must surface the connector's namespace location property");
        // The lookup must use the REMOTE db name (the connector addresses the remote namespace).
        // MUTATION: passing the local name -> the verify fails.
        Mockito.verify(catalog.getConnector().getMetadata(null))
                .getDatabase(Mockito.any(), Mockito.eq("remote_db"));
    }

    @Test
    public void getLocationReturnsEmptyWhenNamespaceHasNoLocation() {
        // A connector with no namespace location (paimon/jdbc/es default getDatabase -> empty props) yields ""
        // so SHOW CREATE DATABASE renders no LOCATION clause. MUTATION: defaulting to a non-empty value -> red.
        PluginDrivenExternalCatalog catalog =
                catalogReturning(new ConnectorDatabaseMetadata("remote_db", Collections.emptyMap()));
        PluginDrivenExternalDatabase db =
                new PluginDrivenExternalDatabase(catalog, 1L, "db1", "remote_db");

        Assertions.assertEquals("", db.getLocation());
    }

    @Test
    public void getLocationReturnsEmptyWhenConnectorAbsent() {
        // MUTATION: dropping the null-connector guard NPEs here — a not-yet-built / read-only connector must
        // degrade to "" rather than crash SHOW CREATE DATABASE.
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(null);
        PluginDrivenExternalDatabase db =
                new PluginDrivenExternalDatabase(catalog, 1L, "db1", "remote_db");

        Assertions.assertEquals("", db.getLocation());
    }

    @Test
    public void getLocationReturnsEmptyWhenCatalogNotPluginDriven() {
        // MUTATION: dropping the instanceof guard ClassCast/NPEs — the defensive guard returns "" if the
        // owning catalog is somehow not plugin-driven.
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        PluginDrivenExternalDatabase db =
                new PluginDrivenExternalDatabase(catalog, 1L, "db1", "remote_db");

        Assertions.assertEquals("", db.getLocation());
    }
}
