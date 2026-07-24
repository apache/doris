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
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins {@link PluginDrivenExternalCatalog#listTableNamesFromRemote}'s view re-merge. A view-exposing
 * connector (iceberg) subtracts view names from {@code listTableNames}, so the catalog must merge the
 * connector's {@code listViewNames} back into {@code SHOW TABLES} — byte-faithful to legacy
 * {@code IcebergExternalCatalog.listTableNamesFromRemote}. A view-less connector (jdbc/es) must skip the
 * merge entirely (no {@code listViewNames} round-trip) and return the table list verbatim.
 */
public class PluginDrivenExternalCatalogViewListingTest {

    private Connector connector;
    private ConnectorMetadata metadata;
    private TestableCatalog catalog;

    @BeforeEach
    public void setUp() {
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        catalog = new TestableCatalog(connector, session);
    }

    @Test
    public void mergesViewNamesWhenConnectorSupportsView() {
        Mockito.when(connector.getCapabilities())
                .thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW));
        Mockito.when(metadata.listTableNames(Mockito.any(), Mockito.eq("db1")))
                .thenReturn(Arrays.asList("t1", "t2"));
        Mockito.when(metadata.listViewNames(Mockito.any(), Mockito.eq("db1")))
                .thenReturn(Arrays.asList("v1", "v2"));

        List<String> result = catalog.listTableNamesFromRemote(null, "db1");

        // WHY: legacy IcebergExternalCatalog re-merges views into SHOW TABLES (its listTableNames subtracts
        // them). MUTATION: dropping the merge / the capability gate -> views vanish from SHOW TABLES -> red.
        Assertions.assertEquals(Arrays.asList("t1", "t2", "v1", "v2"), result);
    }

    @Test
    public void skipsMergeAndViewRoundTripWhenConnectorIsViewLess() {
        Mockito.when(connector.getCapabilities())
                .thenReturn(EnumSet.noneOf(ConnectorCapability.class));
        Mockito.when(metadata.listTableNames(Mockito.any(), Mockito.eq("db1")))
                .thenReturn(Arrays.asList("t1", "t2"));

        List<String> result = catalog.listTableNamesFromRemote(null, "db1");

        // WHY: jdbc/es have no views; the capability gate must skip both the merge AND the listViewNames
        // round-trip, returning the table list verbatim. MUTATION: dropping the gate -> an extra
        // listViewNames call (caught by verify(never)) and a needless merge -> red.
        Assertions.assertEquals(Arrays.asList("t1", "t2"), result);
        Mockito.verify(metadata, Mockito.never()).listViewNames(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void returnsTableListWhenViewCapableButNoViews() {
        Mockito.when(connector.getCapabilities())
                .thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW));
        Mockito.when(metadata.listTableNames(Mockito.any(), Mockito.eq("db1")))
                .thenReturn(Arrays.asList("t1"));
        Mockito.when(metadata.listViewNames(Mockito.any(), Mockito.eq("db1")))
                .thenReturn(Collections.emptyList());

        List<String> result = catalog.listTableNamesFromRemote(null, "db1");

        // An empty view set yields exactly the table list (the merge is a no-op).
        Assertions.assertEquals(Arrays.asList("t1"), result);
    }

    /** A PluginDrivenExternalCatalog wired to a mock connector, skipping the real local-object/auth setup. */
    private static final class TestableCatalog extends PluginDrivenExternalCatalog {
        private final ConnectorSession sessionMock;

        TestableCatalog(Connector initial, ConnectorSession session) {
            super(1L, "test-catalog", null, testProps(), "", initial);
            this.sessionMock = session;
            this.initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // no-op: connector is injected via the constructor.
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return sessionMock;
        }

        private static Map<String, String> testProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "test");
            return props;
        }
    }
}
