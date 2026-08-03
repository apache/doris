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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The connector's ownership of its lake sibling: how it is built, cached, routed to and closed.
 *
 * <p>None of this touches a fluss cluster, which is also what the production code guarantees: a query that
 * never reads a lake table must not build a paimon catalog, and reaching a lake table's scan provider must
 * not open a fluss connection.
 */
public class FlussConnectorLakeSiblingTest {

    /** A lake configuration, i.e. what {@code PaimonSiblingProperties} hands the factory. */
    private static Map<String, String> lakeProperties(String warehouse) {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("warehouse", warehouse);
        return properties;
    }

    /** Records what the engine was asked to build, and answers with whatever the test staged. */
    private static final class RecordingContext implements ConnectorContext {

        private final List<String> requestedTypes = new ArrayList<>();
        private final List<Map<String, String>> requestedProperties = new ArrayList<>();
        private boolean providerAvailable = true;

        @Override
        public String getCatalogName() {
            return "fluss_catalog";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
            requestedTypes.add(catalogType);
            requestedProperties.add(properties);
            return providerAvailable ? new RecordingLakeSibling(properties) : null;
        }
    }

    private static FlussConnector connector(ConnectorContext context) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("fluss.bootstrap.servers", "127.0.0.1:9123");
        return new FlussConnector(catalogProperties, context);
    }

    @Test
    public void siblingIsBuiltAsAPaimonConnectorFromTheGivenProperties() {
        RecordingContext context = new RecordingContext();
        FlussConnector connector = connector(context);

        Map<String, String> properties = lakeProperties("/lake");
        Connector sibling = connector.getOrCreateLakeSibling(properties);

        Assertions.assertNotNull(sibling);
        // The type string is what the engine matches against the installed plugins; the fluss plugin
        // bundles no paimon class at all, so this literal is the whole contract.
        Assertions.assertEquals(Collections.singletonList("paimon"), context.requestedTypes);
        Assertions.assertEquals(properties, context.requestedProperties.get(0));
    }

    @Test
    public void oneLakeConfigurationBuildsOneSibling() {
        RecordingContext context = new RecordingContext();
        FlussConnector connector = connector(context);

        Connector first = connector.getOrCreateLakeSibling(lakeProperties("/lake"));
        Connector second = connector.getOrCreateLakeSibling(lakeProperties("/lake"));

        // Equal-by-value properties, not the same map instance: every statement synthesizes a fresh map
        // from the table's properties, so an identity-keyed cache would build a paimon catalog per query.
        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, context.requestedTypes.size());
    }

    @Test
    public void secondLakeConfigurationFailsLoud() {
        RecordingContext context = new RecordingContext();
        FlussConnector connector = connector(context);
        connector.getOrCreateLakeSibling(lakeProperties("/lake"));

        // Two paimon siblings answer "is this handle yours?" with the same class test, so the second could
        // never be routed apart from the first — a table would silently read the wrong warehouse. Refusing
        // is the only honest answer; the message points at the fix (refresh the catalog).
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getOrCreateLakeSibling(lakeProperties("/other")));
        Assertions.assertTrue(failure.getMessage().contains("refresh the catalog"), failure.getMessage());
        // Storage credentials live in these maps; a message that dumps them ends up in the FE audit log.
        Assertions.assertFalse(failure.getMessage().contains("/other"), failure.getMessage());
        Assertions.assertEquals(1, context.requestedTypes.size(), "the refused one must not be built");
    }

    @Test
    public void missingPaimonPluginFailsLoudAndIsNotCached() {
        RecordingContext context = new RecordingContext();
        context.providerAvailable = false;
        FlussConnector connector = connector(context);

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getOrCreateLakeSibling(lakeProperties("/lake")));
        Assertions.assertTrue(failure.getMessage().contains("paimon"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("fluss_catalog"), failure.getMessage());

        // Not memoizing the failure is what lets a later-installed plugin recover without a catalog
        // refresh; a cached null would keep answering "not available" forever.
        context.providerAvailable = true;
        Assertions.assertNotNull(connector.getOrCreateLakeSibling(lakeProperties("/lake")));
    }

    @Test
    public void lakeHandleIsPlannedByTheSibling() {
        RecordingContext context = new RecordingContext();
        FlussConnector connector = connector(context);
        RecordingLakeSibling sibling =
                (RecordingLakeSibling) connector.getOrCreateLakeSibling(lakeProperties("/lake"));

        // Routing is by handle OWNERSHIP, asked of the sibling — the fluss side cannot recognize a foreign
        // handle by class across the plugin split, so "is it yours?" is the only question available.
        Assertions.assertSame(sibling.scanPlanProvider,
                connector.getScanPlanProvider(new RecordingLakeSibling.Handle("db", "t")));
    }

    @Test
    public void onlyFlussHandlesAreOwnedByThisConnector() {
        RecordingContext context = new RecordingContext();
        FlussConnector connector = connector(context);

        // The engine asks this to route a handle back to the connector that made it. Claiming a sibling's
        // handle would make the engine plan a paimon table with fluss's planner.
        Assertions.assertFalse(connector.ownsHandle(new RecordingLakeSibling.Handle("db", "t")));
    }

    @Test
    public void closingTheConnectorClosesTheSibling() throws IOException {
        RecordingContext context = new RecordingContext();
        FlussConnector connector = connector(context);
        RecordingLakeSibling sibling =
                (RecordingLakeSibling) connector.getOrCreateLakeSibling(lakeProperties("/lake"));

        connector.close();

        Assertions.assertTrue(sibling.closed);
        // And the reference must be dropped, or a reopened catalog would keep routing to a closed sibling.
        // A now-different lake configuration being accepted is what proves the slot is free again.
        Assertions.assertNotSame(sibling, connector.getOrCreateLakeSibling(lakeProperties("/other")));
    }
}
