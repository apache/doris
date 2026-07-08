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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Pins the HMS-cutover §4.4 S2 embedded-sibling holder: a flipped hms gateway lazily builds ONE embedded
 * iceberg connector (via {@link org.apache.doris.connector.spi.ConnectorContext#createSiblingConnector}) that it
 * delegates its iceberg-on-HMS tables to, and forwards {@code close()} to it.
 *
 * <p>The whole surface is dormant until hms enters {@code SPI_READY_TYPES}: no production path calls
 * {@code getOrCreateIcebergSibling()} yet, so these assertions are a Rule-9 guard that the holder's contract
 * (single sibling per gateway, hms-flavor synthesized props, fail-loud when the plugin is absent, lifecycle
 * forwarding) is correct BEFORE the flip wires a consumer.
 */
public class HiveConnectorSiblingTest {

    @Test
    public void buildsIcebergSiblingWithSynthesizedHmsFlavorProps() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("hive.metastore.uris", "thrift://host:9083");
        catalogProps.put("iceberg.catalog.type", "rest"); // a stray flavor the sibling must override to hms
        FakeSibling sibling = new FakeSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(catalogProps, context);

        Connector built = connector.getOrCreateIcebergSibling();

        Assertions.assertSame(sibling, built, "the accessor must return the context-built sibling");
        // The sibling is always an iceberg connector — the delegate type must reach the seam verbatim.
        Assertions.assertEquals("iceberg", context.lastType, "the sibling connector type must be iceberg");
        // Props are the gateway's catalog map + the hms flavor forced on (S1 synthesis), so the embedded
        // connector reaches the SAME metastore/storage and always resolves the hms flavor.
        Assertions.assertEquals("hms", context.lastProps.get("iceberg.catalog.type"),
                "the sibling must be built as the hms flavor, overriding any stray gateway value");
        Assertions.assertEquals("thrift://host:9083", context.lastProps.get("hive.metastore.uris"),
                "the gateway's metastore uri must be carried to the sibling");
    }

    @Test
    public void memoizesSingleSiblingPerGateway() {
        // The iceberg connector holds per-catalog caches (latest-snapshot / manifest / scan->write delete stash)
        // shared across its tables; a per-op sibling would fragment them. There must be exactly one build.
        RecordingSiblingContext context = new RecordingSiblingContext(new FakeSibling());
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);

        Connector first = connector.getOrCreateIcebergSibling();
        Connector second = connector.getOrCreateIcebergSibling();

        Assertions.assertSame(first, second, "repeated access must return the same single sibling");
        Assertions.assertEquals(1, context.buildCount, "the sibling must be built exactly once per gateway");
    }

    @Test
    public void failsLoudWhenIcebergPluginAbsent() {
        // A missing iceberg provider surfaces as a null sibling from the seam; the gateway must fail loud with a
        // catalog-scoped message rather than NPE deep in a later delegation.
        RecordingSiblingContext context = new RecordingSiblingContext(null);
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                connector::getOrCreateIcebergSibling);
        Assertions.assertTrue(ex.getMessage().contains("test_catalog"),
                "the failure must name the catalog it could not serve");
    }

    @Test
    public void failLoudIsNotMemoized() {
        // The absence is not cached: a plugin that becomes available (or a transient factory failure that
        // clears) must be picked up on the next access, not permanently poison the gateway.
        RecordingSiblingContext context = new RecordingSiblingContext(null);
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);
        Assertions.assertThrows(DorisConnectorException.class, connector::getOrCreateIcebergSibling);

        FakeSibling sibling = new FakeSibling();
        context.siblingToReturn = sibling;
        Assertions.assertSame(sibling, connector.getOrCreateIcebergSibling(),
                "a later-available sibling must be built after an earlier fail-loud");
        Assertions.assertEquals(2, context.buildCount, "the failed build must be retried, not memoized");
    }

    @Test
    public void closeForwardsToSiblingAndClearsIt() throws Exception {
        // The engine closes only the primary connector; the gateway owns the sibling's lifecycle, and a second
        // close must not double-close a released sibling.
        FakeSibling sibling = new FakeSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);
        connector.getOrCreateIcebergSibling();

        connector.close();
        connector.close();

        Assertions.assertEquals(1, sibling.closeCount, "close must forward to the sibling exactly once");
    }

    @Test
    public void closeIsNoOpWhenSiblingNeverBuilt() throws Exception {
        // Dormant path: a gateway that never delegated must not build a sibling just to close it.
        RecordingSiblingContext context = new RecordingSiblingContext(new FakeSibling());
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);

        connector.close();

        Assertions.assertEquals(0, context.buildCount, "close must not trigger a sibling build");
    }

    /** Records the {@code createSiblingConnector} call and returns a configurable (possibly null) sibling. */
    private static final class RecordingSiblingContext extends FakeConnectorContext {
        int buildCount;
        String lastType;
        Map<String, String> lastProps;
        Connector siblingToReturn;

        RecordingSiblingContext(Connector siblingToReturn) {
            this.siblingToReturn = siblingToReturn;
        }

        @Override
        public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
            buildCount++;
            lastType = catalogType;
            lastProps = properties;
            return siblingToReturn;
        }
    }

    /** A bare {@link Connector} stand-in for the cross-loader iceberg sibling; only close-counting matters here. */
    private static final class FakeSibling implements Connector {
        int closeCount;

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
