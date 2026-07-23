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
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Pins the HMS-cutover embedded-sibling holders: a flipped hms gateway lazily builds ONE embedded iceberg
 * connector and ONE embedded hudi connector (each via
 * {@link org.apache.doris.connector.spi.ConnectorContext#createSiblingConnector}) that it delegates its
 * iceberg-on-HMS / hudi-on-HMS tables to, and forwards {@code close()} to both.
 *
 * <p>Live since the hms flip: {@code getTableHandle} builds the iceberg/hudi siblings via
 * {@code getOrCreateIcebergSibling()} / {@code getOrCreateHudiSibling()}, so these assertions are a Rule-9
 * guard that each holder's contract (single sibling per gateway, correctly synthesized props — hms-flavor for
 * iceberg, verbatim for hudi — fail-loud when the plugin is absent, independent lifecycle forwarding) stays
 * correct.
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

    // ---- hudi sibling holder (mirrors the iceberg cases above; hudi synthesizes props verbatim, no flavor) ----

    @Test
    public void buildsHudiSiblingWithVerbatimProps() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("hive.metastore.uris", "thrift://host:9083");
        catalogProps.put("iceberg.catalog.type", "rest"); // hudi injects NO flavor: a stray key survives verbatim
        FakeSibling sibling = new FakeSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(catalogProps, context);

        Connector built = connector.getOrCreateHudiSibling();

        Assertions.assertSame(sibling, built, "the accessor must return the context-built sibling");
        // The sibling is always a hudi connector — the delegate type must reach the seam verbatim.
        Assertions.assertEquals("hudi", context.lastType, "the sibling connector type must be hudi");
        Assertions.assertEquals("thrift://host:9083", context.lastProps.get("hive.metastore.uris"),
                "the gateway's metastore uri must be carried to the sibling");
        // Unlike iceberg, hudi synthesis injects no flavor: a stray gateway key is carried through unchanged
        // (there is no iceberg.catalog.type analogue for hudi to force).
        Assertions.assertEquals("rest", context.lastProps.get("iceberg.catalog.type"),
                "hudi synthesis injects no flavor — a stray gateway key is carried verbatim, not overridden");
    }

    @Test
    public void memoizesSingleHudiSiblingPerGateway() {
        RecordingSiblingContext context = new RecordingSiblingContext(new FakeSibling());
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);

        Connector first = connector.getOrCreateHudiSibling();
        Connector second = connector.getOrCreateHudiSibling();

        Assertions.assertSame(first, second, "repeated access must return the same single sibling");
        Assertions.assertEquals(1, context.buildCount, "the sibling must be built exactly once per gateway");
    }

    @Test
    public void failsLoudWhenHudiPluginAbsent() {
        RecordingSiblingContext context = new RecordingSiblingContext(null);
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                connector::getOrCreateHudiSibling);
        Assertions.assertTrue(ex.getMessage().contains("test_catalog"),
                "the failure must name the catalog it could not serve");
        Assertions.assertTrue(ex.getMessage().contains("hudi"),
                "the failure must name the missing hudi plugin");
    }

    @Test
    public void hudiFailLoudIsNotMemoized() {
        RecordingSiblingContext context = new RecordingSiblingContext(null);
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);
        Assertions.assertThrows(DorisConnectorException.class, connector::getOrCreateHudiSibling);

        FakeSibling sibling = new FakeSibling();
        context.siblingToReturn = sibling;
        Assertions.assertSame(sibling, connector.getOrCreateHudiSibling(),
                "a later-available sibling must be built after an earlier fail-loud");
        Assertions.assertEquals(2, context.buildCount, "the failed build must be retried, not memoized");
    }

    @Test
    public void closeForwardsToHudiSiblingAndClearsIt() throws Exception {
        FakeSibling sibling = new FakeSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);
        connector.getOrCreateHudiSibling();

        connector.close();
        connector.close();

        Assertions.assertEquals(1, sibling.closeCount, "close must forward to the hudi sibling exactly once");
    }

    @Test
    public void closeForwardsToBothSiblingsIndependently() throws Exception {
        // Regression guard for adding the hudi holder: close() must forward to the iceberg AND the hudi field,
        // each exactly once — adding the hudi arm must not drop or double the iceberg arm. Use a type-dispatching
        // context so the two siblings are distinct instances.
        FakeSibling icebergSibling = new FakeSibling();
        FakeSibling hudiSibling = new FakeSibling();
        FakeConnectorContext context = new FakeConnectorContext() {
            @Override
            public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
                return "hudi".equals(catalogType) ? hudiSibling : icebergSibling;
            }
        };
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);
        connector.getOrCreateIcebergSibling();
        connector.getOrCreateHudiSibling();

        connector.close();

        Assertions.assertEquals(1, icebergSibling.closeCount, "close must forward to the iceberg sibling once");
        Assertions.assertEquals(1, hudiSibling.closeCount, "close must forward to the hudi sibling once");
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

    @Test
    public void refreshHooksForwardToBothBuiltSiblings() {
        // WHY: fe-core routes REFRESH TABLE/DATABASE/CATALOG only to a catalog's PRIMARY connector. If the
        // gateway did not forward its invalidate hooks, the iceberg sibling's latest-snapshot pin could NEVER
        // be dropped by an explicit REFRESH — and its access-based expiry keeps a continuously-queried stale
        // entry alive indefinitely, so staleness would be unbounded (breaks "bounded by TTL + REFRESH").
        FakeSibling icebergSibling = new FakeSibling();
        FakeSibling hudiSibling = new FakeSibling();
        FakeConnectorContext context = new FakeConnectorContext() {
            @Override
            public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
                return "hudi".equals(catalogType) ? hudiSibling : icebergSibling;
            }
        };
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);
        connector.getOrCreateIcebergSibling();
        connector.getOrCreateHudiSibling();

        connector.invalidateTable("db1", "t1");
        connector.invalidateDb("db2");
        connector.invalidateAll();

        for (FakeSibling sibling : new FakeSibling[] {icebergSibling, hudiSibling}) {
            Assertions.assertEquals("db1.t1", sibling.lastInvalidatedTable,
                    "invalidateTable must forward the (db, table) pair to each built sibling");
            Assertions.assertEquals("db2", sibling.lastInvalidatedDb,
                    "invalidateDb must forward the db to each built sibling");
            Assertions.assertEquals(1, sibling.invalidateAllCount,
                    "invalidateAll must forward to each built sibling exactly once");
        }
    }

    @Test
    public void refreshHooksNeverForceBuildSiblings() {
        // WHY: a REFRESH on a pure-hive catalog must not construct sibling connectors — a never-built sibling
        // has no cache to drop, and building one just to flush it would fail-loud spuriously whenever the
        // sibling plugin is not installed (REFRESH would start throwing on plain hive catalogs).
        RecordingSiblingContext context = new RecordingSiblingContext(new FakeSibling());
        HiveConnector connector = new HiveConnector(new HashMap<>(), context);

        connector.invalidateTable("db", "t");
        connector.invalidateDb("db");
        connector.invalidateAll();

        Assertions.assertEquals(0, context.buildCount,
                "invalidate hooks must only forward to ALREADY-BUILT siblings, never force-build one");
    }

    @Test
    public void icebergSiblingWithoutUserSessionCapabilityIsAccepted() {
        // The normal case: an hms-flavor sibling declares no SUPPORTS_USER_SESSION, so the fail-loud guard is
        // inert and the sibling is returned as-is.
        FakeSibling plainSibling = new FakeSibling();
        HiveConnector connector =
                new HiveConnector(new HashMap<>(), new RecordingSiblingContext(plainSibling));

        Assertions.assertSame(plainSibling, connector.getOrCreateIcebergSibling(),
                "a sibling without SUPPORTS_USER_SESSION must be accepted");
    }

    @Test
    public void icebergSiblingDeclaringUserSessionFailsLoud() {
        // Cache-isolation security invariant: the hive gateway FRONT DOOR is never session=user, so fe-core keys
        // its per-user schema/name cache bypass off the front door and would NOT bypass for a delegated sibling.
        // The iceberg sibling is forced iceberg.catalog.type=hms and can never be session=user; if a future change
        // ever broke that, building the sibling must FAIL LOUD rather than silently leak cross-user metadata.
        // MUTATION: dropping the guard -> the session=user sibling is accepted -> no throw -> red.
        FakeSibling userSessionSibling =
                new FakeSibling(EnumSet.of(ConnectorCapability.SUPPORTS_USER_SESSION));
        HiveConnector connector =
                new HiveConnector(new HashMap<>(), new RecordingSiblingContext(userSessionSibling));

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                connector::getOrCreateIcebergSibling);
        Assertions.assertTrue(ex.getMessage().contains("SUPPORTS_USER_SESSION"),
                "the failure must name the broken session=user invariant");
    }

    /**
     * A bare {@link Connector} stand-in for the cross-loader iceberg/hudi sibling; records lifecycle
     * ({@code close}) and invalidation forwarding.
     */
    private static final class FakeSibling implements Connector {
        int closeCount;
        int invalidateAllCount;
        String lastInvalidatedTable;
        String lastInvalidatedDb;
        private final Set<ConnectorCapability> capabilities;

        FakeSibling() {
            this(Collections.emptySet());
        }

        FakeSibling(Set<ConnectorCapability> capabilities) {
            this.capabilities = capabilities;
        }

        @Override
        public Set<ConnectorCapability> getCapabilities() {
            return capabilities;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            lastInvalidatedTable = dbName + "." + tableName;
        }

        @Override
        public void invalidateDb(String dbName) {
            lastInvalidatedDb = dbName;
        }

        @Override
        public void invalidateAll() {
            invalidateAllCount++;
        }
    }
}
