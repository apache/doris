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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Tests PaimonConnector's FIX-4 cache knobs (CI 973411): the {@code meta.cache.paimon.table.ttl-second}
 * mapping to the generic schema-cache TTL override (Axis B). The data-snapshot cache itself is covered by
 * {@link PaimonLatestSnapshotCacheTest}; the end-to-end behavior is gated by the docker e2e.
 */
public class PaimonConnectorCacheTest {

    private static PaimonConnector connector(Map<String, String> props) {
        return new PaimonConnector(props, new RecordingConnectorContext());
    }

    private static Map<String, String> props(String ttl) {
        Map<String, String> m = new HashMap<>();
        if (ttl != null) {
            m.put(PaimonConnector.TABLE_CACHE_TTL_SECOND, ttl);
        }
        return m;
    }

    @Test
    public void schemaTtlOverrideAbsentWhenPropertyUnset() {
        // No meta.cache.paimon.table.ttl-second -> no override -> the catalog keeps the engine-default schema
        // cache TTL (the with-cache catalog: schema is cached). MUTATION: returning a value -> red.
        Assertions.assertEquals(OptionalLong.empty(),
                connector(Collections.emptyMap()).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideZeroDisablesSchemaCache() {
        // The no-cache catalog (meta.cache.paimon.table.ttl-second=0) must drive schema.cache.ttl-second=0 so
        // its schema is served FRESH (Test 2 / L112 of test_paimon_table_meta_cache). MUTATION: not mapping
        // ttl-second -> the no-cache catalog would serve stale schema -> red.
        Assertions.assertEquals(OptionalLong.of(0L), connector(props("0")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverridePositiveIsPassedThrough() {
        Assertions.assertEquals(OptionalLong.of(3600L), connector(props("3600")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideIgnoresUnparseableValue() {
        // A malformed value must not break catalog schema caching; fall back to no override (engine default).
        Assertions.assertEquals(OptionalLong.empty(), connector(props("not-a-number")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void invalidateHooksAreNoThrowOnFreshConnector() {
        // Smoke: the REFRESH TABLE / REFRESH DATABASE / REFRESH CATALOG hooks must be safe on a fresh connector
        // (they only touch the connector-internal latest-snapshot cache + schema memo; the actual db-scoped
        // invalidate semantics are in PaimonLatestSnapshotCacheTest / PaimonSchemaAtMemoTest). invalidateDb
        // wires BOTH caches — a mutation dropping the schemaAtMemo half still passes this smoke but fails those.
        // MUTATION: an NPE on an empty cache -> red.
        PaimonConnector connector = connector(Collections.emptyMap());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db1", "t1"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db1"));
        Assertions.assertDoesNotThrow(connector::invalidateAll);
    }

    // ============ PERF-06: derived partition-view cache A (no session=user gate) + invalidation ============

    @Test
    public void partitionViewCacheAlwaysBuilt() {
        // Unlike iceberg, paimon has NO session=user / per-user credential-isolation cache-disabling
        // convention (a paimon catalog authenticates at catalog-creation time, not per-query session
        // identity), so the connector must construct cache A unconditionally on every flavor. MUTATION: a
        // stray session-like gate leaving the field null on some property combination -> red.
        Assertions.assertNotNull(connector(Collections.emptyMap()).partitionViewCacheForTest(),
                "a fresh paimon connector must always build the partition-view cache");
        Map<String, String> withTtl = props("3600");
        Assertions.assertNotNull(connector(withTtl).partitionViewCacheForTest(),
                "the partition-view cache is independent of meta.cache.paimon.table.ttl-second");
    }

    @Test
    public void refreshHooksInvalidatePartitionViewCache() {
        // The REFRESH hooks must clear cache A too (else external DDL/writes stay invisible beyond the TTL):
        // REFRESH TABLE drops that table's snapshot entries, REFRESH DATABASE that db's, REFRESH CATALOG
        // everything. Asserted via a counting loader (the framework's size() is package-private): after
        // invalidation the loader must run again. MUTATION: an invalidate* hook not routed to the view cache
        // -> the entry survives -> loader not re-run -> a loads assert below red.
        PaimonConnector connector = connector(Collections.emptyMap());
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = connector.partitionViewCacheForTest();
        Assertions.assertNotNull(cache);
        int[] loads = {0};
        Supplier<List<ConnectorPartitionInfo>> loader = () -> {
            loads[0]++;
            return Collections.emptyList();
        };
        ConnectorTableKey db1t1 = new ConnectorTableKey("db1", "t1", 1L, -1L);
        ConnectorTableKey db1t2 = new ConnectorTableKey("db1", "t2", 1L, -1L);
        ConnectorTableKey db2t1 = new ConnectorTableKey("db2", "t1", 1L, -1L);

        // REFRESH TABLE db1.t1 -> only db1.t1 re-loads.
        cache.get(db1t1, loader);
        cache.get(db1t1, loader);
        Assertions.assertEquals(1, loads[0], "second get is a hit");
        connector.invalidateTable("db1", "t1");
        cache.get(db1t1, loader);
        Assertions.assertEquals(2, loads[0], "REFRESH TABLE forces a reload of db1.t1");

        // REFRESH DATABASE db1 -> db1.t2 re-loads; db2.t1 unaffected.
        cache.get(db1t2, loader);   // loads=3 (miss)
        cache.get(db2t1, loader);   // loads=4 (miss)
        cache.get(db1t2, loader);   // hit
        cache.get(db2t1, loader);   // hit
        Assertions.assertEquals(4, loads[0]);
        connector.invalidateDb("db1");
        cache.get(db2t1, loader);   // db2 untouched -> hit
        Assertions.assertEquals(4, loads[0], "REFRESH DATABASE db1 must NOT drop db2's entries");
        cache.get(db1t2, loader);   // db1.t2 dropped -> miss
        Assertions.assertEquals(5, loads[0], "REFRESH DATABASE db1 drops db1's entries");

        // REFRESH CATALOG -> everything re-loads.
        connector.invalidateAll();
        cache.get(db2t1, loader);
        Assertions.assertEquals(6, loads[0], "REFRESH CATALOG drops everything");
    }

    // ============ paimon CachingCatalog eviction: the frozen-Table-after-out-of-band-ALTER fix ============

    @Test
    public void invalidateHooksEvictPaimonCachingCatalogFrozenTable(@TempDir java.nio.file.Path warehouse)
            throws Exception {
        // A real FileSystemCatalog wrapped by paimon's CachingCatalog (the CatalogFactory default), plus a
        // SECOND bare catalog over the same warehouse modelling the other side of the production bug: an
        // ALTER COLUMN executed on ANOTHER FE (DDL forwards to master) or by an external engine. The
        // mutation bypasses THIS CachingCatalog instance, so its alterTable self-invalidation never fires
        // and getTable keeps serving the frozen pre-ALTER FileStoreTable — which the scan path serializes
        // to the BE, where PaimonJniScanner fails with "jni reader fields' size {N} is not matched with
        // paimon fields' size {M}" on every merged-read split until the (default 24h) access-TTL.
        try (Catalog inner = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()));
                Catalog outOfBand = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Catalog caching = CachingCatalog.tryToCreate(inner, new Options());
            Assertions.assertTrue(caching instanceof CachingCatalog,
                    "precondition: the factory default wraps in CachingCatalog (cache.enabled)");
            caching.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            caching.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount());

            // Out-of-band ALTER: the caching instance still serves the frozen 1-column Table (the repro).
            outOfBand.alterTable(id, SchemaChange.addColumn("c2", DataTypes.INT()), false);
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount(),
                    "precondition: an out-of-band ALTER leaves the CachingCatalog serving the frozen Table");

            PaimonConnector connector = connector(Collections.emptyMap());
            connector.setCatalogForTest(caching);

            // REFRESH TABLE / the PluginDrivenExternalCatalog DDL hook (both route through
            // connector.invalidateTable on every FE) must evict the frozen entry. MUTATION: dropping the
            // invalidatePaimonCatalogTable call -> getTable keeps returning 1 column -> red.
            connector.invalidateTable("db1", "t1");
            Assertions.assertEquals(2, caching.getTable(id).rowType().getFieldCount(),
                    "invalidateTable must evict the paimon CachingCatalog's frozen Table");

            // REFRESH DATABASE analogue.
            outOfBand.alterTable(id, SchemaChange.addColumn("c3", DataTypes.INT()), false);
            Assertions.assertEquals(2, caching.getTable(id).rowType().getFieldCount());
            connector.invalidateDb("db1");
            Assertions.assertEquals(3, caching.getTable(id).rowType().getFieldCount(),
                    "invalidateDb must evict the database's frozen Tables");

            // REFRESH CATALOG analogue.
            outOfBand.alterTable(id, SchemaChange.addColumn("c4", DataTypes.INT()), false);
            Assertions.assertEquals(3, caching.getTable(id).rowType().getFieldCount());
            connector.invalidateAll();
            Assertions.assertEquals(4, caching.getTable(id).rowType().getFieldCount(),
                    "invalidateAll must evict every frozen Table");
        }
    }
}
