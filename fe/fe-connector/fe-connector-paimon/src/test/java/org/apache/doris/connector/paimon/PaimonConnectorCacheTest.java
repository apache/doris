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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.cache.ConnectorPartitionViewCache;
import org.apache.doris.connector.cache.PartitionViewCacheKey;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = connector.partitionViewCacheForTest();
        Assertions.assertNotNull(cache);
        int[] loads = {0};
        Supplier<List<ConnectorPartitionInfo>> loader = () -> {
            loads[0]++;
            return Collections.emptyList();
        };
        PartitionViewCacheKey db1t1 = new PartitionViewCacheKey("db1", "t1", 1L, -1L);
        PartitionViewCacheKey db1t2 = new PartitionViewCacheKey("db1", "t2", 1L, -1L);
        PartitionViewCacheKey db2t1 = new PartitionViewCacheKey("db2", "t1", 1L, -1L);

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
}
