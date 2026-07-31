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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.cache.ConnectorTableKey;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * PERF-06 (S6) tests for {@link HiveConnector}'s partition-view cache A wiring: that the cache is ALWAYS built
 * (no session=user gate — hive has no per-query-identity cache-disabling convention; its caches, like
 * {@link HiveFileListingCache}, are already built unconditionally per-catalog) and that the REFRESH hooks +
 * {@code invalidatePartition} route to it.
 */
public class HiveConnectorPartitionViewCacheTest {

    private static Map<String, String> props() {
        Map<String, String> m = new HashMap<>();
        m.put("hive.metastore.uris", "thrift://host:9083");
        return m;
    }

    @Test
    public void partitionViewCacheAlwaysBuilt() {
        // Unlike iceberg, hive has NO session=user / per-user credential-isolation cache-disabling convention (a
        // hive catalog authenticates at catalog-creation time, not per-query session identity), so the connector
        // must construct cache A unconditionally. MUTATION: a stray session-like gate leaving the field null on
        // some property combination -> red.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        Assertions.assertNotNull(connector.partitionViewCacheForTest(),
                "a fresh hive connector must always build the partition-view cache");
    }

    @Test
    public void refreshHooksInvalidatePartitionViewCache() {
        // The REFRESH hooks must clear cache A too (else external DDL/writes stay invisible beyond the TTL):
        // REFRESH TABLE drops that table's entry, REFRESH DATABASE that db's, REFRESH CATALOG everything.
        // Asserted via a counting loader (the framework's size() is package-private): after invalidation the
        // loader must run again. MUTATION: an invalidate* hook not routed to the view cache -> the entry
        // survives -> loader not re-run -> a loads assert below red.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = connector.partitionViewCacheForTest();
        Assertions.assertNotNull(cache);
        int[] loads = {0};
        Supplier<List<ConnectorPartitionInfo>> loader = () -> {
            loads[0]++;
            return Collections.emptyList();
        };
        ConnectorTableKey db1t1 = new ConnectorTableKey("db1", "t1", -1L, -1L);
        ConnectorTableKey db1t2 = new ConnectorTableKey("db1", "t2", -1L, -1L);
        ConnectorTableKey db2t1 = new ConnectorTableKey("db2", "t1", -1L, -1L);

        // REFRESH TABLE db1.t1 -> only db1.t1 re-loads. Uses the public no-client hook (mirrors a never-scanned
        // catalog's REFRESH — hmsClient is null here).
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

    @Test
    public void invalidatePartitionDropsTheWholeTablesCachedView() {
        // Cache A's key carries no partition-name axis (only db/table/-1/-1), so a partition add/drop/alter
        // (HiveConnector.invalidatePartition) cannot be scoped finer than the table's single cached entry --
        // it must invalidate that whole entry. MUTATION: invalidatePartition not routed to the view cache -> the
        // stale entry survives -> the reload assert below red.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = connector.partitionViewCacheForTest();
        int[] loads = {0};
        Supplier<List<ConnectorPartitionInfo>> loader = () -> {
            loads[0]++;
            return Collections.emptyList();
        };
        ConnectorTableKey db1t1 = new ConnectorTableKey("db1", "t1", -1L, -1L);
        ConnectorTableKey db1t2 = new ConnectorTableKey("db1", "t2", -1L, -1L);

        cache.get(db1t1, loader);
        cache.get(db1t2, loader);
        Assertions.assertEquals(2, loads[0]);

        connector.invalidatePartition("db1", "t1", Collections.singletonList("dt=2024-01-01"));

        cache.get(db1t1, loader);
        Assertions.assertEquals(3, loads[0], "invalidatePartition must drop the whole cached view of that table");
        cache.get(db1t2, loader);
        Assertions.assertEquals(3, loads[0], "invalidatePartition must NOT drop another table's cached view");
    }

    @Test
    public void publicHooksAreNoThrowWithoutBuildingAClient() {
        // A fresh connector never built its metastore client (hmsClient == null). The public REFRESH hooks must
        // not force-build one just to route to the view cache, and must not throw.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db", "t"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateAll());
        Assertions.assertDoesNotThrow(() -> {
            connector.invalidatePartition("db", "t", Collections.singletonList("dt=2024-01-01"));
        });
    }
}
