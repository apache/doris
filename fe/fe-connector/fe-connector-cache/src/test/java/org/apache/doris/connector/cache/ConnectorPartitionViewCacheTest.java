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

package org.apache.doris.connector.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link ConnectorPartitionViewCache} — the GENERIC (engine-agnostic) cache A of the
 * external-partition-derived-cache design (design doc {@code 2026-07-20-external-partition-derived-cache-design.md}
 * §5). Mirrors {@link org.apache.doris.connector.iceberg.IcebergPartitionCache}'s test shape (that class is the
 * iceberg-specific ancestor this generic framework is modeled on), but with a generic string value type ({@code V})
 * since this module must not depend on any engine-specific type. Covers: miss-then-hit, distinct-snapshot keying,
 * per-table / per-db / whole-cache invalidation, and the disabled-cache bypass (both {@code enable=false} and
 * {@code ttl-second=0}).
 */
public class ConnectorPartitionViewCacheTest {

    private static final String ENGINE = "testengine";

    private static PartitionViewCacheKey key(String db, String table, long snapshotId, long schemaId) {
        return new PartitionViewCacheKey(db, table, snapshotId, schemaId);
    }

    private static ConnectorPartitionViewCache<String> newCache() {
        return new ConnectorPartitionViewCache<>(ENGINE, new HashMap<>());
    }

    @Test
    public void missThenHitLoaderRunsOnceWithinTtl() {
        AtomicInteger loads = new AtomicInteger();
        ConnectorPartitionViewCache<String> cache = newCache();

        String first = cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "view-v1";
        });
        // Second read of the SAME key must be served from cache, NOT re-invoke the loader -- this is the whole
        // point of memoizing the expensive partition enumeration across queries. MUTATION: always calling the
        // loader -> second != first / loads == 2 -> red.
        String second = cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "view-v2";
        });

        Assertions.assertEquals("view-v1", first);
        Assertions.assertEquals("view-v1", second, "within TTL the cached value must be served, not a fresh load");
        Assertions.assertEquals(1, loads.get(), "the loader must run exactly once for the same key");
        Assertions.assertTrue(cache.isEnabled());
    }

    @Test
    public void differentSnapshotIdIsADistinctEntry() {
        AtomicInteger loads = new AtomicInteger();
        ConnectorPartitionViewCache<String> cache = newCache();

        cache.get(key("db", "t", 1L, 1L), () -> {
            loads.incrementAndGet();
            return "snap-1";
        });
        // A new commit yields a new snapshotId -> a distinct key -> the loader must run again. MUTATION: keying
        // by (db, table) only (ignoring snapshotId/schemaId) -> loads stays 1 / serves the stale snap-1 value.
        String second = cache.get(key("db", "t", 2L, 1L), () -> {
            loads.incrementAndGet();
            return "snap-2";
        });

        Assertions.assertEquals("snap-2", second);
        Assertions.assertEquals(2, loads.get(), "distinct snapshotId must trigger a distinct load");
    }

    @Test
    public void differentSchemaIdIsADistinctEntry() {
        AtomicInteger loads = new AtomicInteger();
        ConnectorPartitionViewCache<String> cache = newCache();

        cache.get(key("db", "t", 1L, 1L), () -> {
            loads.incrementAndGet();
            return "schema-1";
        });
        // schemaId is part of the key too (paimon evolves schema independently of snapshot). MUTATION: dropping
        // schemaId from equals/hashCode -> this second call would incorrectly hit -> loads stays 1.
        String second = cache.get(key("db", "t", 1L, 2L), () -> {
            loads.incrementAndGet();
            return "schema-2";
        });

        Assertions.assertEquals("schema-2", second);
        Assertions.assertEquals(2, loads.get(), "distinct schemaId must trigger a distinct load");
    }

    @Test
    public void invalidateTableEvictsAllSnapshotsOfThatTableOnly() {
        AtomicInteger loads = new AtomicInteger();
        ConnectorPartitionViewCache<String> cache = newCache();

        cache.get(key("db", "t", 1L, 1L), () -> "v1");
        cache.get(key("db", "t", 2L, 1L), () -> "v2");
        cache.get(key("db", "other", 1L, 1L), () -> "v3");

        // REFRESH TABLE db.t must drop BOTH snapshot entries of db.t and leave db.other intact. MUTATION:
        // invalidating a single (db, table, snapshotId) key instead of every snapshot of the table -> the
        // reload below would still hit the stale "v1".
        cache.invalidateTable("db", "t");

        String reload = cache.get(key("db", "t", 1L, 1L), () -> {
            loads.incrementAndGet();
            return "v1-reloaded";
        });
        Assertions.assertEquals("v1-reloaded", reload, "invalidateTable must force a fresh load for db.t");
        Assertions.assertEquals(1, loads.get());

        // db.other must be untouched by the db.t invalidation.
        AtomicInteger otherLoads = new AtomicInteger();
        String other = cache.get(key("db", "other", 1L, 1L), () -> {
            otherLoads.incrementAndGet();
            return "v3-should-not-reload";
        });
        Assertions.assertEquals("v3", other, "invalidateTable(db, t) must not touch db.other");
        Assertions.assertEquals(0, otherLoads.get());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        AtomicInteger loads = new AtomicInteger();
        ConnectorPartitionViewCache<String> cache = newCache();

        cache.get(key("db1", "t1", 1L, 1L), () -> "a");
        cache.get(key("db1", "t2", 1L, 1L), () -> "b");
        cache.get(key("db2", "t1", 1L, 1L), () -> "c");

        // REFRESH DATABASE db1 must drop every table of db1 and leave db2 intact. MUTATION: invalidateDb acting
        // like invalidateAll (or a no-op) -> either db2 also reloads, or db1 does not.
        cache.invalidateDb("db1");

        String reloadedT1 = cache.get(key("db1", "t1", 1L, 1L), () -> {
            loads.incrementAndGet();
            return "a-reloaded";
        });
        Assertions.assertEquals("a-reloaded", reloadedT1);
        Assertions.assertEquals(1, loads.get());

        AtomicInteger db2Loads = new AtomicInteger();
        String db2Value = cache.get(key("db2", "t1", 1L, 1L), () -> {
            db2Loads.incrementAndGet();
            return "c-should-not-reload";
        });
        Assertions.assertEquals("c", db2Value, "invalidateDb(db1) must not touch db2");
        Assertions.assertEquals(0, db2Loads.get());
    }

    @Test
    public void invalidateAllClearsEveryEntry() {
        AtomicInteger loads = new AtomicInteger();
        ConnectorPartitionViewCache<String> cache = newCache();

        cache.get(key("db1", "t1", 1L, 1L), () -> "a");
        cache.get(key("db2", "t2", 1L, 1L), () -> "b");

        cache.invalidateAll();

        String reloaded = cache.get(key("db1", "t1", 1L, 1L), () -> {
            loads.incrementAndGet();
            return "a-reloaded";
        });
        Assertions.assertEquals("a-reloaded", reloaded, "invalidateAll must drop every entry");
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void enableFalseDisablesCacheAlwaysLoads() {
        AtomicInteger loads = new AtomicInteger();
        Map<String, String> props = new HashMap<>();
        props.put("meta.cache." + ENGINE + ".partition_view.enable", "false");
        ConnectorPartitionViewCache<String> cache = new ConnectorPartitionViewCache<>(ENGINE, props);

        String first = cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "v1";
        });
        // enable=false must bypass the cache entirely -- every call re-invokes the loader. MUTATION: ignoring the
        // enable=false property -> second call would return "v1" (cached) / loads would stay 1 -> red.
        String second = cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "v2";
        });

        Assertions.assertEquals("v1", first);
        Assertions.assertEquals("v2", second, "enable=false must call the loader on every get");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(cache.isEnabled());
    }

    @Test
    public void ttlZeroDisablesCacheAlwaysLoads() {
        AtomicInteger loads = new AtomicInteger();
        Map<String, String> props = new HashMap<>();
        props.put("meta.cache." + ENGINE + ".partition_view.ttl-second", "0");
        ConnectorPartitionViewCache<String> cache = new ConnectorPartitionViewCache<>(ENGINE, props);

        cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "v1";
        });
        String second = cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "v2";
        });

        // ttl-second=0 is CacheSpec's explicit disable signal (distinct from -1 = "no expiration"). MUTATION:
        // treating ttl=0 as "no expiration" instead of "disabled" -> second call would return the cached "v1".
        Assertions.assertEquals("v2", second, "ttl-second=0 must always load live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(cache.isEnabled());
    }

    @Test
    public void capacityZeroDisablesCacheAlwaysLoads() {
        AtomicInteger loads = new AtomicInteger();
        Map<String, String> props = new HashMap<>();
        props.put("meta.cache." + ENGINE + ".partition_view.capacity", "0");
        ConnectorPartitionViewCache<String> cache = new ConnectorPartitionViewCache<>(ENGINE, props);

        cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "v1";
        });
        String second = cache.get(key("db", "t", 5L, 1L), () -> {
            loads.incrementAndGet();
            return "v2";
        });

        Assertions.assertEquals("v2", second, "capacity=0 must always load live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(cache.isEnabled());
    }

    @Test
    public void defaultsAreEnabledWithNoProperties() {
        // No meta.cache.<engine>.partition_view.* properties set at all -> the built-in default (TTL 86400s,
        // capacity 1000, ON) applies, matching IcebergPartitionCache's DEFAULT_TABLE_CACHE_CAPACITY. MUTATION:
        // defaulting to disabled -> isEnabled() would be false here.
        ConnectorPartitionViewCache<String> cache = new ConnectorPartitionViewCache<>(ENGINE, new HashMap<>());
        Assertions.assertTrue(cache.isEnabled(), "the cache must be ON by default with no override properties");
    }

    @Test
    public void loaderExceptionPropagatesUnwrappedAndIsNotCached() {
        ConnectorPartitionViewCache<String> cache = newCache();
        // A failed load (e.g. a remote enumeration RPC failure) must propagate to the caller verbatim and must
        // NOT poison the cache -- a transient failure should not make every subsequent query fail for the TTL.
        Assertions.assertThrows(IllegalStateException.class, () -> cache.get(key("db", "t", 5L, 1L), () -> {
            throw new IllegalStateException("boom");
        }));

        String recovered = cache.get(key("db", "t", 5L, 1L), () -> "recovered");
        Assertions.assertEquals("recovered", recovered, "a failed load must not be cached");
    }
}
