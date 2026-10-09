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

import org.apache.doris.connector.cache.ScopedMetaCache.BulkLoadHandle;

import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ScopedMetaCacheLeakTest {
    private static final CacheSpec ENABLED =
            CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 10_000L);
    private static final ScopePath TABLE = ScopePath.table("db", "tbl");
    private static final ScopePath PARTITION = ScopePath.partition("db", "tbl", "p=1");

    @Test
    public void explicitInvalidationReclaimsEveryPossibleLeafLevel() {
        List<ScopePath> scopes = Arrays.asList(
                ScopePath.catalog(),
                ScopePath.database("db"),
                TABLE,
                PARTITION);
        for (ScopePath scope : scopes) {
            try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
                ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
                cache.put("key", scope, "value");
                Assertions.assertEquals(1, registry.metrics().getRegistrationCount());

                registry.invalidate(scope);
                cache.cleanUp();

                assertEmpty(registry, cache);
            }
        }
    }

    @Test
    public void subtreeInvalidationReclaimsOnlyItsDatabaseTableAndPartitionNodes() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            for (int table = 0; table < 4; table++) {
                for (int partition = 0; partition < 5; partition++) {
                    String key = "db1-t" + table + "-p" + partition;
                    cache.put(
                            key,
                            ScopePath.partition("db1", "t" + table, "p=" + partition),
                            key);
                }
            }
            cache.put("db2", ScopePath.partition("db2", "t", "p=1"), "retained");

            registry.invalidate(ScopePath.database("db1"));
            cache.cleanUp();

            ScopedMetaCacheRegistry.ScopeMetrics metrics = registry.metrics();
            Assertions.assertEquals(1, metrics.getDatabaseNodeCount());
            Assertions.assertEquals(1, metrics.getTableNodeCount());
            Assertions.assertEquals(1, metrics.getPartitionNodeCount());
            Assertions.assertEquals(1, metrics.getRegistrationCount());
            Assertions.assertEquals("retained", cache.getIfPresent(
                    "db2", ScopePath.partition("db2", "t", "p=1")));
            Assertions.assertEquals(1L, cache.metrics().getPhysicalEntryCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        }
    }

    @Test
    public void exactKeyInvalidationPrunesLastScopePathAndKeyNode() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            cache.put("key", PARTITION, "value");

            cache.invalidateKey("key");
            cache.cleanUp();

            assertEmpty(registry, cache);
        }
    }

    @Test
    public void capacityEvictionReclaimsRegistrationsKeyNodesAndScopeNodes() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<Integer, Integer> cache = registry.createCache(
                    "test", CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 3L));
            for (int i = 0; i < 100; i++) {
                cache.put(i, ScopePath.partition("db-" + i, "tbl-" + i, "p=" + i), i);
            }
            cache.cleanUp();

            ScopedMetaCacheRegistry.ScopeMetrics metrics = registry.metrics();
            long physical = cache.metrics().getPhysicalEntryCount();
            Assertions.assertTrue(physical <= 3L);
            Assertions.assertEquals(physical, metrics.getRegistrationCount());
            Assertions.assertEquals(physical, cache.metrics().getKeyNodeCount());
            Assertions.assertEquals(physical, metrics.getDatabaseNodeCount());
            Assertions.assertEquals(physical, metrics.getTableNodeCount());
            Assertions.assertEquals(physical, metrics.getPartitionNodeCount());

            registry.invalidate(ScopePath.catalog());
            cache.cleanUp();
            assertEmpty(registry, cache);
        }
    }

    @Test
    public void expirationReclaimsRegistrationsKeyNodesAndScopeNodes() {
        AtomicLong nowNanos = new AtomicLong();
        Ticker ticker = nowNanos::get;
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test", CacheSpec.of(true, 1L, 10L), ticker, null);
            cache.put("key", PARTITION, "value");
            nowNanos.set(java.util.concurrent.TimeUnit.SECONDS.toNanos(2L));
            cache.cleanUp();

            assertEmpty(registry, cache);
        }
    }

    @Test
    public void repeatedReplacementNeverAccumulatesRegistrations() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, Integer> cache = registry.createCache("test", ENABLED);
            for (int i = 0; i < 10_000; i++) {
                cache.put("key", PARTITION, i);
            }
            cache.cleanUp();

            Assertions.assertEquals(1L, cache.metrics().getPhysicalEntryCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, registry.metrics().getDatabaseNodeCount());
            Assertions.assertEquals(1, registry.metrics().getTableNodeCount());
            Assertions.assertEquals(1, registry.metrics().getPartitionNodeCount());
        }
    }

    @Test
    public void highCardinalityChurnRemainsBoundedByLiveEntries() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<Integer, Integer> cache = registry.createCache(
                    "test", CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 32L));
            for (int i = 0; i < 20_000; i++) {
                cache.put(i, ScopePath.partition("db-" + i, "tbl-" + i, "p=" + i), i);
                if ((i & 255) == 0) {
                    cache.cleanUp();
                }
            }
            cache.cleanUp();

            long physical = cache.metrics().getPhysicalEntryCount();
            ScopedMetaCacheRegistry.ScopeMetrics metrics = registry.metrics();
            Assertions.assertTrue(physical <= 32L);
            Assertions.assertEquals(physical, cache.metrics().getKeyNodeCount());
            Assertions.assertEquals(physical, metrics.getRegistrationCount());
            Assertions.assertEquals(physical, metrics.getDatabaseNodeCount());
            Assertions.assertEquals(physical, metrics.getTableNodeCount());
            Assertions.assertEquals(physical, metrics.getPartitionNodeCount());
        }
    }

    @Test
    public void bulkHandleWithoutPublicationReleasesAllAllocatedNodes() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle handle = cache.beginBulkLoad(TABLE);
            Assertions.assertEquals(1, registry.metrics().getDatabaseNodeCount());
            Assertions.assertEquals(1, registry.metrics().getTableNodeCount());
            Assertions.assertEquals(3, registry.metrics().getActiveLoadCount());

            handle.close();

            assertEmpty(registry, cache);
        }
    }

    @Test
    public void invalidationDuringActiveLeaseReclaimsAfterLeaseRelease() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle handle = cache.beginBulkLoad(TABLE);
            registry.invalidate(TABLE);
            Assertions.assertEquals(3, registry.metrics().getActiveLoadCount());

            Assertions.assertFalse(cache.publish(handle, "key", PARTITION, "stale"));
            handle.close();

            assertEmpty(registry, cache);
        }
    }

    @Test
    public void detachedScopeLeasesRemainObservableUntilHandleRelease() {
        List<ScopePath> invalidations = Arrays.asList(
                ScopePath.catalog(),
                ScopePath.database("db"),
                TABLE,
                PARTITION);
        for (ScopePath invalidation : invalidations) {
            try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
                ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
                BulkLoadHandle handle = cache.beginBulkLoad(PARTITION);

                registry.invalidate(invalidation);

                ScopedMetaCacheRegistry.ScopeMetrics metrics = registry.metrics();
                Assertions.assertEquals(1, metrics.getActiveCatalogLoadCount());
                Assertions.assertEquals(1, metrics.getActiveDatabaseLoadCount());
                Assertions.assertEquals(1, metrics.getActiveTableLoadCount());
                Assertions.assertEquals(1, metrics.getActivePartitionLoadCount());
                Assertions.assertEquals(4, metrics.getActiveLoadCount());
                Assertions.assertEquals(1, cache.metrics().getActiveBulkHandleCount());

                handle.close();
                assertEmpty(registry, cache);
            }
        }
    }

    @Test
    public void invalidatingNeverSeenScopesDoesNotCreateDirectoryNodes() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            for (int i = 0; i < 10_000; i++) {
                registry.invalidate(ScopePath.partition("db-" + i, "tbl-" + i, "p=" + i));
            }

            assertEmpty(registry, cache);
        }
    }

    @Test
    public void closingOnePhysicalCachePreservesSharedScopeForOtherCache() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> first = registry.createCache("first", ENABLED);
            ScopedMetaCache<String, String> second = registry.createCache("second", ENABLED);
            first.put("first", PARTITION, "first");
            second.put("second", PARTITION, "second");

            first.close();

            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, registry.metrics().getDatabaseNodeCount());
            Assertions.assertEquals(1, registry.metrics().getTableNodeCount());
            Assertions.assertEquals(1, registry.metrics().getPartitionNodeCount());
            Assertions.assertEquals("second", second.getIfPresent("second", PARTITION));
            second.close();
            assertEmpty(registry, second);
        }
    }

    @Test
    public void registryCloseReclaimsAllPhysicalAndIndexStateAndIsIdempotent() {
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        ScopedMetaCache<String, String> first = registry.createCache("first", ENABLED);
        ScopedMetaCache<String, String> second = registry.createCache("second", ENABLED);
        for (int i = 0; i < 100; i++) {
            ScopePath path = ScopePath.partition("db", "tbl-" + (i % 5), "p=" + i);
            first.put("first-" + i, path, "value");
            second.put("second-" + i, path, "value");
        }

        registry.close();
        registry.close();

        assertEmpty(registry, first);
        Assertions.assertEquals(0L, second.metrics().getPhysicalEntryCount());
        Assertions.assertEquals(0, second.metrics().getKeyNodeCount());
    }

    @Test
    public void closeClearsTombstonesWithOutstandingBulkHandle() {
        assertCloseClearsTombstones(false);
        assertCloseClearsTombstones(true);
    }

    private static void assertCloseClearsTombstones(boolean closeRegistry) {
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
        BulkLoadHandle handle = cache.beginBulkLoad(TABLE);
        for (int i = 0; i < 1_000; i++) {
            cache.invalidateKey("key-" + i);
        }
        Assertions.assertEquals(1_000, cache.metrics().getExactInvalidationTombstoneCount());

        if (closeRegistry) {
            registry.close();
        } else {
            cache.close();
        }
        Assertions.assertEquals(0, cache.metrics().getExactInvalidationTombstoneCount());
        Assertions.assertEquals(1, cache.metrics().getActiveBulkHandleCount());

        handle.close();
        handle.close();
        Assertions.assertEquals(0, cache.metrics().getActiveBulkHandleCount());
        Assertions.assertEquals(0, registry.metrics().getActiveLoadCount());
        registry.close();
    }

    private static void assertEmpty(
            ScopedMetaCacheRegistry registry, ScopedMetaCache<?, ?> cache) {
        ScopedMetaCacheRegistry.ScopeMetrics scopeMetrics = registry.metrics();
        Assertions.assertEquals(0, scopeMetrics.getDatabaseNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getTableNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getPartitionNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getRegistrationCount());
        Assertions.assertEquals(0, scopeMetrics.getActiveLoadCount());
        Assertions.assertEquals(0L, cache.metrics().getPhysicalEntryCount());
        Assertions.assertEquals(0, cache.metrics().getKeyNodeCount());
        Assertions.assertEquals(0, cache.metrics().getInFlightLoadCount());
        Assertions.assertEquals(0, cache.metrics().getActiveBulkHandleCount());
        Assertions.assertEquals(0, cache.metrics().getExactInvalidationTombstoneCount());
    }
}
