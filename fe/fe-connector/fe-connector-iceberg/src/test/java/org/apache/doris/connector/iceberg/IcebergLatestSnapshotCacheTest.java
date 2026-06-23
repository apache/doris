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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link IcebergLatestSnapshotCache} (T08, mirrors PaimonLatestSnapshotCacheTest).
 * Uses an injectable nano-clock so TTL expiry is deterministic without sleeping.
 */
public class IcebergLatestSnapshotCacheTest {

    private static TableIdentifier id() {
        return TableIdentifier.of("db", "t");
    }

    @Test
    public void cachesWithinTtlAndServesStaleSnapshot() {
        AtomicInteger loads = new AtomicInteger();
        AtomicLong now = new AtomicLong(0);
        IcebergLatestSnapshotCache c = new IcebergLatestSnapshotCache(100, 1000, now::get);

        IcebergLatestSnapshotCache.CachedSnapshot first = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(1L, 11L);
        });
        // Second read within TTL must return the CACHED snapshot (1/11), NOT the new live one (2/22) -> this is
        // what pins a query to the old snapshot after an external write. MUTATION: serving live every call ->
        // returns 2/22 -> red.
        IcebergLatestSnapshotCache.CachedSnapshot second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(2L, 22L);
        });
        Assertions.assertEquals(1L, first.snapshotId);
        Assertions.assertEquals(11L, first.schemaId);
        Assertions.assertEquals(1L, second.snapshotId, "within TTL the cached snapshot id must be served");
        // BOTH ids must be pinned atomically. MUTATION: caching only snapshotId and re-reading schemaId live ->
        // second.schemaId == 22 -> red. This is the iceberg-specific deviation from the paimon long-only cache.
        Assertions.assertEquals(11L, second.schemaId, "within TTL the cached schema id must be served too");
        Assertions.assertEquals(1, loads.get(), "the live loader must run exactly once within TTL");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergLatestSnapshotCache c = new IcebergLatestSnapshotCache(0, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(1L, 11L);
        });
        IcebergLatestSnapshotCache.CachedSnapshot second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(2L, 22L);
        });
        // ttl-second=0 (the no-cache catalog) must read live every time. MUTATION: caching despite ttl<=0 ->
        // loads==1 / second==1 -> red.
        Assertions.assertEquals(2L, second.snapshotId, "ttl-second=0 must always read the live snapshot");
        Assertions.assertEquals(22L, second.schemaId);
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void invalidateForcesReload() {
        AtomicInteger loads = new AtomicInteger();
        AtomicLong now = new AtomicLong(0);
        IcebergLatestSnapshotCache c = new IcebergLatestSnapshotCache(100, 1000, now::get);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(1L, 11L);
        });
        c.invalidate(id());
        // After REFRESH TABLE invalidation the next read goes live (sees 2). MUTATION: invalidate not clearing
        // -> returns cached 1 / loads==1 -> red.
        IcebergLatestSnapshotCache.CachedSnapshot after = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(2L, 22L);
        });
        Assertions.assertEquals(2L, after.snapshotId);
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void expiresAfterTtlNanos() {
        AtomicInteger loads = new AtomicInteger();
        AtomicLong now = new AtomicLong(0);
        // ttl = 1 second -> 1e9 ns.
        IcebergLatestSnapshotCache c = new IcebergLatestSnapshotCache(1, 1000, now::get);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(1L, 11L);
        });
        now.set(2_000_000_000L); // 2s later, past the 1s TTL
        IcebergLatestSnapshotCache.CachedSnapshot after = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return new IcebergLatestSnapshotCache.CachedSnapshot(2L, 22L);
        });
        // MUTATION: never expiring -> returns 1 / loads==1 -> red.
        Assertions.assertEquals(2L, after.snapshotId, "an entry past its TTL must be reloaded");
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void invalidateAllClearsEverything() {
        AtomicLong now = new AtomicLong(0);
        IcebergLatestSnapshotCache c = new IcebergLatestSnapshotCache(100, 1000, now::get);
        c.getOrLoad(TableIdentifier.of("db", "t1"),
                () -> new IcebergLatestSnapshotCache.CachedSnapshot(1L, 11L));
        c.getOrLoad(TableIdentifier.of("db", "t2"),
                () -> new IcebergLatestSnapshotCache.CachedSnapshot(2L, 22L));
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }
}
