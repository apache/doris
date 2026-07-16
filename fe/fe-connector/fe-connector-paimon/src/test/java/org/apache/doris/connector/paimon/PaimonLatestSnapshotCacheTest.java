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

import org.apache.paimon.catalog.Identifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link PaimonLatestSnapshotCache} (data-snapshot caching, CI 973411). The cache is now backed
 * by the shared {@link org.apache.doris.connector.cache.MetaCacheEntry} framework; these tests cover the
 * adapter's contract — within-TTL stability, the {@code ttl <= 0} disable, and invalidation. Timed-expiry
 * mechanics are the framework's responsibility (the ttl→duration mapping is unit-tested in the framework
 * module's {@code CacheSpecTest}; Caffeine {@code expireAfterAccess} itself is the library's behavior), so they
 * are not re-proven here (no injectable clock).
 */
public class PaimonLatestSnapshotCacheTest {

    private static Identifier id() {
        return Identifier.create("db", "t");
    }

    @Test
    public void cachesWithinTtlAndServesStaleId() {
        AtomicInteger loads = new AtomicInteger();
        PaimonLatestSnapshotCache c = new PaimonLatestSnapshotCache(100, 1000);

        long first = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 1L;
        });
        // Second read within TTL must return the CACHED id (1), NOT the new live id (2) -> this is what
        // pins the with-cache catalog to the old snapshot after an external write. MUTATION: serving live
        // every call -> returns 2 -> red.
        long second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 2L;
        });
        Assertions.assertEquals(1L, first);
        Assertions.assertEquals(1L, second, "within TTL the cached snapshot id must be served");
        Assertions.assertEquals(1, loads.get(), "the live loader must run exactly once within TTL");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        PaimonLatestSnapshotCache c = new PaimonLatestSnapshotCache(0, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 1L;
        });
        long second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 2L;
        });
        // ttl-second=0 (the no-cache catalog) must read live every time. MUTATION: caching despite ttl<=0
        // -> loads==1 / second==1 -> red.
        Assertions.assertEquals(2L, second, "ttl-second=0 must always read the live id");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void negativeTtlDisablesCachingAlwaysLive() {
        // ttl-second=-1 (or any negative) is still the no-cache catalog. Guards the CacheSpec trap where
        // ttl == -1 means "no expiration (enabled)": the adapter must translate "<= 0" to disabled, NOT pass
        // -1 through. MUTATION: passing ttlSeconds straight into CacheSpec -> -1 becomes a never-expiring cache
        // -> loads==1 / second==1 -> red.
        AtomicInteger loads = new AtomicInteger();
        PaimonLatestSnapshotCache c = new PaimonLatestSnapshotCache(-1, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 1L;
        });
        long second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 2L;
        });
        Assertions.assertEquals(2L, second, "ttl-second=-1 must always read the live id");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateForcesReload() {
        AtomicInteger loads = new AtomicInteger();
        PaimonLatestSnapshotCache c = new PaimonLatestSnapshotCache(100, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 1L;
        });
        c.invalidate(id());
        // After REFRESH TABLE invalidation the next read goes live (sees 2). MUTATION: invalidate not
        // clearing -> returns cached 1 / loads==1 -> red.
        long after = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return 2L;
        });
        Assertions.assertEquals(2L, after);
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void invalidateAllClearsEverything() {
        PaimonLatestSnapshotCache c = new PaimonLatestSnapshotCache(100, 1000);
        c.getOrLoad(Identifier.create("db", "t1"), () -> 1L);
        c.getOrLoad(Identifier.create("db", "t2"), () -> 2L);
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        AtomicInteger loads = new AtomicInteger();
        PaimonLatestSnapshotCache c = new PaimonLatestSnapshotCache(100, 1000);
        c.getOrLoad(Identifier.create("db1", "t1"), () -> 1L);
        c.getOrLoad(Identifier.create("db1", "t2"), () -> 2L);
        c.getOrLoad(Identifier.create("db2", "t1"), () -> 3L);
        Assertions.assertEquals(3, c.size());

        // REFRESH DATABASE db1 (or a Doris DROP DATABASE db1) must drop BOTH db1 tables and leave db2 intact.
        // MUTATION: invalidateDb a no-op (the inherited SPI default this fix replaces) -> db1.t1 still cached
        // -> loads stays 0 / after == 1 -> red.
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's single entry must survive");

        long afterDb1 = c.getOrLoad(Identifier.create("db1", "t1"), () -> {
            loads.incrementAndGet();
            return 9L;
        });
        Assertions.assertEquals(9L, afterDb1, "db1.t1 must reload live after invalidateDb");
        Assertions.assertEquals(1, loads.get());

        long db2 = c.getOrLoad(Identifier.create("db2", "t1"), () -> {
            loads.incrementAndGet();
            return 7L;
        });
        Assertions.assertEquals(3L, db2, "db2 must keep its cached id (not dropped by invalidateDb(db1))");
        Assertions.assertEquals(1, loads.get(), "db2 read must be a hit (no extra load)");
    }
}
