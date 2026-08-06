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

/**
 * Unit tests for {@link IcebergFormatCache} (PERF-03). Mirrors {@link IcebergPartitionCacheTest} but keys by
 * {@code (TableIdentifier, snapshotId)} and stores the inferred format-name String. Covers within-TTL stability,
 * the {@code ttl <= 0} disable, invalidation, and the not-cached-on-failure guarantee that makes a transient
 * remote-IO failure retry on the next query (legacy parity).
 */
public class IcebergFormatCacheTest {

    private static IcebergFormatCache.Key key(String db, String tbl, long snapshotId) {
        return new IcebergFormatCache.Key(TableIdentifier.of(db, tbl), snapshotId);
    }

    @Test
    public void cachesWithinTtlAndServesTheSameValue() {
        AtomicInteger loads = new AtomicInteger();
        IcebergFormatCache c = new IcebergFormatCache(100, 1000);

        String first = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "orc";
        });
        // Second read of the SAME (table, snapshot) within TTL returns the cached format, NOT a fresh whole-table
        // planFiles() inference -> this is what collapses the per-query #64134 fallback. MUTATION: inferring live
        // every call -> returns "parquet" / loads==2 -> red.
        String second = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "parquet";
        });
        Assertions.assertEquals("orc", first);
        Assertions.assertEquals("orc", second, "within TTL the cached format must be served");
        Assertions.assertEquals(1, loads.get(), "the live inference must run exactly once within TTL");
        Assertions.assertEquals(1, c.loadCountForTest(), "the metric-gate load count must be 1");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void differentSnapshotIdIsADifferentKey() {
        AtomicInteger loads = new AtomicInteger();
        IcebergFormatCache c = new IcebergFormatCache(100, 1000);
        c.getOrLoad(key("db", "t", 1L), () -> {
            loads.incrementAndGet();
            return "parquet";
        });
        // A new commit yields a new snapshot id -> a distinct key -> a live inference (freshness across snapshots,
        // e.g. a rewrite that changed the write format). MUTATION: keying by table only -> loads==1 -> red.
        String s2 = c.getOrLoad(key("db", "t", 2L), () -> {
            loads.incrementAndGet();
            return "orc";
        });
        Assertions.assertEquals("orc", s2);
        Assertions.assertEquals(2, loads.get());
        Assertions.assertEquals(2, c.size());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergFormatCache c = new IcebergFormatCache(0, 1000);
        c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "orc";
        });
        String second = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "parquet";
        });
        // ttl-second=0 (the no-cache catalog) infers live every time. MUTATION: caching despite ttl<=0 ->
        // second=="orc" / loads==1 -> red.
        Assertions.assertEquals("parquet", second, "ttl-second=0 must always infer live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void negativeTtlDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergFormatCache c = new IcebergFormatCache(-1, 1000);
        c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "orc";
        });
        String second = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "parquet";
        });
        Assertions.assertEquals("parquet", second, "ttl-second=-1 must always infer live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateDropsAllSnapshotsOfOneTable() {
        AtomicInteger loads = new AtomicInteger();
        IcebergFormatCache c = new IcebergFormatCache(100, 1000);
        c.getOrLoad(key("db", "t", 1L), () -> "parquet");
        c.getOrLoad(key("db", "t", 2L), () -> "orc");
        c.getOrLoad(key("db", "other", 1L), () -> "parquet");
        Assertions.assertEquals(3, c.size());

        // REFRESH TABLE db.t must drop BOTH snapshot entries of db.t and leave db.other intact. MUTATION:
        // invalidating a single (table, snapshot) key -> the other snapshot survives -> size 2 here -> red.
        c.invalidate(TableIdentifier.of("db", "t"));
        Assertions.assertEquals(1, c.size(), "only db.other's entry must survive");
        String reload = c.getOrLoad(key("db", "t", 1L), () -> {
            loads.incrementAndGet();
            return "orc";
        });
        Assertions.assertEquals("orc", reload);
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        IcebergFormatCache c = new IcebergFormatCache(100, 1000);
        c.getOrLoad(key("db1", "t1", 1L), () -> "parquet");
        c.getOrLoad(key("db1", "t2", 1L), () -> "orc");
        c.getOrLoad(key("db2", "t1", 1L), () -> "parquet");
        Assertions.assertEquals(3, c.size());
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's entry must survive REFRESH DATABASE db1");
    }

    @Test
    public void invalidateAllClearsEverything() {
        IcebergFormatCache c = new IcebergFormatCache(100, 1000);
        c.getOrLoad(key("db", "t1", 1L), () -> "parquet");
        c.getOrLoad(key("db", "t2", 1L), () -> "orc");
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }

    @Test
    public void loaderFailureIsNotCachedSoNextQueryRetries() {
        // A transient remote-IO failure during inference (planFiles/close) must NOT be memoized, or the parquet
        // fallback would stick for the whole TTL. The MetaCacheEntry manual-miss-load path re-throws the loader's
        // RuntimeException verbatim and does not store it. MUTATION: caching on failure -> the second call would
        // not re-run the loader / size==1 -> red.
        IcebergFormatCache c = new IcebergFormatCache(100, 1000);
        AtomicInteger loads = new AtomicInteger();
        Assertions.assertThrows(RuntimeException.class, () -> c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            throw new RuntimeException("transient manifest read failure");
        }));
        Assertions.assertEquals(0, c.size(), "a failed inference must not be cached");
        // The next query at the same key re-runs the loader (retry), and a success is then cached.
        String ok = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return "orc";
        });
        Assertions.assertEquals("orc", ok);
        Assertions.assertEquals(2, loads.get(), "the loader must re-run after a failure (no sticky failure)");
        Assertions.assertEquals(1, c.size());
    }
}
