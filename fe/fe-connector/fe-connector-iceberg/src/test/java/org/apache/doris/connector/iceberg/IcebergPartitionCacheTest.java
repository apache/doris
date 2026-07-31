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

import org.apache.doris.connector.iceberg.IcebergPartitionUtils.IcebergRawPartition;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link IcebergPartitionCache} (PERF-02). Mirrors {@link IcebergTableCacheTest} but keys by
 * {@code (TableIdentifier, snapshotId)} and stores the raw partition list. Covers within-TTL stability, the
 * {@code ttl <= 0} disable, invalidation, and the exception-propagation guarantee that {@code listPartitions}'
 * dropped-partition-source-column degradation depends on.
 */
public class IcebergPartitionCacheTest {

    private static IcebergPartitionCache.Key key(String db, String tbl, long snapshotId) {
        return new IcebergPartitionCache.Key(TableIdentifier.of(db, tbl), snapshotId);
    }

    /** A raw partition list of the given size, distinguishable by size. */
    private static List<IcebergRawPartition> raws(int n) {
        List<IcebergRawPartition> list = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(new IcebergRawPartition("p" + i, Collections.singletonList("c"),
                    Collections.singletonList("v" + i), Collections.singletonList("identity"), 0L, 0L));
        }
        return list;
    }

    @Test
    public void cachesWithinTtlAndServesTheSameList() {
        AtomicInteger loads = new AtomicInteger();
        IcebergPartitionCache c = new IcebergPartitionCache(100, 1000);

        List<IcebergRawPartition> first = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return raws(3);
        });
        // Second read of the SAME (table, snapshot) within TTL returns the cached list, NOT a fresh scan -> this
        // is what collapses the per-query and per-MTMV-refresh PARTITIONS scans. MUTATION: scanning live every
        // call -> returns the 7-element list / loads==2 -> red.
        List<IcebergRawPartition> second = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return raws(7);
        });
        Assertions.assertEquals(3, first.size());
        Assertions.assertSame(first, second, "within TTL the cached partition list must be served");
        Assertions.assertEquals(1, loads.get(), "the live scan must run exactly once within TTL");
        Assertions.assertEquals(1, c.loadCountForTest(), "the metric-gate load count must be 1");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void differentSnapshotIdIsADifferentKey() {
        AtomicInteger loads = new AtomicInteger();
        IcebergPartitionCache c = new IcebergPartitionCache(100, 1000);
        c.getOrLoad(key("db", "t", 1L), () -> {
            loads.incrementAndGet();
            return raws(1);
        });
        // A new commit yields a new snapshot id -> a distinct key -> a live scan (freshness across snapshots).
        // MUTATION: keying by table only (ignoring snapshotId) -> loads==1 / serves the snapshot-1 list -> red.
        List<IcebergRawPartition> s2 = c.getOrLoad(key("db", "t", 2L), () -> {
            loads.incrementAndGet();
            return raws(9);
        });
        Assertions.assertEquals(9, s2.size());
        Assertions.assertEquals(2, loads.get());
        Assertions.assertEquals(2, c.size());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergPartitionCache c = new IcebergPartitionCache(0, 1000);
        c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return raws(3);
        });
        List<IcebergRawPartition> second = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return raws(7);
        });
        // ttl-second=0 (the no-cache catalog) scans live every time. MUTATION: caching despite ttl<=0 ->
        // second.size()==3 / loads==1 -> red.
        Assertions.assertEquals(7, second.size(), "ttl-second=0 must always scan live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void negativeTtlDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergPartitionCache c = new IcebergPartitionCache(-1, 1000);
        c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return raws(3);
        });
        List<IcebergRawPartition> second = c.getOrLoad(key("db", "t", 5L), () -> {
            loads.incrementAndGet();
            return raws(7);
        });
        Assertions.assertEquals(7, second.size(), "ttl-second=-1 must always scan live");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateDropsAllSnapshotsOfOneTable() {
        AtomicInteger loads = new AtomicInteger();
        IcebergPartitionCache c = new IcebergPartitionCache(100, 1000);
        c.getOrLoad(key("db", "t", 1L), () -> raws(1));
        c.getOrLoad(key("db", "t", 2L), () -> raws(2));
        c.getOrLoad(key("db", "other", 1L), () -> raws(3));
        Assertions.assertEquals(3, c.size());

        // REFRESH TABLE db.t must drop BOTH snapshot entries of db.t and leave db.other intact. MUTATION:
        // invalidateKey on a single (table, snapshot) key -> the other snapshot survives -> size 2 here -> red.
        c.invalidate(TableIdentifier.of("db", "t"));
        Assertions.assertEquals(1, c.size(), "only db.other's entry must survive");
        List<IcebergRawPartition> reload = c.getOrLoad(key("db", "t", 1L), () -> {
            loads.incrementAndGet();
            return raws(9);
        });
        Assertions.assertEquals(9, reload.size());
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        IcebergPartitionCache c = new IcebergPartitionCache(100, 1000);
        c.getOrLoad(key("db1", "t1", 1L), () -> raws(1));
        c.getOrLoad(key("db1", "t2", 1L), () -> raws(2));
        c.getOrLoad(key("db2", "t1", 1L), () -> raws(3));
        Assertions.assertEquals(3, c.size());
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's entry must survive REFRESH DATABASE db1");
    }

    @Test
    public void invalidateAllClearsEverything() {
        IcebergPartitionCache c = new IcebergPartitionCache(100, 1000);
        c.getOrLoad(key("db", "t1", 1L), () -> raws(1));
        c.getOrLoad(key("db", "t2", 1L), () -> raws(2));
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }

    @Test
    public void loaderExceptionPropagatesUnwrapped() {
        // listPartitions catches ValidationException (dropped partition source column) to degrade to an empty
        // list. Routing the scan through this cache must NOT wrap it, or the degradation would break. The
        // MetaCacheEntry manual-miss-load path re-throws the loader's RuntimeException verbatim, and a failed
        // scan is not cached. MUTATION: wrapping the loader exception -> assertThrows(ValidationException) fails.
        IcebergPartitionCache c = new IcebergPartitionCache(100, 1000);
        Assertions.assertThrows(ValidationException.class, () -> c.getOrLoad(key("db", "t", 5L), () -> {
            throw new ValidationException("Cannot find source column for partition field");
        }));
        Assertions.assertEquals(0, c.size(), "a failed scan must not be cached");
    }
}
