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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link IcebergTableCache} (PERF-01). The cross-query RAW-table cache mirrors
 * {@link IcebergLatestSnapshotCache} exactly (same {@link org.apache.doris.connector.cache.MetaCache}
 * backing) but stores the whole {@link Table} instead of the {@code (snapshotId, schemaId)} pin, restoring the
 * table-caching half of the legacy {@code IcebergExternalMetaCache}. These tests cover the adapter's contract —
 * within-TTL stability, the {@code ttl <= 0} disable, invalidation, and the exception-propagation guarantee the
 * partition-view readers depend on. Timed-expiry mechanics are the framework's responsibility (unit-tested in
 * the framework module), so they are not re-proven here.
 */
public class IcebergTableCacheTest {

    private static TableIdentifier id() {
        return TableIdentifier.of("db", "t");
    }

    /** A distinct fake table, distinguishable by {@link Table#name()}. */
    private static Table table(String name) {
        return new FakeIcebergTable(name,
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), "s3://b/" + name, Collections.emptyMap());
    }

    @Test
    public void cachesWithinTtlAndServesTheSameTable() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000);

        Table first = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        // Second read within TTL must return the CACHED table (first), NOT a freshly-loaded one -> this is what
        // lets consecutive queries (and one query's analysis/planning phases) reuse a single load. MUTATION:
        // loading live every call -> returns "second" / loads==2 -> red.
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        Assertions.assertEquals("first", first.name());
        Assertions.assertSame(first, second, "within TTL the cached table instance must be served");
        Assertions.assertEquals(1, loads.get(), "the live loader must run exactly once within TTL");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(0, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        // ttl-second=0 (the no-cache catalog) reads live every time. MUTATION: caching despite ttl<=0 ->
        // second=="first" / loads==1 -> red.
        Assertions.assertEquals("second", second.name(), "ttl-second=0 must always read the live table");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void negativeTtlDisablesCachingAlwaysLive() {
        // ttl-second=-1 (or any negative) is still the no-cache catalog. Guards the CacheSpec trap where
        // ttl == -1 means "no expiration (enabled)": the adapter must translate "<= 0" to disabled.
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(-1, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        Assertions.assertEquals("second", second.name(), "ttl-second=-1 must always read the live table");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateWaitsForActiveBorrowerBeforeCleaning() {
        AtomicInteger cleanerCalls = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000, table -> cleanerCalls::incrementAndGet);
        IcebergTableCache.TableLease lease = c.borrow(id(), () -> table("first"));
        c.invalidate(id());
        Assertions.assertEquals(0, cleanerCalls.get(), "invalidation must not close an active statement table");
        Assertions.assertEquals("first", lease.table().name());
        lease.close();
        Assertions.assertEquals(1, cleanerCalls.get(), "removing a cached table must release its resources");
    }

    @Test
    public void disabledCacheCleansAfterBorrowerRelease() {
        AtomicInteger cleanerCalls = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(0, 1000, table -> cleanerCalls::incrementAndGet);
        IcebergTableCache.TableLease lease = c.borrow(id(), () -> table("uncached"));
        Assertions.assertEquals(0, c.size());
        Assertions.assertEquals(0, cleanerCalls.get(), "the returned uncached table is still borrowed");
        lease.close();
        Assertions.assertEquals(1, cleanerCalls.get());
    }

    @Test
    public void capacityEvictionWaitsForActiveBorrower() {
        AtomicInteger cleanerCalls = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1, table -> cleanerCalls::incrementAndGet);
        IcebergTableCache.TableLease first = c.borrow(
                TableIdentifier.of("db", "first"), () -> table("first"));
        IcebergTableCache.TableLease second = c.borrow(
                TableIdentifier.of("db", "second"), () -> table("second"));
        Assertions.assertEquals(0, cleanerCalls.get(), "an evicted active table must remain usable");
        Assertions.assertEquals("first", first.table().name());
        Assertions.assertEquals("second", second.table().name());
        first.close();
        second.close();
        c.invalidateAll();
        Assertions.assertEquals(2, cleanerCalls.get());
    }

    @Test
    public void connectorCloseWaitsForActiveBorrowerBeforeClosingCatalogResources() {
        AtomicInteger tableCleanerCalls = new AtomicInteger();
        AtomicInteger catalogCleanerCalls = new AtomicInteger();
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergTableCache c = new IcebergTableCache(
                100, 1000, table -> tableCleanerCalls::incrementAndGet, tracker);

        IcebergTableCache.TableLease lease = c.borrow(id(), () -> table("first"));
        c.invalidateAll();
        tracker.close(catalogCleanerCalls::incrementAndGet);

        Assertions.assertEquals(0, tableCleanerCalls.get(),
                "cache invalidation must not clean a table still borrowed by a statement");
        Assertions.assertEquals(0, catalogCleanerCalls.get(),
                "connector close must not close catalog resources used by that table");
        Assertions.assertEquals("first", lease.table().name());

        lease.close();
        Assertions.assertEquals(1, tableCleanerCalls.get());
        Assertions.assertEquals(1, catalogCleanerCalls.get());
    }

    @Test
    public void closeRejectsAndCleansLoaderThatPublishesLate() throws Exception {
        AtomicInteger tableCleanerCalls = new AtomicInteger();
        AtomicInteger catalogCleanerCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergTableCache cache = new IcebergTableCache(
                100, 1000, table -> tableCleanerCalls::incrementAndGet, tracker);

        CompletableFuture<Void> load = CompletableFuture.runAsync(() -> Assertions.assertThrows(
                IllegalStateException.class, () -> cache.borrow(id(), () -> {
                    loaderEntered.countDown();
                    try {
                        Assertions.assertTrue(releaseLoader.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return table("late");
                })));

        Assertions.assertTrue(loaderEntered.await(10, TimeUnit.SECONDS));
        cache.close();
        tracker.close(catalogCleanerCalls::incrementAndGet);
        releaseLoader.countDown();
        load.get(10, TimeUnit.SECONDS);

        Assertions.assertEquals(0, cache.size(), "a post-close publication must not remain cache-owned");
        Assertions.assertEquals(1, tableCleanerCalls.get());
        Assertions.assertEquals(1, catalogCleanerCalls.get());
    }

    @Test
    public void invalidateForcesReload() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        c.invalidate(id());
        // After REFRESH TABLE invalidation the next read goes live. MUTATION: invalidate not clearing ->
        // returns cached "first" / loads==1 -> red.
        Table after = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        Assertions.assertEquals("second", after.name());
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void invalidateAllClearsEverything() {
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        c.getOrLoad(TableIdentifier.of("db", "t1"), () -> table("t1"));
        c.getOrLoad(TableIdentifier.of("db", "t2"), () -> table("t2"));
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        c.getOrLoad(TableIdentifier.of("db1", "t1"), () -> table("db1.t1"));
        c.getOrLoad(TableIdentifier.of("db1", "t2"), () -> table("db1.t2"));
        c.getOrLoad(TableIdentifier.of("db2", "t1"), () -> table("db2.t1"));
        Assertions.assertEquals(3, c.size());

        // REFRESH DATABASE db1 (or a Doris DROP DATABASE db1) must drop BOTH db1 tables and leave db2 intact.
        // MUTATION: invalidateDb a no-op -> db1.t1 still cached -> loads stays 0 / after=="db1.t1" -> red.
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's single entry must survive");

        Table afterDb1 = c.getOrLoad(TableIdentifier.of("db1", "t1"), () -> {
            loads.incrementAndGet();
            return table("db1.t1.reloaded");
        });
        Assertions.assertEquals("db1.t1.reloaded", afterDb1.name(), "db1.t1 must reload live after invalidateDb");
        Assertions.assertEquals(1, loads.get());

        Table db2 = c.getOrLoad(TableIdentifier.of("db2", "t1"), () -> {
            loads.incrementAndGet();
            return table("db2.t1.reloaded");
        });
        Assertions.assertEquals("db2.t1", db2.name(), "db2 must keep its cached table (not dropped by invalidateDb(db1))");
        Assertions.assertEquals(1, loads.get(), "db2 read must be a hit (no extra load)");
    }

    @Test
    public void loaderExceptionPropagatesUnwrapped() {
        // The partition-view readers (listPartitions / listPartitionNames) catch NoSuchTableException to degrade
        // a concurrent-drop race to an empty list. Routing them through this cache must NOT wrap that exception,
        // or the degradation would break and they'd throw instead. The MetaCache manual-miss-load path
        // re-throws the loader's RuntimeException verbatim. MUTATION: wrapping the loader exception ->
        // assertThrows(NoSuchTableException) fails (a different type is thrown) -> red.
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        Assertions.assertThrows(NoSuchTableException.class, () -> c.getOrLoad(id(), () -> {
            throw new NoSuchTableException("simulated concurrent drop");
        }));
    }
}
