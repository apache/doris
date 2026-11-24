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

package org.apache.doris.connector.fluss;

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FlussMetadataCacheTest {
    private static final TablePath TABLE = TablePath.of("db", "t");

    private static TableInfo info() {
        return FlussTestTables.builder(TABLE).column("id", DataTypes.INT()).build();
    }

    @Test
    public void tableInfoExpiresAfterWriteAndRefreshInvalidatesIt() {
        AtomicLong clock = new AtomicLong();
        FlussTableInfoCache cache = new FlussTableInfoCache(10, clock::get);
        TableInfo first = info();
        TableInfo second = info();
        Assertions.assertSame(first, cache.get(TABLE, () -> first));
        clock.set(TimeUnit.SECONDS.toNanos(9));
        Assertions.assertSame(first, cache.get(TABLE, () -> second));
        clock.set(TimeUnit.SECONDS.toNanos(10));
        Assertions.assertSame(second, cache.get(TABLE, () -> second));
        cache.invalidate(TABLE);
        Assertions.assertSame(first, cache.get(TABLE, () -> first));
        cache.invalidateDb("db");
        Assertions.assertSame(second, cache.get(TABLE, () -> second));
        cache.invalidateAll();
        Assertions.assertSame(first, cache.get(TABLE, () -> first));
    }

    @Test
    public void disabledCachesAlwaysReadLiveAndDoNotKeepFailures() {
        AtomicInteger loads = new AtomicInteger();
        FlussTableInfoCache cache = new FlussTableInfoCache(0);
        for (int i = 0; i < 3; i++) {
            cache.get(TABLE, () -> {
                loads.incrementAndGet();
                return info();
            });
        }
        Assertions.assertEquals(3, loads.get());
        FlussTableInfoCache enabled = new FlussTableInfoCache(10);
        Assertions.assertThrows(IllegalStateException.class, () -> enabled.get(TABLE, () -> {
            throw new IllegalStateException("metadata unavailable");
        }));
        TableInfo recovered = info();
        Assertions.assertSame(recovered, enabled.get(TABLE, () -> recovered));
    }

    @Test
    public void partitionListsAreSnapshotsWithIndependentExpiry() {
        AtomicLong clock = new AtomicLong();
        FlussPartitionListCache cache = new FlussPartitionListCache(5, clock::get);
        List<PartitionInfo> source = new ArrayList<>();
        List<PartitionInfo> cached = cache.get(TABLE, () -> source);
        source.add(null);
        Assertions.assertTrue(cached.isEmpty());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> cached.add(null));
        clock.set(TimeUnit.SECONDS.toNanos(5));
        Assertions.assertEquals(1, cache.get(TABLE, () -> source).size());
        cache.invalidate(TABLE);
        Assertions.assertTrue(cache.get(TABLE, Collections::emptyList).isEmpty());
    }

    @Test
    public void refreshCannotBeUndoneByAnInFlightLoad() throws Exception {
        FlussTableInfoCache cache = new FlussTableInfoCache(10);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TableInfo old = info();
        TableInfo fresh = info();
        CompletableFuture<TableInfo> pending = CompletableFuture.supplyAsync(() -> cache.get(TABLE, () -> {
            loading.countDown();
            try {
                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return old;
        }));
        try {
            Assertions.assertTrue(loading.await(10, TimeUnit.SECONDS));
            cache.invalidate(TABLE);
            Assertions.assertSame(fresh, cache.get(TABLE, () -> fresh));
        } finally {
            release.countDown();
        }
        Assertions.assertSame(old, pending.get(10, TimeUnit.SECONDS));
        Assertions.assertSame(fresh, cache.get(TABLE, () -> old));
    }

    @Test
    public void lakePropertiesAreIsolatedByTheirSnapshotAndRefresh() {
        FlussLakePropertiesCache cache = new FlussLakePropertiesCache(10);
        List<Object> first = List.of("db", "t", 1L);
        List<Object> next = List.of("db", "t", 2L);
        Map<String, String> old = Map.of("snapshot", "1");
        Map<String, String> fresh = Map.of("snapshot", "2");
        Assertions.assertEquals(old, cache.get(first, () -> old));
        Assertions.assertEquals(old, cache.get(first, () -> fresh));
        Assertions.assertEquals(fresh, cache.get(next, () -> fresh));
        cache.invalidateAll();
        Assertions.assertEquals(fresh, cache.get(first, () -> fresh));
    }
}
