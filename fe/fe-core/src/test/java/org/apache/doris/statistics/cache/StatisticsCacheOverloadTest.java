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

package org.apache.doris.statistics.cache;

import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.PartitionColumnStatistic;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Timeout(30)
class StatisticsCacheOverloadTest {
    private final ThreadPoolExecutor loads = pool();
    private final ThreadPoolExecutor maintenance = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(16), new ThreadPoolExecutor.AbortPolicy());
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch drained = new CountDownLatch(1);

    private static ThreadPoolExecutor pool() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        release.countDown();
        loads.shutdownNow();
        maintenance.shutdownNow();
        Assertions.assertTrue(loads.awaitTermination(10, TimeUnit.SECONDS));
        Assertions.assertTrue(maintenance.awaitTermination(10, TimeUnit.SECONDS));
    }

    private void saturate() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        loads.execute(() -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        loads.execute(drained::countDown);
    }

    private StatisticsCacheLoader<Integer, Integer> loader() {
        return new StatisticsCacheLoader<>(new BasicAsyncCacheLoader<Integer, Integer>() {
            @Override
            protected Integer doLoad(Integer key) {
                return key;
            }
        }, loads);
    }

    @Test
    void rejectedColdLoadsLeaveNoPendingEntriesAndCanRecover() throws Exception {
        saturate();
        AsyncLoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
                .maximumSize(1).executor(maintenance).buildAsync(loader());
        for (int i = 0; i < 1000; i++) {
            final int key = i;
            Assertions.assertThrows(RejectedExecutionException.class, () -> cache.get(key));
        }
        Assertions.assertTrue(cache.asMap().isEmpty());
        Assertions.assertEquals(1, loads.getQueue().size());
        release.countDown();
        Assertions.assertTrue(drained.await(10, TimeUnit.SECONDS));
        Assertions.assertEquals(7, cache.get(7).get(10, TimeUnit.SECONDS));
    }

    @Test
    void pendingKeysAreDeduplicatedAndShareTheExecutorLimit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        loads.execute(() -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        AsyncLoadingCache<Integer, Integer> first = Caffeine.newBuilder()
                .maximumSize(1).executor(maintenance).buildAsync(loader());
        AsyncLoadingCache<Integer, Integer> second = Caffeine.newBuilder()
                .maximumSize(1).executor(maintenance).buildAsync(loader());
        CompletableFuture<Integer> pending = first.get(1);
        Assertions.assertSame(pending, first.get(1));
        Assertions.assertThrows(RejectedExecutionException.class, () -> second.get(2));
        Assertions.assertTrue(second.asMap().isEmpty());
        release.countDown();
        Assertions.assertEquals(1, pending.get(10, TimeUnit.SECONDS));
    }

    @Test
    void automaticRefreshRetainsOldValueAndDoesNotBlockMaintenance() throws Exception {
        saturate();
        AtomicLong clock = new AtomicLong();
        AsyncLoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
                .maximumSize(1).ticker(clock::get).refreshAfterWrite(Duration.ofSeconds(1))
                .executor(maintenance).buildAsync(loader());
        cache.put(7, CompletableFuture.completedFuture(70));
        clock.set(TimeUnit.SECONDS.toNanos(2));
        Assertions.assertEquals(70, cache.get(7).get(10, TimeUnit.SECONDS));
        Assertions.assertEquals(70, cache.getIfPresent(7).join());
        CountDownLatch maintained = new CountDownLatch(1);
        // A maintenance task can run while SQL loading remains saturated.
        maintenance.execute(maintained::countDown);
        Assertions.assertTrue(maintained.await(10, TimeUnit.SECONDS));
    }

    @Test
    void statisticsLookupsAndPreheatDegradeWithoutPropagatingRejection() throws Exception {
        saturate();
        StatisticsCache cache = new StatisticsCache(loads, maintenance);
        Assertions.assertSame(ColumnStatistic.UNKNOWN,
                cache.getColumnStatistics(1, 1, 1, -1, "c", null));
        Assertions.assertSame(PartitionColumnStatistic.UNKNOWN,
                cache.getPartitionColumnStatistics(1, 1, 1, -1, "p", "c", null));
        Assertions.assertNull(cache.getHistogram(1, 1, 1, "c"));
        Assertions.assertDoesNotThrow(() -> cache.refreshHistogramSync(1, 1, 1, -1, "c"));
        boolean previous = FeConstants.disablePreHeat;
        try {
            FeConstants.disablePreHeat = false;
            Assertions.assertDoesNotThrow(cache::preHeat);
        } finally {
            FeConstants.disablePreHeat = previous;
        }
        Assertions.assertEquals(5, cache.getRejectedLoadCount());
    }

    @Test
    void rejectedExplicitRefreshInvalidatesYoungOldValue() throws Exception {
        saturate();
        StatisticsCache cache = new StatisticsCache(loads, maintenance);
        ColumnStatistic old = new ColumnStatisticBuilder(10).setNdv(5).build();
        cache.updateColStatsCache(1, 1, 1, -1, "c", old);
        Assertions.assertSame(old, cache.getColumnStatistics(1, 1, 1, -1, "c", null));
        Assertions.assertDoesNotThrow(() -> cache.refreshColStatsSync(1, 1, 1, -1, "c"));
        Assertions.assertSame(ColumnStatistic.UNKNOWN,
                cache.getColumnStatistics(1, 1, 1, -1, "c", null));
    }
}
