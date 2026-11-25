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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.PartitionColumnStatistic;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        blockWorkers(loads, 1);
        loads.execute(drained::countDown);
    }

    private void blockWorkers(ThreadPoolExecutor pool, int workers) throws InterruptedException {
        CountDownLatch started = new CountDownLatch(workers);
        for (int i = 0; i < workers; i++) {
            pool.execute(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
    }

    private StatisticsCacheLoader<Integer, Integer> loader() {
        return new StatisticsCacheLoader<>(integerLoader(), loads);
    }

    private static BasicAsyncCacheLoader<Integer, Integer> integerLoader() {
        return new BasicAsyncCacheLoader<Integer, Integer>() {
            @Override
            protected Integer doLoad(Integer key) {
                return key;
            }
        };
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
        blockWorkers(loads, 1);
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
    void automaticRefreshBacksOffAndRecoversWhenQueueDrains() throws Exception {
        saturate();
        AtomicLong clock = new AtomicLong();
        AtomicInteger attempts = new AtomicInteger();
        StatisticsCacheLoader<Integer, Integer> loader = new StatisticsCacheLoader<>(integerLoader(), task -> {
            attempts.incrementAndGet();
            loads.execute(task);
        }, clock::get);
        AsyncLoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
                .maximumSize(1).ticker(clock::get).refreshAfterWrite(Duration.ofSeconds(1))
                .executor(Runnable::run).buildAsync(loader);
        cache.put(7, CompletableFuture.completedFuture(70));
        clock.set(TimeUnit.SECONDS.toNanos(2));

        for (int i = 0; i < 1000; i++) {
            Assertions.assertEquals(70, cache.get(7).join());
        }
        Assertions.assertEquals(1, attempts.get());

        release.countDown();
        Assertions.assertTrue(drained.await(10, TimeUnit.SECONDS));
        Assertions.assertEquals(70, cache.get(7).join());
        Assertions.assertEquals(1, attempts.get());

        clock.addAndGet(StatisticsCacheLoader.REFRESH_RETRY_DELAY_NANOS);
        cache.get(7);
        // The single worker publishes the refreshed value before starting the next task.
        CountDownLatch refreshed = new CountDownLatch(1);
        loads.getQueue().put(refreshed::countDown);
        Assertions.assertTrue(refreshed.await(10, TimeUnit.SECONDS));
        Assertions.assertEquals(2, attempts.get());
        Assertions.assertEquals(7, cache.synchronous().policy().getIfPresentQuietly(7));
    }

    @Test
    void expiredEntrySubmitsOnlyOneExplicitRefreshAndIsInvalidatedOnRejection() throws Exception {
        saturate();
        AtomicLong clock = new AtomicLong();
        AtomicInteger attempts = new AtomicInteger();
        StatisticsCacheLoader<Integer, Integer> loader = new StatisticsCacheLoader<>(integerLoader(), task -> {
            attempts.incrementAndGet();
            loads.execute(task);
        }, clock::get);
        AsyncLoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
                .maximumSize(1).ticker(clock::get).refreshAfterWrite(Duration.ofSeconds(1))
                .executor(Runnable::run).buildAsync(loader);
        cache.put(7, CompletableFuture.completedFuture(70));
        clock.set(TimeUnit.SECONDS.toNanos(2));

        StatisticsCacheLoader.refresh(cache, 7);

        Assertions.assertEquals(1, attempts.get());
        Assertions.assertTrue(cache.asMap().isEmpty());
    }

    @Test
    void productionQueueLimitRejectsPromptly() throws Exception {
        String metricName = "stats_cache_load_rejected";
        List<Metric> previousMetrics = MetricRepo.DORIS_METRIC_REGISTER.getMetricsByName(metricName);
        Field registryField = ThreadPoolManager.class.getDeclaredField("nameToThreadPoolMap");
        registryField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ThreadPoolExecutor> registeredPools = (Map<String, ThreadPoolExecutor>) registryField.get(null);
        ThreadPoolExecutor previousLoads = registeredPools.get("STATS_FETCH");
        ThreadPoolExecutor previousMaintenance = registeredPools.get("STATS_CACHE_MAINTENANCE");
        int previousQueueSize = Config.stats_fetch_queue_size;
        ThreadPoolExecutor productionLoads = null;
        ThreadPoolExecutor productionMaintenance = null;
        try {
            Config.stats_fetch_queue_size = 1;
            StatisticsCache cache = new StatisticsCache();
            productionLoads = Deencapsulation.getField(cache, "threadPool");
            productionMaintenance = registeredPools.get("STATS_CACHE_MAINTENANCE");
            blockWorkers(productionLoads, 10);
            productionLoads.execute(drained::countDown);
            Assertions.assertEquals(0, productionLoads.getQueue().remainingCapacity());
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                    Assertions.assertSame(ColumnStatistic.UNKNOWN,
                            cache.getColumnStatistics(1, 1, 1, -1, "c", null)));
            Assertions.assertEquals(1, cache.getRejectedLoadCount());
            Assertions.assertEquals(1, productionLoads.getQueue().size());
        } finally {
            Config.stats_fetch_queue_size = previousQueueSize;
            release.countDown();
            if (productionLoads != null) {
                productionLoads.shutdownNow();
                registeredPools.computeIfPresent("STATS_FETCH", (name, pool) ->
                        pool.isShutdown() ? previousLoads : pool);
                Assertions.assertTrue(productionLoads.awaitTermination(10, TimeUnit.SECONDS));
            }
            if (productionMaintenance != null && productionMaintenance != previousMaintenance) {
                productionMaintenance.shutdownNow();
                registeredPools.computeIfPresent("STATS_CACHE_MAINTENANCE", (name, pool) ->
                        pool.isShutdown() ? previousMaintenance : pool);
                Assertions.assertTrue(productionMaintenance.awaitTermination(10, TimeUnit.SECONDS));
            }
            MetricRepo.DORIS_METRIC_REGISTER.removeMetrics(metricName);
            previousMetrics.forEach(MetricRepo.DORIS_METRIC_REGISTER::addMetrics);
        }
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
