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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/lib/trino-cache/src/test/java/io/trino/cache/TestEvictableCache.java
// and modified by Doris

package org.apache.doris.common;

import org.apache.doris.common.EvictableCacheBuilder.DisabledCacheImplementation;

import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class TestEvictableCache {
    private static class TestingTicker extends Ticker {
        private volatile long time;

        public TestingTicker() {
        }

        public long read() {
            return this.time;
        }

        public synchronized void increment(long delta, TimeUnit unit) {
            Preconditions.checkArgument(delta >= 0L, "delta is negative");
            this.time += unit.toNanos(delta);
        }
    }

    private static final int TEST_TIMEOUT_SECONDS = 10;

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoad()
            throws Exception {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();
        Assert.assertEquals("abc", cache.get(42, () -> "abc"));
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testEvictBySize()
            throws Exception {
        int maximumSize = 10;
        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .build();

        for (int i = 0; i < 10_000; i++) {
            int value = i * 10;
            Assert.assertEquals(value, (Object) cache.get(i, () -> value));
        }
        cache.cleanUp();
        Assert.assertEquals(maximumSize, cache.size());
        Assert.assertEquals(maximumSize, ((EvictableCache<?, ?>) cache).tokensCount());

        // Ensure cache is effective, i.e. some entries preserved
        int lastKey = 10_000 - 1;
        Assert.assertEquals(lastKey * 10, (Object) cache.get(lastKey, () -> {
            throw new UnsupportedOperationException();
        }));
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testEvictByWeight() throws Exception {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(20)
                .weigher((Integer key, String value) -> value.length())
                .build();

        for (int i = 0; i < 10; i++) {
            String value = String.join("", Collections.nCopies(i, "a"));
            Assert.assertEquals(value, cache.get(i, () -> value));
        }

        cache.cleanUp();
        // It's not deterministic which entries get evicted
        int cacheSize = Math.toIntExact(cache.size());

        Assert.assertEquals(cacheSize, ((EvictableCache<?, ?>) cache).tokensCount());
        Assert.assertEquals(cacheSize, cache.asMap().keySet().size());

        int keySum = cache.asMap().keySet().stream()
                .mapToInt(i -> i)
                .sum();
        Assert.assertTrue("key sum should be <= 20", keySum <= 20);

        Assert.assertEquals(cacheSize, cache.asMap().values().size());

        int valuesLengthSum = cache.asMap().values().stream()
                .mapToInt(String::length)
                .sum();
        Assert.assertTrue("values length sum should be <= 20", valuesLengthSum <= 20);

        // Ensure cache is effective, i.e. some entries preserved
        int lastKey = 9; // 10 - 1
        String expected = String.join("", Collections.nCopies(lastKey, "a")); // java8 替代 repeat
        Assert.assertEquals(expected, cache.get(lastKey, () -> {
            throw new UnsupportedOperationException();
        }));
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testEvictByTime() throws Exception {
        TestingTicker ticker = new TestingTicker();
        int ttl = 100;
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                .build();

        Assert.assertEquals("1 ala ma kota", cache.get(1, () -> "1 ala ma kota"));
        ticker.increment(ttl, TimeUnit.MILLISECONDS);
        Assert.assertEquals("2 ala ma kota", cache.get(2, () -> "2 ala ma kota"));
        cache.cleanUp();

        // First entry should be expired and its token removed
        int cacheSize = Math.toIntExact(cache.size());
        Assert.assertEquals(1, cacheSize);
        Assert.assertEquals(cacheSize, ((EvictableCache<?, ?>) cache).tokensCount());
        Assert.assertEquals(cacheSize, cache.asMap().keySet().size());
        Assert.assertEquals(cacheSize, cache.asMap().values().size());
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testPreserveValueLoadedAfterTimeExpiration() throws Exception {
        TestingTicker ticker = new TestingTicker();
        int ttl = 100;
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
                .build();
        int key = 11;

        Assert.assertEquals("11 ala ma kota", cache.get(key, () -> "11 ala ma kota"));
        Assert.assertEquals(1, ((EvictableCache<?, ?>) cache).tokensCount());

        Assert.assertEquals("11 ala ma kota", cache.get(key, () -> "something else"));
        Assert.assertEquals(1, ((EvictableCache<?, ?>) cache).tokensCount());

        ticker.increment(ttl, TimeUnit.MILLISECONDS);
        Assert.assertEquals("new value", cache.get(key, () -> "new value"));
        Assert.assertEquals(1, ((EvictableCache<?, ?>) cache).tokensCount());

        Assert.assertEquals("new value", cache.get(key, () -> "something yet different"));
        Assert.assertEquals(1, ((EvictableCache<?, ?>) cache).tokensCount());

        Assert.assertEquals(1, cache.size());
        Assert.assertEquals(1, ((EvictableCache<?, ?>) cache).tokensCount());
        Assert.assertEquals(1, cache.asMap().keySet().size());
        Assert.assertEquals(1, cache.asMap().values().size());
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testReplace() throws Exception {
        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10)
                .build();

        int key = 10;
        int initialValue = 20;
        int replacedValue = 21;

        cache.get(key, () -> initialValue);

        Assert.assertTrue("Should successfully replace value", cache.asMap().replace(key, initialValue, replacedValue));
        Assert.assertEquals("Cache should contain replaced value", replacedValue, cache.getIfPresent(key).intValue());

        Assert.assertFalse("Should not replace when current value is different", cache.asMap().replace(key, initialValue, replacedValue));
        Assert.assertEquals("Cache should maintain replaced value", replacedValue, cache.getIfPresent(key).intValue());

        Assert.assertFalse("Should not replace non-existent key", cache.asMap().replace(100000, replacedValue, 22));
        Assert.assertEquals("Cache should only contain original key", ImmutableSet.of(key), cache.asMap().keySet());
        Assert.assertEquals("Original key should maintain its value", replacedValue, cache.getIfPresent(key).intValue());

        int anotherKey = 13;
        int anotherInitialValue = 14;
        cache.get(anotherKey, () -> anotherInitialValue);
        cache.invalidate(anotherKey);

        Assert.assertFalse("Should not replace after invalidation", cache.asMap().replace(anotherKey, anotherInitialValue, 15));
        Assert.assertEquals("Cache should only contain original key after invalidation", ImmutableSet.of(key), cache.asMap().keySet());
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testDisabledCache() throws Exception {
        Exception exception = Assert.assertThrows(IllegalStateException.class, () ->
                EvictableCacheBuilder.newBuilder()
                        .maximumSize(0)
                        .build());

        Assert.assertEquals("Even when cache is disabled, the loads are synchronized and both load results and failures are shared between threads. "
                        + "This is rarely desired, thus builder caller is expected to either opt-in into this behavior with shareResultsAndFailuresEvenIfDisabled(), "
                        + "or choose not to share results (and failures) between concurrent invocations with shareNothingWhenDisabled().",
                exception.getMessage());

        testDisabledCache(
                EvictableCacheBuilder.newBuilder()
                        .maximumSize(0)
                        .shareNothingWhenDisabled()
                        .build());

        testDisabledCache(
                EvictableCacheBuilder.newBuilder()
                        .maximumSize(0)
                        .shareResultsAndFailuresEvenIfDisabled()
                        .build());
    }

    private void testDisabledCache(Cache<Integer, Integer> cache) throws Exception {
        for (int i = 0; i < 10; i++) {
            int value = i * 10;
            Assert.assertEquals(value, cache.get(i, () -> value).intValue());
        }

        cache.cleanUp();
        Assert.assertEquals(0, cache.size());
        Assert.assertTrue(cache.asMap().keySet().isEmpty());
        Assert.assertTrue(cache.asMap().values().isEmpty());
    }

    private static class CacheStatsAssertions {
        public static CacheStatsAssertions assertCacheStats(Cache<?, ?> cache) {
            Objects.requireNonNull(cache, "cache is null");
            return assertCacheStats(cache::stats);
        }

        public static CacheStatsAssertions assertCacheStats(Supplier<CacheStats> statsSupplier) {
            return new CacheStatsAssertions(statsSupplier);
        }

        private final Supplier<CacheStats> stats;

        private long loads;
        private long hits;
        private long misses;

        private CacheStatsAssertions(Supplier<CacheStats> stats) {
            this.stats = Objects.requireNonNull(stats, "stats is null");
        }

        public CacheStatsAssertions loads(long value) {
            this.loads = value;
            return this;
        }

        public CacheStatsAssertions hits(long value) {
            this.hits = value;
            return this;
        }

        public CacheStatsAssertions misses(long value) {
            this.misses = value;
            return this;
        }

        public void afterRunning(Runnable runnable) {
            try {
                calling(() -> {
                    runnable.run();
                    return null;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public <T> T calling(Callable<T> callable)
                throws Exception {
            CacheStats beforeStats = stats.get();
            T value = callable.call();
            CacheStats afterStats = stats.get();

            long loadDelta = afterStats.loadCount() - beforeStats.loadCount();
            long missesDelta = afterStats.missCount() - beforeStats.missCount();
            long hitsDelta = afterStats.hitCount() - beforeStats.hitCount();

            Assert.assertEquals(loads, loadDelta);
            Assert.assertEquals(hits, hitsDelta);
            Assert.assertEquals(misses, missesDelta);

            return value;
        }
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoadStats()
            throws Exception {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build();

        Assert.assertEquals(new CacheStats(0, 0, 0, 0, 0, 0), cache.stats());

        String value = CacheStatsAssertions.assertCacheStats(cache)
                .misses(1)
                .loads(1)
                .calling(() -> cache.get(42, () -> "abc"));
        Assert.assertEquals("abc", value);

        value = CacheStatsAssertions.assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(42, () -> "xyz"));
        Assert.assertEquals("abc", value);

        // with equal, but not the same key
        value = CacheStatsAssertions.assertCacheStats(cache)
                .hits(1)
                .calling(() -> cache.get(newInteger(42), () -> "xyz"));
        Assert.assertEquals("abc", value);
    }

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoadFailure()
            throws Exception {
        Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(0)
                .expireAfterWrite(0, TimeUnit.DAYS)
                .shareResultsAndFailuresEvenIfDisabled()
                .build();
        int key = 10;

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Exchanger<Thread> exchanger = new Exchanger<>();
            CountDownLatch secondUnblocked = new CountDownLatch(1);

            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                boolean first = i == 0;
                futures.add(executor.submit(() -> {
                    if (!first) {
                        // Wait for the first one to start the call
                        exchanger.exchange(Thread.currentThread(), 10, TimeUnit.SECONDS);
                        // Prove that we are back in RUNNABLE state.
                        secondUnblocked.countDown();
                    }
                    return cache.get(key, () -> {
                        if (first) {
                            Thread secondThread = exchanger.exchange(null, 10, TimeUnit.SECONDS);
                            Assert.assertTrue(secondUnblocked.await(10, TimeUnit.SECONDS));
                            // Wait for the second one to hang inside the cache.get call.
                            long start = System.nanoTime();
                            while (!Thread.currentThread().isInterrupted()) {
                                try {
                                    Assert.assertNotEquals(Thread.State.RUNNABLE, secondThread.getState());
                                    break;
                                } catch (Exception | AssertionError e) {
                                    if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(30)) {
                                        throw e;
                                    }
                                }
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            }
                            throw new RuntimeException("first attempt is poised to fail");
                        }
                        return "success";
                    });
                }));
            }

            List<String> results = new ArrayList<>();
            for (Future<String> future : futures) {
                try {
                    results.add(future.get());
                } catch (ExecutionException e) {
                    results.add(e.getCause().toString());
                }
            }

            // Note: if this starts to fail, that suggests that Guava implementation changed and NoopCache may be redundant now.
            String expectedError = "com.google.common.util.concurrent.UncheckedExecutionException: "
                    + "java.lang.RuntimeException: first attempt is poised to fail";
            Assert.assertEquals(2, results.size());
            Assert.assertEquals(expectedError, results.get(0));
            Assert.assertEquals(expectedError, results.get(1));
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @SuppressModernizer
    private static Integer newInteger(int value) {
        Integer integer = value;
        @SuppressWarnings({"UnnecessaryBoxing", "BoxedPrimitiveConstructor", "CachedNumberConstructorCall", "removal"})
        Integer newInteger = new Integer(value);
        Assert.assertNotSame(integer, newInteger);
        return newInteger;
    }

    /**
     * Test that the loader is invoked only once for concurrent invocations of {{@link LoadingCache#get(Object, Callable)} with equal keys.
     * This is a behavior of Guava Cache as well. While this is necessarily desirable behavior (see
     * <a href="https://github.com/trinodb/trino/issues/11067">https://github.com/trinodb/trino/issues/11067</a>),
     * the test exists primarily to document current state and support discussion, should the current state change.
     */
    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testConcurrentGetWithCallableShareLoad()
            throws Exception {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger concurrentInvocations = new AtomicInteger();

        Cache<Integer, Integer> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10_000)
                .build();

        int threads = 2;
        int invocationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    for (int invocation = 0; invocation < invocationsPerThread; invocation++) {
                        int key = invocation;
                        barrier.await(10, TimeUnit.SECONDS);
                        int value = cache.get(key, () -> {
                            loads.incrementAndGet();
                            int invocations = concurrentInvocations.incrementAndGet();
                            Preconditions.checkState(invocations == 1, "There should be no concurrent invocations, cache should do load sharing when get() invoked for same key");
                            Thread.sleep(1);
                            concurrentInvocations.decrementAndGet();
                            return -key;
                        });
                        Assert.assertEquals(-invocation, value);
                    }
                    return null;
                }));
            }

            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
            Assert.assertTrue(
                    String.format(
                            "loads (%d) should be between %d and %d",
                            loads.intValue(),
                            invocationsPerThread,
                            threads * invocationsPerThread - 1),
                    loads.intValue() >= invocationsPerThread && loads.intValue() <= threads * invocationsPerThread - 1);
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    enum Invalidation {
        INVALIDATE_KEY,
        INVALIDATE_PREDEFINED_KEYS,
        INVALIDATE_SELECTED_KEYS,
        INVALIDATE_ALL,
        /**/;
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testInvalidateOngoingLoad()
            throws Exception {
        for (Invalidation invalidation : Invalidation.values()) {
            Cache<Integer, String> cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(10_000)
                    .build();
            Integer key = 42;

            CountDownLatch loadOngoing = new CountDownLatch(1);
            CountDownLatch invalidated = new CountDownLatch(1);
            CountDownLatch getReturned = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                // thread A
                Future<String> threadA = executor.submit(() -> {
                    String value = cache.get(key, () -> {
                        loadOngoing.countDown(); // 1
                        Assert.assertTrue(invalidated.await(10, TimeUnit.SECONDS)); // 2
                        return "stale value";
                    });
                    getReturned.countDown(); // 3
                    return value;
                });

                // thread B
                Future<String> threadB = executor.submit(() -> {
                    Assert.assertTrue(loadOngoing.await(10, TimeUnit.SECONDS)); // 1

                    switch (invalidation) {
                        case INVALIDATE_KEY:
                            cache.invalidate(key);
                            break;
                        case INVALIDATE_PREDEFINED_KEYS:
                            cache.invalidateAll(ImmutableList.of(key));
                            break;
                        case INVALIDATE_SELECTED_KEYS:
                            Set<Integer> keys = cache.asMap().keySet().stream()
                                    .filter(foundKey -> (int) foundKey == key)
                                    .collect(ImmutableSet.toImmutableSet());
                            cache.invalidateAll(keys);
                            break;
                        case INVALIDATE_ALL:
                            cache.invalidateAll();
                            break;
                        default:
                            throw new IllegalArgumentException();
                    }

                    invalidated.countDown(); // 2
                    // Cache may persist value after loader returned, but before `cache.get(...)` returned. Ensure the latter completed.
                    Assert.assertTrue(getReturned.await(10, TimeUnit.SECONDS)); // 3

                    return cache.get(key, () -> "fresh value");
                });

                Assert.assertEquals("stale value", threadA.get());
                Assert.assertEquals("fresh value", threadB.get());
            } finally {
                executor.shutdownNow();
                Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * Covers https://github.com/google/guava/issues/1881
     */
    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testInvalidateAndLoadConcurrently()
            throws Exception {
        for (Invalidation invalidation : Invalidation.values()) {
            int[] primes = {2, 3, 5, 7};
            AtomicLong remoteState = new AtomicLong(1);

            Cache<Integer, Long> cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(10_000)
                    .build();
            Integer key = 42;
            int threads = 4;

            CyclicBarrier barrier = new CyclicBarrier(threads);
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            try {
                List<Future<Void>> futures = IntStream.range(0, threads)
                        .mapToObj(threadNumber -> executor.submit(() -> {
                            // prime the cache
                            Assert.assertEquals(1L, (long) cache.get(key, remoteState::get));
                            int prime = primes[threadNumber];

                            barrier.await(10, TimeUnit.SECONDS);

                            // modify underlying state
                            remoteState.updateAndGet(current -> current * prime);

                            // invalidate
                            switch (invalidation) {
                                case INVALIDATE_KEY:
                                    cache.invalidate(key);
                                    break;
                                case INVALIDATE_PREDEFINED_KEYS:
                                    cache.invalidateAll(ImmutableList.of(key));
                                    break;
                                case INVALIDATE_SELECTED_KEYS:
                                    Set<Integer> keys = cache.asMap().keySet().stream()
                                            .filter(foundKey -> (int) foundKey == key)
                                            .collect(ImmutableSet.toImmutableSet());
                                    cache.invalidateAll(keys);
                                    break;
                                case INVALIDATE_ALL:
                                    cache.invalidateAll();
                                    break;
                                default:
                                    throw new IllegalArgumentException();
                            }

                            // read through cache
                            long current = cache.get(key, remoteState::get);
                            if (current % prime != 0) {
                                throw new AssertionError(String.format("The value read through cache (%s) in thread (%s) is not divisible by (%s)", current, threadNumber, prime));
                            }

                            return (Void) null;
                        }))
                        .collect(ImmutableList.toImmutableList());

                for (Future<?> future : futures) {
                    try {
                        future.get(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        throw new RuntimeException("Failed to get future value", e);
                    }
                }

                Assert.assertEquals(2 * 3 * 5 * 7, remoteState.get());
                Assert.assertEquals(remoteState.get(), (long) cache.get(key, remoteState::get));
            } finally {
                executor.shutdownNow();
                Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testPutOnEmptyCacheImplementation() {
        for (DisabledCacheImplementation disabledCacheImplementation : DisabledCacheImplementation.values()) {
            Cache<Object, Object> cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(0)
                    .disabledCacheImplementation(disabledCacheImplementation)
                    .build();
            Map<Object, Object> cacheMap = cache.asMap();

            int key = 0;
            int value = 1;
            Assert.assertNull(cacheMap.put(key, value));
            Assert.assertNull(cacheMap.put(key, value));
            Assert.assertNull(cacheMap.putIfAbsent(key, value));
            Assert.assertNull(cacheMap.putIfAbsent(key, value));
        }
    }

    @Test
    public void testPutOnNonEmptyCacheImplementation() {
        Cache<Object, Object> cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(10)
                .build();
        Map<Object, Object> cacheMap = cache.asMap();

        int key = 0;
        int value = 1;

        Exception putException = Assert.assertThrows("put operation should throw UnsupportedOperationException",
                UnsupportedOperationException.class,
                () -> cacheMap.put(key, value));
        Assert.assertEquals(
                "The operation is not supported, as in inherently races with cache invalidation. Use get(key, callable) instead.",
                putException.getMessage());

        Exception putIfAbsentException = Assert.assertThrows("putIfAbsent operation should throw UnsupportedOperationException",
                UnsupportedOperationException.class,
                () -> cacheMap.putIfAbsent(key, value));
        Assert.assertEquals(
                "The operation is not supported, as in inherently races with cache invalidation",
                putIfAbsentException.getMessage());
    }
}
