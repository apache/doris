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

package org.apache.doris.foundation.util;

import com.google.gson.Gson;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class ConcurrentLong2LongHashMapTest {

    @Test
    void testPutAndGet() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        Assertions.assertEquals(100L, map.get(1L));
        map.put(1L, 200L);
        Assertions.assertEquals(200L, map.get(1L));
    }

    @Test
    void testGetMissingKeyReturnsDefaultReturnValue() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        // Default return value is 0L
        Assertions.assertEquals(0L, map.get(999L));
    }

    @Test
    void testGetOrDefault() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        Assertions.assertEquals(100L, map.getOrDefault(1L, -1L));
        Assertions.assertEquals(-1L, map.getOrDefault(2L, -1L));
    }

    @Test
    void testRemove() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        Assertions.assertEquals(100L, map.remove(1L));
        Assertions.assertFalse(map.containsKey(1L));
        // Remove non-existent key returns defaultReturnValue
        Assertions.assertEquals(0L, map.remove(1L));
    }

    @Test
    void testContainsKey() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        Assertions.assertFalse(map.containsKey(1L));
        map.put(1L, 0L);
        Assertions.assertTrue(map.containsKey(1L));
    }

    @Test
    void testContainsValue() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        map.put(2L, 200L);
        Assertions.assertTrue(map.containsValue(100L));
        Assertions.assertFalse(map.containsValue(300L));
    }

    @Test
    void testSizeAndIsEmpty() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        Assertions.assertTrue(map.isEmpty());
        Assertions.assertEquals(0, map.size());
        map.put(1L, 100L);
        map.put(2L, 200L);
        Assertions.assertFalse(map.isEmpty());
        Assertions.assertEquals(2, map.size());
    }

    @Test
    void testClear() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        map.put(2L, 200L);
        map.clear();
        Assertions.assertTrue(map.isEmpty());
        Assertions.assertEquals(0, map.size());
    }

    @Test
    void testPutAll() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        Map<Long, Long> source = new HashMap<>();
        source.put(1L, 100L);
        source.put(2L, 200L);
        source.put(3L, 300L);
        map.putAll(source);
        Assertions.assertEquals(3, map.size());
        Assertions.assertEquals(200L, map.get(2L));
    }

    @Test
    void testPutIfAbsent() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        Assertions.assertEquals(0L, map.putIfAbsent(1L, 100L));
        Assertions.assertEquals(100L, map.putIfAbsent(1L, 200L));
        Assertions.assertEquals(100L, map.get(1L));
    }

    @Test
    void testComputeIfAbsent() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        long val = map.computeIfAbsent(1L, k -> k * 10);
        Assertions.assertEquals(10L, val);
        // Should not recompute
        long val2 = map.computeIfAbsent(1L, k -> k * 20);
        Assertions.assertEquals(10L, val2);
    }

    // ---- addTo tests ----

    @Test
    void testAddToNewKey() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        long result = map.addTo(1L, 5L);
        Assertions.assertEquals(5L, result);
        Assertions.assertEquals(5L, map.get(1L));
    }

    @Test
    void testAddToExistingKey() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 10L);
        long result = map.addTo(1L, 5L);
        Assertions.assertEquals(15L, result);
        Assertions.assertEquals(15L, map.get(1L));
    }

    @Test
    void testAddToNegative() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 10L);
        long result = map.addTo(1L, -3L);
        Assertions.assertEquals(7L, result);
    }

    // ---- Iteration tests ----

    @Test
    void testEntrySet() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        map.put(2L, 200L);

        ObjectSet<Long2LongMap.Entry> entries = map.long2LongEntrySet();
        Assertions.assertEquals(2, entries.size());

        Set<Long> keys = new HashSet<>();
        for (Long2LongMap.Entry entry : entries) {
            keys.add(entry.getLongKey());
        }
        Assertions.assertTrue(keys.contains(1L));
        Assertions.assertTrue(keys.contains(2L));
    }

    @Test
    void testKeySet() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(10L, 100L);
        map.put(20L, 200L);
        LongSet keys = map.keySet();
        Assertions.assertEquals(2, keys.size());
        Assertions.assertTrue(keys.contains(10L));
        Assertions.assertTrue(keys.contains(20L));
    }

    @Test
    void testValues() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        map.put(2L, 200L);
        it.unimi.dsi.fastutil.longs.LongCollection values = map.values();
        Assertions.assertEquals(2, values.size());
        Assertions.assertTrue(values.contains(100L));
        Assertions.assertTrue(values.contains(200L));
    }

    @Test
    void testForEach() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(1L, 100L);
        map.put(2L, 200L);
        Map<Long, Long> collected = new HashMap<>();
        map.forEach((ConcurrentLong2LongHashMap.LongLongConsumer) collected::put);
        Assertions.assertEquals(2, collected.size());
        Assertions.assertEquals(100L, (long) collected.get(1L));
    }

    // ---- Large map test ----

    @Test
    void testLargeMap() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        int count = 100_000;
        for (long i = 0; i < count; i++) {
            map.put(i, i * 3);
        }
        Assertions.assertEquals(count, map.size());
        for (long i = 0; i < count; i++) {
            Assertions.assertEquals(i * 3, map.get(i));
        }
    }

    @Test
    void testCustomSegmentCount() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap(4);
        for (long i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        Assertions.assertEquals(1000, map.size());
    }

    @Test
    void testInvalidSegmentCount() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConcurrentLong2LongHashMap(3));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConcurrentLong2LongHashMap(0));
    }

    // ---- Concurrency tests ----

    @Test
    void testConcurrentPuts() throws Exception {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        int threads = 8;
        int keysPerThread = 10_000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < keysPerThread; i++) {
                    long key = (long) threadId * keysPerThread + i;
                    map.put(key, key * 2);
                }
                latch.countDown();
            });
        }
        latch.await();
        executor.shutdown();
        Assertions.assertEquals(threads * keysPerThread, map.size());
    }

    @Test
    void testConcurrentAddTo() throws Exception {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        int threads = 16;
        int incrementsPerThread = 10_000;
        long key = 42L;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                for (int i = 0; i < incrementsPerThread; i++) {
                    map.addTo(key, 1L);
                }
                latch.countDown();
            });
        }
        latch.await();
        executor.shutdown();

        Assertions.assertEquals((long) threads * incrementsPerThread, map.get(key));
    }

    @Test
    void testConcurrentReadWrite() throws Exception {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        for (long i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < 5000; i++) {
                        long key = i % 1000;
                        if (threadId % 2 == 0) {
                            map.get(key);
                            map.containsKey(key);
                        } else {
                            map.put(key + 1000L * threadId, (long) i);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();
        Assertions.assertEquals(0, errors.get());
    }

    @Test
    void testConcurrentComputeIfAbsent() throws Exception {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        int threads = 16;
        long sharedKey = 42L;
        AtomicInteger computeCount = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<Long>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(executor.submit(() ->
                    map.computeIfAbsent(sharedKey, k -> {
                        computeCount.incrementAndGet();
                        return k * 10;
                    })
            ));
        }
        Set<Long> results = new HashSet<>();
        for (Future<Long> f : futures) {
            results.add(f.get());
        }
        executor.shutdown();

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.contains(420L));
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    void testConcurrentIterationDuringModification() throws Exception {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        for (long i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        int threads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        if (threadId % 2 == 0) {
                            map.keySet();
                            map.values();
                            map.long2LongEntrySet();
                        } else {
                            map.put(1000L + threadId * 100 + i, (long) i);
                            map.remove((long) (i % 500));
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();
        Assertions.assertEquals(0, errors.get());
    }

    // ---- Gson serialization tests ----

    @Test
    void testGsonRoundTrip() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        map.put(100L, 1000L);
        map.put(200L, 2000L);

        String json = new Gson().toJson(map);

        ConcurrentLong2LongHashMap deserialized = new Gson().fromJson(json, ConcurrentLong2LongHashMap.class);

        Assertions.assertEquals(2, deserialized.size());
        Assertions.assertEquals(1000L, deserialized.get(100L));
        Assertions.assertEquals(2000L, deserialized.get(200L));
    }

    @Test
    void testGsonFormatCompatibleWithConcurrentHashMap() {
        ConcurrentHashMap<Long, Long> chm = new ConcurrentHashMap<>();
        chm.put(1L, 100L);
        chm.put(2L, 200L);
        String chmJson = new Gson().toJson(chm);

        ConcurrentLong2LongHashMap fastMap = new ConcurrentLong2LongHashMap();
        fastMap.put(1L, 100L);
        fastMap.put(2L, 200L);
        String fastJson = new Gson().toJson(fastMap);

        Gson gson = new Gson();
        Map<?, ?> chmParsed = gson.fromJson(chmJson, Map.class);
        Map<?, ?> fastParsed = gson.fromJson(fastJson, Map.class);
        Assertions.assertEquals(chmParsed, fastParsed);
    }

    @Test
    void testDefaultReturnValueBehavior() {
        ConcurrentLong2LongHashMap map = new ConcurrentLong2LongHashMap();
        // Primitive get returns 0L (defaultReturnValue) for missing keys
        Assertions.assertEquals(0L, map.get(999L));

        // Store 0L explicitly
        map.put(1L, 0L);
        Assertions.assertTrue(map.containsKey(1L));
        Assertions.assertEquals(0L, map.get(1L));

        // getOrDefault returns the specified default for missing keys
        Assertions.assertEquals(0L, map.getOrDefault(999L, map.defaultReturnValue()));
        // Boxed get via Map<Long,Long> interface returns null for missing keys
        Assertions.assertNull(((Map<Long, Long>) map).get(999L));

        // Changing defaultReturnValue is not supported
        Assertions.assertThrows(UnsupportedOperationException.class, () -> map.defaultReturnValue(-1L));
    }
}
