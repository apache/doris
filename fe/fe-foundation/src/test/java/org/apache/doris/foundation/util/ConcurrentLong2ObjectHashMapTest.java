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
import com.google.gson.reflect.TypeToken;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
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

class ConcurrentLong2ObjectHashMapTest {

    @Test
    void testPutAndGet() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Assertions.assertNull(map.put(1L, "one"));
        Assertions.assertEquals("one", map.get(1L));
        Assertions.assertEquals("one", map.put(1L, "ONE"));
        Assertions.assertEquals("ONE", map.get(1L));
    }

    @Test
    void testGetMissingKey() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Assertions.assertNull(map.get(999L));
    }

    @Test
    void testGetOrDefault() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        Assertions.assertEquals("one", map.getOrDefault(1L, "default"));
        Assertions.assertEquals("default", map.getOrDefault(2L, "default"));
    }

    @Test
    void testRemove() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        Assertions.assertEquals("one", map.remove(1L));
        Assertions.assertNull(map.get(1L));
        Assertions.assertNull(map.remove(1L));
    }

    @Test
    void testContainsKey() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Assertions.assertFalse(map.containsKey(1L));
        map.put(1L, "one");
        Assertions.assertTrue(map.containsKey(1L));
    }

    @Test
    void testContainsValue() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        map.put(2L, "two");
        Assertions.assertTrue(map.containsValue("one"));
        Assertions.assertFalse(map.containsValue("three"));
    }

    @Test
    void testSizeAndIsEmpty() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Assertions.assertTrue(map.isEmpty());
        Assertions.assertEquals(0, map.size());
        map.put(1L, "one");
        map.put(2L, "two");
        Assertions.assertFalse(map.isEmpty());
        Assertions.assertEquals(2, map.size());
    }

    @Test
    void testClear() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        map.put(2L, "two");
        map.clear();
        Assertions.assertTrue(map.isEmpty());
        Assertions.assertEquals(0, map.size());
    }

    @Test
    void testPutAll() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Map<Long, String> source = new HashMap<>();
        source.put(1L, "one");
        source.put(2L, "two");
        source.put(3L, "three");
        map.putAll(source);
        Assertions.assertEquals(3, map.size());
        Assertions.assertEquals("two", map.get(2L));
    }

    @Test
    void testPutIfAbsent() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Assertions.assertNull(map.putIfAbsent(1L, "one"));
        Assertions.assertEquals("one", map.putIfAbsent(1L, "ONE"));
        Assertions.assertEquals("one", map.get(1L));
    }

    @Test
    void testComputeIfAbsent() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        String val = map.computeIfAbsent(1L, k -> "computed-" + k);
        Assertions.assertEquals("computed-1", val);
        // Should not recompute
        String val2 = map.computeIfAbsent(1L, k -> "recomputed-" + k);
        Assertions.assertEquals("computed-1", val2);
    }

    @Test
    void testComputeIfPresent() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        // Not present — should return null
        Assertions.assertNull(map.computeIfPresent(1L, (k, v) -> v + "-updated"));

        map.put(1L, "one");
        String val = map.computeIfPresent(1L, (k, v) -> v + "-updated");
        Assertions.assertEquals("one-updated", val);
        Assertions.assertEquals("one-updated", map.get(1L));

        // Return null to remove
        Assertions.assertNull(map.computeIfPresent(1L, (k, v) -> null));
        Assertions.assertFalse(map.containsKey(1L));
    }

    @Test
    void testEntrySet() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        map.put(2L, "two");

        ObjectSet<Long2ObjectMap.Entry<String>> entries = map.long2ObjectEntrySet();
        Assertions.assertEquals(2, entries.size());

        Set<Long> keys = new HashSet<>();
        for (Long2ObjectMap.Entry<String> entry : entries) {
            keys.add(entry.getLongKey());
        }
        Assertions.assertTrue(keys.contains(1L));
        Assertions.assertTrue(keys.contains(2L));
    }

    @Test
    void testKeySet() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(10L, "ten");
        map.put(20L, "twenty");
        LongSet keys = map.keySet();
        Assertions.assertEquals(2, keys.size());
        Assertions.assertTrue(keys.contains(10L));
        Assertions.assertTrue(keys.contains(20L));
    }

    @Test
    void testValues() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        map.put(2L, "two");
        ObjectCollection<String> values = map.values();
        Assertions.assertEquals(2, values.size());
        Assertions.assertTrue(values.contains("one"));
        Assertions.assertTrue(values.contains("two"));
    }

    @Test
    void testStream() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        for (long i = 0; i < 100; i++) {
            map.put(i, "val-" + i);
        }
        long count = map.values().stream().filter(v -> v.startsWith("val-")).count();
        Assertions.assertEquals(100, count);
    }

    @Test
    void testForEach() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(1L, "one");
        map.put(2L, "two");
        Map<Long, String> collected = new HashMap<>();
        map.forEach((ConcurrentLong2ObjectHashMap.LongObjConsumer<String>) collected::put);
        Assertions.assertEquals(2, collected.size());
        Assertions.assertEquals("one", collected.get(1L));
    }

    @Test
    void testNullValuesRejected() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        Assertions.assertThrows(NullPointerException.class, () -> map.put(1L, null));
        Assertions.assertThrows(NullPointerException.class, () -> map.putIfAbsent(1L, null));
        Assertions.assertThrows(NullPointerException.class, () -> map.replace(1L, null));
        Assertions.assertThrows(NullPointerException.class, () -> map.replace(1L, "old", null));
    }

    @Test
    void testLargeMap() {
        ConcurrentLong2ObjectHashMap<Long> map = new ConcurrentLong2ObjectHashMap<>();
        int count = 100_000;
        for (long i = 0; i < count; i++) {
            map.put(i, Long.valueOf(i * 2));
        }
        Assertions.assertEquals(count, map.size());
        for (long i = 0; i < count; i++) {
            Assertions.assertEquals(Long.valueOf(i * 2), map.get(i));
        }
    }

    @Test
    void testCustomSegmentCount() {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>(4);
        for (long i = 0; i < 1000; i++) {
            map.put(i, "v" + i);
        }
        Assertions.assertEquals(1000, map.size());
    }

    @Test
    void testInvalidSegmentCount() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConcurrentLong2ObjectHashMap<>(3));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConcurrentLong2ObjectHashMap<>(0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConcurrentLong2ObjectHashMap<>(-1));
    }

    // ---- Concurrency tests ----

    @Test
    void testConcurrentPuts() throws Exception {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        int threads = 8;
        int keysPerThread = 10_000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < keysPerThread; i++) {
                    long key = (long) threadId * keysPerThread + i;
                    map.put(key, "t" + threadId + "-" + i);
                }
                latch.countDown();
            });
        }
        latch.await();
        executor.shutdown();

        Assertions.assertEquals(threads * keysPerThread, map.size());
    }

    @Test
    void testConcurrentReadWrite() throws Exception {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        // Pre-populate
        for (long i = 0; i < 1000; i++) {
            map.put(i, "v" + i);
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
                            // Reader
                            map.get(key);
                            map.containsKey(key);
                        } else {
                            // Writer
                            map.put(key + 1000L * threadId, "new-" + i);
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
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        int threads = 16;
        long sharedKey = 42L;
        AtomicInteger computeCount = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<String>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(executor.submit(() ->
                    map.computeIfAbsent(sharedKey, k -> {
                        computeCount.incrementAndGet();
                        return "computed";
                    })
            ));
        }
        Set<String> results = new HashSet<>();
        for (Future<String> f : futures) {
            results.add(f.get());
        }
        executor.shutdown();

        // All threads should get the same value
        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.contains("computed"));
        // The function should have been called exactly once
        Assertions.assertEquals(1, computeCount.get());
    }

    @Test
    void testConcurrentIterationDuringModification() throws Exception {
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        for (long i = 0; i < 1000; i++) {
            map.put(i, "v" + i);
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
                            // Iterator - should not throw
                            map.keySet();
                            map.values();
                            map.long2ObjectEntrySet();
                        } else {
                            // Modifier
                            map.put(1000L + threadId * 100 + i, "new");
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
        ConcurrentLong2ObjectHashMap<String> map = new ConcurrentLong2ObjectHashMap<>();
        map.put(100L, "hundred");
        map.put(200L, "two-hundred");

        String json = new Gson().toJson(map);

        Type type = new TypeToken<ConcurrentLong2ObjectHashMap<String>>() {}.getType();
        ConcurrentLong2ObjectHashMap<String> deserialized = new Gson().fromJson(json, type);

        Assertions.assertEquals(2, deserialized.size());
        Assertions.assertEquals("hundred", deserialized.get(100L));
        Assertions.assertEquals("two-hundred", deserialized.get(200L));
    }

    @Test
    void testGsonFormatCompatibleWithConcurrentHashMap() {
        // Verify the JSON format matches what ConcurrentHashMap<Long, String> produces
        ConcurrentHashMap<Long, String> chm = new ConcurrentHashMap<>();
        chm.put(1L, "one");
        chm.put(2L, "two");
        String chmJson = new Gson().toJson(chm);

        ConcurrentLong2ObjectHashMap<String> fastMap = new ConcurrentLong2ObjectHashMap<>();
        fastMap.put(1L, "one");
        fastMap.put(2L, "two");
        String fastJson = new Gson().toJson(fastMap);

        // Both should be parseable as the same JSON object
        Gson gson = new Gson();
        Map<?, ?> chmParsed = gson.fromJson(chmJson, Map.class);
        Map<?, ?> fastParsed = gson.fromJson(fastJson, Map.class);
        Assertions.assertEquals(chmParsed, fastParsed);
    }
}
