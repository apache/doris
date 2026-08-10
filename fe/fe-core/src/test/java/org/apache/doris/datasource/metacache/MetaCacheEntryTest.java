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

package org.apache.doris.datasource.metacache;

import org.apache.doris.connector.cache.CacheSpec;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaCacheEntryTest {
    private static final CacheSpec ENABLED = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100L);

    @Test
    public void testLoadMutationAndStatsUseSharedRuntime() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger loads = new AtomicInteger();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "objects", key -> loads.incrementAndGet(), ENABLED, executor, false, 8);

            Assert.assertEquals(Integer.valueOf(1), entry.get("key"));
            Assert.assertEquals(Integer.valueOf(1), entry.get("key"));
            Assert.assertEquals(1, loads.get());
            Assert.assertEquals(Integer.valueOf(2), entry.compute("key", (key, value) -> value + 1));
            Assert.assertEquals(Integer.valueOf(2), entry.getIfPresent("key"));
            entry.invalidateKey("key");
            Assert.assertNull(entry.getIfPresent("key"));

            MetaCacheEntryStats stats = entry.stats();
            Assert.assertTrue(stats.isEffectiveEnabled());
            Assert.assertEquals(1L, stats.getLoadSuccessCount());
            Assert.assertTrue(stats.getRequestCount() >= 4L);
            Assert.assertTrue(stats.getInvalidateCount() >= 1L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testContextualOnlyAndDisabledEntry() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> contextual = new MetaCacheEntry<>(
                    "contextual", null, ENABLED, executor, false, true);
            Assert.assertThrows(UnsupportedOperationException.class, () -> contextual.get("key"));
            Assert.assertEquals(Integer.valueOf(3), contextual.get("key", String::length));

            CacheSpec disabledSpec = CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 100L);
            AtomicInteger actions = new AtomicInteger();
            MetaCacheEntry<String, Integer> disabled = new MetaCacheEntry<>(
                    "disabled", String::length, disabledSpec, executor, false);
            Assert.assertEquals(Integer.valueOf(3), disabled.getAndRunIfCurrent(
                    "key", (key, value) -> actions.incrementAndGet()));
            Assert.assertEquals(1, actions.get());
            Assert.assertNull(disabled.getIfPresent("key"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testMutationAndAuxiliaryIndexSharePublicationWindow() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<String> index = new ArrayList<>();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "objects", String::length, ENABLED, executor, false);

            entry.computeAndRun("table", (key, value) -> 5, () -> index.add("table"));
            Assert.assertEquals(Integer.valueOf(5), entry.getIfPresent("table"));
            Assert.assertEquals(List.of("table"), index);

            entry.invalidateKeyAndRun("table", index::clear);
            Assert.assertNull(entry.getIfPresent("table"));
            Assert.assertTrue(index.isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFailedFinalValidationDoesNotModifyCachedObject() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<>(
                    "objects", String::length, ENABLED, executor, false);
            entry.put("table", 1);

            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.computeAfterValidation(
                            "table", (key, value) -> 2, () -> {
                                throw new IllegalStateException("identity conflict");
                            }));

            Assert.assertEquals(Integer.valueOf(1), entry.getIfPresent("table"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentIdentityConflictPublishesOneConsistentPair() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch prechecksComplete = new CountDownLatch(2);
        CountDownLatch startPublication = new CountDownLatch(1);
        try {
            MetaCacheEntry<String, Long> entry = new MetaCacheEntry<>(
                    "objects", key -> -1L, ENABLED, refreshExecutor, false);
            IdNameIndex index = new IdNameIndex("table");

            Future<Long> first = workers.submit(() -> publishIdentity(
                    entry, index, 1L, prechecksComplete, startPublication));
            Future<Long> second = workers.submit(() -> publishIdentity(
                    entry, index, 2L, prechecksComplete, startPublication));
            Assert.assertTrue(prechecksComplete.await(3L, TimeUnit.SECONDS));
            startPublication.countDown();

            Long firstResult = resultOrNull(first);
            Long secondResult = resultOrNull(second);
            Assert.assertTrue((firstResult == null) != (secondResult == null));

            long winner = firstResult == null ? secondResult : firstResult;
            long loser = winner == 1L ? 2L : 1L;
            Assert.assertEquals(Long.valueOf(winner), entry.getIfPresent("table"));
            Assert.assertEquals("table", index.getName(winner));
            Assert.assertNull(index.getName(loser));
        } finally {
            startPublication.countDown();
            workers.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentInvalidationRejectsStaleAuxiliaryIndexAction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch valueLoaded = new CountDownLatch(1);
        CountDownLatch continueAction = new CountDownLatch(1);
        AtomicInteger actions = new AtomicInteger();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "objects", String::length, ENABLED, refreshExecutor, false) {
                @Override
                protected void beforeCurrentValueActionForTest(String key, Integer value) {
                    valueLoaded.countDown();
                    await(continueAction);
                }
            };

            Future<Integer> load = worker.submit(() -> entry.getAndRunIfCurrent(
                    "table", (key, value) -> actions.incrementAndGet()));
            Assert.assertTrue(valueLoaded.await(3L, TimeUnit.SECONDS));
            entry.invalidateKey("table");
            continueAction.countDown();

            Assert.assertEquals(Integer.valueOf(5), load.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, actions.get());
            Assert.assertNull(entry.getIfPresent("table"));
            Assert.assertEquals(0, entry.activeActionReferenceCountForTest());
        } finally {
            continueAction.countDown();
            worker.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testFailedValidationDoesNotCancelCurrentAuxiliaryIndexAction() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch valueLoaded = new CountDownLatch(1);
        CountDownLatch continueAction = new CountDownLatch(1);
        AtomicInteger actions = new AtomicInteger();
        try {
            MetaCacheEntry<String, Integer> entry = new MetaCacheEntry<String, Integer>(
                    "objects", String::length, ENABLED, refreshExecutor, false) {
                @Override
                protected void beforeCurrentValueActionForTest(String key, Integer value) {
                    valueLoaded.countDown();
                    await(continueAction);
                }
            };

            Future<Integer> load = worker.submit(() -> entry.getAndRunIfCurrent(
                    "table", (key, value) -> actions.incrementAndGet()));
            Assert.assertTrue(valueLoaded.await(3L, TimeUnit.SECONDS));
            Assert.assertThrows(IllegalStateException.class,
                    () -> entry.computeAfterValidation(
                            "table", (key, value) -> 6, () -> {
                                throw new IllegalStateException("identity conflict");
                            }));
            continueAction.countDown();

            Assert.assertEquals(Integer.valueOf(5), load.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(1, actions.get());
            Assert.assertEquals(Integer.valueOf(5), entry.getIfPresent("table"));
        } finally {
            continueAction.countDown();
            worker.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testRemovalListenerAndLazyStripeAllocation() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<String> removals = new ArrayList<>();
        try {
            MetaCacheEntry<String, Integer> entry = MetaCacheEntry.withSyncRemovalListener(
                    "objects", String::length, ENABLED, executor, 8,
                    (key, value, cause) -> removals.add(key + "=" + value));
            Assert.assertEquals(0, entry.initializedStripeCountForTest());
            entry.put("table", 5);
            Assert.assertEquals(1, entry.initializedStripeCountForTest());
            entry.invalidateAll();
            Assert.assertEquals(List.of("table=5"), removals);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(3L, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static long publishIdentity(MetaCacheEntry<String, Long> entry, IdNameIndex index, long id,
            CountDownLatch prechecksComplete, CountDownLatch startPublication) {
        index.checkCanPut(id, "table");
        prechecksComplete.countDown();
        await(startPublication);
        return entry.computeAfterValidation("table", (key, value) -> id, () -> index.put(id, "table"));
    }

    private static Long resultOrNull(Future<Long> future) throws Exception {
        try {
            return future.get(3L, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            return null;
        }
    }
}
