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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FeMetaCacheEntryTest {
    private static final CacheSpec ENABLED = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100L);

    @Test
    public void testLoadMutationAndStatsUseSharedRuntime() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger loads = new AtomicInteger();
        try {
            FeMetaCacheEntry<String, Integer> entry = new FeMetaCacheEntry<>(
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
            FeMetaCacheEntry<String, Integer> contextual = new FeMetaCacheEntry<>(
                    "contextual", null, ENABLED, executor, false, true);
            Assert.assertThrows(UnsupportedOperationException.class, () -> contextual.get("key"));
            Assert.assertEquals(Integer.valueOf(3), contextual.get("key", String::length));

            CacheSpec disabledSpec = CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, 100L);
            AtomicInteger actions = new AtomicInteger();
            FeMetaCacheEntry<String, Integer> disabled = new FeMetaCacheEntry<>(
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
    public void testDisabledEntryDoesNotRetireTheReturnedValue() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean usable = new AtomicBoolean(true);
        try {
            CacheSpec disabledSpec = CacheSpec.of(true, 0L, 100L);
            FeMetaCacheEntry<String, AtomicBoolean> entry = FeMetaCacheEntry.withSyncRemovalListener(
                    "databases", ignored -> usable, disabledSpec, executor,
                    (key, value, cause) -> value.set(false));

            AtomicBoolean loaded = entry.get("db");

            Assert.assertSame(usable, loaded);
            Assert.assertTrue(loaded.get());
            Assert.assertNull(entry.getIfPresent("db"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidationDoesNotRetireARejectedMissValue() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch loadedBeforePublication = new CountDownLatch(1);
        CountDownLatch continuePublication = new CountDownLatch(1);
        AtomicBoolean usable = new AtomicBoolean(true);
        try {
            FeMetaCacheEntry<String, AtomicBoolean> entry = new FeMetaCacheEntry<String, AtomicBoolean>(
                    "databases", ignored -> usable, ENABLED, refreshExecutor, false, false, 1,
                    (key, value, cause) -> value.set(false)) {
                @Override
                void beforeManualCachePutForTest(String key, AtomicBoolean value) {
                    loadedBeforePublication.countDown();
                    await(continuePublication);
                }
            };

            Future<AtomicBoolean> load = worker.submit(() -> entry.get("db"));
            await(loadedBeforePublication);
            entry.invalidateKey("db");
            continuePublication.countDown();

            AtomicBoolean returned = load.get(3L, TimeUnit.SECONDS);
            Assert.assertSame(usable, returned);
            Assert.assertTrue(returned.get());
            Assert.assertNull(entry.getIfPresent("db"));
        } finally {
            continuePublication.countDown();
            worker.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testMutationAndAuxiliaryIndexSharePublicationWindow() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<String> index = new ArrayList<>();
        try {
            FeMetaCacheEntry<String, Integer> entry = new FeMetaCacheEntry<>(
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
            FeMetaCacheEntry<String, Integer> entry = new FeMetaCacheEntry<>(
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
            FeMetaCacheEntry<String, Long> entry = new FeMetaCacheEntry<>(
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
            FeMetaCacheEntry<String, Integer> entry = new FeMetaCacheEntry<String, Integer>(
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
    public void testColdValueRemainsHiddenUntilAuxiliaryIndexPublication() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch stripeHeld = new CountDownLatch(1);
        CountDownLatch releaseStripe = new CountDownLatch(1);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        CountDownLatch publicationReady = new CountDownLatch(1);
        List<String> index = new ArrayList<>();
        try {
            FeMetaCacheEntry<String, Integer> entry = new FeMetaCacheEntry<String, Integer>(
                    "objects", key -> {
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return key.length();
                    }, ENABLED, refreshExecutor, false, 1) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    if ("blocker".equals(key)) {
                        stripeHeld.countDown();
                        await(releaseStripe);
                    }
                }

                @Override
                protected void beforeCurrentValueActionForTest(String key, Integer value) {
                    publicationReady.countDown();
                }
            };

            Future<Integer> load = workers.submit(() -> entry.getAndRunIfCurrent(
                    "table", (key, value) -> index.add(key)));
            await(loaderStarted);
            Future<Integer> blocker = workers.submit(() -> entry.compute("blocker", (key, value) -> 1));
            await(stripeHeld);
            releaseLoader.countDown();
            await(publicationReady);

            Assert.assertNull(entry.getIfPresent("table"));
            Assert.assertTrue(index.isEmpty());

            releaseStripe.countDown();
            Assert.assertEquals(Integer.valueOf(1), blocker.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(5), load.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(Integer.valueOf(5), entry.getIfPresent("table"));
            Assert.assertEquals(List.of("table"), index);
        } finally {
            releaseLoader.countDown();
            releaseStripe.countDown();
            workers.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void testDatabaseNameRemapRetriesAfterConcurrentRefresh() throws Exception {
        assertNameRemapRetriesAfterConcurrentRefresh("databaseNames");
    }

    @Test
    public void testTableNameRemapRetriesAfterConcurrentRefresh() throws Exception {
        assertNameRemapRetriesAfterConcurrentRefresh("tableNames");
    }

    @Test
    public void testOuterNameRetryDoesNotRemoveIdentityPreservingObject() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch firstObjectPublished = new CountDownLatch(1);
        CountDownLatch continueNamePublication = new CountDownLatch(1);
        AtomicInteger nameAttempts = new AtomicInteger();
        AtomicInteger objectRemovals = new AtomicInteger();
        AtomicBoolean objectInitialized = new AtomicBoolean();
        Object database = new Object();
        try {
            FeMetaCacheEntry<String, Object> objects = FeMetaCacheEntry.withSyncRemovalListener(
                    "databases", ignored -> database, ENABLED, refreshExecutor, 1,
                    (key, value, cause) -> {
                        objectRemovals.incrementAndGet();
                        objectInitialized.set(false);
                    });
            FeMetaCacheEntry<String, Set<String>> names = new FeMetaCacheEntry<String, Set<String>>(
                    "databaseNames", key -> Set.of(), ENABLED, refreshExecutor, false, 1) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    if (nameAttempts.incrementAndGet() == 1) {
                        firstObjectPublished.countDown();
                        await(continueNamePublication);
                    }
                }
            };
            names.putSharedForTest("names", Set.of("initial"));

            Future<Set<String>> update = worker.submit(() -> names.computeAfterValidation(
                    "names",
                    (key, current) -> {
                        Set<String> updated = new HashSet<>(current);
                        updated.add("incremental");
                        return Set.copyOf(updated);
                    },
                    () -> objects.computeAfterValidation(
                            "db", (key, current) -> database, () -> {
                            })));
            await(firstObjectPublished);
            Assert.assertSame(database, objects.getIfPresent("db"));
            objectInitialized.set(true);

            // Model an auto-refresh replacing the outer name snapshot after the first nested object publish.
            // The outer CAS retries, so its validation republishes the already-current database identity.
            names.putSharedForTest("names", Set.of("initial", "refreshed"));
            continueNamePublication.countDown();

            Assert.assertEquals(Set.of("initial", "refreshed", "incremental"),
                    update.get(3L, TimeUnit.SECONDS));
            Assert.assertSame(database, objects.getIfPresent("db"));
            Assert.assertTrue(objectInitialized.get());
            Assert.assertEquals(0, objectRemovals.get());
            Assert.assertEquals(2, nameAttempts.get());
        } finally {
            continueNamePublication.countDown();
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
            FeMetaCacheEntry<String, Integer> entry = new FeMetaCacheEntry<String, Integer>(
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
            FeMetaCacheEntry<String, Integer> entry = FeMetaCacheEntry.withSyncRemovalListener(
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

    private static void assertNameRemapRetriesAfterConcurrentRefresh(String entryName) throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch remapCalculated = new CountDownLatch(1);
        CountDownLatch continuePublication = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        try {
            FeMetaCacheEntry<String, Set<String>> entry = new FeMetaCacheEntry<String, Set<String>>(
                    entryName, key -> Set.of(), ENABLED, refreshExecutor, false, 1) {
                @Override
                void beforePublicMutationWriteForTest(String key) {
                    if (attempts.incrementAndGet() == 1) {
                        remapCalculated.countDown();
                        await(continuePublication);
                    }
                }
            };
            entry.putSharedForTest("names", Set.of("initial"));

            Future<Set<String>> remap = worker.submit(() -> entry.compute("names", (key, current) -> {
                Set<String> updated = new HashSet<>(current);
                updated.add("incremental");
                return Set.copyOf(updated);
            }));
            await(remapCalculated);
            entry.putSharedForTest("names", Set.of("initial", "refreshed"));
            continuePublication.countDown();

            Set<String> expected = Set.of("initial", "refreshed", "incremental");
            Assert.assertEquals(expected, remap.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(expected, entry.getIfPresent("names"));
            Assert.assertEquals(2, attempts.get());
        } finally {
            continuePublication.countDown();
            worker.shutdownNow();
            refreshExecutor.shutdownNow();
        }
    }

    private static long publishIdentity(FeMetaCacheEntry<String, Long> entry, IdNameIndex index, long id,
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
