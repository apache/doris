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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.cache.ScopedMetaCache.BulkLoadHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ScopedMetaCacheConcurrencyTest {
    private static final long TIMEOUT_SECONDS = 10L;
    private static final CacheSpec ENABLED =
            CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1_000L);
    private static final ScopePath TABLE = ScopePath.table("db", "tbl");
    private static final ScopePath PARTITION = ScopePath.partition("db", "tbl", "p=1");

    @Test
    public void concurrentMissesForTheSameKeyRunOneLoader() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, Integer> cache = registry.createCache("test", ENABLED);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            AtomicInteger loads = new AtomicInteger();
            Future<Integer> first = executor.submit(() -> cache.get("key", TABLE, ignored -> {
                loads.incrementAndGet();
                loaderStarted.countDown();
                await(releaseLoader);
                return 7;
            }));
            await(loaderStarted);
            Future<Integer> second = executor.submit(
                    () -> cache.get("key", TABLE, ignored -> {
                        loads.incrementAndGet();
                        return 8;
                    }));

            releaseLoader.countDown();
            Assertions.assertEquals(7, first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals(7, second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals(1, loads.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void closeDuringRefreshAdmissionDoesNotRetainRefreshMarker() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch refreshRegistered = new CountDownLatch(1);
        CountDownLatch continueAdmission = new CountDownLatch(1);
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        try {
            ScopedMetaCache<String, String> cache = registry.createCacheWithRefresh(
                    "test", ENABLED, Duration.ofNanos(1L), Runnable::run, () -> {
                        refreshRegistered.countDown();
                        await(continueAdmission);
                    });
            Assertions.assertEquals("v1", cache.get("key", TABLE, ignored -> "v1"));

            Future<String> refresh = executor.submit(
                    () -> cache.get("key", TABLE, ignored -> "v2"));
            await(refreshRegistered);
            registry.close();
            continueAdmission.countDown();

            Exception failure = Assertions.assertThrows(
                    Exception.class, () -> refresh.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertTrue(failure.getCause() instanceof IllegalStateException);
            Assertions.assertEquals(0, cache.refreshingCountForTest());
            Assertions.assertEquals(0, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(0, registry.metrics().getActiveLoadCount());
        } finally {
            continueAdmission.countDown();
            registry.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void evictedRefreshDoesNotOverwriteNewerMissValue() throws Exception {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        CacheSpec capacityOne = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCacheWithRefresh(
                    "test", capacityOne, Duration.ofNanos(1L), refreshExecutor, () -> {
                    });
            Assertions.assertEquals("v1", cache.get("key", TABLE, ignored -> "v1"));
            Assertions.assertEquals("v1", cache.get("key", TABLE, ignored -> {
                refreshStarted.countDown();
                await(releaseRefresh);
                return "stale-refresh";
            }));
            await(refreshStarted);

            cache.put("other", ScopePath.table("db", "other"), "other");
            cache.cleanUp();
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            Assertions.assertEquals("v2", cache.get("key", TABLE, ignored -> "v2"));

            releaseRefresh.countDown();
            refreshExecutor.shutdown();
            Assertions.assertTrue(refreshExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals("v2", cache.getIfPresent("key", TABLE));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(0, registry.metrics().getActiveLoadCount());
            Assertions.assertEquals(0, cache.refreshingCountForTest());
        } finally {
            releaseRefresh.countDown();
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void everyContainingInvalidationFencesAnInFlightSingleKeyLoad() throws Exception {
        List<ScopePath> invalidations = Arrays.asList(
                ScopePath.catalog(),
                ScopePath.database("db"),
                TABLE,
                PARTITION);
        for (ScopePath invalidation : invalidations) {
            assertInFlightLoadPublication(invalidation, false);
        }
    }

    @Test
    public void descendantAndSiblingInvalidationsDoNotFenceUnrelatedSingleKeyLoad() throws Exception {
        List<ScopePath> invalidations = Arrays.asList(
                ScopePath.partition("db", "tbl", "other"),
                ScopePath.table("db", "other"),
                ScopePath.database("other"));
        for (ScopePath invalidation : invalidations) {
            assertInFlightLoadPublication(invalidation, true);
        }
    }

    @Test
    public void exactKeyInvalidationFencesInFlightLoadButNotOtherKeys() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            cache.put("other", TABLE, "other");
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            Future<String> result = executor.submit(() -> cache.get("key", TABLE, ignored -> {
                loaderStarted.countDown();
                await(releaseLoader);
                return "stale";
            }));
            await(loaderStarted);

            cache.invalidateKey("key");
            releaseLoader.countDown();

            Assertions.assertEquals("stale", result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            Assertions.assertEquals("other", cache.getIfPresent("other", TABLE));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void everyRelevantScopeInvalidationFencesUnknownKeyBulkPublication() {
        List<ScopePath> invalidations = Arrays.asList(
                ScopePath.catalog(),
                ScopePath.database("db"),
                TABLE,
                PARTITION,
                ScopePath.partition("db", "tbl", "sibling"));
        for (ScopePath invalidation : invalidations) {
            try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
                ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
                try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                    registry.invalidate(invalidation);
                    Assertions.assertFalse(
                            cache.publish(handle, "key", PARTITION, "stale"),
                            invalidation + " must fence a table bulk load");
                    Assertions.assertNull(cache.getIfPresent("key", PARTITION));
                }
            }
        }
    }

    @Test
    public void siblingScopesDoNotFenceUnknownKeyBulkPublication() {
        List<ScopePath> invalidations = Arrays.asList(
                ScopePath.table("db", "sibling"),
                ScopePath.database("sibling"));
        for (ScopePath invalidation : invalidations) {
            try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
                ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
                try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                    registry.invalidate(invalidation);
                    Assertions.assertTrue(
                            cache.publish(handle, "key", PARTITION, "current"),
                            invalidation + " must not fence an unrelated table bulk load");
                }
                Assertions.assertEquals("current", cache.getIfPresent("key", PARTITION));
            }
        }
    }

    @Test
    public void exactKeyInvalidationFencesUnknownKeyBulkPublication() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                cache.invalidateKey("key");
                Assertions.assertFalse(cache.publish(handle, "key", PARTITION, "stale"));
            }
            Assertions.assertNull(cache.getIfPresent("key", PARTITION));
        }
    }

    @Test
    public void exactKeyInvalidationDoesNotFenceUnrelatedBulkPublication() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                cache.invalidateKey("unrelated");
                Assertions.assertTrue(cache.publish(handle, "key", PARTITION, "current"));
                Assertions.assertEquals(1, cache.metrics().getExactInvalidationTombstoneCount());
            }
            Assertions.assertEquals("current", cache.getIfPresent("key", PARTITION));
            Assertions.assertEquals(0, cache.metrics().getActiveBulkHandleCount());
            Assertions.assertEquals(0, cache.metrics().getExactInvalidationTombstoneCount());
        }
    }

    @Test
    public void stagedBulkValueIsNeverVisibleAfterHandleInvalidation() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch valueStaged = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    null,
                    () -> {
                        valueStaged.countDown();
                        await(allowCommit);
                    });
            cache.put("key", PARTITION, "current");
            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Future<Boolean> publication = executor.submit(
                        () -> cache.publish(handle, "key", PARTITION, "stale"));
                await(valueStaged);

                registry.invalidate(ScopePath.partition("db", "tbl", "sibling"));
                Assertions.assertEquals("current", cache.getIfPresent("key", PARTITION));

                allowCommit.countDown();
                Assertions.assertFalse(publication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            Assertions.assertEquals("current", cache.getIfPresent("key", PARTITION));
            registry.invalidate(ScopePath.catalog());
            assertEmpty(registry, cache);
        } finally {
            allowCommit.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void failedBulkPublicationDoesNotFenceIndependentDirectLoader() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch allowLoaderReturn = new CountDownLatch(1);
        CountDownLatch valueStaged = new CountDownLatch(1);
        CountDownLatch allowBulkCommit = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    null,
                    () -> {
                        valueStaged.countDown();
                        await(allowBulkCommit);
                    });
            Future<String> directLoad = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                loaderStarted.countDown();
                await(allowLoaderReturn);
                return "direct";
            }));
            await(loaderStarted);

            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Future<Boolean> bulkPublication = executor.submit(
                        () -> cache.publish(handle, "key", PARTITION, "stale"));
                await(valueStaged);
                registry.invalidate(ScopePath.partition("db", "tbl", "sibling"));

                allowBulkCommit.countDown();
                Assertions.assertFalse(bulkPublication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            allowLoaderReturn.countDown();

            Assertions.assertEquals("direct", directLoad.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals("direct", cache.getIfPresent("key", PARTITION));
        } finally {
            allowBulkCommit.countDown();
            allowLoaderReturn.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void invalidationAfterPartialBulkPublicationRemovesPublishedPartAndFencesTheRest() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            ScopePath firstPartition = ScopePath.partition("db", "tbl", "p=1");
            ScopePath secondPartition = ScopePath.partition("db", "tbl", "p=2");
            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Assertions.assertTrue(cache.publish(handle, "first", firstPartition, "first"));
                registry.invalidate(firstPartition);
                Assertions.assertFalse(cache.publish(handle, "second", secondPartition, "second"));
            }

            Assertions.assertNull(cache.getIfPresent("first", firstPartition));
            Assertions.assertNull(cache.getIfPresent("second", secondPartition));
            assertEmpty(registry, cache);
        }
    }

    @Test
    public void delayedOldRemovalCallbackCannotDeleteReplacementRegistration() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch oldRemovalStarted = new CountDownLatch(1);
        CountDownLatch releaseOldRemoval = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        if ("old".equals(value)) {
                            oldRemovalStarted.countDown();
                            await(releaseOldRemoval);
                        }
                    });
            cache.put("key", TABLE, "old");
            Future<?> replacement = executor.submit(() -> cache.put("key", TABLE, "new"));
            await(oldRemovalStarted);

            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            releaseOldRemoval.countDown();
            replacement.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Assertions.assertEquals("new", cache.getIfPresent("key", TABLE));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        } finally {
            releaseOldRemoval.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void collidingKeyDoesNotWaitForUnrelatedRemoteLoader() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<CollisionKey, String> cache = registry.createCache("test", ENABLED);
            CollisionKey firstKey = new CollisionKey("first");
            CollisionKey secondKey = new CollisionKey("second");
            Future<String> first = executor.submit(() -> cache.get(firstKey, TABLE, ignored -> {
                loaderStarted.countDown();
                await(releaseLoader);
                return "first";
            }));
            await(loaderStarted);

            Future<?> second = executor.submit(() -> cache.put(secondKey, TABLE, "second"));
            second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.assertEquals("second", cache.getIfPresent(secondKey, TABLE));

            releaseLoader.countDown();
            Assertions.assertEquals("first", first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            releaseLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void directPutWinsOverAnEarlierInFlightLoader() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicReference<String> retired = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test", ENABLED, null, (key, value) -> retired.set(value));
            Future<String> loaded = executor.submit(() -> cache.get("key", TABLE, ignored -> {
                loaderStarted.countDown();
                await(releaseLoader);
                return "stale-load-result";
            }));
            await(loaderStarted);

            cache.put("key", TABLE, "direct-put");
            releaseLoader.countDown();

            Assertions.assertEquals(
                    "stale-load-result", loaded.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals("direct-put", cache.getIfPresent("key", TABLE));
            Assertions.assertEquals("stale-load-result", retired.get());
        } finally {
            releaseLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void postInvalidationQueryDoesNotJoinStaleSingleFlight() throws Exception {
        List<ScopePath> invalidations = Arrays.asList(
                ScopePath.catalog(),
                ScopePath.database("db"),
                TABLE,
                PARTITION);
        for (ScopePath invalidation : invalidations) {
            assertPostInvalidationQueryStartsFreshLoad(invalidation, false);
        }
        assertPostInvalidationQueryStartsFreshLoad(TABLE, true);
    }

    @Test
    public void siblingInvalidationStillSharesCurrentSingleFlight() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch oldLoaderStarted = new CountDownLatch(1);
        CountDownLatch releaseOldLoader = new CountDownLatch(1);
        AtomicInteger secondLoads = new AtomicInteger();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            Future<String> first = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                oldLoaderStarted.countDown();
                await(releaseOldLoader);
                return "shared";
            }));
            await(oldLoaderStarted);

            registry.invalidate(ScopePath.table("other-db", "other-table"));
            Future<String> second = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                secondLoads.incrementAndGet();
                return "unexpected";
            }));
            releaseOldLoader.countDown();

            Assertions.assertEquals("shared", first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals("shared", second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals(0, secondLoads.get());
        } finally {
            releaseOldLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void leaderRechecksCacheAfterDirectPutWinsElectionWindow() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch leaderElected = new CountDownLatch(1);
        CountDownLatch allowLeader = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    null,
                    () -> {
                        leaderElected.countDown();
                        await(allowLeader);
                    },
                    () -> {
                    });
            Future<String> result = executor.submit(() -> cache.get("key", TABLE, ignored -> {
                loads.incrementAndGet();
                return "stale";
            }));
            await(leaderElected);

            cache.put("key", TABLE, "direct");
            allowLeader.countDown();

            Assertions.assertEquals("direct", result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals(0, loads.get());
            Assertions.assertEquals("direct", cache.getIfPresent("key", TABLE));
        } finally {
            allowLeader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void leaderRechecksCacheAfterBulkPublishWinsElectionWindow() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch leaderElected = new CountDownLatch(1);
        CountDownLatch allowLeader = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    null,
                    () -> {
                        leaderElected.countDown();
                        await(allowLeader);
                    },
                    () -> {
                    });
            Future<String> result = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                loads.incrementAndGet();
                return "stale";
            }));
            await(leaderElected);

            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Assertions.assertTrue(cache.publish(handle, "key", PARTITION, "bulk"));
            }
            allowLeader.countDown();

            Assertions.assertEquals("bulk", result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals(0, loads.get());
            Assertions.assertEquals("bulk", cache.getIfPresent("key", PARTITION));
        } finally {
            allowLeader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void failedBulkCandidatePreservesExistingValueAndCapacity() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch candidateReady = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L),
                    null,
                    null,
                    () -> {
                        candidateReady.countDown();
                        await(allowCommit);
                    });
            cache.put("key", PARTITION, "current");
            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Future<Boolean> publication = executor.submit(
                        () -> cache.publish(handle, "candidate", PARTITION, "stale"));
                await(candidateReady);

                registry.invalidate(ScopePath.partition("db", "tbl", "sibling"));
                Assertions.assertEquals("current", cache.getIfPresent("key", PARTITION));
                Assertions.assertEquals(1L, cache.metrics().getPhysicalEntryCount());

                allowCommit.countDown();
                Assertions.assertFalse(publication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            Assertions.assertEquals("current", cache.getIfPresent("key", PARTITION));
            Assertions.assertNull(cache.getIfPresent("candidate", PARTITION));
        } finally {
            allowCommit.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void bulkReplacementRemovalCanReenterInvalidation() {
        AtomicBoolean reentered = new AtomicBoolean(false);
        AtomicReference<ScopedMetaCache<String, String>> cacheReference = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        if ("old".equals(value) && reentered.compareAndSet(false, true)) {
                            cacheReference.get().invalidateKey(key);
                            registry.invalidate(TABLE);
                        }
                    });
            cacheReference.set(cache);
            cache.put("key", TABLE, "old");

            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Assertions.assertTrue(cache.publish(handle, "key", TABLE, "new"));
            }

            Assertions.assertTrue(reentered.get());
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            assertEmpty(registry, cache);
        }
    }

    @Test
    public void directPutReplacementRemovalRunsAfterPublicationLocks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicBoolean reentered = new AtomicBoolean(false);
        AtomicReference<Throwable> reentryFailure = new AtomicReference<>();
        AtomicReference<ScopedMetaCache<String, String>> cacheReference = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        if (!"old".equals(value) || !reentered.compareAndSet(false, true)) {
                            return;
                        }
                        try {
                            Future<Boolean> guardedPublication = executor.submit(
                                    () -> cacheReference.get().compareAndSet(key, TABLE, "new", "guarded"));
                            guardedPublication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                            Future<?> invalidation = executor.submit(() -> registry.invalidate(TABLE));
                            invalidation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        } catch (Throwable throwable) {
                            reentryFailure.set(throwable);
                        }
                    });
            cacheReference.set(cache);
            cache.put("key", TABLE, "old");

            cache.put("key", TABLE, "new");

            Assertions.assertTrue(reentered.get());
            Assertions.assertNull(reentryFailure.get());
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            assertEmpty(registry, cache);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void bulkStagingDoesNotHoldPublicationKey() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> guardedFailure = new AtomicReference<>();
        AtomicReference<ScopedMetaCache<String, String>> cacheReference = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    null,
                    () -> {
                        try {
                            Future<String> guardedPublication = executor.submit(
                                    () -> cacheReference.get().get("key", TABLE, ignored -> "guarded"));
                            Assertions.assertEquals(
                                    "guarded", guardedPublication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                        } catch (Throwable throwable) {
                            guardedFailure.set(throwable);
                        }
                    });
            cacheReference.set(cache);

            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Assertions.assertTrue(cache.publish(handle, "key", TABLE, "bulk"));
            }

            Assertions.assertNull(guardedFailure.get());
            Assertions.assertEquals("bulk", cache.getIfPresent("key", TABLE));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void electedLoaderStaleRemovalRunsAfterPublicationKey() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch stateReplaced = new CountDownLatch(1);
        CountDownLatch startCleanup = new CountDownLatch(1);
        AtomicBoolean reentered = new AtomicBoolean(false);
        AtomicReference<Throwable> reentryFailure = new AtomicReference<>();
        AtomicReference<Future<?>> cleanupReference = new AtomicReference<>();
        AtomicReference<ScopedMetaCache<String, String>> cacheReference = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        if (!"old".equals(value) || !reentered.compareAndSet(false, true)) {
                            return;
                        }
                        try {
                            Future<Boolean> guardedPublication = executor.submit(
                                    () -> cacheReference.get().compareAndSet(key, TABLE, null, "guarded"));
                            guardedPublication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                            Future<?> invalidation = executor.submit(() -> registry.invalidate(TABLE));
                            invalidation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        } catch (Throwable throwable) {
                            reentryFailure.set(throwable);
                        }
                    },
                    () -> {
                        cacheReference.get().put("key", TABLE, "old");
                        cleanupReference.set(executor.submit(() -> registry.invalidate(TABLE, () -> {
                            stateReplaced.countDown();
                            await(startCleanup);
                        })));
                        await(stateReplaced);
                    },
                    () -> {
                    });
            cacheReference.set(cache);

            Assertions.assertEquals("loaded", cache.get("key", TABLE, ignored -> "loaded"));
            startCleanup.countDown();
            cleanupReference.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Assertions.assertTrue(reentered.get());
            Assertions.assertNull(reentryFailure.get());
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            assertEmpty(registry, cache);
        } finally {
            startCleanup.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void compareAndSetReplacementRemovalCanReenterInvalidation() {
        AtomicBoolean reentered = new AtomicBoolean(false);
        AtomicReference<ScopedMetaCache<String, String>> cacheReference = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        if ("old".equals(value) && reentered.compareAndSet(false, true)) {
                            cacheReference.get().invalidateKey(key);
                            registry.invalidate(TABLE);
                        }
                    });
            cacheReference.set(cache);
            cache.put("key", TABLE, "old");

            Assertions.assertTrue(cache.compareAndSet("key", TABLE, "old", "new"));

            Assertions.assertTrue(reentered.get());
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            assertEmpty(registry, cache);
        }
    }

    @Test
    public void bulkReplacementRemovalCanReenterBulkPublication() {
        AtomicBoolean reentered = new AtomicBoolean(false);
        AtomicBoolean nestedPublished = new AtomicBoolean(false);
        AtomicInteger removalCallbacks = new AtomicInteger();
        AtomicReference<ScopedMetaCache<String, String>> cacheReference = new AtomicReference<>();
        AtomicReference<BulkLoadHandle> handleReference = new AtomicReference<>();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 1L),
                    null,
                    (key, value) -> {
                        removalCallbacks.incrementAndGet();
                        if ("old".equals(value) && reentered.compareAndSet(false, true)) {
                            nestedPublished.set(cacheReference.get().publish(
                                    handleReference.get(), "nested", TABLE, "nested"));
                            throw new AssertionError("injected removal callback error");
                        }
                    });
            cacheReference.set(cache);
            cache.put("key", TABLE, "old");

            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                handleReference.set(handle);
                Assertions.assertTrue(cache.publish(handle, "key", TABLE, "new"));
            }

            Assertions.assertTrue(reentered.get());
            Assertions.assertTrue(nestedPublished.get());
            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            Assertions.assertEquals("nested", cache.getIfPresent("nested", TABLE));
            Assertions.assertEquals(2, removalCallbacks.get());
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        }
    }

    @Test
    public void bulkRemovalFailureDoesNotChangeCommittedPublication() {
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        throw new IllegalStateException("injected removal callback failure");
                    });
            cache.put("key", TABLE, "old");

            try (BulkLoadHandle handle = cache.beginBulkLoad(TABLE)) {
                Assertions.assertTrue(cache.publish(handle, "key", TABLE, "new"));
            }

            Assertions.assertEquals("new", cache.getIfPresent("key", TABLE));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        }
    }

    @Test
    public void childCreationPinPreventsParentPrune() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch parentPinned = new CountDownLatch(1);
        CountDownLatch allowChildInsert = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry(
                (level, key) -> {
                    if (level == ScopePath.Level.TABLE) {
                        parentPinned.countDown();
                        await(allowChildInsert);
                    }
                },
                (level, key) -> {
                },
                (level, key) -> {
                })) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle databaseHandle = cache.beginBulkLoad(ScopePath.database("db"));
            Future<BulkLoadHandle> tableHandleFuture = executor.submit(() -> cache.beginBulkLoad(TABLE));
            await(parentPinned);

            databaseHandle.close();
            allowChildInsert.countDown();
            try (BulkLoadHandle tableHandle = tableHandleFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                Assertions.assertTrue(cache.publish(tableHandle, "key", TABLE, "value"));
            }
            Assertions.assertEquals("value", cache.getIfPresent("key", TABLE));
        } finally {
            allowChildInsert.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void parentPruneMarkerMakesChildCreationRetry() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch parentMarked = new CountDownLatch(1);
        CountDownLatch allowParentRemoval = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry(
                (level, key) -> {
                },
                (level, key) -> {
                },
                (level, key) -> {
                    if (level == ScopePath.Level.DATABASE) {
                        parentMarked.countDown();
                        await(allowParentRemoval);
                    }
                })) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle databaseHandle = cache.beginBulkLoad(ScopePath.database("db"));
            Future<?> prune = executor.submit(databaseHandle::close);
            await(parentMarked);

            Future<BulkLoadHandle> tableHandleFuture = executor.submit(() -> cache.beginBulkLoad(TABLE));
            Assertions.assertFalse(tableHandleFuture.isDone());
            allowParentRemoval.countDown();
            prune.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            try (BulkLoadHandle tableHandle = tableHandleFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                Assertions.assertTrue(cache.publish(tableHandle, "key", TABLE, "value"));
            }
            Assertions.assertEquals("value", cache.getIfPresent("key", TABLE));
        } finally {
            allowParentRemoval.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void parentPruneRechecksChildInsertedAfterFastCheck() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch parentObservedEmpty = new CountDownLatch(1);
        CountDownLatch allowParentMark = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry(
                (level, key) -> {
                },
                (level, key) -> {
                    if (level == ScopePath.Level.DATABASE) {
                        parentObservedEmpty.countDown();
                        await(allowParentMark);
                    }
                },
                (level, key) -> {
                })) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle databaseHandle = cache.beginBulkLoad(ScopePath.database("db"));
            Future<?> prune = executor.submit(databaseHandle::close);
            await(parentObservedEmpty);

            try (BulkLoadHandle tableHandle = cache.beginBulkLoad(TABLE)) {
                allowParentMark.countDown();
                prune.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                Assertions.assertTrue(cache.publish(tableHandle, "key", TABLE, "value"));
            }
            Assertions.assertEquals("value", cache.getIfPresent("key", TABLE));
        } finally {
            allowParentMark.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void parentPruneRetriesAfterConcurrentStateReplacement() throws Exception {
        assertScopePruneRetriesAfterConcurrentStateReplacement(
                ScopePath.database("db"), ScopePath.Level.DATABASE);
        assertScopePruneRetriesAfterConcurrentStateReplacement(TABLE, ScopePath.Level.TABLE);
        assertScopePruneRetriesAfterConcurrentStateReplacement(PARTITION, ScopePath.Level.PARTITION);
    }

    @Test
    public void pruneMarkerIsReleasedWhenPostMarkActionFails() {
        AtomicBoolean failOnce = new AtomicBoolean(true);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry(
                (level, key) -> {
                },
                (level, key) -> {
                },
                (level, key) -> {
                    if (level == ScopePath.Level.DATABASE && failOnce.compareAndSet(true, false)) {
                        throw new IllegalStateException("injected post-mark failure");
                    }
                })) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle failedHandle = cache.beginBulkLoad(ScopePath.database("db"));
            IllegalStateException failure = Assertions.assertThrows(
                    IllegalStateException.class, failedHandle::close);
            Assertions.assertEquals("injected post-mark failure", failure.getMessage());
            Assertions.assertFalse(failOnce.get());

            try (BulkLoadHandle recoveredHandle = cache.beginBulkLoad(ScopePath.database("db"))) {
                Assertions.assertTrue(cache.publish(
                        recoveredHandle, "key", ScopePath.database("db"), "value"));
            }
            Assertions.assertEquals("value", cache.getIfPresent("key", ScopePath.database("db")));
            registry.invalidate(ScopePath.database("db"));
            assertEmpty(registry, cache);
        }
    }

    private static void assertScopePruneRetriesAfterConcurrentStateReplacement(
            ScopePath scope, ScopePath.Level level) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch scopeMarked = new CountDownLatch(1);
        CountDownLatch allowPruneRecheck = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry(
                (ignoredLevel, key) -> {
                },
                (ignoredLevel, key) -> {
                },
                (markedLevel, key) -> {
                    if (markedLevel == level) {
                        scopeMarked.countDown();
                        await(allowPruneRecheck);
                    }
                })) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            BulkLoadHandle handle = cache.beginBulkLoad(scope);
            Future<?> prune = executor.submit(handle::close);
            await(scopeMarked);

            registry.invalidate(scope);
            allowPruneRecheck.countDown();
            prune.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEmpty(registry, cache);
        } finally {
            allowPruneRecheck.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void keyPublicationAfterInvalidationLinearizationSurvivesCleanup() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch stateReplaced = new CountDownLatch(1);
        CountDownLatch continueCleanup = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            cache.put("key", TABLE, "old");
            Future<?> invalidation = executor.submit(() -> cache.invalidateKey("key", () -> {
                stateReplaced.countDown();
                await(continueCleanup);
            }));
            await(stateReplaced);

            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            cache.put("key", TABLE, "new");
            continueCleanup.countDown();
            invalidation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Assertions.assertEquals("new", cache.getIfPresent("key", TABLE));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        } finally {
            continueCleanup.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void staleScopeValueIsUnreadableBeforePhysicalCleanupStarts() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch stateReplaced = new CountDownLatch(1);
        CountDownLatch startCleanup = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            cache.put("key", TABLE, "old");
            Future<?> invalidation = executor.submit(() -> registry.invalidate(TABLE, () -> {
                stateReplaced.countDown();
                await(startCleanup);
            }));
            await(stateReplaced);

            Assertions.assertNull(cache.getIfPresent("key", TABLE));
            startCleanup.countDown();
            invalidation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEmpty(registry, cache);
        } finally {
            startCleanup.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void scopePublicationAfterInvalidationLinearizationSurvivesCleanup() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch oldRemovalStarted = new CountDownLatch(1);
        CountDownLatch continueCleanup = new CountDownLatch(1);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache(
                    "test",
                    ENABLED,
                    null,
                    (key, value) -> {
                        if ("old".equals(value)) {
                            oldRemovalStarted.countDown();
                            await(continueCleanup);
                        }
                    });
            cache.put("old", TABLE, "old");
            Future<?> invalidation = executor.submit(() -> registry.invalidate(TABLE));
            await(oldRemovalStarted);

            cache.put("new", TABLE, "new");
            continueCleanup.countDown();
            invalidation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Assertions.assertEquals("new", cache.getIfPresent("new", TABLE));
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
        } finally {
            continueCleanup.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void cacheAndRegistryCloseFenceInFlightPublications() throws Exception {
        assertCloseFencesPublication(false);
        assertCloseFencesPublication(true);
    }

    @Test
    public void closeCannotRaceWithLateExactInvalidationTombstone() throws Exception {
        assertCloseFencesLateExactInvalidation(false);
        assertCloseFencesLateExactInvalidation(true);
    }

    @Test
    public void concurrentPutEvictAndInvalidationFinishWithoutDeadlock() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        ScopedMetaCache<Integer, Integer> cache = registry.createCache(
                "test", CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 16L));
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> writer = executor.submit(() -> {
                await(start);
                for (int i = 0; i < 2_000; i++) {
                    cache.put(i % 64, ScopePath.partition("db", "tbl", i % 8), i);
                }
            });
            Future<?> keyInvalidator = executor.submit(() -> {
                await(start);
                for (int i = 0; i < 2_000; i++) {
                    cache.invalidateKey(i % 64);
                }
            });
            Future<?> scopeInvalidator = executor.submit(() -> {
                await(start);
                for (int i = 0; i < 500; i++) {
                    registry.invalidate(ScopePath.partition("db", "tbl", i % 8));
                }
            });
            Future<?> cleaner = executor.submit(() -> {
                await(start);
                for (int i = 0; i < 2_000; i++) {
                    cache.cleanUp();
                }
            });
            start.countDown();
            writer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            keyInvalidator.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            scopeInvalidator.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            cleaner.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            registry.invalidate(ScopePath.catalog());
            cache.cleanUp();
            assertEmpty(registry, cache);
        } finally {
            registry.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void concurrentSameKeyPublicationsLeaveOneExactRegistration() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, Integer> cache = registry.createCache("test", ENABLED);
            CountDownLatch start = new CountDownLatch(1);
            Future<?> first = submitSameKeyWriter(executor, start, cache, "db1");
            Future<?> second = submitSameKeyWriter(executor, start, cache, "db2");
            Future<?> third = submitSameKeyWriter(executor, start, cache, "db3");
            Future<?> fourth = submitSameKeyWriter(executor, start, cache, "db4");
            start.countDown();
            first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            third.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            fourth.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            ScopePath finalScope = ScopePath.table("final", "tbl");
            cache.put("key", finalScope, 100);
            cache.cleanUp();

            Assertions.assertEquals(100, cache.getIfPresent("key", finalScope));
            Assertions.assertEquals(1L, cache.metrics().getPhysicalEntryCount());
            Assertions.assertEquals(1, cache.metrics().getKeyNodeCount());
            Assertions.assertEquals(1, registry.metrics().getRegistrationCount());
            Assertions.assertEquals(1, registry.metrics().getDatabaseNodeCount());
            Assertions.assertEquals(1, registry.metrics().getTableNodeCount());
            Assertions.assertEquals(0, registry.metrics().getPartitionNodeCount());
        } finally {
            executor.shutdownNow();
        }
    }

    private static void assertInFlightLoadPublication(
            ScopePath invalidation, boolean expectedCached) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            CountDownLatch loaderStarted = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            Future<String> result = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                loaderStarted.countDown();
                await(releaseLoader);
                return "loaded";
            }));
            await(loaderStarted);
            registry.invalidate(invalidation);
            releaseLoader.countDown();

            Assertions.assertEquals("loaded", result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            if (expectedCached) {
                Assertions.assertEquals("loaded", cache.getIfPresent("key", PARTITION));
            } else {
                Assertions.assertNull(cache.getIfPresent("key", PARTITION));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static void assertCloseFencesPublication(boolean closeRegistry) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try {
            Future<String> result = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                loaderStarted.countDown();
                await(releaseLoader);
                return "loaded";
            }));
            await(loaderStarted);
            if (closeRegistry) {
                registry.close();
            } else {
                cache.close();
            }
            releaseLoader.countDown();
            Assertions.assertEquals("loaded", result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEmpty(registry, cache);
        } finally {
            releaseLoader.countDown();
            registry.close();
            executor.shutdownNow();
        }
    }

    private static void assertCloseFencesLateExactInvalidation(boolean closeRegistry) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
        ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
        BulkLoadHandle handle = cache.beginBulkLoad(TABLE);
        CountDownLatch passedOpenCheck = new CountDownLatch(1);
        CountDownLatch enterInvalidationLock = new CountDownLatch(1);
        try {
            Future<?> invalidation = executor.submit(() -> cache.invalidateKey(
                    "key",
                    () -> {
                        passedOpenCheck.countDown();
                        await(enterInvalidationLock);
                    },
                    () -> {
                    }));
            await(passedOpenCheck);

            if (closeRegistry) {
                registry.close();
            } else {
                cache.close();
            }
            enterInvalidationLock.countDown();
            invalidation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Assertions.assertEquals(0, cache.metrics().getExactInvalidationTombstoneCount());
            handle.close();
            Assertions.assertEquals(0, cache.metrics().getActiveBulkHandleCount());
            Assertions.assertEquals(0, registry.metrics().getActiveLoadCount());
        } finally {
            enterInvalidationLock.countDown();
            handle.close();
            registry.close();
            executor.shutdownNow();
        }
    }

    private static void assertPostInvalidationQueryStartsFreshLoad(
            ScopePath invalidation, boolean exactKey) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch oldLoaderStarted = new CountDownLatch(1);
        CountDownLatch releaseOldLoader = new CountDownLatch(1);
        AtomicInteger freshLoads = new AtomicInteger();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCache("test", ENABLED);
            Future<String> oldResult = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                oldLoaderStarted.countDown();
                await(releaseOldLoader);
                return "stale";
            }));
            await(oldLoaderStarted);
            if (exactKey) {
                cache.invalidateKey("key");
            } else {
                registry.invalidate(invalidation);
            }

            Future<String> freshResult = executor.submit(() -> cache.get("key", PARTITION, ignored -> {
                freshLoads.incrementAndGet();
                return "fresh";
            }));
            Assertions.assertEquals("fresh", freshResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals(1, freshLoads.get());

            releaseOldLoader.countDown();
            Assertions.assertEquals("stale", oldResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertEquals("fresh", cache.getIfPresent("key", PARTITION));
        } finally {
            releaseOldLoader.countDown();
            executor.shutdownNow();
        }
    }

    private static Future<?> submitSameKeyWriter(
            ExecutorService executor,
            CountDownLatch start,
            ScopedMetaCache<String, Integer> cache,
            String database) {
        return executor.submit(() -> {
            await(start);
            ScopePath path = ScopePath.table(database, "tbl");
            for (int i = 0; i < 1_000; i++) {
                cache.put("key", path, i);
            }
        });
    }

    private static void assertEmpty(
            ScopedMetaCacheRegistry registry, ScopedMetaCache<?, ?> cache) {
        ScopedMetaCacheRegistry.ScopeMetrics scopeMetrics = registry.metrics();
        Assertions.assertEquals(0, scopeMetrics.getDatabaseNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getTableNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getPartitionNodeCount());
        Assertions.assertEquals(0, scopeMetrics.getRegistrationCount());
        Assertions.assertEquals(0, scopeMetrics.getActiveLoadCount());
        Assertions.assertEquals(0L, cache.metrics().getPhysicalEntryCount());
        Assertions.assertEquals(0, cache.metrics().getKeyNodeCount());
        Assertions.assertEquals(0, cache.metrics().getInFlightLoadCount());
        Assertions.assertEquals(0, cache.metrics().getActiveBulkHandleCount());
        Assertions.assertEquals(0, cache.metrics().getExactInvalidationTombstoneCount());
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for test latch", e);
        }
    }

    private static final class CollisionKey {
        private final String value;

        private CollisionKey(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CollisionKey && value.equals(((CollisionKey) obj).value);
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }
}
