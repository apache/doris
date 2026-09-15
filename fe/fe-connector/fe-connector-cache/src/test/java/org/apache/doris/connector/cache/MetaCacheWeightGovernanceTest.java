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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class MetaCacheWeightGovernanceTest {
    @Test
    void exactInvalidationCleanupPreservesNewGenerationReservation() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(4096L));
        MetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                7L, "iceberg", "table", "table", OptionalLong.empty(), OptionalLong.empty());
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry()) {
            ScopedMetaCache<String, String> cache = registry.createCacheWithMetaRemovalListener(
                    "table", CacheSpec.of(true, -1L, 100L), null, null, null, null,
                    (key, value) -> MetaCacheSizeEstimate.complete(512L), budget);
            cache.put("key", ScopePath.catalog(), "old");
            cache.invalidateKey("key", () -> cache.put("key", ScopePath.catalog(), "new"));

            Assertions.assertEquals(1024L, manager.getGlobalUsedWeight(),
                    "delayed cleanup must not release the replacement's reservation");
            Assertions.assertEquals("new", cache.getIfPresent("key", ScopePath.catalog()));
            cache.invalidateKey("key");
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void admissionRemovalAndCloseKeepHierarchicalAccountingBalanced() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(4096L));
        CatalogMetaCache owner = new CatalogMetaCache(manager, 7L, "iceberg", Collections.emptyMap());
        MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                .<String, String>builder("table", CacheSpec.ofWeight(true, -1L, 100L, 2048L),
                        ignored -> ScopePath.catalog())
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(512L))
                .build());

        entry.put("one", "value");
        Assertions.assertEquals(1024L, entry.metrics().getEstimatedWeight());
        Assertions.assertEquals(1024L, manager.getGlobalUsedWeight());

        entry.invalidateKey("one");
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        owner.close();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void oversizedValueIsReturnedButNotCached() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(1024L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 8L, "paimon", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("partition", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(1024L))
                    .build());

            Assertions.assertEquals("value", entry.get("key", ignored -> "value"));
            Assertions.assertNull(entry.getIfPresent("key"));
            Assertions.assertEquals(1L, entry.metrics().getWeightRejectCount());
            Assertions.assertEquals("entry_too_large", entry.metrics().getLastWeightRejectReason());
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void sameKeyReplacementUsesOnlyTheLargerGenerationWeight() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(1024L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 9L, "iceberg", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("table", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(512L))
                    .build());

            entry.put("key", "generation-one");
            entry.put("key", "generation-two");

            Assertions.assertEquals("generation-two", entry.getIfPresent("key"));
            Assertions.assertEquals(1024L, entry.metrics().getEstimatedWeight());
            Assertions.assertEquals(0L, entry.metrics().getWeightRejectCount());
            entry.invalidateKey("key");
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void failedCompareAndSetPreservesOldReservation() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(2048L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 10L, "iceberg", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("table", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(
                            "old".equals(value) ? 512L : 1024L))
                    .build());
            entry.put("key", "old");
            IllegalStateException failure = new IllegalStateException("commit failed");
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                    () -> entry.compareAndSet("key", "old", "new", () -> {
                        throw failure;
                    })));
            Assertions.assertEquals("old", entry.getIfPresent("key"));
            Assertions.assertEquals(1024L, manager.getGlobalUsedWeight());
            entry.invalidateKey("key");
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void failedCompareAndSetRollsBackBeforeConcurrentRemoval() throws Exception {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(2048L));
        MetaCacheBudgetManager.EntryBudget budget = manager.createEntryBudget(
                11L, "iceberg", "table", "table", OptionalLong.empty(), OptionalLong.empty());
        AtomicLong ticker = new AtomicLong();
        CountDownLatch removalStarted = new CountDownLatch(1);
        CountDownLatch removalFinished = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ScopedMetaCacheRegistry registry = new ScopedMetaCacheRegistry();
                ScopedMetaCache<String, String> cache = new ScopedMetaCache<>(
                        registry, "table", CacheSpec.of(true, 1L, 100L), ticker::get,
                        (key, value, cause) -> removalStarted.countDown(), null, null, null,
                        () -> { }, () -> { }, () -> { },
                        (key, value) -> MetaCacheSizeEstimate.complete(
                                "old".equals(value) ? 512L : 1024L), budget)) {
            cache.put("key", ScopePath.catalog(), "old");
            cache.cleanUp();
            AtomicReference<Future<?>> removal = new AtomicReference<>();
            IllegalStateException failure = new IllegalStateException("commit failed");
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                    () -> cache.compareAndSet("key", ScopePath.catalog(), "old", "new", () -> {
                        waitForRemovalAtGuardExit(cache, removalFinished);
                        ticker.set(TimeUnit.SECONDS.toNanos(2L));
                        removal.set(executor.submit(() -> {
                            try {
                                cache.cleanUp();
                            } finally {
                                removalFinished.countDown();
                            }
                        }));
                        awaitRemoval(removalStarted);
                        throw failure;
                    })));
            removal.get().get(10L, TimeUnit.SECONDS);
            Assertions.assertNull(cache.getIfPresent("key", ScopePath.catalog()));
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight(),
                    "rollback must not reactivate a reservation after its value was removed");
        } finally {
            executor.shutdownNow();
        }
    }

    // Freeze the legal schedule after the publication locks unlock, before the outer CAS finally.
    // This white-box seam avoids a new production hook or a timing-dependent race assertion.
    private static void waitForRemovalAtGuardExit(ScopedMetaCache<?, ?> cache, CountDownLatch finished) {
        try {
            Field deferralsField = ScopedMetaCache.class.getDeclaredField("removalDeferrals");
            deferralsField.setAccessible(true);
            Object deferral = ((ThreadLocal<?>) deferralsField.get(cache)).get();
            Field removalsField = deferral.getClass().getDeclaredField("removals");
            removalsField.setAccessible(true);
            removalsField.set(deferral, new ArrayDeque<Object>() {
                @Override
                public Object pollFirst() {
                    awaitRemoval(finished);
                    return super.pollFirst();
                }
            });
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void awaitRemoval(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10L, TimeUnit.SECONDS), "removal did not reach its test gate");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void budgetPressureReclaimsOneColdEntryBeforeTheNextAdmission() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(2048L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 12L, "iceberg", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("partition", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(512L))
                    .build());

            entry.put("one", "value-one");
            entry.put("two", "value-two");
            entry.put("three", "value-three");

            Assertions.assertNull(entry.getIfPresent("three"));
            awaitReclaimed(entry, 1L, 1024L);
            entry.put("three", "value-three");
            Assertions.assertEquals("value-three", entry.getIfPresent("three"));
            Assertions.assertEquals(2L, entry.size());
            Assertions.assertEquals(2048L, entry.metrics().getEstimatedWeight());
            Assertions.assertEquals(1L, entry.metrics().getWeightRejectCount());
            Assertions.assertEquals(1L, entry.metrics().getEvictionCount());
        }
    }

    @Test
    void managedOwnerClosesWhenPhysicalCacheConstructionFails() {
        long catalogId = Long.MIN_VALUE + 31L;
        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalogId).isEmpty());
        CatalogMetaCache owner = CatalogMetaCache.managed(catalogId, "iceberg", Collections.emptyMap());

        Assertions.assertThrows(IllegalArgumentException.class, () -> owner.create(MetaCacheDefinition
                .<String, String>builder("invalid", CacheSpec.of(true, -1L, -1L),
                        ignored -> ScopePath.catalog())
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(512L))
                .build()));

        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalogId).isEmpty());
    }

    @Test
    void concurrentRegistrationCannotBeDetachedByLastOwnerRemoval() throws Exception {
        long catalogId = Long.MIN_VALUE + 32L;
        CatalogMetaCache retiring = CatalogMetaCache.managed(
                catalogId, "iceberg", Collections.emptyMap());
        CatalogMetaCache replacement = new CatalogMetaCache(
                new ScopedMetaCacheRegistry(), MetaCacheGovernance.budgetManager(), catalogId,
                "iceberg", OptionalLong.empty(), true);
        BlockingEmptySet<CatalogMetaCache> owners = new BlockingEmptySet<>();
        owners.add(retiring);
        owners.arm();
        catalogCacheRegistry().put(catalogId, owners);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> unregister = null;
        Future<?> register = null;
        try {
            unregister = executor.submit(retiring::close);
            Assertions.assertTrue(owners.awaitEmptyObservation(),
                    "the retiring owner must reach its empty-set decision");
            CountDownLatch registerStarted = new CountDownLatch(1);
            register = executor.submit(() -> {
                registerStarted.countDown();
                MetaCacheGovernance.register(replacement);
            });
            Assertions.assertTrue(registerStarted.await(10L, TimeUnit.SECONDS));
            boolean registeredBeforeRemovalCompleted = owners.awaitAdd();

            owners.releaseEmptyObservation();
            unregister.get(10L, TimeUnit.SECONDS);
            register.get(10L, TimeUnit.SECONDS);

            Assertions.assertFalse(registeredBeforeRemovalCompleted,
                    "registration must serialize with removal for the same catalog id");
            Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalogId).contains(replacement),
                    "the replacement owner must remain discoverable after the retiring owner closes");
        } finally {
            owners.releaseEmptyObservation();
            if (unregister != null) {
                unregister.cancel(true);
            }
            if (register != null) {
                register.cancel(true);
            }
            retiring.close();
            replacement.close();
            executor.shutdownNow();
        }
    }

    @Test
    void estimatorIsNotInvokedWhenNoWeightLimitIsConfigured() {
        AtomicInteger estimates = new AtomicInteger();
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.empty());
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 10L, "hive", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("file", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator((key, value) -> {
                        estimates.incrementAndGet();
                        return MetaCacheSizeEstimate.complete(512L);
                    })
                    .build());

            entry.put("key", "value");
            Assertions.assertEquals("value", entry.getIfPresent("key"));
            Assertions.assertFalse(entry.isWeightBounded());
            Assertions.assertEquals(0, estimates.get());
        }
    }

    @Test
    void estimatorFailureReturnsTheLoadedValueWithoutCachingIt() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(4096L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 11L, "hive", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("table", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator((key, value) -> {
                        throw new IllegalStateException("unsupported value layout");
                    })
                    .build());

            Assertions.assertEquals("value", entry.get("key", ignored -> "value"));
            Assertions.assertNull(entry.getIfPresent("key"));
            Assertions.assertEquals(1L, entry.metrics().getWeightRejectCount());
            Assertions.assertTrue(entry.metrics().getLastWeightRejectReason()
                    .startsWith("incomplete_estimate:estimator_failure:"));
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void overlappingCatalogOwnersShareCatalogBudgetWithoutNameCollision() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(4096L));
        CatalogMetaCache firstOwner = new CatalogMetaCache(
                manager, 12L, "iceberg", Collections.emptyMap());
        CatalogMetaCache secondOwner = new CatalogMetaCache(
                manager, 12L, "iceberg", Collections.emptyMap());
        try {
            MetaCache<String, String> first = weightedStringEntry(firstOwner, "table", 512L);
            MetaCache<String, String> second = weightedStringEntry(secondOwner, "table", 512L);

            first.put("first", "value");
            second.put("second", "value");
            Assertions.assertEquals(2048L, manager.getGlobalUsedWeight());

            firstOwner.close();
            Assertions.assertEquals(1024L, manager.getGlobalUsedWeight());
        } finally {
            firstOwner.close();
            secondOwner.close();
        }
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void physicalCachesInOneBudgetGroupShareEntryLimit() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.empty());
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 18L, "iceberg", Collections.emptyMap())) {
            CacheSpec spec = CacheSpec.ofWeight(true, -1L, 100L, 1536L);
            MetaCache<String, String> first = owner.create(MetaCacheDefinition
                    .<String, String>builder("mvcc-partition-view", spec, ignored -> ScopePath.catalog())
                    .budgetGroup("partition_view")
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(1024L))
                    .build());
            MetaCache<String, String> second = owner.create(MetaCacheDefinition
                    .<String, String>builder("list-partitions-view", spec, ignored -> ScopePath.catalog())
                    .budgetGroup("partition_view")
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(1024L))
                    .build());

            first.put("first", "value");
            second.put("second", "value");

            Assertions.assertNull(second.getIfPresent("second"));
            Assertions.assertTrue(manager.getGlobalUsedWeight() <= 1536L);
        }
    }

    @Test
    void weightedCompareAndSetRejectionCommitsAndInvalidatesOldValue() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(1024L));
        AtomicInteger commits = new AtomicInteger();
        AtomicReference<String> discarded = new AtomicReference<>();
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 13L, "default", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("table", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .discardListener((key, value) -> discarded.set(value))
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(
                            "old".equals(value) ? 512L : 1024L))
                    .build());
            entry.put("key", "old");

            Assertions.assertTrue(entry.compareAndSet("key", "old", "new", commits::incrementAndGet));
            Assertions.assertEquals(1, commits.get());
            Assertions.assertEquals("new", discarded.get());
            Assertions.assertNull(entry.getIfPresent("key"));
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void weightedPutRejectionInvalidatesOldValue() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(1024L));
        AtomicReference<String> discarded = new AtomicReference<>();
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 14L, "default", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("table", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .discardListener((key, value) -> discarded.set(value))
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(
                            "old".equals(value) ? 512L : 1024L))
                    .build());
            entry.put("key", "old");

            entry.put("key", "new");
            Assertions.assertEquals("new", discarded.get());
            Assertions.assertNull(entry.getIfPresent("key"));
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void weightedRefreshRejectionInvalidatesOldValue() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(1024L));
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<String> discarded = new AtomicReference<>();
        try (CatalogMetaCache owner = new CatalogMetaCache(
                manager, 15L, "default", Collections.emptyMap())) {
            MetaCache<String, String> entry = owner.create(MetaCacheDefinition
                    .<String, String>builder("schema", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .loader(key -> "v" + loads.incrementAndGet())
                    .discardListener((key, value) -> discarded.set(value))
                    .refreshAfterWrite(Duration.ofNanos(1L), Runnable::run)
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(
                            "v1".equals(value) ? 512L : 1024L))
                    .build());

            Assertions.assertEquals("v1", entry.get("key"));
            Assertions.assertEquals("v1", entry.get("key"));
            Assertions.assertEquals("v2", discarded.get());
            Assertions.assertNull(entry.getIfPresent("key"));
            Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
        }
    }

    @Test
    void globalOrCatalogWeightLimitRequiresEstimator() {
        MetaCacheBudgetManager globalManager = new MetaCacheBudgetManager(OptionalLong.of(1024L));
        try (CatalogMetaCache owner = new CatalogMetaCache(
                globalManager, 16L, "default", Collections.emptyMap())) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> owner.create(unestimatedStringDefinition("global")));
        }

        MetaCacheBudgetManager catalogManager = new MetaCacheBudgetManager(OptionalLong.empty());
        try (CatalogMetaCache owner = new CatalogMetaCache(catalogManager, 17L, "default",
                Collections.singletonMap(MetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY, "1KB"))) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> owner.create(unestimatedStringDefinition("catalog")));
        }
    }

    private static MetaCache<String, String> weightedStringEntry(
            CatalogMetaCache owner, String name, long estimatedBytes) {
        return owner.create(MetaCacheDefinition
                .<String, String>builder(name, CacheSpec.of(true, -1L, 100L),
                        ignored -> ScopePath.catalog())
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(estimatedBytes))
                .build());
    }

    private static MetaCacheDefinition<String, String> unestimatedStringDefinition(String name) {
        return MetaCacheDefinition.<String, String>builder(
                name, CacheSpec.of(true, -1L, 100L), ignored -> ScopePath.catalog()).build();
    }

    private static void awaitReclaimed(MetaCache<?, ?> cache, long expectedSize, long expectedWeight) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10L).toNanos();
        // Physical removal precedes the deferred callback that releases the reservation. Waiting only for
        // size races the next admission against that callback, even though peer reclamation is asynchronous.
        while ((cache.size() != expectedSize || cache.metrics().getEstimatedWeight() != expectedWeight)
                && System.nanoTime() < deadline) {
            Thread.yield();
        }
        Assertions.assertEquals(expectedSize, cache.size());
        Assertions.assertEquals(expectedWeight, cache.metrics().getEstimatedWeight());
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Set<CatalogMetaCache>> catalogCacheRegistry() throws ReflectiveOperationException {
        Field field = MetaCacheGovernance.class.getDeclaredField("CATALOG_CACHES");
        field.setAccessible(true);
        return (Map<Long, Set<CatalogMetaCache>>) field.get(null);
    }

    private static final class BlockingEmptySet<E> extends AbstractSet<E> {
        private final Set<E> delegate = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final CountDownLatch emptyObserved = new CountDownLatch(1);
        private final CountDownLatch releaseEmpty = new CountDownLatch(1);
        private final CountDownLatch addObserved = new CountDownLatch(1);
        private volatile boolean armed;

        void arm() {
            armed = true;
        }

        boolean awaitEmptyObservation() throws InterruptedException {
            return emptyObserved.await(10L, TimeUnit.SECONDS);
        }

        boolean awaitAdd() throws InterruptedException {
            return addObserved.await(250L, TimeUnit.MILLISECONDS);
        }

        void releaseEmptyObservation() {
            releaseEmpty.countDown();
        }

        @Override
        public Iterator<E> iterator() {
            return delegate.iterator();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean add(E value) {
            boolean added = delegate.add(value);
            if (armed) {
                addObserved.countDown();
            }
            return added;
        }

        @Override
        public boolean remove(Object value) {
            return delegate.remove(value);
        }

        @Override
        public boolean isEmpty() {
            boolean empty = delegate.isEmpty();
            if (armed && empty) {
                emptyObserved.countDown();
                try {
                    releaseEmpty.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while controlling registry removal", e);
                }
            }
            return empty;
        }
    }
}
