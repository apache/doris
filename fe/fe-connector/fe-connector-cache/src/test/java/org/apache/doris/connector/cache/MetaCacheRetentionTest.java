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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class MetaCacheRetentionTest {
    private static CacheSpec spec(boolean weighted) {
        return weighted ? CacheSpec.ofWeight(true, 60, 10, 10000) : CacheSpec.of(true, 60, 10);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void declinedLoadsReturnTheirValueAndReleaseCacheOwnership(boolean weighted) throws Exception {
        AtomicInteger discards = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> cache = owner.create(MetaCacheDefinition.<String, String>builder(
                    "request-owned", spec(weighted), ignored -> ScopePath.table("db", "t"))
                    .retainIf(value -> !value.startsWith("ephemeral"))
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(64))
                    .discardListener((key, value) -> discards.incrementAndGet()).build());
            Assertions.assertEquals("ephemeral-one", cache.get("key", ignored -> "ephemeral-one"));
            Assertions.assertNull(cache.getIfPresent("key"));
            Assertions.assertEquals("ephemeral-two",
                    cache.getOrLoadFuture("key", ignored -> "ephemeral-two").toCompletableFuture()
                            .get(10, TimeUnit.SECONDS));
            Assertions.assertNull(cache.getIfPresent("key"));
            Assertions.assertEquals(2, discards.get());
            Assertions.assertEquals(0, cache.size());
            Assertions.assertEquals(weighted ? 0L : -1L, cache.metrics().getEstimatedWeight());

            Assertions.assertEquals("retained", cache.get("key", ignored -> "retained"));
            Assertions.assertEquals("retained", cache.get("key", ignored -> "unused"));
            Assertions.assertEquals(1, cache.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void directBulkAndReplacementPublicationHonorTheSamePredicate(boolean weighted) {
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> cache = owner.create(MetaCacheDefinition.<String, String>builder(
                    "publications", spec(weighted), ignored -> ScopePath.table("db", "t"))
                    .retainIf(value -> !value.startsWith("ephemeral"))
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(64)).build());
            cache.put("direct", "ephemeral-direct");
            Assertions.assertNull(cache.getIfPresent("direct"));
            try (MetaCache.BulkLoad<String, String> bulk = cache.beginBulkLoad(ScopePath.catalog())) {
                Assertions.assertFalse(bulk.publish("bulk", "ephemeral-bulk"));
                Assertions.assertTrue(bulk.publish("plain", "retained"));
            }
            Assertions.assertNull(cache.getIfPresent("bulk"));
            Assertions.assertEquals("retained", cache.getIfPresent("plain"));
            Assertions.assertTrue(cache.compareAndSet("plain", "retained", "ephemeral-replacement"));
            Assertions.assertNull(cache.getIfPresent("plain"),
                    "the authoritative update commits while declining retention of its new value");
            owner.invalidateTable("db", "t");
            Assertions.assertEquals(0, cache.size());
            Assertions.assertEquals(weighted ? 0L : -1L, cache.metrics().getEstimatedWeight());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void refreshRetiresThePreviousValueWhenTheNewValueCannotBeRetained(boolean weighted) {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger removals = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> cache = owner.create(MetaCacheDefinition.<String, String>builder(
                    "refresh-retention", spec(weighted), ignored -> ScopePath.table("db", "t"))
                    .loader(key -> loads.incrementAndGet() == 1 ? "retained" : "ephemeral")
                    .retainIf(value -> !value.equals("ephemeral"))
                    .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(64))
                    .discardListener((key, value) -> discards.incrementAndGet())
                    .removalListener((key, value, reason) -> removals.incrementAndGet())
                    .refreshAfterWrite(Duration.ofNanos(1), Runnable::run).build());
            Assertions.assertEquals("retained", cache.get("key"));
            Assertions.assertEquals("retained", cache.get("key"));
            Assertions.assertEquals(2, loads.get());
            Assertions.assertNull(cache.getIfPresent("key"));
            Assertions.assertEquals(1, discards.get());
            Assertions.assertEquals(1, removals.get());
            Assertions.assertEquals(weighted ? 0L : -1L, cache.metrics().getEstimatedWeight());
        }
        Assertions.assertEquals(1, discards.get());
        Assertions.assertEquals(1, removals.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void requestScopesDoNotShareDeclinedInFlightValues(boolean future) throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> cache = owner.create(MetaCacheDefinition.<String, String>builder(
                    "isolated", spec(false), ignored -> ScopePath.table("db", "t"))
                    .retainIf(value -> false).build());
            Future<String> first = workers.submit(() -> scopedRead(cache, future, "first", entered, release));
            Future<String> second = workers.submit(() -> scopedRead(cache, future, "second", entered, release));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS), "both request loaders must run");
            release.countDown();
            Assertions.assertEquals("first", first.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals("second", second.get(10, TimeUnit.SECONDS));
            Assertions.assertNull(cache.getIfPresent("key"));
        } finally {
            release.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static String scopedRead(MetaCache<String, String> cache, boolean future, String value,
            CountDownLatch entered, CountDownLatch release) {
        Function<String, String> loader = ignored -> {
            entered.countDown();
            await(release);
            return value;
        };
        return future ? cache.getOrLoadFutureForLoadScope("key", new Object(), loader).toCompletableFuture().join()
                : cache.getForLoadScope("key", new Object(), loader);
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
