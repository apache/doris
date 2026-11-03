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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class MetaCacheSharedLoadTest {
    private static MetaCache<String, String> cache(CatalogMetaCache owner, boolean enabled) {
        return owner.create(MetaCacheDefinition.<String, String>builder("shared-stage",
                CacheSpec.of(enabled, -1, 100), ignored -> ScopePath.table("db", "tbl")).build());
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void sharedObserverReturnsBeforeTheElectedLoadAndCannotCancelIt() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> cache = cache(owner, true);
            Function<String, String> loader = key -> {
                loads.incrementAndGet();
                entered.countDown();
                await(release);
                return "value";
            };
            Future<CompletionStage<String>> elected = workers.submit(() -> cache.getOrLoadFuture("key", loader));
            await(entered);
            CompletionStage<String> shared = workers.submit(() -> cache.getOrLoadFuture("key", loader))
                    .get(2, TimeUnit.SECONDS);
            Assertions.assertFalse(shared.toCompletableFuture().isDone());
            Assertions.assertEquals("other query", workers.submit(() -> "other query").get(2, TimeUnit.SECONDS));
            Assertions.assertTrue(shared.toCompletableFuture().cancel(false));
            CompletableFuture<String> survivor = shared.toCompletableFuture();
            CompletableFuture<String> brokenCallback = shared.<String>thenApply(value -> {
                throw new IllegalArgumentException("observer failed");
            }).toCompletableFuture();
            release.countDown();
            Assertions.assertEquals("value", elected.get(5, TimeUnit.SECONDS).toCompletableFuture().get());
            Assertions.assertEquals("value", survivor.get(5, TimeUnit.SECONDS));
            Assertions.assertThrows(ExecutionException.class, brokenCallback::get);
            Assertions.assertEquals(1, loads.get());
            Assertions.assertEquals("value", cache.getIfPresent("key"));
            Assertions.assertEquals("value", cache.getOrLoadFuture("key", key -> {
                throw new AssertionError("cached value must not load");
            }).toCompletableFuture().get());
        } finally {
            release.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void sharedFailureReachesObserversAndAllowsRetry() throws Exception {
        for (Throwable expected : new Throwable[] {new IllegalStateException("remote failure"),
                new AssertionError("remote error")}) {
            ExecutorService worker = Executors.newSingleThreadExecutor();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                MetaCache<String, String> cache = cache(owner, true);
                Future<?> elected = worker.submit(() -> cache.getOrLoadFuture("key", key -> {
                    entered.countDown();
                    await(release);
                    if (expected instanceof Error) {
                        throw (Error) expected;
                    }
                    throw (RuntimeException) expected;
                }));
                await(entered);
                CompletionStage<String> shared = cache.getOrLoadFuture("key", key -> {
                    throw new AssertionError("duplicate load");
                });
                release.countDown();
                Assertions.assertSame(expected,
                        Assertions.assertThrows(ExecutionException.class, () -> elected.get(5, TimeUnit.SECONDS))
                                .getCause());
                Assertions.assertSame(expected, Assertions.assertThrows(ExecutionException.class,
                        () -> shared.toCompletableFuture().get(5, TimeUnit.SECONDS)).getCause());
                Assertions.assertEquals("retried", cache.getOrLoadFuture("key", key -> "retried")
                        .toCompletableFuture().get());
                Assertions.assertEquals(0, cache.metrics().getInFlightLoadCount());
            } finally {
                release.countDown();
                worker.shutdownNow();
            }
        }
    }

    @Test
    void invalidationSeparatesNewLoadWithoutChangingTheOldObservers() throws Exception {
        for (String operation : new String[] {"key", "table", "catalog", "close"}) {
            ExecutorService worker = Executors.newSingleThreadExecutor();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                MetaCache<String, String> cache = cache(owner, true);
                Future<?> elected = worker.submit(() -> cache.getOrLoadFuture("key", key -> {
                    entered.countDown();
                    await(release);
                    return "old snapshot";
                }));
                await(entered);
                CompletionStage<String> observer = cache.getOrLoadFuture("key", key -> {
                    throw new AssertionError("duplicate old load");
                });
                switch (operation) {
                    case "key":
                        cache.invalidateKey("key");
                        break;
                    case "table":
                        owner.invalidateTable("db", "tbl");
                        break;
                    case "catalog":
                        owner.invalidateCatalog();
                        break;
                    case "close":
                        owner.close();
                        break;
                    default:
                        throw new AssertionError(operation);
                }
                if (!operation.equals("close")) {
                    Assertions.assertEquals("new snapshot", cache.getOrLoadFuture("key", key -> "new snapshot")
                            .toCompletableFuture().get(2, TimeUnit.SECONDS));
                }
                release.countDown();
                elected.get(5, TimeUnit.SECONDS);
                Assertions.assertEquals("old snapshot", observer.toCompletableFuture().get(5, TimeUnit.SECONDS));
                if (operation.equals("close")) {
                    Assertions.assertThrows(IllegalStateException.class,
                            () -> cache.getOrLoadFuture("key", key -> "closed"));
                } else {
                    Assertions.assertEquals("new snapshot", cache.getIfPresent("key"),
                            "old completion must not replace a newer generation");
                }
            } finally {
                release.countDown();
                worker.shutdownNow();
            }
        }
    }

    @Test
    void disabledAndNullLoadsRetainNoValue() throws Exception {
        for (boolean enabled : new boolean[] {false, true}) {
            AtomicInteger loads = new AtomicInteger();
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                MetaCache<String, String> cache = cache(owner, enabled);
                for (int i = 0; i < 2; i++) {
                    Assertions.assertNull(cache.getOrLoadFuture("null", key -> {
                        loads.incrementAndGet();
                        return null;
                    }).toCompletableFuture().get());
                }
                Assertions.assertEquals(2, loads.get());
                Assertions.assertEquals(0, cache.size());
                if (!enabled) {
                    Assertions.assertEquals("uncached", cache.getOrLoadFuture("key", key -> "uncached")
                            .toCompletableFuture().get());
                    Assertions.assertNull(cache.getIfPresent("key"));
                }
            }
        }
    }
}
