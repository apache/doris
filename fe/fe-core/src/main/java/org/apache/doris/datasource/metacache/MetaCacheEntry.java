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

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Unified cache entry abstraction.
 * It stores one logical cache dataset and provides optional lazy loading,
 * key/predicate/full invalidation, and lightweight runtime stats.
 */
public class MetaCacheEntry<K, V> {
    private static final int SINGLE_KEY_STRIPES = 1;

    private final String name;
    @Nullable
    private final Function<K, V> loader;
    private final CacheSpec cacheSpec;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    private final int stripeCount;
    private final LoadingCache<K, V> loadingData;
    // Use the plain cache view for manual miss load so slow I/O does not happen in Caffeine's sync load path.
    private final Cache<K, V> data;
    // Protect one stripe at a time to deduplicate concurrent miss loads with bounded lock count.
    private final Object[] loadLocks;
    // Serialize short publication windows so public mutations cannot race with stale write-back.
    private final Object[] publishLocks;
    // Track per-stripe invalidation generations so unrelated keys do not invalidate each other.
    private final AtomicLongArray generations;
    private final AtomicLong invalidateCount = new AtomicLong(0);
    // Track load statistics outside Caffeine because manual miss loads bypass the built-in load counters.
    private final AtomicLong loadSuccessCount = new AtomicLong(0);
    private final AtomicLong loadFailureCount = new AtomicLong(0);
    private final AtomicLong totalLoadTimeNanos = new AtomicLong(0);
    private final AtomicLong lastLoadSuccessTimeMs = new AtomicLong(-1L);
    private final AtomicLong lastLoadFailureTimeMs = new AtomicLong(-1L);
    private final AtomicReference<String> lastError = new AtomicReference<>("");

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor) {
        this(name, loader, cacheSpec, refreshExecutor, true, false, defaultObjectStripeCount(), null, false);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, defaultObjectStripeCount(), null, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly,
                defaultObjectStripeCount(), null, false);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh, int stripeCount) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false, stripeCount, null, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly, int stripeCount) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, contextualOnly, stripeCount, null, false);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, RemovalListener<K, V> removalListener) {
        return withSyncRemovalListener(name, loader, cacheSpec, refreshExecutor,
                defaultObjectStripeCount(), removalListener);
    }

    public static <K, V> MetaCacheEntry<K, V> withSyncRemovalListener(String name, Function<K, V> loader,
            CacheSpec cacheSpec, ExecutorService refreshExecutor, int stripeCount,
            RemovalListener<K, V> removalListener) {
        return new MetaCacheEntry<>(
                name,
                loader,
                cacheSpec,
                refreshExecutor,
                false,
                false,
                stripeCount,
                Objects.requireNonNull(removalListener, "removalListener can not be null"),
                true);
    }

    private MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly,
            int stripeCount, @Nullable RemovalListener<K, V> removalListener, boolean syncRemovalListener) {
        this.name = Objects.requireNonNull(name, "name can not be null");
        if (contextualOnly) {
            if (loader != null) {
                throw new IllegalArgumentException("contextual-only entry loader must be null");
            }
            if (autoRefresh) {
                throw new IllegalArgumentException("contextual-only entry can not enable auto refresh");
            }
        } else {
            Objects.requireNonNull(loader, "loader can not be null");
        }
        if (syncRemovalListener && autoRefresh) {
            throw new IllegalArgumentException("sync removal listener cache can not enable refreshAfterWrite");
        }
        if (removalListener != null && !syncRemovalListener) {
            throw new IllegalArgumentException("asynchronous removal listener is not supported");
        }
        this.loader = loader;
        this.cacheSpec = Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.autoRefresh = autoRefresh;
        if (stripeCount < 1) {
            throw new IllegalArgumentException("stripeCount must be positive");
        }
        this.stripeCount = stripeCount;
        this.loadLocks = new Object[stripeCount];
        this.publishLocks = new Object[stripeCount];
        this.generations = new AtomicLongArray(stripeCount);
        Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
        this.effectiveEnabled = CacheSpec.isCacheEnabled(
                this.cacheSpec.isEnable(), this.cacheSpec.getTtlSecond(), this.cacheSpec.getCapacity());
        OptionalLong expireAfterAccessSec =
                effectiveEnabled ? CacheSpec.toExpireAfterAccess(this.cacheSpec.getTtlSecond()) : OptionalLong.empty();
        OptionalLong refreshAfterWriteSec =
                effectiveEnabled && autoRefresh
                        ? OptionalLong.of(Config.external_cache_refresh_time_minutes * 60)
                        : OptionalLong.empty();
        long maxSize = effectiveEnabled ? this.cacheSpec.getCapacity() : 0L;
        CacheFactory cacheFactory = new CacheFactory(
                expireAfterAccessSec,
                refreshAfterWriteSec,
                maxSize,
                true,
                null);
        // Build through a dedicated loader so refresh reload can check generation before publishing.
        CacheLoader<K, V> cacheLoader = newCacheLoader(refreshExecutor);
        if (syncRemovalListener) {
            this.loadingData = cacheFactory.buildCacheWithSyncRemovalListener(cacheLoader, removalListener);
        } else {
            this.loadingData = cacheFactory.buildCache(cacheLoader, refreshExecutor);
        }
        this.data = loadingData;
        // Initialize striped locks eagerly to keep the hot path allocation-free.
        for (int i = 0; i < loadLocks.length; i++) {
            loadLocks[i] = new Object();
            publishLocks[i] = new Object();
        }
    }

    public String name() {
        return name;
    }

    public V get(K key) {
        return getWithManualLoad(key, this::applyDefaultLoader);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
        return getWithManualLoad(key, loadFunction);
    }

    public V getIfPresent(K key) {
        if (!effectiveEnabled) {
            return null;
        }
        return data.getIfPresent(key);
    }

    public void put(K key, V value) {
        // Public mutations participate in generation control so in-flight loads cannot overwrite them later.
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(value, "value can not be null");
        if (!effectiveEnabled) {
            return;
        }
        synchronized (publishLock(key)) {
            bumpGeneration(key);
            beforePublicMutationWriteForTest(key);
            data.put(key, value);
        }
    }

    public V compute(K key, BiFunction<K, V, V> remappingFunction) {
        // Public compute must also advance the stripe generation before mutating the cache state.
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
        if (!effectiveEnabled) {
            return null;
        }
        synchronized (publishLock(key)) {
            bumpGeneration(key);
            beforePublicMutationWriteForTest(key);
            return data.asMap().compute(key, remappingFunction);
        }
    }

    public void invalidateKey(K key) {
        Objects.requireNonNull(key, "key can not be null");
        synchronized (publishLock(key)) {
            bumpGeneration(key);
            if (data.asMap().remove(key) != null) {
                invalidateCount.incrementAndGet();
            }
        }
    }

    public void invalidateIf(Predicate<K> predicate) {
        bumpAllGenerations();
        data.asMap().keySet().removeIf(key -> {
            if (predicate.test(key)) {
                invalidateCount.incrementAndGet();
                return true;
            }
            return false;
        });
    }

    public void invalidateAll() {
        bumpAllGenerations();
        long size = data.estimatedSize();
        data.invalidateAll();
        invalidateCount.addAndGet(size);
    }

    public void forEach(BiConsumer<K, V> consumer) {
        data.asMap().forEach(consumer);
    }

    public MetaCacheEntryStats stats() {
        CacheStats cacheStats = loadingData.stats();
        long successCount = loadSuccessCount.get();
        long failureCount = loadFailureCount.get();
        long totalLoadTime = totalLoadTimeNanos.get();
        long totalLoadCount = successCount + failureCount;
        return new MetaCacheEntryStats(
                cacheSpec.isEnable(),
                effectiveEnabled,
                autoRefresh,
                cacheSpec.getTtlSecond(),
                cacheSpec.getCapacity(),
                data.estimatedSize(),
                cacheStats.requestCount(),
                cacheStats.hitCount(),
                cacheStats.missCount(),
                cacheStats.hitRate(),
                successCount,
                failureCount,
                totalLoadTime,
                totalLoadCount == 0 ? 0D : (double) totalLoadTime / totalLoadCount,
                cacheStats.evictionCount(),
                invalidateCount.get(),
                lastLoadSuccessTimeMs.get(),
                lastLoadFailureTimeMs.get(),
                lastError.get());
    }

    // Execute slow miss loads outside Caffeine's sync load path and suppress stale write-back after invalidation.
    private V getWithManualLoad(K key, Function<K, V> loadFunction) {
        if (!effectiveEnabled) {
            // Bypass cache entirely when the entry is disabled so manual miss load does not relax disable semantics.
            return loadAndTrack(key, loadFunction);
        }

        V value = data.getIfPresent(key);
        if (value != null) {
            return value;
        }

        synchronized (loadLock(key)) {
            value = data.asMap().get(key);
            if (value != null) {
                return value;
            }

            long generation;
            // Snapshot generation only after re-checking the cache under the publication lock so
            // public mutations cannot slip between the miss observation and the captured version.
            synchronized (publishLock(key)) {
                value = data.asMap().get(key);
                if (value != null) {
                    return value;
                }
                generation = generationOf(key);
            }
            V loaded = loadAndTrack(key, loadFunction);
            if (loaded == null) {
                return null;
            }

            synchronized (publishLock(key)) {
                if (generation != generationOf(key)) {
                    return loaded;
                }
                // Leave a narrow hook for tests to pause exactly before the cache put race window.
                beforeManualCachePutForTest(key, loaded);
                putLoadedValueWithoutGenerationBump(key, loaded);
                if (generation != generationOf(key)) {
                    removeLoadedValueWithoutGenerationBump(key, loaded);
                }
            }
            return loaded;
        }
    }

    // Keep internal load write-back separate from public mutation so it does not advance generation.
    private void putLoadedValueWithoutGenerationBump(K key, V loaded) {
        data.put(key, loaded);
    }

    // Remove only the value loaded by the current request and keep newer replacements intact.
    private void removeLoadedValueWithoutGenerationBump(K key, V loaded) {
        data.asMap().computeIfPresent(key, (ignored, currentValue) -> currentValue == loaded ? null : currentValue);
    }

    private CacheLoader<K, V> newCacheLoader(ExecutorService refreshExecutor) {
        return new CacheLoader<K, V>() {
            @Override
            public V load(K key) {
                return loadFromDefaultLoader(key);
            }

            @Override
            public CompletableFuture<V> asyncReload(K key, V oldValue, Executor executor) {
                long generation;
                synchronized (publishLock(key)) {
                    V current = data.getIfPresent(key);
                    if (current != null && !Objects.equals(current, oldValue)) {
                        CompletableFuture<V> cancelled = new CompletableFuture<>();
                        cancelled.cancel(false);
                        return cancelled;
                    }
                    generation = generationOf(key);
                }
                CompletableFuture<V> result = new CompletableFuture<>();
                CompletableFuture.supplyAsync(() -> loadFromDefaultLoader(key), refreshExecutor)
                        .whenComplete((loaded, error) -> {
                            synchronized (publishLock(key)) {
                                if (error != null) {
                                    result.completeExceptionally(error);
                                } else if (canPublishAsyncReload(key, oldValue, generation)) {
                                    // Complete the future while holding the publish lock so Caffeine's
                                    // refresh write-back cannot interleave with same-key invalidation.
                                    beforeAsyncReloadCompleteForTest(key, loaded);
                                    result.complete(loaded);
                                } else {
                                    result.cancel(false);
                                }
                            }
                        });
                return result;
            }
        };
    }

    private boolean canPublishAsyncReload(K key, V oldValue, long generation) {
        return generation == generationOf(key) && Objects.equals(data.getIfPresent(key), oldValue);
    }

    private int stripe(K key) {
        int hash = key == null ? 0 : key.hashCode();
        return (hash & Integer.MAX_VALUE) % stripeCount;
    }

    // Map keys to a fixed lock stripe set to bound memory usage while keeping same-key deduplication.
    private Object loadLock(K key) {
        return loadLocks[stripe(key)];
    }

    private Object publishLock(K key) {
        return publishLocks[stripe(key)];
    }

    private long generationOf(K key) {
        return generations.get(stripe(key));
    }

    private void bumpGeneration(K key) {
        generations.incrementAndGet(stripe(key));
    }

    private void bumpAllGenerations() {
        for (int i = 0; i < stripeCount; i++) {
            generations.incrementAndGet(i);
        }
    }

    public static int defaultObjectStripeCount() {
        return Config.external_meta_cache_object_entry_lock_stripes;
    }

    public static int singleKeyStripeCount() {
        return SINGLE_KEY_STRIPES;
    }

    int stripeCountForTest() {
        return stripeCount;
    }

    // Let tests pause between the first generation check and data.put without affecting production behavior.
    void beforeManualCachePutForTest(K key, V loaded) {
    }

    void beforePublicMutationWriteForTest(K key) {
    }

    void beforeAsyncReloadCompleteForTest(K key, V loaded) {
    }

    private V loadFromDefaultLoader(K key) {
        return loadAndTrack(key, this::applyDefaultLoader);
    }

    // Resolve the default loader separately so the manual path can share tracking without double counting.
    private V applyDefaultLoader(K key) {
        if (loader == null) {
            throw new UnsupportedOperationException(
                    String.format("Entry '%s' requires a contextual miss loader.", name));
        }
        return loader.apply(key);
    }

    // Track load outcomes locally because manual miss loads do not contribute to Caffeine load statistics.
    private V loadAndTrack(K key, Function<K, V> loadFunction) {
        long startNanos = System.nanoTime();
        try {
            V value = loadFunction.apply(key);
            loadSuccessCount.incrementAndGet();
            totalLoadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            lastLoadSuccessTimeMs.set(System.currentTimeMillis());
            return value;
        } catch (RuntimeException | Error e) {
            loadFailureCount.incrementAndGet();
            totalLoadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            lastLoadFailureTimeMs.set(System.currentTimeMillis());
            lastError.set(e.toString());
            throw e;
        }
    }
}
