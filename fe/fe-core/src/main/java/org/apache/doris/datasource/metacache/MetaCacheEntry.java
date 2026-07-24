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
import java.util.function.BiPredicate;
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
        CacheLoader<K, V> cacheLoader = newCacheLoader();
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
        return getWithManualLoad(key, this::applyDefaultLoader, null, null);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
        return getWithManualLoad(key, loadFunction, null, null);
    }

    /**
     * Get the current value and run a short local action under the same per-key publication protocol.
     *
     * <p>For an enabled entry, the action only runs when a hot value is still current or a miss load is accepted for
     * publication. For a disabled entry, the value is not cached, but the action still runs when no invalidation
     * occurred during the load. A generation-rejected load may still be returned to its caller, but never runs the
     * action.
     */
    public V getAndRunIfCurrent(K key, BiConsumer<K, V> currentValueAction) {
        return getAndRunIfCurrent(key, (ignored, value) -> true, currentValueAction);
    }

    /**
     * Get the current value and conditionally run a short local action under the per-key publication protocol.
     *
     * <p>A hot value returns without entering the publication lock when {@code actionRequired} is false. When the
     * action is required, both the value identity and the condition are re-checked under the lock before running it.
     */
    public V getAndRunIfCurrent(K key, BiPredicate<K, V> actionRequired,
            BiConsumer<K, V> currentValueAction) {
        BiPredicate<K, V> required = Objects.requireNonNull(
                actionRequired, "actionRequired can not be null");
        BiConsumer<K, V> action = Objects.requireNonNull(
                currentValueAction, "currentValueAction can not be null");
        return getWithManualLoad(key, this::applyDefaultLoader, required, action);
    }

    public V getIfPresent(K key) {
        if (!effectiveEnabled) {
            return null;
        }
        return data.getIfPresent(key);
    }

    @Nullable
    public V findIfPresent(Predicate<K> keyPredicate) {
        if (!effectiveEnabled) {
            return null;
        }
        // Replay-only fallback needs a cache-only scan over current hot keys without triggering load-through.
        for (java.util.Map.Entry<K, V> entry : data.asMap().entrySet()) {
            if (keyPredicate.test(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
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

    /**
     * Compute the cached value and update related local state in one per-key publication window.
     *
     * <p>The action always runs, including when the cache is disabled or the remapping function keeps a cold key
     * absent. This allows callers to maintain lightweight auxiliary indexes without warming the object entry.
     */
    public V computeAndRun(K key, BiFunction<K, V, V> remappingFunction, Runnable afterMutation) {
        Objects.requireNonNull(key, "key can not be null");
        Objects.requireNonNull(remappingFunction, "remappingFunction can not be null");
        Runnable action = Objects.requireNonNull(afterMutation, "afterMutation can not be null");
        synchronized (publishLock(key)) {
            bumpGeneration(key);
            beforePublicMutationWriteForTest(key);
            V value = effectiveEnabled ? data.asMap().compute(key, remappingFunction) : null;
            action.run();
            return value;
        }
    }

    public void invalidateKey(K key) {
        invalidateKeyAndRun(key, () -> {
        });
    }

    /**
     * Invalidate one cached key and update related local state in the same publication window.
     *
     * <p>The action runs even if the object entry is already absent or disabled because auxiliary indexes may
     * intentionally outlive object-cache eviction.
     */
    public void invalidateKeyAndRun(K key, Runnable afterInvalidation) {
        Objects.requireNonNull(key, "key can not be null");
        Runnable action = Objects.requireNonNull(afterInvalidation, "afterInvalidation can not be null");
        synchronized (publishLock(key)) {
            bumpGeneration(key);
            if (data.asMap().remove(key) != null) {
                invalidateCount.incrementAndGet();
            }
            action.run();
        }
    }

    public void invalidateIf(Predicate<K> predicate) {
        Objects.requireNonNull(predicate, "predicate can not be null");
        // Cover in-flight manual loads whose keys are still outside the cache map.
        bumpAllGenerations();
        for (K key : data.asMap().keySet()) {
            if (predicate.test(key)) {
                invalidateKey(key);
            }
        }
    }

    public void invalidateAll() {
        // Cover in-flight manual loads whose keys are still outside the cache map.
        bumpAllGenerations();
        for (K key : data.asMap().keySet()) {
            invalidateKey(key);
        }
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
    private V getWithManualLoad(K key, Function<K, V> loadFunction,
            @Nullable BiPredicate<K, V> currentValueActionRequired,
            @Nullable BiConsumer<K, V> currentValueAction) {
        if (!effectiveEnabled) {
            if (currentValueAction == null) {
                // Preserve the ordinary disabled-entry path without adding publication coordination.
                return loadAndTrack(key, loadFunction);
            }
            // Bypass object publication when disabled, but still fence an auxiliary-index update against invalidation.
            long generation;
            synchronized (publishLock(key)) {
                generation = generationOf(key);
            }
            V loaded = loadAndTrack(key, loadFunction);
            if (loaded == null) {
                return loaded;
            }
            beforeCurrentValueActionForTest(key, loaded);
            synchronized (publishLock(key)) {
                if (generation == generationOf(key)
                        && currentValueActionRequired.test(key, loaded)) {
                    currentValueAction.accept(key, loaded);
                }
            }
            return loaded;
        }

        V value = data.getIfPresent(key);
        if (value != null) {
            runCurrentValueActionIfPresent(
                    key, value, currentValueActionRequired, currentValueAction);
            return value;
        }

        // Keep the slow miss load under the per-key load lock so concurrent misses for the same key
        // are still deduplicated. publishLock(key) only protects the short publication window.
        synchronized (loadLock(key)) {
            value = data.asMap().get(key);
            if (value != null) {
                runCurrentValueActionIfPresent(
                        key, value, currentValueActionRequired, currentValueAction);
                return value;
            }

            long generation;
            // Snapshot generation only after re-checking the cache under the publication lock so
            // public mutations cannot slip between the miss observation and the captured version.
            synchronized (publishLock(key)) {
                value = data.asMap().get(key);
                if (value != null) {
                    if (currentValueAction != null
                            && currentValueActionRequired.test(key, value)) {
                        currentValueAction.accept(key, value);
                    }
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
                // Re-check after the put because invalidateAll()/invalidateIf() may bump stripe generations
                // outside publishLock while this thread is publishing a loaded value.
                if (generation != generationOf(key)) {
                    removeLoadedValueWithoutGenerationBump(key, loaded);
                } else if (currentValueAction != null
                        && currentValueActionRequired.test(key, loaded)) {
                    currentValueAction.accept(key, loaded);
                }
            }
            return loaded;
        }
    }

    private void runCurrentValueActionIfPresent(K key, V value,
            @Nullable BiPredicate<K, V> currentValueActionRequired,
            @Nullable BiConsumer<K, V> currentValueAction) {
        if (currentValueAction == null) {
            return;
        }
        if (!currentValueActionRequired.test(key, value)) {
            return;
        }
        beforeCurrentValueActionForTest(key, value);
        synchronized (publishLock(key)) {
            if (data.asMap().get(key) == value
                    && currentValueActionRequired.test(key, value)) {
                currentValueAction.accept(key, value);
            }
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

    private CacheLoader<K, V> newCacheLoader() {
        return new CacheLoader<K, V>() {
            @Override
            public V load(K key) {
                return loadFromDefaultLoader(key);
            }

            @Override
            public CompletableFuture<V> asyncReload(K key, V oldValue, Executor executor) {
                long generation = generationOf(key);
                CompletableFuture<V> result = new CompletableFuture<>();
                CompletableFuture.supplyAsync(() -> loadFromDefaultLoader(key), executor)
                        .whenComplete((loaded, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                                return;
                            }
                            synchronized (publishLock(key)) {
                                if (generation == generationOf(key)) {
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

    // Let tests pause after a hot value is observed but before its related local state is published.
    protected void beforeCurrentValueActionForTest(K key, V value) {
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
