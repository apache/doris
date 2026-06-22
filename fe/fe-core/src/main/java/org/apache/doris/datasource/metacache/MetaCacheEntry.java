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
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Unified cache entry abstraction.
 * It stores one logical cache dataset and provides optional lazy loading,
 * key/predicate/full invalidation, and lightweight runtime stats.
 */
public class MetaCacheEntry<K, V> {
    // Use striped locks to deduplicate slow external loads without managing per-key lock lifecycle.
    private static final int LOAD_LOCK_STRIPES = 128;

    private final String name;
    @Nullable
    private final Function<K, V> loader;
    private final CacheSpec cacheSpec;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    // Keep the loading cache for refreshAfterWrite and the legacy sync-load path when the feature is disabled.
    private final LoadingCache<K, V> loadingData;
    // Use the plain cache view for manual miss load so slow I/O does not happen in Caffeine's sync load path.
    private final Cache<K, V> data;
    // Protect one key stripe at a time to deduplicate concurrent miss loads with bounded lock count.
    private final Object[] loadLocks = new Object[LOAD_LOCK_STRIPES];
    private final AtomicLong invalidateCount = new AtomicLong(0);
    // Bump generation before invalidation so in-flight manual loads do not repopulate stale values.
    private final AtomicLong invalidateGeneration = new AtomicLong(0);
    // Track load statistics outside Caffeine because manual miss loads bypass the built-in load counters.
    private final AtomicLong loadSuccessCount = new AtomicLong(0);
    private final AtomicLong loadFailureCount = new AtomicLong(0);
    private final AtomicLong totalLoadTimeNanos = new AtomicLong(0);
    private final AtomicLong lastLoadSuccessTimeMs = new AtomicLong(-1L);
    private final AtomicLong lastLoadFailureTimeMs = new AtomicLong(-1L);
    private final AtomicReference<String> lastError = new AtomicReference<>("");

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor) {
        this(name, loader, cacheSpec, refreshExecutor, true, false);
    }

    public MetaCacheEntry(String name, Function<K, V> loader, CacheSpec cacheSpec, ExecutorService refreshExecutor,
            boolean autoRefresh) {
        this(name, loader, cacheSpec, refreshExecutor, autoRefresh, false);
    }

    public MetaCacheEntry(String name, @Nullable Function<K, V> loader, CacheSpec cacheSpec,
            ExecutorService refreshExecutor, boolean autoRefresh, boolean contextualOnly) {
        this.name = name;
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
        this.loader = loader;
        this.cacheSpec = Objects.requireNonNull(cacheSpec, "cacheSpec can not be null");
        this.autoRefresh = autoRefresh;
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
        this.loadingData = cacheFactory.buildCache(this::loadFromDefaultLoader, refreshExecutor);
        this.data = loadingData;
        // Initialize striped locks eagerly to keep the hot path allocation-free.
        for (int i = 0; i < loadLocks.length; i++) {
            loadLocks[i] = new Object();
        }
    }

    public String name() {
        return name;
    }

    public V get(K key) {
        if (!isManualMissLoadEnabled()) {
            return loadingData.get(key);
        }
        return getWithManualLoad(key, this::applyDefaultLoader);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
        if (!isManualMissLoadEnabled()) {
            return loadingData.get(key, typedKey -> loadAndTrack(typedKey, loadFunction));
        }
        return getWithManualLoad(key, loadFunction);
    }

    public V getIfPresent(K key) {
        return data.getIfPresent(key);
    }

    public void put(K key, V value) {
        data.put(key, value);
    }

    public void invalidateKey(K key) {
        invalidateGeneration.incrementAndGet();
        if (data.asMap().remove(key) != null) {
            invalidateCount.incrementAndGet();
        }
    }

    public void invalidateIf(Predicate<K> predicate) {
        invalidateGeneration.incrementAndGet();
        data.asMap().keySet().removeIf(key -> {
            if (predicate.test(key)) {
                invalidateCount.incrementAndGet();
                return true;
            }
            return false;
        });
    }

    public void invalidateAll() {
        invalidateGeneration.incrementAndGet();
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

    // Read the config dynamically so existing cache entries follow runtime config updates.
    private boolean isManualMissLoadEnabled() {
        return Config.enable_external_meta_cache_manual_miss_load;
    }

    // Execute slow miss loads outside Caffeine's sync load path and suppress stale write-back after invalidation.
    private V getWithManualLoad(K key, Function<K, V> loadFunction) {
        V value = data.getIfPresent(key);
        if (value != null) {
            return value;
        }

        synchronized (loadLock(key)) {
            value = data.asMap().get(key);
            if (value != null) {
                return value;
            }

            long generation = invalidateGeneration.get();
            V loaded = loadAndTrack(key, loadFunction);
            if (generation != invalidateGeneration.get()) {
                return loaded;
            }

            // Leave a narrow hook for tests to pause exactly before the cache put race window.
            beforeManualCachePutForTest(key, loaded);
            data.put(key, loaded);
            if (generation != invalidateGeneration.get()) {
                removeLoadedValue(key, loaded);
            }
            return loaded;
        }
    }

    // Remove only the value loaded by the current request and keep newer replacements intact.
    private void removeLoadedValue(K key, V loaded) {
        data.asMap().computeIfPresent(key, (ignored, currentValue) -> currentValue == loaded ? null : currentValue);
    }

    // Map keys to a fixed lock stripe set to bound memory usage while keeping same-key deduplication.
    private Object loadLock(K key) {
        int hash = key == null ? 0 : key.hashCode();
        return loadLocks[(hash & Integer.MAX_VALUE) % loadLocks.length];
    }

    // Let tests pause between the first generation check and data.put without affecting production behavior.
    void beforeManualCachePutForTest(K key, V loaded) {
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
