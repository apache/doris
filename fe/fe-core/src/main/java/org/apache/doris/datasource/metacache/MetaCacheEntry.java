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
    private final String name;
    @Nullable
    private final Function<K, V> loader;
    private final CacheSpec cacheSpec;
    private final boolean effectiveEnabled;
    private final boolean autoRefresh;
    private final LoadingCache<K, V> data;
    private final AtomicLong invalidateCount = new AtomicLong(0);
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
        this.data = cacheFactory.buildCache(this::loadFromDefaultLoader, refreshExecutor);
    }

    public String name() {
        return name;
    }

    public V get(K key) {
        return data.get(key);
    }

    public V get(K key, Function<K, V> missLoader) {
        Function<K, V> loadFunction = Objects.requireNonNull(missLoader, "missLoader can not be null");
        return data.get(key, typedKey -> loadAndTrack(typedKey, loadFunction));
    }

    public V getIfPresent(K key) {
        return data.getIfPresent(key);
    }

    public void put(K key, V value) {
        data.put(key, value);
    }

    public void invalidateKey(K key) {
        if (data.asMap().remove(key) != null) {
            invalidateCount.incrementAndGet();
        }
    }

    public void invalidateIf(Predicate<K> predicate) {
        data.asMap().keySet().removeIf(key -> {
            if (predicate.test(key)) {
                invalidateCount.incrementAndGet();
                return true;
            }
            return false;
        });
    }

    public void invalidateAll() {
        long size = data.estimatedSize();
        data.invalidateAll();
        invalidateCount.addAndGet(size);
    }

    public void forEach(BiConsumer<K, V> consumer) {
        data.asMap().forEach(consumer);
    }

    public MetaCacheEntryStats stats() {
        CacheStats cacheStats = data.stats();
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
                cacheStats.loadSuccessCount(),
                cacheStats.loadFailureCount(),
                cacheStats.totalLoadTime(),
                cacheStats.averageLoadPenalty(),
                cacheStats.evictionCount(),
                invalidateCount.get(),
                lastLoadSuccessTimeMs.get(),
                lastLoadFailureTimeMs.get(),
                lastError.get());
    }

    private V loadFromDefaultLoader(K key) {
        if (loader == null) {
            throw new UnsupportedOperationException(
                    String.format("Entry '%s' requires a contextual miss loader.", name));
        }
        return loadAndTrack(key, loader);
    }

    private V loadAndTrack(K key, Function<K, V> loadFunction) {
        try {
            V value = loadFunction.apply(key);
            lastLoadSuccessTimeMs.set(System.currentTimeMillis());
            return value;
        } catch (RuntimeException | Error e) {
            lastLoadFailureTimeMs.set(System.currentTimeMillis());
            lastError.set(e.toString());
            throw e;
        }
    }
}
