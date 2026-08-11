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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

/**
 * Factory to create Caffeine cache.
 *
 * <p>Connector-side copy of fe-core {@code org.apache.doris.common.CacheFactory} (independent-copy meta-cache
 * migration): connector plugins cannot import fe-core, so the framework is duplicated under
 * {@code org.apache.doris.connector.cache}. This type is framework-internal — its public methods RETURN
 * Caffeine types, which must never cross to connector (child-first) code; connectors only touch the
 * Caffeine-free {@link MetaCacheEntry} API. Keep behaviourally in sync with the fe-core original.
 *
 * <p>This class is used to create Caffeine cache with specified parameters.
 * It is used to create both sync and async cache.
 * The cache is created with the following parameters:
 * - expireAfterAccessSec: The duration after which the cache entries will expire.
 * - refreshAfterWriteSec: The duration after which the cache entries will be refreshed.
 * - maxSize: The maximum size of the cache.
 * - enableStats: Whether to enable stats for the cache.
 * - ticker: The ticker to use for the cache.
 */
public class CacheFactory {

    private OptionalLong expireAfterAccessSec;
    private OptionalLong refreshAfterWriteSec;
    private long maxSize;
    private boolean enableStats;
    // Ticker is used to provide a time source for the cache.
    // Only used for test, to provide a fake time source.
    // If not provided, the system time is used.
    private Ticker ticker;

    public CacheFactory(
            OptionalLong expireAfterAccessSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            boolean enableStats,
            Ticker ticker) {
        this.expireAfterAccessSec = expireAfterAccessSec;
        this.refreshAfterWriteSec = refreshAfterWriteSec;
        this.maxSize = maxSize;
        this.enableStats = enableStats;
        this.ticker = ticker;
    }

    // Build a loading cache, without executor, it will use fork-join pool for refresh
    public <K, V> LoadingCache<K, V> buildCache(CacheLoader<K, V> cacheLoader) {
        Caffeine<Object, Object> builder = buildWithParams();
        return builder.build(cacheLoader);
    }

    // Build a loading cache, with executor, it will use given executor for refresh
    public <K, V> LoadingCache<K, V> buildCache(CacheLoader<K, V> cacheLoader,
            ExecutorService executor) {
        Caffeine<Object, Object> builder = buildWithParams();
        builder.executor(executor);
        return builder.build(cacheLoader);
    }

    // Build cache with sync removal listener to prevent deadlock when listener calls invalidateAll()
    public <K, V> LoadingCache<K, V> buildCacheWithSyncRemovalListener(CacheLoader<K, V> cacheLoader,
            RemovalListener<K, V> removalListener) {
        Caffeine<Object, Object> builder = buildWithParams();
        if (removalListener != null) {
            builder.removalListener(removalListener);
        }
        builder.executor(Runnable::run);  // Sync execution to avoid thread pool deadlock
        return builder.build(cacheLoader);
    }

    // Build an async loading cache
    public <K, V> AsyncLoadingCache<K, V> buildAsyncCache(AsyncCacheLoader<K, V> cacheLoader,
            ExecutorService executor) {
        Caffeine<Object, Object> builder = buildWithParams();
        builder.executor(executor);
        return builder.buildAsync(cacheLoader);
    }

    private Caffeine<Object, Object> buildWithParams() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        builder.maximumSize(maxSize);

        if (expireAfterAccessSec.isPresent()) {
            builder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSec.getAsLong()));
        }
        if (refreshAfterWriteSec.isPresent()) {
            builder.refreshAfterWrite(Duration.ofSeconds(refreshAfterWriteSec.getAsLong()));
        }

        if (enableStats) {
            builder.recordStats();
        }

        if (ticker != null) {
            builder.ticker(ticker);
        }
        return builder;
    }
}
