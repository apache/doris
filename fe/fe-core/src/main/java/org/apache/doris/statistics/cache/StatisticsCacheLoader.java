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

package org.apache.doris.statistics.cache;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Keeps statistics SQL off the cache maintenance executor. */
final class StatisticsCacheLoader<K, V> implements AsyncCacheLoader<K, V> {
    @VisibleForTesting
    static final long REFRESH_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final ThreadLocal<Boolean> EXPLICIT_REFRESH = new ThreadLocal<>();

    private final BasicAsyncCacheLoader<K, V> delegate;
    private final Executor executor;
    private final Ticker ticker;
    private final AtomicLong retryAfter;
    private final CompletableFuture<V> skippedRefresh = new CompletableFuture<>();

    StatisticsCacheLoader(BasicAsyncCacheLoader<K, V> delegate, Executor executor) {
        this(delegate, executor, Ticker.systemTicker());
    }

    @VisibleForTesting
    StatisticsCacheLoader(BasicAsyncCacheLoader<K, V> delegate, Executor executor, Ticker ticker) {
        this.delegate = delegate;
        this.executor = executor;
        this.ticker = ticker;
        retryAfter = new AtomicLong(ticker.read());
        skippedRefresh.cancel(false);
    }

    static <K, V> void refresh(AsyncLoadingCache<K, V> cache, K key) {
        // Caffeine invokes asyncReload synchronously while submitting an explicit refresh.
        EXPLICIT_REFRESH.set(true);
        try {
            cache.synchronous().refresh(key);
        } catch (RejectedExecutionException e) {
            // A cold load was rejected before publishing a future.
        } finally {
            EXPLICIT_REFRESH.remove();
        }
    }

    @Override
    public CompletableFuture<V> asyncLoad(K key, Executor ignored) {
        // Throw synchronously on rejection, so Caffeine does not publish a pending entry.
        return delegate.asyncLoad(key, executor);
    }

    @Override
    public CompletableFuture<V> asyncReload(K key, V oldValue, Executor ignored) {
        boolean explicit = Boolean.TRUE.equals(EXPLICIT_REFRESH.get());
        if (!explicit && retryAfter.get() - ticker.read() > 0) {
            return skippedRefresh;
        }
        try {
            return asyncLoad(key, ignored);
        } catch (RejectedExecutionException e) {
            long nextRetry = ticker.read() + REFRESH_RETRY_DELAY_NANOS;
            retryAfter.accumulateAndGet(nextRetry, (current, next) -> next - current > 0 ? next : current);
            // A null reload removes only the old entry Caffeine captured for an explicit refresh.
            // An automatic refresh cancellation preserves the old value during backoff.
            return explicit ? CompletableFuture.completedFuture(null) : skippedRefresh;
        }
    }
}
