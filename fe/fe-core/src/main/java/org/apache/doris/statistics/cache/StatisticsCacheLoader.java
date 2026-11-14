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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/** Keeps statistics SQL off the cache maintenance executor. */
final class StatisticsCacheLoader<K, V> implements AsyncCacheLoader<K, V> {
    private final BasicAsyncCacheLoader<K, V> delegate;
    private final Executor executor;

    StatisticsCacheLoader(BasicAsyncCacheLoader<K, V> delegate, Executor executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<V> asyncLoad(K key, Executor ignored) {
        // Throw synchronously on rejection, so Caffeine does not publish a pending entry.
        return delegate.asyncLoad(key, executor);
    }

    @Override
    public CompletableFuture<V> asyncReload(K key, V oldValue, Executor ignored) {
        try {
            return asyncLoad(key, ignored);
        } catch (RejectedExecutionException e) {
            // Caffeine retains the old value on cancellation without logging each rejected refresh.
            CompletableFuture<V> cancelled = new CompletableFuture<>();
            cancelled.cancel(false);
            return cancelled;
        }
    }
}
