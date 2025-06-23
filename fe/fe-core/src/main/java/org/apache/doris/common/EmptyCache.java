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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/lib/trino-cache/src/main/java/io/trino/cache/EmptyCache.java
// and modified by Doris

package org.apache.doris.common;

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

class EmptyCache<K, V>
        extends AbstractLoadingCache<K, V> {
    private final CacheLoader<? super K, V> loader;
    private final StatsCounter statsCounter;

    EmptyCache(CacheLoader<? super K, V> loader, boolean recordStats) {
        this.loader = Objects.requireNonNull(loader, "loader is null");
        this.statsCounter = recordStats ? new SimpleStatsCounter() : new NoopStatsCounter();
    }

    @Override
    public V getIfPresent(Object key) {
        statsCounter.recordMisses(1);
        return null;
    }

    @Override
    public V get(K key)
            throws ExecutionException {
        return get(key, () -> loader.load(key));
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys)
            throws ExecutionException {
        try {
            Set<K> keySet = ImmutableSet.copyOf(keys);
            statsCounter.recordMisses(keySet.size());
            @SuppressWarnings("unchecked") // safe since all keys extend K
            ImmutableMap<K, V> result = (ImmutableMap<K, V>) loader.loadAll(keySet);
            for (K key : keySet) {
                if (!result.containsKey(key)) {
                    throw new InvalidCacheLoadException("loadAll failed to return a value for " + key);
                }
            }
            statsCounter.recordLoadSuccess(1);
            return result;
        } catch (RuntimeException e) {
            statsCounter.recordLoadException(1);
            throw new UncheckedExecutionException(e);
        } catch (Exception e) {
            statsCounter.recordLoadException(1);
            throw new ExecutionException(e);
        }
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        statsCounter.recordMisses(1);
        try {
            V value = valueLoader.call();
            statsCounter.recordLoadSuccess(1);
            return value;
        } catch (RuntimeException e) {
            statsCounter.recordLoadException(1);
            throw new UncheckedExecutionException(e);
        } catch (Exception e) {
            statsCounter.recordLoadException(1);
            throw new ExecutionException(e);
        }
    }

    @Override
    public void put(K key, V value) {
        // Cache, even if configured to evict everything immediately, should allow writes.
    }

    @Override
    public void refresh(K key) {}

    @Override
    public void invalidate(Object key) {}

    @Override
    public void invalidateAll(Iterable<?> keys) {}

    @Override
    public void invalidateAll() {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return statsCounter.snapshot();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return new ConcurrentMap<K, V>() {
            @Override
            public V putIfAbsent(K key, V value) {
                // Cache, even if configured to evict everything immediately, should allow writes.
                // putIfAbsent returns the previous value
                return null;
            }

            @Override
            public boolean remove(Object key, Object value) {
                return false;
            }

            @Override
            public boolean replace(K key, V oldValue, V newValue) {
                return false;
            }

            @Override
            public V replace(K key, V value) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return true;
            }

            @Override
            public boolean containsKey(Object key) {
                return false;
            }

            @Override
            public boolean containsValue(Object value) {
                return false;
            }

            @Override
            @Nullable
            public V get(Object key) {
                return null;
            }

            @Override
            @Nullable
            public V put(K key, V value) {
                // Cache, even if configured to evict everything immediately, should allow writes.
                return null;
            }

            @Override
            @Nullable
            public V remove(Object key) {
                return null;
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                // Cache, even if configured to evict everything immediately, should allow writes.
            }

            @Override
            public void clear() {

            }

            @Override
            public Set<K> keySet() {
                return ImmutableSet.of();
            }

            @Override
            public Collection<V> values() {
                return ImmutableSet.of();
            }

            @Override
            public Set<Entry<K, V>> entrySet() {
                return ImmutableSet.of();
            }
        };
    }

    private static class NoopStatsCounter
            implements StatsCounter {
        private static final CacheStats EMPTY_STATS = new SimpleStatsCounter().snapshot();

        @Override
        public void recordHits(int count) {}

        @Override
        public void recordMisses(int count) {}

        @Override
        public void recordLoadSuccess(long loadTime) {}

        @Override
        public void recordLoadException(long loadTime) {}

        @Override
        public void recordEviction() {}

        @Override
        public CacheStats snapshot() {
            return EMPTY_STATS;
        }
    }
}
