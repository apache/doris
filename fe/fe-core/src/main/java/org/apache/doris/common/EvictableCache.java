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
// https://github.com/trinodb/trino/blob/438/lib/trino-cache/src/main/java/io/trino/cache/EvictableCache.java
// and modified by Doris

package org.apache.doris.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Verify;
import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

/**
 * A {@link Cache} and {@link LoadingCache} implementation similar to ones
 * produced by {@link CacheBuilder#build()}, but one that does not
 * exhibit <a href="https://github.com/google/guava/issues/1881">Guava issue #1881</a>:
 * a cache inspection with {@link #getIfPresent(Object)} or {@link #get(Object, Callable)}
 * is guaranteed to return fresh state after {@link #invalidate(Object)},
 * {@link #invalidateAll(Iterable)} or {@link #invalidateAll()} were called.
 *
 * @see EvictableCacheBuilder
 */
// @ElementTypesAreNonnullByDefault
class EvictableCache<K, V>
        extends AbstractLoadingCache<K, V>
        implements LoadingCache<K, V> {
    // Invariant: for every (K, token) entry in the tokens map, there is a live
    // cache entry (token, ?) in dataCache, that, upon eviction, will cause the tokens'
    // entry to be removed.
    private final ConcurrentHashMap<K, Token<K>> tokens = new ConcurrentHashMap<>();
    // The dataCache can have entries with no corresponding tokens in the tokens map.
    // For example, this can happen when invalidation concurs with load.
    // The dataCache must be bounded.
    private final LoadingCache<Token<K>, V> dataCache;

    private final AtomicInteger invalidations = new AtomicInteger();

    EvictableCache(CacheBuilder<? super Token<K>, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader) {
        dataCache = buildUnsafeCache(
                cacheBuilder
                        .<Token<K>, V>removalListener(removal -> {
                            Token<K> token = removal.getKey();
                            Verify.verify(token != null, "token is null");
                            if (removal.getCause() != RemovalCause.REPLACED) {
                                tokens.remove(token.getKey(), token);
                            }
                        }),
                new TokenCacheLoader<>(cacheLoader));
    }

    // @SuppressModernizer // CacheBuilder.build(CacheLoader) is forbidden,
    // advising to use this class as a safety-adding wrapper.
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder,
            CacheLoader<? super K, V> cacheLoader) {
        return cacheBuilder.build(cacheLoader);
    }

    @Override
    public V getIfPresent(Object key) {
        @SuppressWarnings("SuspiciousMethodCalls") // Object passed to map as key K
        Token<K> token = tokens.get(key);
        if (token == null) {
            return null;
        }
        return dataCache.getIfPresent(token);
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        Token<K> newToken = new Token<>(key);
        int invalidations = this.invalidations.get();
        Token<K> token = tokens.computeIfAbsent(key, ignored -> newToken);
        try {
            V value = dataCache.get(token, valueLoader);
            if (invalidations == this.invalidations.get()) {
                // Revive token if it got expired before reloading
                if (tokens.putIfAbsent(key, token) == null) {
                    // Revived
                    if (!dataCache.asMap().containsKey(token)) {
                        // We revived, but the token does not correspond to a live entry anymore.
                        // It would stay in tokens forever, so let's remove it.
                        tokens.remove(key, token);
                    }
                }
            }
            return value;
        } catch (Throwable e) {
            if (newToken == token) {
                // Failed to load and it was our new token persisted in tokens map.
                // No cache entry exists for the token (unless concurrent load happened),
                // so we need to remove it.
                tokens.remove(key, newToken);
            }
            throw e;
        }
    }

    @Override
    public V get(K key)
            throws ExecutionException {
        Token<K> newToken = new Token<>(key);
        int invalidations = this.invalidations.get();
        Token<K> token = tokens.computeIfAbsent(key, ignored -> newToken);
        try {
            V value = dataCache.get(token);
            if (invalidations == this.invalidations.get()) {
                // Revive token if it got expired before reloading
                if (tokens.putIfAbsent(key, token) == null) {
                    // Revived
                    if (!dataCache.asMap().containsKey(token)) {
                        // We revived, but the token does not correspond to a live entry anymore.
                        // It would stay in tokens forever, so let's remove it.
                        tokens.remove(key, token);
                    }
                }
            }
            return value;
        } catch (Throwable e) {
            if (newToken == token) {
                // Failed to load and it was our new token persisted in tokens map.
                // No cache entry exists for the token (unless concurrent load happened),
                // so we need to remove it.
                tokens.remove(key, newToken);
            }
            throw e;
        }
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys)
            throws ExecutionException {
        List<Token<K>> newTokens = new ArrayList<>();
        List<Token<K>> temporaryTokens = new ArrayList<>();
        try {
            Map<K, V> result = new LinkedHashMap<>();
            for (K key : keys) {
                if (result.containsKey(key)) {
                    continue;
                }
                // This is not bulk, but is fast local operation
                Token<K> newToken = new Token<>(key);
                Token<K> oldToken = tokens.putIfAbsent(key, newToken);
                if (oldToken != null) {
                    // Token exists but a data may not exist (e.g. due to concurrent eviction)
                    V value = dataCache.getIfPresent(oldToken);
                    if (value != null) {
                        result.put(key, value);
                        continue;
                    }
                    // Old token exists but value wasn't found. This can happen when there is concurrent
                    // eviction/invalidation or when the value is still being loaded.
                    // The new token is not registered in tokens, so won't be used by subsequent invocations.
                    temporaryTokens.add(newToken);
                }
                newTokens.add(newToken);
            }

            Map<Token<K>, V> values = dataCache.getAll(newTokens);
            for (Map.Entry<Token<K>, V> entry : values.entrySet()) {
                Token<K> newToken = entry.getKey();
                result.put(newToken.getKey(), entry.getValue());
            }
            return ImmutableMap.copyOf(result);
        } catch (Throwable e) {
            for (Token<K> token : newTokens) {
                // Failed to load and it was our new token (potentially) persisted in tokens map.
                // No cache entry exists for the token (unless concurrent load happened),
                // so we need to remove it.
                tokens.remove(token.getKey(), token);
            }
            throw e;
        } finally {
            dataCache.invalidateAll(temporaryTokens);
        }
    }

    @Override
    public void refresh(K key) {
        // The refresh loads a new entry, if it wasn't in the cache yet. Thus, we would create a new Token.
        // However, dataCache.refresh is asynchronous and may fail, so no cache entry may be created.
        // In such case we would leak the newly created token.
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return dataCache.size();
    }

    @Override
    public void cleanUp() {
        dataCache.cleanUp();
    }

    @VisibleForTesting
    int tokensCount() {
        return tokens.size();
    }

    @Override
    public void invalidate(Object key) {
        invalidations.incrementAndGet();
        @SuppressWarnings("SuspiciousMethodCalls") // Object passed to map as key K
        Token<K> token = tokens.remove(key);
        if (token != null) {
            dataCache.invalidate(token);
        }
    }

    @Override
    public void invalidateAll() {
        invalidations.incrementAndGet();
        dataCache.invalidateAll();
        tokens.clear();
    }

    // Not thread safe, test only.
    @VisibleForTesting
    void clearDataCacheOnly() {
        Map<K, Token<K>> tokensCopy = new HashMap<>(tokens);
        dataCache.asMap().clear();
        Verify.verify(tokens.isEmpty(), "Clearing dataCache should trigger tokens eviction");
        tokens.putAll(tokensCopy);
    }

    @Override
    public CacheStats stats() {
        return dataCache.stats();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return new ConcurrentMap<K, V>() {
            private final ConcurrentMap<Token<K>, V> dataCacheMap = dataCache.asMap();

            @Override
            public V putIfAbsent(K key, V value) {
                throw new UnsupportedOperationException("The operation is not supported,"
                        + " as in inherently races with cache invalidation");
            }

            @Override
            public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
                // default implementation of ConcurrentMap#compute uses not supported putIfAbsent in some cases
                throw new UnsupportedOperationException("The operation is not supported, as in inherently"
                        + " races with cache invalidation");
            }

            @Override
            public boolean remove(Object key, Object value) {
                @SuppressWarnings("SuspiciousMethodCalls") // Object passed to map as key K
                Token<K> token = tokens.get(key);
                if (token != null) {
                    return dataCacheMap.remove(token, value);
                }
                return false;
            }

            @Override
            public boolean replace(K key, V oldValue, V newValue) {
                Token<K> token = tokens.get(key);
                if (token != null) {
                    return dataCacheMap.replace(token, oldValue, newValue);
                }
                return false;
            }

            @Override
            public V replace(K key, V value) {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently races"
                        + " with cache invalidation");
            }

            @Override
            public int size() {
                return dataCache.asMap().size();
            }

            @Override
            public boolean isEmpty() {
                return dataCache.asMap().isEmpty();
            }

            @Override
            public boolean containsKey(Object key) {
                return tokens.containsKey(key);
            }

            @Override
            public boolean containsValue(Object value) {
                return values().contains(value);
            }

            @Override
            @Nullable
            public V get(Object key) {
                return getIfPresent(key);
            }

            @Override
            public V put(K key, V value) {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently"
                        + " races with cache invalidation. Use get(key, callable) instead.");
            }

            @Override
            @Nullable
            public V remove(Object key) {
                Token<K> token = tokens.remove(key);
                if (token != null) {
                    return dataCacheMap.remove(token);
                }
                return null;
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                throw new UnsupportedOperationException("The operation is not supported, as in inherently"
                        + " races with cache invalidation. Use get(key, callable) instead.");
            }

            @Override
            public void clear() {
                dataCacheMap.clear();
                tokens.clear();
            }

            @Override
            public Set<K> keySet() {
                return tokens.keySet();
            }

            @Override
            public Collection<V> values() {
                return dataCacheMap.values();
            }

            @Override
            public Set<Map.Entry<K, V>> entrySet() {
                throw new UnsupportedOperationException();
            }
        };
    }

    // instance-based equality
    static final class Token<K> {
        private final K key;

        Token(K key) {
            this.key = Objects.requireNonNull(key, "key is null");
        }

        K getKey() {
            return key;
        }

        @Override
        public String toString() {
            return String.format("CacheToken(%s; %s)", Integer.toHexString(hashCode()), key);
        }
    }

    private static class TokenCacheLoader<K, V>
            extends CacheLoader<Token<K>, V> {
        private final CacheLoader<? super K, V> delegate;

        public TokenCacheLoader(CacheLoader<? super K, V> delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        }

        @Override
        public V load(Token<K> token)
                throws Exception {
            return delegate.load(token.getKey());
        }

        @Override
        public ListenableFuture<V> reload(Token<K> token, V oldValue)
                throws Exception {
            return delegate.reload(token.getKey(), oldValue);
        }

        @Override
        public Map<Token<K>, V> loadAll(Iterable<? extends Token<K>> tokens)
                throws Exception {
            List<Token<K>> tokenList = ImmutableList.copyOf(tokens);
            List<K> keys = new ArrayList<>();
            for (Token<K> token : tokenList) {
                keys.add(token.getKey());
            }
            Map<? super K, V> values;
            try {
                values = delegate.loadAll(keys);
            } catch (UnsupportedLoadingOperationException e) {
                // Guava uses UnsupportedLoadingOperationException in LoadingCache.loadAll
                // to fall back from bulk loading (without load sharing) to loading individual
                // values (with load sharing). EvictableCache implementation does not currently
                // support the fallback mechanism, so the individual values would be loaded
                // without load sharing. This would be an unintentional and non-obvious behavioral
                // discrepancy between EvictableCache and Guava Caches, so the mechanism is disabled.
                throw new UnsupportedOperationException("LoadingCache.getAll() is not supported by EvictableCache"
                        + " when CacheLoader.loadAll is not implemented", e);
            }

            ImmutableMap.Builder<Token<K>, V> result = ImmutableMap.builder();
            for (int i = 0; i < tokenList.size(); i++) {
                Token<K> token = tokenList.get(i);
                K key = keys.get(i);
                V value = values.get(key);
                // CacheLoader.loadAll is not guaranteed to return values for all the keys
                if (value != null) {
                    result.put(token, value);
                }
            }
            return result.buildOrThrow();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .addValue(delegate)
                    .toString();
        }
    }
}
