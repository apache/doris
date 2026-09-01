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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Declarative definition of one physical metadata cache in a catalog.
 *
 * <p>The scope resolver is mandatory. It is the only invalidation dependency a framework-owned cache declares:
 * every value published by the cache is registered at that scope, so catalog/database/table/partition invalidation
 * automatically covers it without a connector-maintained list of sibling caches.
 */
public final class MetaCacheDefinition<K, V> {
    private final String name;
    private final CacheSpec cacheSpec;
    private final Function<K, ScopePath> scopeResolver;
    private final Function<K, V> loader;
    private final MetaCacheRemovalListener<K, V> removalListener;
    private final BiConsumer<K, V> discardListener;
    private final Duration refreshAfterWrite;
    private final Executor refreshExecutor;

    private MetaCacheDefinition(Builder<K, V> builder) {
        name = requireName(builder.name);
        cacheSpec = Objects.requireNonNull(builder.cacheSpec, "cacheSpec can not be null");
        scopeResolver = Objects.requireNonNull(builder.scopeResolver, "scopeResolver can not be null");
        loader = builder.loader;
        removalListener = builder.removalListener;
        discardListener = builder.discardListener;
        refreshAfterWrite = builder.refreshAfterWrite;
        refreshExecutor = builder.refreshExecutor;
        if (refreshAfterWrite != null && loader == null) {
            throw new IllegalArgumentException("refresh-after-write requires a default loader");
        }
    }

    public static <K, V> Builder<K, V> builder(
            String name, CacheSpec cacheSpec, Function<K, ScopePath> scopeResolver) {
        return new Builder<>(name, cacheSpec, scopeResolver);
    }

    public String name() {
        return name;
    }

    CacheSpec cacheSpec() {
        return cacheSpec;
    }

    ScopePath scope(K key) {
        return Objects.requireNonNull(
                scopeResolver.apply(Objects.requireNonNull(key, "key can not be null")),
                "scopeResolver can not return null");
    }

    Function<K, V> loader() {
        return loader;
    }

    MetaCacheRemovalListener<K, V> removalListener() {
        return removalListener;
    }

    BiConsumer<K, V> discardListener() {
        return discardListener;
    }

    Duration refreshAfterWrite() {
        return refreshAfterWrite;
    }

    Executor refreshExecutor() {
        return refreshExecutor;
    }

    private static String requireName(String name) {
        String nonNullName = Objects.requireNonNull(name, "name can not be null");
        if (nonNullName.isEmpty()) {
            throw new IllegalArgumentException("name can not be empty");
        }
        return nonNullName;
    }

    public static final class Builder<K, V> {
        private final String name;
        private final CacheSpec cacheSpec;
        private final Function<K, ScopePath> scopeResolver;
        private Function<K, V> loader;
        private MetaCacheRemovalListener<K, V> removalListener;
        private BiConsumer<K, V> discardListener;
        private Duration refreshAfterWrite;
        private Executor refreshExecutor;

        private Builder(String name, CacheSpec cacheSpec, Function<K, ScopePath> scopeResolver) {
            this.name = name;
            this.cacheSpec = cacheSpec;
            this.scopeResolver = scopeResolver;
        }

        public Builder<K, V> loader(Function<K, V> loader) {
            this.loader = Objects.requireNonNull(loader, "loader can not be null");
            return this;
        }

        public Builder<K, V> removalListener(MetaCacheRemovalListener<K, V> removalListener) {
            this.removalListener = Objects.requireNonNull(removalListener, "removalListener can not be null");
            return this;
        }

        /**
         * Registers cleanup for a loaded value that never becomes cache-owned, for example because caching is
         * disabled or invalidation rejects publication. The caller still owns and may return the value.
         */
        public Builder<K, V> discardListener(BiConsumer<K, V> discardListener) {
            this.discardListener = Objects.requireNonNull(discardListener, "discardListener can not be null");
            return this;
        }

        public Builder<K, V> refreshAfterWrite(Duration duration, Executor executor) {
            Duration nonNullDuration = Objects.requireNonNull(duration, "duration can not be null");
            if (nonNullDuration.isZero() || nonNullDuration.isNegative()) {
                throw new IllegalArgumentException("refresh-after-write duration must be positive");
            }
            this.refreshAfterWrite = nonNullDuration;
            this.refreshExecutor = Objects.requireNonNull(executor, "refreshExecutor can not be null");
            return this;
        }

        public MetaCacheDefinition<K, V> build() {
            return new MetaCacheDefinition<>(this);
        }
    }
}
