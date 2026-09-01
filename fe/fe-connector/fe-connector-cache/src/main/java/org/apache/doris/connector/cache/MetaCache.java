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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/** One typed physical cache owned by a {@link CatalogMetaCache}. */
public final class MetaCache<K, V> {
    private final MetaCacheDefinition<K, V> definition;
    private final ScopedMetaCache<K, V> delegate;

    MetaCache(MetaCacheDefinition<K, V> definition, ScopedMetaCache<K, V> delegate) {
        this.definition = Objects.requireNonNull(definition, "definition can not be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate can not be null");
    }

    public String name() {
        return definition.name();
    }

    public V get(K key) {
        Function<K, V> loader = definition.loader();
        if (loader == null) {
            throw new IllegalStateException("Meta cache '" + name() + "' has no default loader");
        }
        return get(key, loader);
    }

    public V get(K key, Function<K, V> loader) {
        K nonNullKey = Objects.requireNonNull(key, "key can not be null");
        return delegate.get(nonNullKey, definition.scope(nonNullKey), loader);
    }

    /**
     * Loads a missing value while allowing an adapter to publish an auxiliary side effect immediately before the
     * cache value at the same guarded commit point. The coordinator must invoke the supplied commit callback once;
     * the callback argument is run only if the load is still current and is fenced with cache publication.
     */
    public V getWithPublicationAction(K key, Function<K, V> loader,
            BiConsumer<V, Consumer<Runnable>> publicationCoordinator) {
        K nonNullKey = Objects.requireNonNull(key, "key can not be null");
        return delegate.getWithPublicationAction(nonNullKey, definition.scope(nonNullKey), loader,
                publicationCoordinator);
    }

    public V getIfPresent(K key) {
        K nonNullKey = Objects.requireNonNull(key, "key can not be null");
        return delegate.getIfPresent(nonNullKey, definition.scope(nonNullKey));
    }

    public void put(K key, V value) {
        K nonNullKey = Objects.requireNonNull(key, "key can not be null");
        delegate.put(nonNullKey, definition.scope(nonNullKey), value);
    }

    public boolean compareAndSet(K key, V expectedValue, V updatedValue) {
        return compareAndSet(key, expectedValue, updatedValue, () -> {
        });
    }

    /**
     * Updates a value only if it is still current, and runs an auxiliary action inside the same guarded commit.
     * Concurrent cache loads can not publish between the action and the value update.
     * The action runs while the publication guards and key lock are held, so it must be short, non-blocking, and
     * must not re-enter this cache. If it throws, the value is not updated, but completed external side effects are
     * not rolled back.
     */
    public boolean compareAndSet(K key, V expectedValue, V updatedValue, Runnable commitAction) {
        K nonNullKey = Objects.requireNonNull(key, "key can not be null");
        return delegate.compareAndSet(nonNullKey, definition.scope(nonNullKey), expectedValue, updatedValue,
                Objects.requireNonNull(commitAction, "commitAction can not be null"));
    }

    public void invalidateKey(K key) {
        delegate.invalidateKey(Objects.requireNonNull(key, "key can not be null"));
    }

    /**
     * Starts a bulk remote load whose results all belong below {@code parentScope}. Publishing through the
     * returned handle is fenced against concurrent scope and exact-key invalidation.
     */
    public BulkLoad<K, V> beginBulkLoad(ScopePath parentScope) {
        return new BulkLoad<>(this, delegate.beginBulkLoad(parentScope));
    }

    public ScopedMetaCache.CacheMetrics metrics() {
        return delegate.metrics();
    }

    public boolean isEnabled() {
        return metrics().isEffectiveEnabled();
    }

    public long size() {
        return metrics().getPhysicalEntryCount();
    }

    public long loadSuccessCount() {
        return metrics().getLoadSuccessCount();
    }

    public CacheSpec cacheSpec() {
        return definition.cacheSpec();
    }

    public boolean isAutoRefresh() {
        return definition.refreshAfterWrite() != null;
    }

    public void forEach(BiConsumer<K, V> consumer) {
        delegate.forEach(consumer);
    }

    public static final class BulkLoad<K, V> implements AutoCloseable {
        private final MetaCache<K, V> owner;
        private final ScopedMetaCache.BulkLoadHandle delegate;

        private BulkLoad(MetaCache<K, V> owner, ScopedMetaCache.BulkLoadHandle delegate) {
            this.owner = owner;
            this.delegate = delegate;
        }

        /** Returns false when an invalidation raced the remote load and the value was intentionally not cached. */
        public boolean publish(K key, V value) {
            K nonNullKey = Objects.requireNonNull(key, "key can not be null");
            return owner.delegate.publish(delegate, nonNullKey, owner.definition.scope(nonNullKey), value);
        }

        /** Returns whether this load is still current for {@code key}, without publishing a value. */
        public boolean isCurrent(K key) {
            K nonNullKey = Objects.requireNonNull(key, "key can not be null");
            return owner.delegate.isBulkLoadCurrent(delegate, nonNullKey);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
