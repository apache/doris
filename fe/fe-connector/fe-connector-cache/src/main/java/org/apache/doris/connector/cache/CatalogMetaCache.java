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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Catalog-local owner of all framework metadata caches.
 *
 * <p>Connectors invalidate a semantic scope once. Every physical cache created through this owner participates
 * automatically because its values are registered in the shared {@link ScopedMetaCacheRegistry}.
 */
public final class CatalogMetaCache implements AutoCloseable {
    private final ScopedMetaCacheRegistry registry;
    private final Set<String> names = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CatalogMetaCache() {
        this(new ScopedMetaCacheRegistry());
    }

    CatalogMetaCache(ScopedMetaCacheRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry can not be null");
    }

    public <K, V> MetaCache<K, V> create(MetaCacheDefinition<K, V> definition) {
        MetaCacheDefinition<K, V> nonNullDefinition =
                Objects.requireNonNull(definition, "definition can not be null");
        checkOpen();
        if (!names.add(nonNullDefinition.name())) {
            throw new IllegalArgumentException("Duplicate meta cache name: " + nonNullDefinition.name());
        }
        try {
            return new MetaCache<>(nonNullDefinition,
                    registry.createCacheWithMetaRemovalListener(nonNullDefinition.name(),
                            nonNullDefinition.cacheSpec(), nonNullDefinition.removalListener(),
                            nonNullDefinition.discardListener(),
                            nonNullDefinition.refreshAfterWrite(), nonNullDefinition.refreshExecutor()));
        } catch (RuntimeException | Error throwable) {
            names.remove(nonNullDefinition.name());
            throw throwable;
        }
    }

    public void invalidateCatalog() {
        registry.invalidate(ScopePath.catalog());
    }

    public void invalidateDatabase(String database) {
        registry.invalidate(ScopePath.database(database));
    }

    public void invalidateTable(String database, String table) {
        registry.invalidate(ScopePath.table(database, table));
    }

    public void invalidatePartition(String database, String table, Object partition) {
        registry.invalidate(ScopePath.partition(database, table, partition));
    }

    public void invalidatePartitionCollection(String database, String table) {
        registry.invalidate(ScopePath.partitionCollection(database, table));
    }

    /**
     * Atomically invalidates the partition collection and the specified partition identities for one table.
     * Logical invalidation has one publication linearization point; physical cleanup runs after publication is
     * released so unrelated cache operations do not wait for Caffeine removal callbacks.
     */
    public void invalidatePartitions(String database, String table, Collection<?> partitions) {
        Objects.requireNonNull(partitions, "partitions can not be null");
        List<ScopePath> paths = new ArrayList<>(partitions.size() + 1);
        paths.add(ScopePath.partitionCollection(database, table));
        partitions.forEach(partition -> paths.add(ScopePath.partition(database, table, partition)));
        registry.invalidate(paths);
    }

    public ScopedMetaCacheRegistry.ScopeMetrics metrics() {
        return registry.metrics();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            registry.close();
            names.clear();
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Catalog meta cache is closed");
        }
    }
}
