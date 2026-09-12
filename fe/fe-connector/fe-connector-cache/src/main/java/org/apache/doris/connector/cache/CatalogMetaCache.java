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
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
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
    private final MetaCacheBudgetManager budgetManager;
    private final long catalogId;
    private final String engine;
    private final OptionalLong catalogMaxWeight;
    private final boolean managed;
    private final Set<String> names = ConcurrentHashMap.newKeySet();
    private final Map<String, MetaCache<?, ?>> entries = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private CatalogMetaCache() {
        this(new ScopedMetaCacheRegistry(), new MetaCacheBudgetManager(OptionalLong.empty()),
                0L, "standalone", OptionalLong.empty(), false);
    }

    CatalogMetaCache(ScopedMetaCacheRegistry registry) {
        this(registry, new MetaCacheBudgetManager(OptionalLong.empty()),
                0L, "standalone", OptionalLong.empty(), false);
    }

    public CatalogMetaCache(MetaCacheBudgetManager budgetManager, long catalogId,
            String engine, Map<String, String> catalogProperties) {
        this(new ScopedMetaCacheRegistry(), budgetManager, catalogId, engine,
                budgetManager.parseCatalogMaxWeight(catalogProperties), false);
    }

    /**
     * Creates an isolated owner without shared global/catalog budgets or unified statistics registration.
     * Entry-local weight limits still apply. Use for independent caches and tests; governed catalog caches
     * must use {@link #managed}.
     */
    public static CatalogMetaCache unmanaged() {
        return new CatalogMetaCache();
    }

    public static CatalogMetaCache managed(long catalogId, String engine,
            Map<String, String> catalogProperties) {
        MetaCacheBudgetManager manager = MetaCacheGovernance.budgetManager();
        CatalogMetaCache cache = new CatalogMetaCache(new ScopedMetaCacheRegistry(), manager,
                catalogId, engine, manager.parseCatalogMaxWeight(catalogProperties), true);
        MetaCacheGovernance.register(cache);
        return cache;
    }

    CatalogMetaCache(ScopedMetaCacheRegistry registry, MetaCacheBudgetManager budgetManager,
            long catalogId, String engine, OptionalLong catalogMaxWeight, boolean managed) {
        this.registry = Objects.requireNonNull(registry, "registry can not be null");
        this.budgetManager = Objects.requireNonNull(budgetManager, "budgetManager can not be null");
        this.catalogId = catalogId;
        this.engine = Objects.requireNonNull(engine, "engine can not be null");
        this.catalogMaxWeight = Objects.requireNonNull(catalogMaxWeight, "catalogMaxWeight can not be null");
        this.managed = managed;
    }

    public <K, V> MetaCache<K, V> create(MetaCacheDefinition<K, V> definition) {
        MetaCacheDefinition<K, V> nonNullDefinition =
                Objects.requireNonNull(definition, "definition can not be null");
        checkOpen();
        if (!names.add(nonNullDefinition.name())) {
            throw new IllegalArgumentException("Duplicate meta cache name: " + nonNullDefinition.name());
        }
        MetaCacheBudgetManager.EntryBudget entryBudget = null;
        try {
            boolean weightLimited = budgetManager.hasLimit(
                    catalogMaxWeight, nonNullDefinition.cacheSpec().getMaxWeight());
            if (weightLimited && nonNullDefinition.sizeEstimator() == null) {
                throw new IllegalArgumentException("Weighted metadata cache requires a size estimator: "
                        + nonNullDefinition.name());
            }
            if (weightLimited) {
                entryBudget = budgetManager.createEntryBudget(catalogId, engine,
                        nonNullDefinition.name(), nonNullDefinition.budgetGroup(), catalogMaxWeight,
                        nonNullDefinition.cacheSpec().getMaxWeight());
            }
            MetaCache<K, V> created = new MetaCache<>(nonNullDefinition,
                    registry.createCacheWithMetaRemovalListener(nonNullDefinition.name(),
                            nonNullDefinition.cacheSpec(), nonNullDefinition.removalListener(),
                            nonNullDefinition.discardListener(),
                            nonNullDefinition.refreshAfterWrite(), nonNullDefinition.refreshExecutor(),
                            entryBudget == null ? null : nonNullDefinition.sizeEstimator(), entryBudget));
            entries.put(nonNullDefinition.name(), created);
            return created;
        } catch (RuntimeException | Error throwable) {
            if (entryBudget != null) {
                entryBudget.close();
            }
            names.remove(nonNullDefinition.name());
            if (managed) {
                try {
                    close();
                } catch (RuntimeException | Error closeFailure) {
                    throwable.addSuppressed(closeFailure);
                }
            }
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

    public Map<String, MetaCache<?, ?>> entries() {
        return java.util.Collections.unmodifiableMap(entries);
    }

    public long catalogId() {
        return catalogId;
    }

    public String engine() {
        return engine;
    }

    public OptionalLong catalogMaxWeight() {
        return catalogMaxWeight;
    }

    public boolean hasEnclosingWeightLimit() {
        return budgetManager.hasLimit(catalogMaxWeight, OptionalLong.empty());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                registry.close();
            } finally {
                names.clear();
                entries.clear();
                if (managed) {
                    MetaCacheGovernance.unregister(this);
                }
            }
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Catalog meta cache is closed");
        }
    }
}
