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

package org.apache.doris.connector.metacache;

import org.apache.doris.connector.metacache.spi.CacheSpec;
import org.apache.doris.connector.metacache.spi.MetaCacheEntryDef;
import org.apache.doris.connector.metacache.spi.MetaCacheEntryStats;
import org.apache.doris.connector.metacache.spi.MetaCacheLifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Data-source-neutral metadata cache lifecycle.
 *
 * <p>The runtime owns the engine -&gt; catalog -&gt; entry layout, typed entry lookup,
 * invalidation and statistics. FE catalog lookup, schema validation, edit-log handling
 * and REFRESH orchestration belong in fe-core adapters.
 */
public abstract class AbstractMetaCache implements MetaCacheLifecycle {
    private final String engine;
    private final ExecutorService refreshExecutor;
    private final long refreshAfterWriteSeconds;
    private final int objectStripeCount;
    private final Map<Long, CatalogEntryGroup> catalogEntries = new ConcurrentHashMap<>();
    private final Map<String, MetaCacheEntryDef<?, ?>> entryDefs = new ConcurrentHashMap<>();

    protected AbstractMetaCache(String engine, ExecutorService refreshExecutor,
            long refreshAfterWriteSeconds, int objectStripeCount) {
        this.engine = Objects.requireNonNull(engine, "engine can not be null");
        this.refreshExecutor = Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
        if (refreshAfterWriteSeconds <= 0) {
            throw new IllegalArgumentException("refreshAfterWriteSeconds must be positive");
        }
        if (objectStripeCount <= 0) {
            throw new IllegalArgumentException("objectStripeCount must be positive");
        }
        this.refreshAfterWriteSeconds = refreshAfterWriteSeconds;
        this.objectStripeCount = objectStripeCount;
    }

    public final String engine() {
        return engine;
    }

    public Collection<String> aliases() {
        return Collections.singleton(engine);
    }

    public final void initCatalog(long catalogId, Map<String, String> catalogProperties) {
        Map<String, String> safeCatalogProperties = CacheSpec.applyCompatibilityMap(
                catalogProperties, catalogPropertyCompatibilityMap());
        catalogEntries.computeIfAbsent(catalogId, id -> buildCatalogEntryGroup(safeCatalogProperties));
    }

    public final void checkCatalogInitialized(long catalogId) {
        requireCatalogEntryGroup(catalogId);
    }

    public final boolean isCatalogInitialized(long catalogId) {
        return catalogEntries.containsKey(catalogId);
    }

    /**
     * Optional compatibility mapping in the form {@code legacyKey -> newKey}.
     */
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    public final <K, V> MetaCacheEntry<K, V> entry(
            long catalogId, String entryName, Class<K> keyType, Class<V> valueType) {
        CatalogEntryGroup group = requireCatalogEntryGroup(catalogId);
        MetaCacheEntryDef<?, ?> def = requireEntryDef(entryName);
        ensureTypeCompatible(def, keyType, valueType);

        MetaCacheEntry<?, ?> cacheEntry = group.get(entryName);
        if (cacheEntry == null) {
            throw new IllegalStateException(String.format(
                    "Entry '%s' is not initialized for engine '%s', catalog %d.",
                    entryName, engine, catalogId));
        }
        return (MetaCacheEntry<K, V>) cacheEntry;
    }

    public final void invalidateCatalog(long catalogId) {
        CatalogEntryGroup removed = catalogEntries.remove(catalogId);
        if (removed != null) {
            removed.invalidateAll();
        }
    }

    public final void invalidateCatalogEntries(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group != null) {
            group.invalidateAll();
        }
    }

    public final void invalidateDb(long catalogId, String dbName) {
        invalidateEntries(catalogId, entryDef -> entryDef.getInvalidation().dbPredicate(dbName));
    }

    public final void invalidateTable(long catalogId, String dbName, String tableName) {
        invalidateEntries(catalogId, entryDef -> entryDef.getInvalidation().tablePredicate(dbName, tableName));
    }

    public final void invalidatePartitions(
            long catalogId, String dbName, String tableName, List<String> partitions) {
        invalidateEntries(catalogId,
                entryDef -> entryDef.getInvalidation().partitionPredicate(dbName, tableName, partitions));
    }

    public final Map<String, MetaCacheEntryStats> stats(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        return group == null ? Collections.emptyMap() : group.stats();
    }

    public void close() {
        catalogEntries.values().forEach(CatalogEntryGroup::invalidateAll);
        catalogEntries.clear();
    }

    protected final <K, V> void registerEntryDef(MetaCacheEntryDef<K, V> entryDef) {
        Objects.requireNonNull(entryDef, "entryDef");
        if (!catalogEntries.isEmpty()) {
            throw new IllegalStateException(
                    String.format("Can not register entry '%s' after catalog initialization for engine '%s'.",
                            entryDef.getName(), engine));
        }
        MetaCacheEntryDef<?, ?> existing = entryDefs.putIfAbsent(entryDef.getName(), entryDef);
        if (existing != null) {
            throw new IllegalArgumentException(
                    String.format("Duplicated entry definition '%s' for engine '%s'.",
                            entryDef.getName(), engine));
        }
    }

    protected final <K, V> EntryHandle<K, V> registerEntry(MetaCacheEntryDef<K, V> entryDef) {
        registerEntryDef(entryDef);
        return new EntryHandle<>(entryDef);
    }

    protected final <K, V> MetaCacheEntry<K, V> entry(long catalogId, MetaCacheEntryDef<K, V> entryDef) {
        validateRegisteredEntryDef(entryDef);
        return entry(catalogId, entryDef.getName(), entryDef.getKeyType(), entryDef.getValueType());
    }

    protected final String metaCacheTtlKey(String entryName) {
        return CacheSpec.metaCacheTtlKey(engine, entryName);
    }

    protected final Map<String, String> singleCompatibilityMap(String legacyKey, String entryName) {
        return Collections.singletonMap(legacyKey, metaCacheTtlKey(entryName));
    }

    /**
     * Adapter hook for value validation or other local decoration before a loader is installed.
     */
    protected <K, V> Function<K, V> decorateLoader(Function<K, V> loader, Class<V> valueType) {
        return loader;
    }

    private CatalogEntryGroup requireCatalogEntryGroup(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group == null) {
            throw new IllegalStateException(String.format(
                    "Catalog %d is not initialized for engine '%s'.", catalogId, engine));
        }
        return group;
    }

    private MetaCacheEntryDef<?, ?> requireEntryDef(String entryName) {
        MetaCacheEntryDef<?, ?> entryDef = entryDefs.get(entryName);
        if (entryDef == null) {
            throw new IllegalArgumentException(String.format(
                    "Entry '%s' is not registered for engine '%s'.", entryName, engine));
        }
        return entryDef;
    }

    private void ensureTypeCompatible(MetaCacheEntryDef<?, ?> entryDef, Class<?> keyType, Class<?> valueType) {
        if (!entryDef.getKeyType().equals(keyType) || !entryDef.getValueType().equals(valueType)) {
            throw new IllegalArgumentException(String.format(
                    "Entry '%s' for engine '%s' expects key/value types (%s, %s), but got (%s, %s).",
                    entryDef.getName(), engine, entryDef.getKeyType().getName(), entryDef.getValueType().getName(),
                    keyType.getName(), valueType.getName()));
        }
    }

    private <K, V> void validateRegisteredEntryDef(MetaCacheEntryDef<K, V> entryDef) {
        MetaCacheEntryDef<?, ?> registered = requireEntryDef(entryDef.getName());
        ensureTypeCompatible(registered, entryDef.getKeyType(), entryDef.getValueType());
    }

    private void invalidateEntries(long catalogId, Function<MetaCacheEntryDef<?, ?>, Predicate<?>> predicateFactory) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group == null) {
            return;
        }
        entryDefs.values().forEach(entryDef -> invalidateEntryIfMatched(group, entryDef, predicateFactory));
    }

    @SuppressWarnings("unchecked")
    private <K, V> void invalidateEntryIfMatched(CatalogEntryGroup group, MetaCacheEntryDef<K, V> entryDef,
            Function<MetaCacheEntryDef<?, ?>, Predicate<?>> predicateFactory) {
        Predicate<K> predicate = (Predicate<K>) predicateFactory.apply(entryDef);
        if (predicate == null) {
            return;
        }
        MetaCacheEntry<K, V> entry = (MetaCacheEntry<K, V>) group.get(entryDef.getName());
        if (entry != null) {
            entry.invalidateIf(predicate);
        }
    }

    private CatalogEntryGroup buildCatalogEntryGroup(Map<String, String> catalogProperties) {
        CatalogEntryGroup group = new CatalogEntryGroup();
        entryDefs.values()
                .forEach(entryDef -> group.put(entryDef.getName(), newMetaCacheEntry(entryDef, catalogProperties)));
        return group;
    }

    @SuppressWarnings("unchecked")
    private <K, V> MetaCacheEntry<K, V> newMetaCacheEntry(
            MetaCacheEntryDef<?, ?> rawEntryDef, Map<String, String> catalogProperties) {
        MetaCacheEntryDef<K, V> entryDef = (MetaCacheEntryDef<K, V>) rawEntryDef;
        CacheSpec cacheSpec = CacheSpec.fromProperties(
                catalogProperties, engine, entryDef.getName(), entryDef.getDefaultCacheSpec());
        return new MetaCacheEntry<>(
                entryDef.getName(),
                decorateLoader(entryDef.getLoader(), entryDef.getValueType()),
                cacheSpec,
                refreshExecutor,
                entryDef.isAutoRefresh(),
                entryDef.isContextualOnly(),
                objectStripeCount,
                refreshAfterWriteSeconds,
                true);
    }

    protected final class EntryHandle<K, V> {
        private final MetaCacheEntryDef<K, V> entryDef;

        private EntryHandle(MetaCacheEntryDef<K, V> entryDef) {
            this.entryDef = entryDef;
        }

        public MetaCacheEntry<K, V> get(long catalogId) {
            return entry(catalogId, entryDef);
        }

        public MetaCacheEntry<K, V> getIfInitialized(long catalogId) {
            return isCatalogInitialized(catalogId) ? get(catalogId) : null;
        }
    }
}
