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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Base implementation of {@link ExternalMetaCache}.
 * It keeps the shared in-memory layout
 * Map&lt;CatalogId, CatalogEntryGroup&gt;, implements default
 * lifecycle behavior, and provides conservative invalidation fallback.
 * Subclasses register entry definitions during construction and expect callers
 * to initialize a catalog explicitly before accessing entries.
 */
public abstract class AbstractExternalMetaCache implements ExternalMetaCache {
    protected static CacheSpec defaultEntryCacheSpec() {
        return CacheSpec.of(
                true,
                Config.external_cache_expire_time_seconds_after_access,
                Config.max_external_table_cache_num);
    }

    protected static CacheSpec defaultSchemaCacheSpec() {
        return CacheSpec.of(
                true,
                Config.external_cache_expire_time_seconds_after_access,
                Config.max_external_schema_cache_num);
    }

    private final String engine;
    private final ExecutorService refreshExecutor;
    private final Map<Long, CatalogEntryGroup> catalogEntries = Maps.newConcurrentMap();
    private final Map<String, MetaCacheEntryDef<?, ?>> metaCacheEntryDefs = Maps.newConcurrentMap();

    protected AbstractExternalMetaCache(String engine, ExecutorService refreshExecutor) {
        this.engine = engine;
        this.refreshExecutor = Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
    }

    @Override
    public String engine() {
        return engine;
    }

    @Override
    public Collection<String> aliases() {
        return Collections.singleton(engine);
    }

    @Override
    public void initCatalog(long catalogId, Map<String, String> catalogProperties) {
        Map<String, String> safeCatalogProperties = CacheSpec.applyCompatibilityMap(
                catalogProperties, catalogPropertyCompatibilityMap());
        catalogEntries.computeIfAbsent(catalogId, id -> buildCatalogEntryGroup(safeCatalogProperties));
    }

    @Override
    public void checkCatalogInitialized(long catalogId) {
        requireCatalogEntryGroup(catalogId);
    }

    @Override
    public boolean isCatalogInitialized(long catalogId) {
        return catalogEntries.containsKey(catalogId);
    }

    /**
     * Optional compatibility map for legacy catalog properties.
     *
     * <p>Map format: {@code legacyKey -> newKey}. The mapping is applied before
     * entry cache specs are parsed. If both keys exist, new key keeps precedence.
     */
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return Collections.emptyMap();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> MetaCacheEntry<K, V> entry(long catalogId, String entryName, Class<K> keyType, Class<V> valueType) {
        CatalogEntryGroup group = requireCatalogEntryGroup(catalogId);
        MetaCacheEntryDef<?, ?> def = requireMetaCacheEntryDef(entryName);
        ensureTypeCompatible(def, keyType, valueType);

        MetaCacheEntry<?, ?> cacheEntry = group.get(entryName);
        if (cacheEntry == null) {
            throw new IllegalStateException(String.format(
                    "Entry '%s' is not initialized for engine '%s', catalog %d.",
                    entryName, engine, catalogId));
        }
        return (MetaCacheEntry<K, V>) cacheEntry;
    }

    @Override
    public void invalidateCatalog(long catalogId) {
        CatalogEntryGroup removed = catalogEntries.remove(catalogId);
        if (removed != null) {
            removed.invalidateAll();
        }
    }

    @Override
    public void invalidateCatalogEntries(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group != null) {
            group.invalidateAll();
        }
    }

    @Override
    public void invalidateDb(long catalogId, String dbName) {
        invalidateEntries(catalogId, entryDef -> entryDef.getInvalidation().dbPredicate(dbName));
    }

    @Override
    public void invalidateTable(long catalogId, String dbName, String tableName) {
        invalidateEntries(catalogId, entryDef -> entryDef.getInvalidation().tablePredicate(dbName, tableName));
    }

    @Override
    public void invalidatePartitions(long catalogId, String dbName, String tableName, List<String> partitions) {
        invalidateEntries(catalogId,
                entryDef -> entryDef.getInvalidation().partitionPredicate(dbName, tableName, partitions));
    }

    @Override
    public Map<String, MetaCacheEntryStats> stats(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        return group == null ? Maps.newHashMap() : group.stats();
    }

    @Override
    public void close() {
        catalogEntries.values().forEach(CatalogEntryGroup::invalidateAll);
        catalogEntries.clear();
    }

    protected final <K, V> void registerMetaCacheEntryDef(MetaCacheEntryDef<K, V> entryDef) {
        Objects.requireNonNull(entryDef, "entryDef");
        if (!catalogEntries.isEmpty()) {
            throw new IllegalStateException(
                    String.format("Can not register entry '%s' after catalog initialization for engine '%s'.",
                            entryDef.getName(), engine));
        }
        MetaCacheEntryDef<?, ?> existing = metaCacheEntryDefs.putIfAbsent(entryDef.getName(), entryDef);
        if (existing != null) {
            throw new IllegalArgumentException(
                    String.format("Duplicated entry definition '%s' for engine '%s'.", entryDef.getName(), engine));
        }
    }

    protected final <K, V> EntryHandle<K, V> registerEntry(MetaCacheEntryDef<K, V> entryDef) {
        registerMetaCacheEntryDef(entryDef);
        return new EntryHandle<>(entryDef);
    }

    protected final <K, V> MetaCacheEntry<K, V> entry(long catalogId, MetaCacheEntryDef<K, V> entryDef) {
        validateRegisteredMetaCacheEntryDef(entryDef);
        return entry(catalogId, entryDef.getName(), entryDef.getKeyType(), entryDef.getValueType());
    }

    protected final String metaCacheTtlKey(String entryName) {
        return "meta.cache." + engine + "." + entryName + ".ttl-second";
    }

    protected final Map<String, String> singleCompatibilityMap(String legacyKey, String entryName) {
        return Collections.singletonMap(legacyKey, metaCacheTtlKey(entryName));
    }

    protected final boolean matchDb(NameMapping nameMapping, String dbName) {
        return nameMapping.getLocalDbName().equals(dbName);
    }

    protected final boolean matchTable(NameMapping nameMapping, String dbName, String tableName) {
        return matchDb(nameMapping, dbName) && nameMapping.getLocalTblName().equals(tableName);
    }

    protected final ExternalTable findExternalTable(NameMapping nameMapping, String engineNameForError) {
        CatalogIf<?> catalog = getCatalog(nameMapping.getCtlId());
        if (!(catalog instanceof ExternalCatalog)) {
            throw new CacheException("catalog %s is not external when loading %s schema cache",
                    null, nameMapping.getCtlId(), engineNameForError);
        }
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        return externalCatalog.getDb(nameMapping.getLocalDbName())
                .flatMap(db -> db.getTable(nameMapping.getLocalTblName()))
                .orElseThrow(() -> new CacheException(
                        "table %s.%s.%s not found when loading %s schema cache",
                        null, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                        nameMapping.getLocalTblName(), engineNameForError));
    }

    private CatalogEntryGroup requireCatalogEntryGroup(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group == null) {
            throw new IllegalStateException(String.format(
                    "Catalog %d is not initialized for engine '%s'.",
                    catalogId, engine));
        }
        return group;
    }

    protected CatalogIf<?> getCatalog(long catalogId) {
        if (Env.getCurrentEnv() == null || Env.getCurrentEnv().getCatalogMgr() == null) {
            return null;
        }
        return Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
    }

    private MetaCacheEntryDef<?, ?> requireMetaCacheEntryDef(String entryName) {
        MetaCacheEntryDef<?, ?> entryDef = metaCacheEntryDefs.get(entryName);
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

    private <K, V> void validateRegisteredMetaCacheEntryDef(MetaCacheEntryDef<K, V> entryDef) {
        MetaCacheEntryDef<?, ?> registered = requireMetaCacheEntryDef(entryDef.getName());
        ensureTypeCompatible(registered, entryDef.getKeyType(), entryDef.getValueType());
    }

    private void invalidateEntries(long catalogId, Function<MetaCacheEntryDef<?, ?>, Predicate<?>> predicateFactory) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group == null) {
            return;
        }
        metaCacheEntryDefs.values().forEach(entryDef -> invalidateEntryIfMatched(group, entryDef, predicateFactory));
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
        metaCacheEntryDefs.values()
                .forEach(entryDef -> group.put(entryDef.getName(), newMetaCacheEntry(entryDef, catalogProperties)));
        return group;
    }

    @SuppressWarnings("unchecked")
    private <K, V> MetaCacheEntry<K, V> newMetaCacheEntry(
            MetaCacheEntryDef<?, ?> rawEntryDef, Map<String, String> catalogProperties) {
        MetaCacheEntryDef<K, V> entryDef = (MetaCacheEntryDef<K, V>) rawEntryDef;
        CacheSpec cacheSpec = CacheSpec.fromProperties(
                catalogProperties, engine, entryDef.getName(), entryDef.getDefaultCacheSpec());
        return new MetaCacheEntry<>(entryDef.getName(),
                wrapSchemaValidator(entryDef.getLoader(), entryDef.getValueType()),
                cacheSpec,
                refreshExecutor, entryDef.isAutoRefresh(), entryDef.isContextualOnly());
    }

    private <K, V> Function<K, V> wrapSchemaValidator(Function<K, V> loader, Class<V> valueType) {
        if (loader == null) {
            return null;
        }
        if (!SchemaCacheValue.class.isAssignableFrom(valueType)) {
            return loader;
        }
        return key -> {
            V value = loader.apply(key);
            ((SchemaCacheValue) value).validateSchema();
            return value;
        };
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
