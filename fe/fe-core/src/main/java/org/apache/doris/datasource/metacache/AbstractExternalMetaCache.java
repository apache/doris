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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
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
    private static final Logger LOG = LogManager.getLogger(AbstractExternalMetaCache.class);

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
    private final ExternalMetaCacheBudgetManager budgetManager;
    private final Map<Long, CatalogEntryGroup> catalogEntries = Maps.newConcurrentMap();
    private final Map<String, MetaCacheEntryDef<?, ?>> metaCacheEntryDefs = Maps.newConcurrentMap();

    protected AbstractExternalMetaCache(String engine, ExecutorService refreshExecutor) {
        this(engine, refreshExecutor, new ExternalMetaCacheBudgetManager(OptionalLong.empty()));
    }

    private volatile LongConsumer catalogPreparer;
    // Test hook; production probes the catalog manager. See catalogPermanentlyDropped.
    private volatile java.util.function.LongPredicate droppedCatalogProbeForTest;

    protected AbstractExternalMetaCache(String engine, ExecutorService refreshExecutor,
            ExternalMetaCacheBudgetManager budgetManager) {
        this.engine = engine;
        this.refreshExecutor = Objects.requireNonNull(refreshExecutor, "refreshExecutor can not be null");
        this.budgetManager = Objects.requireNonNull(budgetManager, "budgetManager can not be null");
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
    public void validateCatalogProperties(Map<String, String> catalogProperties) {
        Map<String, String> safeCatalogProperties = CacheSpec.applyCompatibilityMap(
                catalogProperties, catalogPropertyCompatibilityMap());
        validateMappedCatalogProperties(safeCatalogProperties, true);
    }

    @Override
    public Map<String, String> sanitizeCatalogPropertiesForRuntime(Map<String, String> catalogProperties) {
        return sanitizeCatalogPropertiesForRuntime(catalogProperties, warning -> LOG.debug(warning));
    }

    @Override
    public void validateCatalogPropertiesForRuntime(Map<String, String> catalogProperties) {
        Map<String, String> safeCatalogProperties = CacheSpec.applyCompatibilityMap(
                catalogProperties, catalogPropertyCompatibilityMap());
        validateMappedCatalogProperties(safeCatalogProperties, false);
    }

    /**
     * Exactly what initCatalog keeps: mapped legacy keys, only known entries/options in this
     * engine's namespace, a parsable catalog max-weight, and entry max-weights within it.
     */
    private Map<String, String> sanitizeCatalogPropertiesForRuntime(
            Map<String, String> catalogProperties, Consumer<String> warningConsumer) {
        Map<String, String> safeCatalogProperties = CacheSpec.applyCompatibilityMap(
                catalogProperties, catalogPropertyCompatibilityMap());
        safeCatalogProperties = CacheSpec.sanitizeEnginePropertiesForRuntime(
                safeCatalogProperties, engine, metaCacheEntryDefs, warningConsumer);
        try {
            budgetManager.parseCatalogMaxWeight(safeCatalogProperties);
        } catch (IllegalArgumentException e) {
            safeCatalogProperties.remove(ExternalMetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY);
            warningConsumer.accept("Ignoring invalid persisted external metadata cache property '"
                    + ExternalMetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY + "': " + e.getMessage());
        }
        OptionalLong runtimeCatalogMaxWeight = budgetManager.parseCatalogMaxWeight(safeCatalogProperties);
        for (MetaCacheEntryDef<?, ?> entryDef : metaCacheEntryDefs.values()) {
            if (entryDef.getSizeEstimator() == null) {
                continue;
            }
            String maxWeightKey = CacheSpec.metaCacheKeyPrefix(engine)
                    + entryDef.getName() + ".max-weight";
            if (!safeCatalogProperties.containsKey(maxWeightKey)) {
                continue;
            }
            CacheSpec cacheSpec = CacheSpec.fromProperties(
                    safeCatalogProperties, engine, entryDef.getName(), entryDef.getDefaultCacheSpec());
            try {
                budgetManager.validateCatalogEntryHierarchy(
                        runtimeCatalogMaxWeight, cacheSpec.getMaxWeight());
            } catch (IllegalArgumentException e) {
                safeCatalogProperties.remove(maxWeightKey);
                warningConsumer.accept("Ignoring invalid persisted external metadata cache property '"
                        + maxWeightKey + "': " + e.getMessage());
            }
        }
        return safeCatalogProperties;
    }

    @Override
    public void initCatalog(long catalogId, Map<String, String> catalogProperties) {
        if (catalogEntries.containsKey(catalogId)) {
            return;
        }
        synchronized (this) {
            if (catalogEntries.containsKey(catalogId)) {
                return;
            }
            Map<String, String> safeCatalogProperties = sanitizeCatalogPropertiesForRuntime(
                    catalogProperties,
                    warning -> LOG.warn("{} (engine={}, catalog={})", warning, engine, catalogId));
            validateMappedCatalogProperties(safeCatalogProperties, false);
            catalogEntries.put(catalogId, buildCatalogEntryGroup(catalogId, safeCatalogProperties));
        }
    }

    private void validateMappedCatalogProperties(
            Map<String, String> catalogProperties, boolean validateAgainstLocalGlobalLimit) {
        CacheSpec.validateEngineProperties(catalogProperties, engine, metaCacheEntryDefs);
        OptionalLong catalogMaxWeight = budgetManager.parseCatalogMaxWeight(catalogProperties);
        metaCacheEntryDefs.values().stream()
                .filter(entryDef -> entryDef.getSizeEstimator() != null)
                .map(entryDef -> CacheSpec.fromProperties(
                        catalogProperties, engine, entryDef.getName(), entryDef.getDefaultCacheSpec()))
                .forEach(cacheSpec -> {
                    if (validateAgainstLocalGlobalLimit) {
                        budgetManager.validateHierarchy(catalogMaxWeight, cacheSpec.getMaxWeight());
                    } else {
                        budgetManager.validateCatalogEntryHierarchy(catalogMaxWeight, cacheSpec.getMaxWeight());
                    }
                });
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

        beforeCatalogEntryLookupForTest(catalogId, entryName);
        MetaCacheEntry<?, ?> cacheEntry = group.get(entryName);
        if (cacheEntry == null) {
            throw new IllegalStateException(String.format(
                    "Entry '%s' is not initialized for engine '%s', catalog %d.",
                    entryName, engine, catalogId));
        }
        return (MetaCacheEntry<K, V>) cacheEntry;
    }

    @Override
    public synchronized void invalidateCatalog(long catalogId) {
        CatalogEntryGroup removed = catalogEntries.remove(catalogId);
        if (removed != null) {
            removed.close();
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
    public synchronized void close() {
        catalogEntries.values().forEach(CatalogEntryGroup::close);
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

    // Let tests pause after capturing a group and before looking up its entry.
    void beforeCatalogEntryLookupForTest(long catalogId, String entryName) {
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

    // A contended cache-policy handoff resolves within this window; see requireCatalogEntryGroup.
    private static final long PREPARE_RETRY_WINDOW_NANOS = 2_000_000_000L;
    private static final long PREPARE_RETRY_SLEEP_MS = 50L;

    private CatalogEntryGroup requireCatalogEntryGroup(long catalogId) {
        CatalogEntryGroup group = catalogEntries.get(catalogId);
        if (group == null && catalogPreparer != null) {
            // The caller prepared the catalog before capturing this engine, but a cache-policy
            // ALTER retired the group in between. Re-prepare under the lifecycle fence so the
            // lookup observes the new policy instead of failing a valid catalog. The preparer
            // never blocks on the fence (a nested default loader may hold a Caffeine bin lock
            // that retirement itself needs), so a contended handoff is absorbed with a bounded
            // sleep-and-retry: the ALTER finishes within the window, or the lookup fails as
            // before without any deadlock.
            long deadlineNanos = System.nanoTime() + PREPARE_RETRY_WINDOW_NANOS;
            while (!catalogPermanentlyDropped(catalogId)) {
                catalogPreparer.accept(catalogId);
                group = catalogEntries.get(catalogId);
                if (group != null || System.nanoTime() >= deadlineNanos
                        || catalogPermanentlyDropped(catalogId)) {
                    break;
                }
                try {
                    Thread.sleep(PREPARE_RETRY_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (group == null) {
            if (catalogPermanentlyDropped(catalogId)) {
                throw new IllegalStateException(String.format(
                        "Catalog %d was dropped; engine '%s' serves no metadata for it.",
                        catalogId, engine));
            }
            throw new IllegalStateException(String.format(
                    "Catalog %d is not initialized for engine '%s'.",
                    catalogId, engine));
        }
        return group;
    }

    /**
     * DROP CATALOG is terminal: ids are never reused, and the catalog manager removes the
     * catalog before any engine group is detached or closed, so a lookup that finds neither a
     * group nor a live catalog can fail immediately instead of consuming the contended-handoff
     * retry window. A rename keeps the catalog registered under the same id, so its transient
     * group absence still gets the bounded retry, and no per-engine dropped-id state is retained.
     */
    private boolean catalogPermanentlyDropped(long catalogId) {
        java.util.function.LongPredicate probe = droppedCatalogProbeForTest;
        if (probe != null) {
            return probe.test(catalogId);
        }
        org.apache.doris.catalog.Env env = org.apache.doris.catalog.Env.getCurrentEnv();
        org.apache.doris.datasource.CatalogMgr catalogMgr = env == null ? null : env.getCatalogMgr();
        // An absent manager (isolated construction/boot) proves nothing; keep the bounded retry.
        return catalogMgr != null && catalogMgr.getCatalog(catalogId) == null;
    }

    /** Test hook: overrides the live-catalog probe used to detect a permanent DROP. */
    public void bindDroppedCatalogProbeForTest(java.util.function.LongPredicate probe) {
        this.droppedCatalogProbeForTest = probe;
    }

    @Override
    public void bindCatalogPreparer(LongConsumer catalogPreparer) {
        this.catalogPreparer = catalogPreparer;
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

    private CatalogEntryGroup buildCatalogEntryGroup(long catalogId, Map<String, String> catalogProperties) {
        CatalogEntryGroup group = new CatalogEntryGroup();
        try {
            metaCacheEntryDefs.values().forEach(entryDef -> group.put(
                    entryDef.getName(), newMetaCacheEntry(catalogId, entryDef, catalogProperties)));
            return group;
        } catch (RuntimeException | Error e) {
            group.close();
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> MetaCacheEntry<K, V> newMetaCacheEntry(
            long catalogId, MetaCacheEntryDef<?, ?> rawEntryDef, Map<String, String> catalogProperties) {
        MetaCacheEntryDef<K, V> entryDef = (MetaCacheEntryDef<K, V>) rawEntryDef;
        CacheSpec cacheSpec = CacheSpec.fromProperties(
                catalogProperties, engine, entryDef.getName(), entryDef.getDefaultCacheSpec());
        OptionalLong catalogMaxWeight = budgetManager.parseCatalogMaxWeight(catalogProperties);
        if (cacheSpec.isWeightBounded() && entryDef.getSizeEstimator() == null) {
            throw new IllegalArgumentException(String.format(
                    "Entry '%s' for engine '%s' configures max-weight but has no estimator.",
                    entryDef.getName(), engine));
        }
        boolean enableWeight = entryDef.getSizeEstimator() != null
                && (cacheSpec.isWeightBounded()
                        || catalogMaxWeight.isPresent()
                        || budgetManager.getGlobalMaxWeight().isPresent());
        ExternalMetaCacheBudgetManager.EntryBudget entryBudget = null;
        if (enableWeight) {
            entryBudget = budgetManager.createEntryBudget(
                    catalogId, engine, entryDef.getName(), catalogMaxWeight, cacheSpec.getMaxWeight());
            cacheSpec = cacheSpec.withMaxWeight(entryBudget.getEffectiveMaxWeight());
        }
        try {
            return new MetaCacheEntry<>(entryDef.getName(),
                    wrapSchemaValidator(entryDef.getLoader(), entryDef.getValueType()),
                    cacheSpec,
                    refreshExecutor, entryDef.isAutoRefresh(), entryDef.isContextualOnly(),
                    entryDef.getSizeEstimator(), entryBudget, entryDef.getReplacementListener(),
                    entryDef.getRemovalTokenExtractor(), entryDef.getRemovalListener(),
                    entryDef.getUnpublishedValueRetirer(), entryDef.usesSoftValues());
        } catch (RuntimeException | Error e) {
            if (entryBudget != null) {
                entryBudget.close();
            }
            throw e;
        }
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

        @SuppressWarnings("unchecked")
        public MetaCacheEntry<K, V> getIfInitialized(long catalogId) {
            // Read the group once. A concurrent invalidation may close that captured entry, which
            // is safe; looking the group up a second time could instead throw after the first check.
            CatalogEntryGroup group = catalogEntries.get(catalogId);
            return group == null ? null : (MetaCacheEntry<K, V>) group.get(entryDef.getName());
        }
    }
}
