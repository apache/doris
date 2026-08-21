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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;
import org.apache.doris.datasource.metacache.paimon.PaimonLatestSnapshotProjectionLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonPartitionInfoLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonTableLoader;

import org.apache.paimon.table.Table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Paimon engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code table}: loaded Paimon table handle per table mapping</li>
 *   <li>{@code snapshot}: immutable partition projection keyed by a captured snapshot/schema fence</li>
 *   <li>{@code schema}: schema cache keyed by table identity + schema id</li>
 * </ul>
 *
 * <p>The latest main-branch snapshot is captured once as a fence and loaded through an independent
 * contextual entry. Branch/tag/options projections remain statement-local and are not aliased to
 * this main-snapshot key.
 *
 * <p>Invalidation behavior:
 * <ul>
 *   <li>db/table invalidation clears table, snapshot and schema entries by matching local names</li>
 *   <li>partition-level invalidation falls back to table-level invalidation</li>
 * </ul>
 */
public class PaimonExternalMetaCache extends AbstractExternalMetaCache {
    public static final String ENGINE = "paimon";
    public static final String ENTRY_TABLE = "table";
    public static final String ENTRY_SNAPSHOT = "snapshot";
    public static final String ENTRY_SCHEMA = "schema";

    private final EntryHandle<NameMapping, PaimonTableCacheValue> tableEntry;
    private final EntryHandle<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshotEntry;
    private final EntryHandle<PaimonSchemaCacheKey, SchemaCacheValue> schemaEntry;
    private final PaimonTableLoader tableLoader;
    private final PaimonLatestSnapshotProjectionLoader latestSnapshotProjectionLoader;
    // Most recently observed latest fence per (table, generation); see getSnapshotCache.
    private final AtomicLong fenceObservations = new AtomicLong();
    private final ConcurrentHashMap<LatestFenceOwner, ObservedFence> latestObservedFences =
            new ConcurrentHashMap<>();

    public PaimonExternalMetaCache(ExecutorService refreshExecutor) {
        this(refreshExecutor, new ExternalMetaCacheBudgetManager(java.util.OptionalLong.empty()));
    }

    public PaimonExternalMetaCache(ExecutorService refreshExecutor, ExternalMetaCacheBudgetManager budgetManager) {
        super(ENGINE, refreshExecutor, budgetManager);
        tableLoader = new PaimonTableLoader();
        latestSnapshotProjectionLoader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(), this::getPaimonSchemaCacheValuePreAuthenticated);
        tableEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_TABLE, NameMapping.class, PaimonTableCacheValue.class,
                this::loadTableCacheValue, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping))
                .withSizeEstimator((key, value) -> value.prepareForCachePublication(key))
                .withReplacementListener(this::retireTableGeneration)
                .withRemovalListener(PaimonTableCacheValue::getGeneration, this::retireRemovedTableGeneration));
        snapshotEntry = registerEntry(MetaCacheEntryDef.contextualOnly(ENTRY_SNAPSHOT,
                PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(PaimonSnapshotEntryKey::getNameMapping))
                .withSizeEstimator((key, value) -> value.prepareForCachePublication(key)));
        // Schema values are keyed by an immutable (generation, schemaId) pair, so a timed
        // refresh can never observe new content; it would also invoke the default loader
        // outside the catalog execution authenticator scope. Misses load contextually.
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, PaimonSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(), false,
                MetaCacheEntryInvalidation.forNameMapping(PaimonSchemaCacheKey::getNameMapping)));
    }

    public Table getPaimonTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getPaimonTable();
    }

    public Table getPaimonTable(NameMapping nameMapping) {
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getPaimonTable();
    }

    public PaimonSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables = tableEntry.get(nameMapping.getCtlId());
        PaimonTableCacheValue tableValue = tables.get(nameMapping);
        PaimonSnapshot fence = loadLatestSnapshotFence(nameMapping, tableValue).getSnapshot();
        if (!tables.isEffectivelyEnabled()) {
            // Projections are keyed by the synthetic generation of a published table handle. An
            // ineffective table entry publishes nothing, so nothing keyed by this load could ever
            // be looked up again: serve it directly instead of churning the snapshot entry.
            return executeForGeneration(tableValue, nameMapping,
                    () -> latestSnapshotProjectionLoader.loadAtFence(
                            nameMapping, fence, tableValue.getGeneration()));
        }
        // Order fence observations, not snapshot ids: a rollback moves the latest snapshot
        // backwards, and a concurrent call may finish after a later observation (reversed
        // completion). Either way the most recently observed fence is the one future lookups read.
        long observation = fenceObservations.incrementAndGet();
        PaimonSnapshotEntryKey key = PaimonSnapshotEntryKey.of(
                nameMapping, fence, tableValue.getGeneration());
        MetaCacheEntry<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> entry =
                snapshotEntry.get(nameMapping.getCtlId());
        AtomicBoolean loaded = new AtomicBoolean();
        PaimonSnapshotCacheValue snapshotValue = entry.get(key,
                ignored -> executeForGeneration(tableValue, nameMapping, () -> {
                    loaded.set(true);
                    return latestSnapshotProjectionLoader.loadAtFence(
                            nameMapping, fence, tableValue.getGeneration());
                }));
        LatestFenceOwner owner = new LatestFenceOwner(nameMapping, tableValue.getGeneration());
        ObservedFence latest = latestObservedFences.compute(owner, (ignored, current) ->
                current == null || current.observation < observation ? new ObservedFence(observation, key) : current);
        if (loaded.get()) {
            retireSupersededLatestProjections(entry, owner, latest.key);
        }
        if (!isCurrentTableGeneration(nameMapping, tableValue.getGeneration())) {
            entry.invalidateKeyIfSame(key, snapshotValue);
            // A generation that is not published (rejected admission, replaced or invalidated
            // mid-load) can never be observed again; drop the owner this call registered so
            // persistently rejected tables cannot grow the map, and so a delayed old-generation
            // load cannot resurrect an owner that catalog cleanup already removed.
            latestObservedFences.remove(owner);
        }
        return snapshotValue;
    }

    /**
     * Only the projection of the most recently observed latest fence of a table generation is
     * reachable: every later call re-reads the fence and looks up that key. After a load, retire
     * every other projection of the generation, including this load itself when a concurrent call
     * observed a later fence and finished first, so a busy table never accumulates projections.
     */
    private static void retireSupersededLatestProjections(
            MetaCacheEntry<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> entry,
            LatestFenceOwner owner, PaimonSnapshotEntryKey latestKey) {
        entry.invalidateIf(key -> owner.owns(key) && !key.equals(latestKey));
    }

    private void forgetObservedFences(NameMapping nameMapping, java.util.function.LongPredicate retiredGeneration) {
        latestObservedFences.keySet().removeIf(owner -> owner.nameMapping.equals(nameMapping)
                && retiredGeneration.test(owner.generation));
    }

    private static final class LatestFenceOwner {
        private final NameMapping nameMapping;
        private final long generation;

        private LatestFenceOwner(NameMapping nameMapping, long generation) {
            this.nameMapping = nameMapping;
            this.generation = generation;
        }

        private boolean owns(PaimonSnapshotEntryKey key) {
            return key.getTableGeneration() == generation && key.getNameMapping().equals(nameMapping);
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof LatestFenceOwner)) {
                return false;
            }
            LatestFenceOwner that = (LatestFenceOwner) object;
            return generation == that.generation && nameMapping.equals(that.nameMapping);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(nameMapping, generation);
        }
    }

    private static final class ObservedFence {
        private final long observation;
        private final PaimonSnapshotEntryKey key;

        private ObservedFence(long observation, PaimonSnapshotEntryKey key) {
            this.observation = observation;
            this.key = key;
        }
    }

    public PaimonSnapshotCacheValue loadSnapshotProjection(ExternalTable dorisTable, Table effectiveTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return executeAuthenticated(nameMapping,
                () -> latestSnapshotProjectionLoader.load(nameMapping, effectiveTable));
    }

    public PaimonSnapshotCacheValue loadLatestSnapshotFence(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        PaimonTableCacheValue tableValue = tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        return loadLatestSnapshotFence(nameMapping, tableValue);
    }

    public PaimonSnapshotCacheValue loadSnapshotAtFence(
            ExternalTable dorisTable, PaimonSnapshot fence) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return executeAuthenticated(nameMapping,
                () -> latestSnapshotProjectionLoader.loadAtFence(nameMapping, fence));
    }

    public PaimonSnapshotCacheValue loadSnapshotAtFence(
            ExternalTable dorisTable, Table effectiveTable, PaimonSnapshot fence) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return executeAuthenticated(nameMapping,
                () -> latestSnapshotProjectionLoader.loadEffectiveAtFence(
                        nameMapping, effectiveTable, fence));
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(NameMapping nameMapping, long schemaId) {
        PaimonTableCacheValue tableValue = tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        return getPaimonSchemaCacheValue(nameMapping, schemaId, tableValue.getGeneration(),
                tableValue.getPaimonTable(), tableValue.getAuthenticator());
    }

    PaimonSchemaCacheValue getPaimonSchemaCacheValue(
            NameMapping nameMapping, long schemaId, long tableGeneration, Table retainedTable) {
        return getPaimonSchemaCacheValue(nameMapping, schemaId, tableGeneration, retainedTable, null);
    }

    PaimonSchemaCacheValue getPaimonSchemaCacheValue(NameMapping nameMapping, long schemaId,
            long tableGeneration, Table retainedTable,
            @Nullable org.apache.doris.common.security.authentication.ExecutionAuthenticator authenticator) {
        return getPaimonSchemaCacheValueInternal(nameMapping, schemaId, tableGeneration, retainedTable,
                load -> executeForCapturedAuthenticator(authenticator, nameMapping, load));
    }

    /**
     * Schema resolution for a projection load that is already running inside the caller's
     * captured authenticated context: re-resolving the catalog here could observe a concurrent
     * property reset, so the load executes directly.
     */
    private PaimonSchemaCacheValue getPaimonSchemaCacheValuePreAuthenticated(
            NameMapping nameMapping, long schemaId, long tableGeneration, Table retainedTable) {
        return getPaimonSchemaCacheValueInternal(nameMapping, schemaId, tableGeneration, retainedTable,
                load -> {
                    try {
                        return load.call();
                    } catch (Exception e) {
                        throw new CacheException("failed to load paimon schema %s.%s.%s: %s",
                                e, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                                nameMapping.getLocalTblName(), e.getMessage());
                    }
                });
    }

    private PaimonSchemaCacheValue getPaimonSchemaCacheValueInternal(NameMapping nameMapping, long schemaId,
            long tableGeneration, Table retainedTable,
            java.util.function.Function<java.util.concurrent.Callable<SchemaCacheValue>,
                    SchemaCacheValue> executor) {
        java.util.concurrent.Callable<SchemaCacheValue> load =
                () -> loadSchemaCacheValue(new PaimonSchemaCacheKey(
                        nameMapping, tableGeneration, schemaId), retainedTable);
        java.util.function.Supplier<SchemaCacheValue> authenticated = () -> executor.apply(load);
        PaimonSchemaCacheKey key = new PaimonSchemaCacheKey(nameMapping, tableGeneration, schemaId);
        if (tableGeneration <= 0L || !tableEntry.get(nameMapping.getCtlId()).isEffectivelyEnabled()) {
            // See getSnapshotCache: without a published table handle no generation-keyed
            // projection is reachable again.
            return (PaimonSchemaCacheValue) authenticated.get();
        }
        MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> entry = schemaEntry.get(nameMapping.getCtlId());
        SchemaCacheValue schemaCacheValue = entry.get(key, ignored -> authenticated.get());
        if (!isCurrentTableGeneration(nameMapping, tableGeneration)) {
            entry.invalidateKeyIfSame(key, schemaCacheValue);
        }
        return (PaimonSchemaCacheValue) schemaCacheValue;
    }

    /**
     * Snapshot and schema projections are keyed by the synthetic generation of the base table
     * handle they were derived from. A generation that is no longer published (replaced, expired,
     * or never admitted because its weight estimate was rejected) can never be looked up again,
     * so its projections must not stay behind in the child entries.
     */
    private boolean isCurrentTableGeneration(NameMapping nameMapping, long tableGeneration) {
        // Revalidation must never re-prepare a retired catalog group (or throw out of a lookup
        // that already holds a valid value): an absent group simply means "not current".
        MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                tableEntry.getIfInitialized(nameMapping.getCtlId());
        PaimonTableCacheValue currentTable = tables == null ? null : tables.peekIfPresent(nameMapping);
        return currentTable != null && currentTable.getGeneration() == tableGeneration;
    }

    private PaimonTableCacheValue loadTableCacheValue(NameMapping nameMapping) {
        try {
            PaimonExternalCatalog catalog = tableLoader.catalog(nameMapping);
            return new PaimonTableCacheValue(
                    tableLoader.load(nameMapping), catalog.getExecutionAuthenticator());
        } catch (java.io.IOException e) {
            throw new CacheException("failed to load paimon table %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                    nameMapping.getLocalTblName(), e.getMessage());
        }
    }

    /**
     * Execute on the authenticated context captured with the table generation, falling back to
     * the catalog's current authenticator for values that predate the capture.
     */
    private <T> T executeForGeneration(PaimonTableCacheValue tableValue,
            NameMapping nameMapping, java.util.concurrent.Callable<T> task) {
        return executeForCapturedAuthenticator(tableValue.getAuthenticator(), nameMapping, task);
    }

    private <T> T executeForCapturedAuthenticator(
            @Nullable org.apache.doris.common.security.authentication.ExecutionAuthenticator authenticator,
            NameMapping nameMapping, java.util.concurrent.Callable<T> task) {
        if (authenticator == null) {
            return executeAuthenticated(nameMapping, task);
        }
        try {
            return authenticator.execute(task);
        } catch (Exception e) {
            throw new CacheException("failed to load authenticated paimon metadata %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                    nameMapping.getLocalTblName(), e.getMessage());
        }
    }

    private SchemaCacheValue loadSchemaCacheValue(PaimonSchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load paimon schema cache value for: %s.%s.%s, schemaId: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getSchemaId()));
    }

    private SchemaCacheValue loadSchemaCacheValue(PaimonSchemaCacheKey key, Table retainedTable) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        if (!(dorisTable instanceof PaimonExternalTable)) {
            SchemaCacheValue fallback = loadSchemaCacheValue(key);
            fallback.validateSchema();
            return fallback;
        }
        dorisTable.setUpdateTime(System.currentTimeMillis());
        SchemaCacheValue value =
                ((PaimonExternalTable) dorisTable).loadSchemaForCache(retainedTable, key.getSchemaId());
        // Contextual miss loaders bypass the default-loader schema validator; ambiguous
        // case-insensitive column names must be rejected on this path too.
        value.validateSchema();
        return value;
    }

    private PaimonSnapshotCacheValue loadLatestSnapshotFence(
            NameMapping nameMapping, PaimonTableCacheValue tableValue) {
        return executeForGeneration(tableValue, nameMapping,
                () -> latestSnapshotProjectionLoader.loadFence(nameMapping, tableValue.getPaimonTable()));
    }

    private <T> T executeAuthenticated(NameMapping nameMapping, java.util.concurrent.Callable<T> task) {
        return tableLoader.executeAuthenticated(nameMapping, task);
    }

    private void retireTableGeneration(NameMapping nameMapping,
            @Nullable PaimonTableCacheValue previousValue, PaimonTableCacheValue currentValue) {
        forgetObservedFences(nameMapping, generation -> generation != currentValue.getGeneration());
        MetaCacheEntry<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots =
                snapshotEntry.getIfInitialized(nameMapping.getCtlId());
        if (snapshots != null) {
            snapshots.invalidateIf(key -> key.getNameMapping().equals(nameMapping)
                    && key.getTableGeneration() != currentValue.getGeneration());
        }
        MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemas =
                schemaEntry.getIfInitialized(nameMapping.getCtlId());
        if (schemas != null) {
            schemas.invalidateIf(key -> key.getNameMapping().equals(nameMapping)
                    && key.getTableGeneration() != currentValue.getGeneration());
        }
    }

    /**
     * An admitted table handle left the entry through eviction, expiry, collection or explicit
     * invalidation without a successor being published. Its synthetic generation can never be
     * looked up again, so the projections keyed by it are garbage. The callback is delayed and
     * fenced by the removed generation: it never touches the generation currently published.
     */
    private void retireRemovedTableGeneration(NameMapping nameMapping, @Nullable Long removedGeneration) {
        MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                tableEntry.getIfInitialized(nameMapping.getCtlId());
        PaimonTableCacheValue currentValue = tables == null ? null : tables.peekIfPresent(nameMapping);
        long currentGeneration = currentValue == null ? -1L : currentValue.getGeneration();
        if (removedGeneration != null && removedGeneration == currentGeneration) {
            // The removed handle was republished; its projections are addressable again.
            return;
        }
        // A collected value left no generation behind: everything that is not derived from the
        // currently published handle is unreachable.
        java.util.function.LongPredicate retired = removedGeneration == null
                ? generation -> generation != currentGeneration
                : generation -> generation == removedGeneration;
        forgetObservedFences(nameMapping, retired);
        MetaCacheEntry<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots =
                snapshotEntry.getIfInitialized(nameMapping.getCtlId());
        if (snapshots != null) {
            snapshots.invalidateIf(key -> key.getNameMapping().equals(nameMapping)
                    && retired.test(key.getTableGeneration()));
        }
        MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemas =
                schemaEntry.getIfInitialized(nameMapping.getCtlId());
        if (schemas != null) {
            schemas.invalidateIf(key -> key.getNameMapping().equals(nameMapping)
                    && retired.test(key.getTableGeneration()));
        }
    }

    @Override
    public void invalidateCatalog(long catalogId) {
        latestObservedFences.keySet().removeIf(owner -> owner.nameMapping.getCtlId() == catalogId);
        super.invalidateCatalog(catalogId);
    }

    @Override
    public void onCatalogPermanentlyRemoved(long catalogId) {
        // A lookup racing the drop may re-insert a fence owner after invalidateCatalog cleaned
        // the map; this hook runs even when the entry group is already retired and the id is
        // never reused, so the owners cannot leak for the FE lifetime.
        latestObservedFences.keySet().removeIf(owner -> owner.nameMapping.getCtlId() == catalogId);
    }

    @Override
    public void invalidateCatalogEntries(long catalogId) {
        latestObservedFences.keySet().removeIf(owner -> owner.nameMapping.getCtlId() == catalogId);
        super.invalidateCatalogEntries(catalogId);
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        Map<String, String> compatibility = new java.util.HashMap<>(
                singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA));
        compatibility.put("meta.cache.paimon.table.enable", "meta.cache.paimon.snapshot.enable");
        compatibility.put("meta.cache.paimon.table.ttl-second", "meta.cache.paimon.snapshot.ttl-second");
        compatibility.put("meta.cache.paimon.table.capacity", "meta.cache.paimon.snapshot.capacity");
        return compatibility;
    }
}
