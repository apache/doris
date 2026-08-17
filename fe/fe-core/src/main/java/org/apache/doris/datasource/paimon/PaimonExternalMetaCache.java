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
import java.util.concurrent.ExecutorService;
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

    public PaimonExternalMetaCache(ExecutorService refreshExecutor) {
        this(refreshExecutor, new ExternalMetaCacheBudgetManager(java.util.OptionalLong.empty()));
    }

    public PaimonExternalMetaCache(ExecutorService refreshExecutor, ExternalMetaCacheBudgetManager budgetManager) {
        super(ENGINE, refreshExecutor, budgetManager);
        tableLoader = new PaimonTableLoader();
        latestSnapshotProjectionLoader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(), this::getPaimonSchemaCacheValue);
        tableEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_TABLE, NameMapping.class, PaimonTableCacheValue.class,
                this::loadTableCacheValue, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping))
                .withSizeEstimator((key, value) -> value.prepareForCachePublication(key))
                .withReplacementListener(this::retireTableGeneration));
        snapshotEntry = registerEntry(MetaCacheEntryDef.contextualOnly(ENTRY_SNAPSHOT,
                PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(PaimonSnapshotEntryKey::getNameMapping))
                .withSizeEstimator((key, value) -> value.prepareForCachePublication(key)));
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, PaimonSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(),
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
        PaimonTableCacheValue tableValue = tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        PaimonSnapshot fence = loadLatestSnapshotFence(nameMapping, tableValue.getPaimonTable()).getSnapshot();
        PaimonSnapshotEntryKey key = PaimonSnapshotEntryKey.of(
                nameMapping, fence, tableValue.getGeneration());
        MetaCacheEntry<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> entry =
                snapshotEntry.get(nameMapping.getCtlId());
        PaimonSnapshotCacheValue snapshotValue = entry.get(key,
                ignored -> executeAuthenticated(nameMapping,
                        () -> latestSnapshotProjectionLoader.loadAtFence(
                                nameMapping, fence, tableValue.getGeneration())));
        if (!isCurrentTableGeneration(nameMapping, tableValue.getGeneration())) {
            entry.invalidateKeyIfSame(key, snapshotValue);
        }
        return snapshotValue;
    }

    public PaimonSnapshotCacheValue loadSnapshotProjection(ExternalTable dorisTable, Table effectiveTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return executeAuthenticated(nameMapping,
                () -> latestSnapshotProjectionLoader.load(nameMapping, effectiveTable));
    }

    public PaimonSnapshotCacheValue loadLatestSnapshotFence(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        PaimonTableCacheValue tableValue = tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        return loadLatestSnapshotFence(nameMapping, tableValue.getPaimonTable());
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
        return getPaimonSchemaCacheValue(
                nameMapping, schemaId, tableValue.getGeneration(), tableValue.getPaimonTable());
    }

    PaimonSchemaCacheValue getPaimonSchemaCacheValue(
            NameMapping nameMapping, long schemaId, long tableGeneration, Table retainedTable) {
        PaimonSchemaCacheKey key = new PaimonSchemaCacheKey(nameMapping, tableGeneration, schemaId);
        if (tableGeneration <= 0L) {
            return (PaimonSchemaCacheValue) executeAuthenticated(nameMapping,
                    () -> loadSchemaCacheValue(key, retainedTable));
        }
        MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> entry = schemaEntry.get(nameMapping.getCtlId());
        SchemaCacheValue schemaCacheValue = entry.get(key,
                ignored -> executeAuthenticated(nameMapping,
                        () -> loadSchemaCacheValue(key, retainedTable)));
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
        PaimonTableCacheValue currentTable = tableEntry.get(nameMapping.getCtlId()).peekIfPresent(nameMapping);
        return currentTable != null && currentTable.getGeneration() == tableGeneration;
    }

    private PaimonTableCacheValue loadTableCacheValue(NameMapping nameMapping) {
        return new PaimonTableCacheValue(tableLoader.load(nameMapping));
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
            return loadSchemaCacheValue(key);
        }
        dorisTable.setUpdateTime(System.currentTimeMillis());
        return ((PaimonExternalTable) dorisTable).loadSchemaForCache(retainedTable, key.getSchemaId());
    }

    private PaimonSnapshotCacheValue loadLatestSnapshotFence(NameMapping nameMapping, Table retainedTable) {
        return executeAuthenticated(nameMapping,
                () -> latestSnapshotProjectionLoader.loadFence(nameMapping, retainedTable));
    }

    private <T> T executeAuthenticated(NameMapping nameMapping, java.util.concurrent.Callable<T> task) {
        return tableLoader.executeAuthenticated(nameMapping, task);
    }

    private void retireTableGeneration(NameMapping nameMapping,
            @Nullable PaimonTableCacheValue previousValue, PaimonTableCacheValue currentValue) {
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
