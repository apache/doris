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
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping)));
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
        PaimonSnapshot fence = tableValue.getLatestSnapshotFence().getSnapshot();
        PaimonSnapshotEntryKey key = PaimonSnapshotEntryKey.of(
                nameMapping, fence, tableValue.getGeneration());
        MetaCacheEntry<PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> entry =
                snapshotEntry.get(nameMapping.getCtlId());
        return entry.get(key, ignored -> latestSnapshotProjectionLoader.loadAtFence(nameMapping, fence));
    }

    public PaimonSnapshotCacheValue loadSnapshotProjection(ExternalTable dorisTable, Table effectiveTable) {
        return latestSnapshotProjectionLoader.load(dorisTable.getOrBuildNameMapping(), effectiveTable);
    }

    public PaimonSnapshotCacheValue loadLatestSnapshotFence(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getLatestSnapshotFence();
    }

    public PaimonSnapshotCacheValue loadSnapshotAtFence(
            ExternalTable dorisTable, PaimonSnapshot fence) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return latestSnapshotProjectionLoader.loadAtFence(nameMapping, fence);
    }

    public PaimonSnapshotCacheValue loadSnapshotAtFence(
            ExternalTable dorisTable, Table effectiveTable, PaimonSnapshot fence) {
        return latestSnapshotProjectionLoader.loadEffectiveAtFence(
                dorisTable.getOrBuildNameMapping(), effectiveTable, fence);
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(NameMapping nameMapping, long schemaId) {
        SchemaCacheValue schemaCacheValue = schemaEntry.get(nameMapping.getCtlId())
                .get(new PaimonSchemaCacheKey(nameMapping, schemaId));
        return (PaimonSchemaCacheValue) schemaCacheValue;
    }

    private PaimonTableCacheValue loadTableCacheValue(NameMapping nameMapping) {
        Table paimonTable = tableLoader.load(nameMapping);
        PaimonSnapshotCacheValue fence = latestSnapshotProjectionLoader.loadFence(nameMapping, paimonTable);
        return new PaimonTableCacheValue(paimonTable, fence);
    }

    private SchemaCacheValue loadSchemaCacheValue(PaimonSchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load paimon schema cache value for: %s.%s.%s, schemaId: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getSchemaId()));
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
