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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.CacheSpec;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class PaimonMetadataCache {

    private final ExecutorService executor;
    private final ExternalCatalog catalog;
    private LoadingCache<PaimonTableCacheKey, PaimonTableCacheValue> tableCache;

    public PaimonMetadataCache(ExternalCatalog catalog, ExecutorService executor) {
        this.executor = executor;
        this.catalog = catalog;
        init();
    }

    public void init() {
        CacheSpec cacheSpec = resolveTableCacheSpec();
        CacheFactory tableCacheFactory = new CacheFactory(
                CacheSpec.toExpireAfterAccess(cacheSpec.getTtlSecond()),
                OptionalLong.empty(),
                cacheSpec.getCapacity(),
                true,
                null);
        this.tableCache = tableCacheFactory.buildCache(key -> loadTableCacheValue(key), executor);
    }

    private CacheSpec resolveTableCacheSpec() {
        return CacheSpec.fromProperties(catalog.getProperties(),
                PaimonExternalCatalog.PAIMON_TABLE_CACHE_ENABLE, true,
                PaimonExternalCatalog.PAIMON_TABLE_CACHE_TTL_SECOND,
                Config.external_cache_expire_time_seconds_after_access,
                PaimonExternalCatalog.PAIMON_TABLE_CACHE_CAPACITY,
                Config.max_external_table_cache_num);
    }

    @NotNull
    private PaimonTableCacheValue loadTableCacheValue(PaimonTableCacheKey key) {
        NameMapping nameMapping = key.getNameMapping();
        try {
            PaimonExternalCatalog externalCatalog = (PaimonExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrException(nameMapping.getCtlId(),
                            id -> new IOException("Catalog not found: " + id));
            Table table = externalCatalog.getPaimonTable(nameMapping);
            return new PaimonTableCacheValue(table);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon table %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                    nameMapping.getLocalTblName(), e.getMessage());
        }
    }

    @NotNull
    private PaimonSnapshotCacheValue loadSnapshot(ExternalTable dorisTable, Table paimonTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        try {
            PaimonSnapshot latestSnapshot = loadLatestSnapshot(paimonTable, nameMapping);
            List<Column> partitionColumns = getPaimonSchemaCacheValue(nameMapping,
                    latestSnapshot.getSchemaId()).getPartitionColumns();
            PaimonPartitionInfo partitionInfo = loadPartitionInfo(nameMapping, partitionColumns);
            return new PaimonSnapshotCacheValue(partitionInfo, latestSnapshot);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon snapshot %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                    nameMapping.getLocalTblName(), e.getMessage());
        }
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(NameMapping nameMapping, long schemaId) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new CacheException("catalog %s not found when getting paimon schema cache value",
                    null, nameMapping.getCtlId());
        }
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog);
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(
                new PaimonSchemaCacheKey(nameMapping, schemaId));
        if (!schemaCacheValue.isPresent()) {
            throw new CacheException("failed to get paimon schema cache value for: %s.%s.%s with schema id: %s",
                    null, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    schemaId);
        }
        return (PaimonSchemaCacheValue) schemaCacheValue.get();
    }

    private PaimonPartitionInfo loadPartitionInfo(NameMapping nameMapping, List<Column> partitionColumns)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return PaimonPartitionInfo.EMPTY;
        }
        PaimonExternalCatalog externalCatalog = (PaimonExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(nameMapping.getCtlId());
        List<Partition> paimonPartitions = externalCatalog.getPaimonPartitions(nameMapping);
        return PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions);
    }

    private PaimonSnapshot loadLatestSnapshot(Table paimonTable, NameMapping nameMapping) {
        Table snapshotTable = paimonTable;
        // snapshotId and schemaId
        Long latestSnapshotId = PaimonSnapshot.INVALID_SNAPSHOT_ID;
        Optional<Snapshot> optionalSnapshot = paimonTable.latestSnapshot();
        if (optionalSnapshot.isPresent()) {
            latestSnapshotId = optionalSnapshot.get().id();
            snapshotTable = paimonTable.copy(
                    Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), latestSnapshotId.toString()));
        }
        DataTable dataTable = (DataTable) paimonTable;
        long latestSchemaId = dataTable.schemaManager().latest().map(TableSchema::id).orElse(0L);
        return new PaimonSnapshot(latestSnapshotId, latestSchemaId, snapshotTable);
    }

    public Table getPaimonTable(ExternalTable dorisTable) {
        PaimonTableCacheKey key = new PaimonTableCacheKey(dorisTable.getOrBuildNameMapping());
        return tableCache.get(key).getPaimonTable();
    }

    public Table getPaimonTable(PaimonTableCacheKey key) {
        return tableCache.get(key).getPaimonTable();
    }

    public PaimonSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        PaimonTableCacheKey key = new PaimonTableCacheKey(dorisTable.getOrBuildNameMapping());
        PaimonTableCacheValue tableCacheValue = tableCache.get(key);
        return tableCacheValue.getSnapshotCacheValue(() -> loadSnapshot(dorisTable,
                tableCacheValue.getPaimonTable()));
    }

    public void invalidateCatalogCache(long catalogId) {
        tableCache.invalidateAll();
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        PaimonTableCacheKey key = new PaimonTableCacheKey(dorisTable.getOrBuildNameMapping());
        tableCache.invalidate(key);
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        tableCache.asMap().keySet().stream()
                .filter(key -> key.getNameMapping().getLocalDbName().equals(dbName))
                .forEach(tableCache::invalidate);
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("paimon_table_cache", ExternalMetaCacheMgr.getCacheStats(tableCache.stats(),
                tableCache.estimatedSize()));
        return res;
    }

    static class PaimonTableCacheKey {
        private final NameMapping nameMapping;

        public PaimonTableCacheKey(NameMapping nameMapping) {
            this.nameMapping = nameMapping;
        }

        public NameMapping getNameMapping() {
            return nameMapping;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PaimonTableCacheKey that = (PaimonTableCacheKey) o;
            return nameMapping.equals(that.nameMapping);
        }

        @Override
        public int hashCode() {
            return nameMapping.hashCode();
        }
    }
}
