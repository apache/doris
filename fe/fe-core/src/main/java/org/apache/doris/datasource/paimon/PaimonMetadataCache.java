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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.UnifiedCacheModuleKey;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class PaimonMetadataCache {
    private static final UnifiedCacheModuleKey TABLE_CACHE_MODULE_KEY =
            UnifiedCacheModuleKey.of(PaimonEngineCache.ENGINE_TYPE, "table");

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
        return TABLE_CACHE_MODULE_KEY.toCacheSpec(catalog.getProperties(),
                true, Config.external_cache_expire_time_seconds_after_access, Config.max_external_table_cache_num);
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
            PaimonSchemaCacheValue schema = buildSchemaCacheValue(catalog, latestSnapshot.getTable(),
                    latestSnapshot.getSchemaId());
            List<Column> partitionColumns = schema.getPartitionColumns();
            PaimonPartitionInfo partitionInfo = loadPartitionInfo(nameMapping, partitionColumns);
            return new PaimonSnapshotCacheValue(partitionInfo, latestSnapshot, schema);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon snapshot %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                    nameMapping.getLocalTblName(), e.getMessage());
        }
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(ExternalTable dorisTable, long schemaId) {
        PaimonSnapshotCacheValue snapshotCacheValue = getSnapshotCache(dorisTable);
        if (snapshotCacheValue.getSnapshot().getSchemaId() == schemaId) {
            return snapshotCacheValue.getSchema();
        }
        return buildSchemaCacheValue(catalog, getPaimonTable(dorisTable), schemaId);
    }

    static PaimonSchemaCacheValue buildSchemaCacheValue(ExternalCatalog catalog, Table table, long schemaId) {
        DataTable dataTable = (DataTable) table;
        TableSchema tableSchema = dataTable.schemaManager().schema(schemaId);
        if (tableSchema == null) {
            throw new CacheException("failed to load paimon schema with schema id: %s", null, schemaId);
        }
        List<DataField> fields = tableSchema.fields();
        List<Column> schema = Lists.newArrayListWithCapacity(fields.size());
        List<Column> partitionColumns = Lists.newArrayList();
        java.util.Set<String> partitionColumnNames = Sets.newHashSet(tableSchema.partitionKeys());
        for (DataField field : fields) {
            Column column = new Column(field.name().toLowerCase(),
                    PaimonUtil.paimonTypeToDorisType(field.type(),
                            catalog.getEnableMappingVarbinary(),
                            catalog.getEnableMappingTimestampTz()),
                    true, null, true, field.description(), true, -1);
            PaimonUtil.updatePaimonColumnUniqueId(column, field);
            if (field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                column.setWithTZExtraInfo();
            }
            schema.add(column);
            if (partitionColumnNames.contains(field.name())) {
                partitionColumns.add(column);
            }
        }
        return new PaimonSchemaCacheValue(schema, partitionColumns, tableSchema);
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

    public void invalidateTableCacheByLocalName(String dbName, String tableName) {
        tableCache.asMap().keySet().stream()
                .filter(key -> key.getNameMapping().getLocalDbName().equals(dbName)
                        && key.getNameMapping().getLocalTblName().equals(tableName))
                .forEach(tableCache::invalidate);
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

    public CacheStats getTableCacheStats() {
        return tableCache.stats();
    }

    public long getTableCacheEstimatedSize() {
        return tableCache.estimatedSize();
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
