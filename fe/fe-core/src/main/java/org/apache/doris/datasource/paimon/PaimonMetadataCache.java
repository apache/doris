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

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.Partition;
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

    private final LoadingCache<PaimonSnapshotCacheKey, PaimonSnapshotCacheValue> snapshotCache;

    public PaimonMetadataCache(ExecutorService executor) {
        CacheFactory snapshotCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.snapshotCache = snapshotCacheFactory.buildCache(key -> loadSnapshot(key), null, executor);
    }

    @NotNull
    private PaimonSnapshotCacheValue loadSnapshot(PaimonSnapshotCacheKey key) {
        NameMapping nameMapping = key.getNameMapping();
        try {
            PaimonSnapshot latestSnapshot = loadLatestSnapshot(key);
            List<Column> partitionColumns = getPaimonSchemaCacheValue(nameMapping,
                    latestSnapshot.getSchemaId()).getPartitionColumns();
            PaimonPartitionInfo partitionInfo = loadPartitionInfo(key, partitionColumns);
            return new PaimonSnapshotCacheValue(partitionInfo, latestSnapshot);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon snapshot %s.%s.%s or reason: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
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

    private PaimonPartitionInfo loadPartitionInfo(PaimonSnapshotCacheKey key, List<Column> partitionColumns)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return PaimonPartitionInfo.EMPTY;
        }
        NameMapping nameMapping = key.getNameMapping();
        PaimonExternalCatalog externalCatalog = (PaimonExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(nameMapping.getCtlId());
        List<Partition> paimonPartitions = externalCatalog.getPaimonPartitions(nameMapping);
        return PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions);
    }

    private PaimonSnapshot loadLatestSnapshot(PaimonSnapshotCacheKey key) throws IOException {
        NameMapping nameMapping = key.getNameMapping();
        PaimonExternalCatalog externalCatalog = (PaimonExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(nameMapping.getCtlId(), id -> new IOException("Catalog not found: " + id));
        Table table = externalCatalog.getPaimonTable(nameMapping);
        Table snapshotTable = table;
        // snapshotId and schemaId
        Long latestSnapshotId = PaimonSnapshot.INVALID_SNAPSHOT_ID;
        long latestSchemaId = 0L;
        Optional<Snapshot> optionalSnapshot = table.latestSnapshot();
        if (optionalSnapshot.isPresent()) {
            latestSnapshotId = optionalSnapshot.get().id();
            latestSchemaId = table.snapshot(latestSnapshotId).schemaId();
            snapshotTable =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), latestSnapshotId.toString()));
        }
        return new PaimonSnapshot(latestSnapshotId, latestSchemaId, snapshotTable);
    }

    public void invalidateCatalogCache(long catalogId) {
        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.getNameMapping().getCtlId() == catalogId)
                .forEach(snapshotCache::invalidate);
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.getNameMapping().getCtlId() == dorisTable.getCatalog().getId()
                        && key.getNameMapping().getLocalDbName().equals(dorisTable.getDbName())
                        && key.getNameMapping().getLocalTblName().equals(dorisTable.getName()))
                .forEach(snapshotCache::invalidate);
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.getNameMapping().getCtlId() == catalogId
                        && key.getNameMapping().getLocalTblName().equals(dbName))
                .forEach(snapshotCache::invalidate);
    }

    public PaimonSnapshotCacheValue getPaimonSnapshot(ExternalTable dorisTable) {
        PaimonSnapshotCacheKey key = new PaimonSnapshotCacheKey(dorisTable.getOrBuildNameMapping());
        return snapshotCache.get(key);
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("paimon_snapshot_cache", ExternalMetaCacheMgr.getCacheStats(snapshotCache.stats(),
                snapshotCache.estimatedSize()));
        return res;
    }
}
