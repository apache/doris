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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class PaimonMetadataCache {

    private final LoadingCache<PaimonSnapshotCacheKey, PaimonSnapshotCacheValue> snapshotCache;

    public PaimonMetadataCache(ExecutorService executor) {
        CacheFactory snapshotCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.snapshotCache = snapshotCacheFactory.buildCache(key -> loadSnapshot(key), null, executor);
    }

    @NotNull
    private PaimonSnapshotCacheValue loadSnapshot(PaimonSnapshotCacheKey key) {
        try {
            PaimonSnapshot latestSnapshot = loadLatestSnapshot(key);
            PaimonExternalTable table = (PaimonExternalTable) key.getCatalog().getDbOrAnalysisException(key.getDbName())
                    .getTableOrAnalysisException(key.getTableName());
            List<Column> partitionColumns = table.getPaimonSchemaCacheValue(latestSnapshot.getSchemaId())
                    .getPartitionColumns();
            PaimonPartitionInfo partitionInfo = loadPartitionInfo(key, partitionColumns);
            return new PaimonSnapshotCacheValue(partitionInfo, latestSnapshot);
        } catch (IOException | AnalysisException e) {
            throw new CacheException("failed to loadSnapshot for: %s.%s.%s",
                    e, key.getCatalog().getName(), key.getDbName(), key.getTableName());
        }
    }

    private PaimonPartitionInfo loadPartitionInfo(PaimonSnapshotCacheKey key, List<Column> partitionColumns)
            throws IOException, AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return new PaimonPartitionInfo();
        }
        List<PaimonPartition> paimonPartitions = loadPartitions(key);
        return PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions);
    }

    private List<PaimonPartition> loadPartitions(PaimonSnapshotCacheKey key)
            throws IOException {
        Table table = ((PaimonExternalCatalog) key.getCatalog()).getPaimonTable(key.getDbName(),
                key.getTableName() + Catalog.SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        List<InternalRow> rows = PaimonUtil.read(table, null, null);
        List<PaimonPartition> res = Lists.newArrayListWithCapacity(rows.size());
        for (InternalRow row : rows) {
            res.add(PaimonUtil.rowToPartition(row));
        }
        return res;
    }

    private PaimonSnapshot loadLatestSnapshot(PaimonSnapshotCacheKey key) throws IOException {
        Table table = ((PaimonExternalCatalog) key.getCatalog()).getPaimonTable(key.getDbName(),
                key.getTableName() + Catalog.SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS);
        // snapshotId and schemaId
        List<InternalRow> rows = PaimonUtil.read(table, new int[][] {{0}, {1}}, null);
        long latestSnapshotId = 0L;
        long latestSchemaId = 0L;
        for (InternalRow row : rows) {
            long snapshotId = row.getLong(0);
            if (snapshotId > latestSnapshotId) {
                latestSnapshotId = snapshotId;
                latestSchemaId = row.getLong(1);
            }
        }
        return new PaimonSnapshot(latestSnapshotId, latestSchemaId);
    }

    public void invalidateCatalogCache(long catalogId) {
        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.getCatalog().getId() == catalogId)
                .forEach(snapshotCache::invalidate);
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.getCatalog().getId() == catalogId && key.getDbName().equals(dbName)
                        && key.getTableName().equals(
                        tblName))
                .forEach(snapshotCache::invalidate);
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        snapshotCache.asMap().keySet().stream()
                .filter(key -> key.getCatalog().getId() == catalogId && key.getDbName().equals(dbName))
                .forEach(snapshotCache::invalidate);
    }

    public PaimonSnapshotCacheValue getPaimonSnapshot(CatalogIf catalog, String dbName, String tbName) {
        PaimonSnapshotCacheKey key = new PaimonSnapshotCacheKey(catalog, dbName, tbName);
        return snapshotCache.get(key);
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("paimon_snapshot_cache", ExternalMetaCacheMgr.getCacheStats(snapshotCache.stats(),
                snapshotCache.estimatedSize()));
        return res;
    }
}
