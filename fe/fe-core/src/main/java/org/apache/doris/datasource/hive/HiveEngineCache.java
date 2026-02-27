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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.CacheModule;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.EngineMetaCache;
import org.apache.doris.datasource.metacache.EnginePartitionInfo;
import org.apache.doris.datasource.metacache.EngineSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-level cache adapter for Hive.
 *
 * <p>Wraps the existing {@link HiveMetaStoreCache} into the unified
 * {@link EngineMetaCache} framework. Engine-specific operations like
 * incremental partition management ({@code addPartitionsCache},
 * {@code dropPartitionsCache}, {@code refreshAffectedPartitions}) and
 * file listing ({@code getFilesByPartitions}) remain accessible through
 * {@link #getMetaStoreCache()}.
 */
public class HiveEngineCache implements EngineMetaCache {
    public static final String ENGINE_TYPE = "hive";
    private static final String PARTITION_VALUES_MODULE_NAME = "partition-values";
    private static final String PARTITION_MODULE_NAME = "partition";
    private static final String FILE_MODULE_NAME = "file";

    private final HiveMetaStoreCache metaStoreCache;
    private List<CacheModule> cacheModules = ImmutableList.of();

    public HiveEngineCache(HiveMetaStoreCache metaStoreCache) {
        this.metaStoreCache = Objects.requireNonNull(metaStoreCache, "metaStoreCache cannot be null");
    }

    @Override
    public String getEngineType() {
        return ENGINE_TYPE;
    }

    @Override
    public void init(ExternalCatalog catalog) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        this.cacheModules = ImmutableList.of(
                new HivePartitionValuesModule(nonNullCatalog, metaStoreCache),
                new HivePartitionModule(nonNullCatalog, metaStoreCache),
                new HiveFileModule(nonNullCatalog, metaStoreCache));
    }

    @Override
    public List<CacheModule> getCacheModules() {
        return cacheModules;
    }

    @Override
    public void invalidateTable(ExternalTable table) {
        // Use HiveMetaStoreCache's table invalidation which requires NameMapping
        metaStoreCache.invalidateTableCache(table.getOrBuildNameMapping());
    }

    @Override
    public EnginePartitionInfo getPartitionInfo(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        HMSExternalTable hmsTable = asHiveTable(table);
        return new HivePartition(hmsTable.getHivePartitionValues(snapshot));
    }

    @Override
    public EngineSnapshot getSnapshot(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        HMSExternalTable hmsTable = asHiveTable(table);
        if (hmsTable.getPartitionType(snapshot) == PartitionType.UNPARTITIONED) {
            return new HiveSnapshotMeta(hmsTable.getName(), hmsTable.getLastDdlTime());
        }
        HiveMetaStoreCache.HivePartitionValues partitionValues = hmsTable.getHivePartitionValues(snapshot);
        List<org.apache.doris.datasource.hive.HivePartition> partitions = metaStoreCache.getAllPartitionsWithCache(
                hmsTable, new ArrayList<>(partitionValues.getPartitionValuesMap().values()));
        if (CollectionUtils.isEmpty(partitions)) {
            return new HiveSnapshotMeta(hmsTable.getName(), 0L);
        }
        org.apache.doris.datasource.hive.HivePartition maxPartition = null;
        long maxTimestamp = 0L;
        List<Column> partitionColumns = hmsTable.getPartitionColumns(snapshot);
        for (org.apache.doris.datasource.hive.HivePartition hivePartition : partitions) {
            long currentTimestamp = hivePartition.getLastModifiedTime();
            if (currentTimestamp > maxTimestamp) {
                maxTimestamp = currentTimestamp;
                maxPartition = hivePartition;
            }
        }
        if (maxPartition == null) {
            return new HiveSnapshotMeta(hmsTable.getName(), 0L);
        }
        return new HiveSnapshotMeta(maxPartition.getPartitionName(partitionColumns), maxTimestamp);
    }

    @Override
    public SchemaCacheValue getSchema(ExternalTable table, long schemaVersion) {
        throw new UnsupportedOperationException("Hive does not support getSchema via engine cache");
    }

    public HiveMetaStoreCache getMetaStoreCache() {
        return metaStoreCache;
    }

    private HMSExternalTable asHiveTable(ExternalTable table) {
        if (!(table instanceof HMSExternalTable)) {
            throw new IllegalArgumentException("Expected HMSExternalTable for hive engine cache, but got "
                    + table.getClass().getSimpleName());
        }
        HMSExternalTable hmsTable = (HMSExternalTable) table;
        if (hmsTable.getDlaType() != HMSExternalTable.DLAType.HIVE) {
            throw new IllegalArgumentException("Expected HIVE DLA type for hive engine cache, but got "
                    + hmsTable.getDlaType() + ", table=" + hmsTable.getNameWithFullQualifiers());
        }
        return hmsTable;
    }

    public static final class HivePartition implements EnginePartitionInfo {
        private final HiveMetaStoreCache.HivePartitionValues partitionValues;

        public HivePartition(HiveMetaStoreCache.HivePartitionValues partitionValues) {
            this.partitionValues = partitionValues;
        }

        public HiveMetaStoreCache.HivePartitionValues getPartitionValues() {
            return partitionValues;
        }
    }

    public static final class HiveSnapshotMeta implements EngineSnapshot {
        private final String partitionName;
        private final long timestamp;

        public HiveSnapshotMeta(String partitionName, long timestamp) {
            this.partitionName = partitionName;
            this.timestamp = timestamp;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private static class HivePartitionValuesModule implements CacheModule {
        private final HiveMetaStoreCache metaStoreCache;
        private final CacheSpec cacheSpec;

        HivePartitionValuesModule(ExternalCatalog catalog, HiveMetaStoreCache metaStoreCache) {
            this.metaStoreCache = metaStoreCache;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, PARTITION_VALUES_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_hive_partition_table_cache_num);
        }

        @Override
        public String getName() {
            return PARTITION_VALUES_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            metaStoreCache.invalidateAll();
        }

        @Override
        public void invalidateDb(String dbName) {
            metaStoreCache.invalidateDbCache(dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            // Table-level invalidation is handled by HiveEngineCache.invalidateTable()
            // which uses the proper NameMapping. This is a no-op to avoid double invalidation.
        }

        @Override
        public CacheStats getStats() {
            return metaStoreCache.getPartitionValuesCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metaStoreCache.getPartitionValuesCacheEstimatedSize();
        }
    }

    private static class HivePartitionModule implements CacheModule {
        private final HiveMetaStoreCache metaStoreCache;
        private final CacheSpec cacheSpec;

        HivePartitionModule(ExternalCatalog catalog, HiveMetaStoreCache metaStoreCache) {
            this.metaStoreCache = metaStoreCache;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, PARTITION_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_hive_partition_cache_num);
        }

        @Override
        public String getName() {
            return PARTITION_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            // Already handled by partition-values module's invalidateAll()
        }

        @Override
        public void invalidateDb(String dbName) {
            // Already handled by partition-values module's invalidateDb()
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            // Table-level invalidation is handled by HiveEngineCache.invalidateTable()
        }

        @Override
        public CacheStats getStats() {
            return metaStoreCache.getPartitionCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metaStoreCache.getPartitionCacheEstimatedSize();
        }
    }

    private static class HiveFileModule implements CacheModule {
        private final HiveMetaStoreCache metaStoreCache;
        private final CacheSpec cacheSpec;

        HiveFileModule(ExternalCatalog catalog, HiveMetaStoreCache metaStoreCache) {
            this.metaStoreCache = metaStoreCache;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, FILE_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_external_file_cache_num);
        }

        @Override
        public String getName() {
            return FILE_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            // Already handled by partition-values module's invalidateAll()
        }

        @Override
        public void invalidateDb(String dbName) {
            // Already handled by partition-values module's invalidateDb()
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            // Table-level invalidation is handled by HiveEngineCache.invalidateTable()
        }

        @Override
        public CacheStats getStats() {
            return metaStoreCache.getFileCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metaStoreCache.getFileCacheEstimatedSize();
        }
    }
}
