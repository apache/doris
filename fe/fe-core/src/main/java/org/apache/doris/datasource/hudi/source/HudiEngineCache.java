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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hudi.HudiMvccSnapshot;
import org.apache.doris.datasource.hudi.HudiUtils;
import org.apache.doris.datasource.metacache.CacheModule;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.EngineMetaCache;
import org.apache.doris.datasource.metacache.EnginePartitionInfo;
import org.apache.doris.datasource.metacache.EngineSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-level cache adapter for Hudi.
 *
 * <p>Wraps the three Hudi cache processors into the unified
 * {@link EngineMetaCache} framework. Engine-specific operations
 * remain accessible through the individual processor getters.
 */
public class HudiEngineCache implements EngineMetaCache {
    public static final String ENGINE_TYPE = "hudi";
    private static final String PARTITION_MODULE_NAME = "partition";
    private static final String FS_VIEW_MODULE_NAME = "fs-view";
    private static final String META_CLIENT_MODULE_NAME = "meta-client";

    private final HudiCachedPartitionProcessor partitionProcessor;
    private final HudiCachedFsViewProcessor fsViewProcessor;
    private final HudiCachedMetaClientProcessor metaClientProcessor;
    private List<CacheModule> cacheModules = ImmutableList.of();

    public HudiEngineCache(HudiCachedPartitionProcessor partitionProcessor,
            HudiCachedFsViewProcessor fsViewProcessor,
            HudiCachedMetaClientProcessor metaClientProcessor) {
        this.partitionProcessor = Objects.requireNonNull(partitionProcessor, "partitionProcessor cannot be null");
        this.fsViewProcessor = Objects.requireNonNull(fsViewProcessor, "fsViewProcessor cannot be null");
        this.metaClientProcessor = Objects.requireNonNull(metaClientProcessor, "metaClientProcessor cannot be null");
    }

    @Override
    public String getEngineType() {
        return ENGINE_TYPE;
    }

    @Override
    public void init(ExternalCatalog catalog) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        this.cacheModules = ImmutableList.of(
                new HudiPartitionModule(nonNullCatalog, partitionProcessor),
                new HudiFsViewModule(nonNullCatalog, fsViewProcessor),
                new HudiMetaClientModule(nonNullCatalog, metaClientProcessor));
    }

    @Override
    public List<CacheModule> getCacheModules() {
        return cacheModules;
    }

    @Override
    public EnginePartitionInfo getPartitionInfo(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        HMSExternalTable hmsTable = asHudiTable(table);
        return new HudiPartition(getPartitionValues(hmsTable, snapshot));
    }

    @Override
    public EngineSnapshot getSnapshot(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        HMSExternalTable hmsTable = asHudiTable(table);
        if (snapshot.isPresent() && snapshot.get() instanceof HudiMvccSnapshot) {
            return new HudiSnapshotMeta(((HudiMvccSnapshot) snapshot.get()).getTimestamp());
        }
        return new HudiSnapshotMeta(HudiUtils.getLastTimeStamp(hmsTable));
    }

    @Override
    public SchemaCacheValue getSchema(ExternalTable table, long schemaVersion) {
        throw new UnsupportedOperationException("Hudi does not support getSchema via engine cache");
    }

    public HudiCachedPartitionProcessor getPartitionProcessor() {
        return partitionProcessor;
    }

    public HudiCachedFsViewProcessor getFsViewProcessor() {
        return fsViewProcessor;
    }

    public HudiCachedMetaClientProcessor getMetaClientProcessor() {
        return metaClientProcessor;
    }

    private HMSExternalTable asHudiTable(ExternalTable table) {
        if (!(table instanceof HMSExternalTable)) {
            throw new IllegalArgumentException("Expected HMSExternalTable for hudi engine cache, but got "
                    + table.getClass().getSimpleName());
        }
        HMSExternalTable hmsTable = (HMSExternalTable) table;
        if (hmsTable.getDlaType() != HMSExternalTable.DLAType.HUDI) {
            throw new IllegalArgumentException("Expected HUDI DLA type for hudi engine cache, but got "
                    + hmsTable.getDlaType() + ", table=" + hmsTable.getNameWithFullQualifiers());
        }
        return hmsTable;
    }

    private TablePartitionValues getPartitionValues(HMSExternalTable table, Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent() && snapshot.get() instanceof HudiMvccSnapshot) {
            return ((HudiMvccSnapshot) snapshot.get()).getTablePartitionValues();
        }
        return HudiUtils.getPartitionValues(Optional.empty(), table);
    }

    public static final class HudiPartition implements EnginePartitionInfo {
        private final TablePartitionValues partitionValues;

        public HudiPartition(TablePartitionValues partitionValues) {
            this.partitionValues = partitionValues;
        }

        public TablePartitionValues getPartitionValues() {
            return partitionValues;
        }
    }

    public static final class HudiSnapshotMeta implements EngineSnapshot {
        private final long timestamp;

        public HudiSnapshotMeta(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private static class HudiPartitionModule implements CacheModule {
        private final HudiCachedPartitionProcessor partitionProcessor;
        private final CacheSpec cacheSpec;

        HudiPartitionModule(ExternalCatalog catalog, HudiCachedPartitionProcessor partitionProcessor) {
            this.partitionProcessor = partitionProcessor;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, PARTITION_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_external_table_cache_num);
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
            partitionProcessor.cleanUp();
        }

        @Override
        public void invalidateDb(String dbName) {
            partitionProcessor.cleanDatabasePartitions(dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            partitionProcessor.cleanTablePartitions(dbName, tableName);
        }

        @Override
        public CacheStats getStats() {
            return partitionProcessor.getPartitionCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return partitionProcessor.getPartitionCacheEstimatedSize();
        }
    }

    private static class HudiFsViewModule implements CacheModule {
        private final HudiCachedFsViewProcessor fsViewProcessor;
        private final CacheSpec cacheSpec;

        HudiFsViewModule(ExternalCatalog catalog, HudiCachedFsViewProcessor fsViewProcessor) {
            this.fsViewProcessor = fsViewProcessor;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, FS_VIEW_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_external_table_cache_num);
        }

        @Override
        public String getName() {
            return FS_VIEW_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            fsViewProcessor.invalidateAll();
        }

        @Override
        public void invalidateDb(String dbName) {
            fsViewProcessor.invalidateDbCache(dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            fsViewProcessor.invalidateTableCache(dbName, tableName);
        }

        @Override
        public CacheStats getStats() {
            return fsViewProcessor.getFsViewCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return fsViewProcessor.getFsViewCacheEstimatedSize();
        }
    }

    private static class HudiMetaClientModule implements CacheModule {
        private final HudiCachedMetaClientProcessor metaClientProcessor;
        private final CacheSpec cacheSpec;

        HudiMetaClientModule(ExternalCatalog catalog, HudiCachedMetaClientProcessor metaClientProcessor) {
            this.metaClientProcessor = metaClientProcessor;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, META_CLIENT_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_external_table_cache_num);
        }

        @Override
        public String getName() {
            return META_CLIENT_MODULE_NAME;
        }

        @Override
        public CacheSpec getSpec() {
            return cacheSpec;
        }

        @Override
        public void invalidateAll() {
            metaClientProcessor.invalidateAll();
        }

        @Override
        public void invalidateDb(String dbName) {
            metaClientProcessor.invalidateDbCache(dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            metaClientProcessor.invalidateTableCache(dbName, tableName);
        }

        @Override
        public CacheStats getStats() {
            return metaClientProcessor.getMetaClientCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metaClientProcessor.getMetaClientCacheEstimatedSize();
        }
    }
}
