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

package org.apache.doris.datasource.maxcompute;

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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-level cache adapter for MaxCompute.
 */
public class MaxComputeEngineCache implements EngineMetaCache {
    public static final String ENGINE_TYPE = "maxcompute";
    private static final String PARTITION_VALUES_MODULE_NAME = "partition-values";

    private final MaxComputeMetadataCache metadataCache;
    private List<CacheModule> cacheModules = ImmutableList.of();

    public MaxComputeEngineCache(MaxComputeMetadataCache metadataCache) {
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache cannot be null");
    }

    @Override
    public String getEngineType() {
        return ENGINE_TYPE;
    }

    @Override
    public void init(ExternalCatalog catalog) {
        ExternalCatalog nonNullCatalog = Objects.requireNonNull(catalog, "catalog cannot be null");
        this.cacheModules = ImmutableList.of(new MaxComputePartitionValuesModule(nonNullCatalog, metadataCache));
    }

    @Override
    public List<CacheModule> getCacheModules() {
        return cacheModules;
    }

    @Override
    public EnginePartitionInfo getPartitionInfo(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        throw new UnsupportedOperationException("MaxCompute does not support getPartitionInfo via engine cache");
    }

    @Override
    public EngineSnapshot getSnapshot(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        throw new UnsupportedOperationException("MaxCompute does not support getSnapshot via engine cache");
    }

    @Override
    public SchemaCacheValue getSchema(ExternalTable table, long schemaVersion) {
        throw new UnsupportedOperationException("MaxCompute does not support getSchema via engine cache");
    }

    public MaxComputeMetadataCache getMetadataCache() {
        return metadataCache;
    }

    private static class MaxComputePartitionValuesModule implements CacheModule {
        private final MaxComputeMetadataCache metadataCache;
        private final CacheSpec cacheSpec;

        MaxComputePartitionValuesModule(ExternalCatalog catalog, MaxComputeMetadataCache metadataCache) {
            this.metadataCache = metadataCache;
            this.cacheSpec = CacheSpec.fromUnifiedProperties(catalog.getProperties(),
                    ENGINE_TYPE, PARTITION_VALUES_MODULE_NAME, true,
                    Config.external_cache_expire_time_seconds_after_access, Config.max_hive_partition_cache_num);
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
            metadataCache.cleanUp();
        }

        @Override
        public void invalidateDb(String dbName) {
            metadataCache.cleanDatabaseCache(dbName);
        }

        @Override
        public void invalidateTable(String dbName, String tableName) {
            metadataCache.cleanTableCache(dbName, tableName);
        }

        @Override
        public CacheStats getStats() {
            return metadataCache.getPartitionValuesCacheStats();
        }

        @Override
        public long getEstimatedSize() {
            return metadataCache.getPartitionValuesCacheEstimatedSize();
        }
    }
}
