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
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.UnifiedCacheModuleKey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MaxComputeMetadataCache {
    private static final UnifiedCacheModuleKey PARTITION_VALUES_MODULE_KEY =
            UnifiedCacheModuleKey.of(MaxComputeEngineCache.ENGINE_TYPE, "partition-values");
    private final Cache<MaxComputeCacheKey, TablePartitionValues> partitionValuesCache;

    public MaxComputeMetadataCache(CacheSpec cacheSpec) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(cacheSpec.getCapacity())
                .recordStats();
        if (cacheSpec.getTtlSecond() != CacheSpec.CACHE_NO_TTL) {
            builder.expireAfterAccess(Math.max(cacheSpec.getTtlSecond(), CacheSpec.CACHE_TTL_DISABLE_CACHE),
                    TimeUnit.SECONDS);
        }
        partitionValuesCache = builder.build();
    }

    public MaxComputeMetadataCache() {
        this(PARTITION_VALUES_MODULE_KEY.toCacheSpec(java.util.Collections.emptyMap(),
                true,
                Config.external_cache_expire_time_seconds_after_access, Config.max_hive_partition_cache_num));
    }

    public static MaxComputeMetadataCache fromCatalog(ExternalCatalog catalog) {
        Objects.requireNonNull(catalog, "catalog cannot be null");
        CacheSpec cacheSpec = PARTITION_VALUES_MODULE_KEY.toCacheSpec(catalog.getProperties(),
                true,
                Config.external_cache_expire_time_seconds_after_access, Config.max_hive_partition_cache_num);
        return new MaxComputeMetadataCache(cacheSpec);
    }

    public TablePartitionValues getCachedPartitionValues(MaxComputeCacheKey tablePartitionKey,
            Function<? super MaxComputeCacheKey, ? extends TablePartitionValues> loader) {
        return partitionValuesCache.get(tablePartitionKey, loader);
    }

    public void cleanUp() {
        partitionValuesCache.invalidateAll();
    }

    public void cleanDatabaseCache(String dbName) {
        List<MaxComputeCacheKey> removeCacheList = partitionValuesCache.asMap().keySet()
                .stream()
                .filter(k -> k.getDbName().equalsIgnoreCase(dbName))
                .collect(Collectors.toList());
        partitionValuesCache.invalidateAll(removeCacheList);
    }

    public void cleanTableCache(String dbName, String tblName) {
        MaxComputeCacheKey cacheKey = new MaxComputeCacheKey(dbName, tblName);
        partitionValuesCache.invalidate(cacheKey);
    }

    public CacheStats getPartitionValuesCacheStats() {
        return partitionValuesCache.stats();
    }

    public long getPartitionValuesCacheEstimatedSize() {
        return partitionValuesCache.estimatedSize();
    }
}
