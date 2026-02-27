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
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.UnifiedCacheModuleKey;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Per-catalog cache container for Hudi metadata processors.
 */
public final class HudiCatalogCache {
    private static final UnifiedCacheModuleKey PARTITION_MODULE_KEY =
            UnifiedCacheModuleKey.of(HudiEngineCache.ENGINE_TYPE, "partition");
    private static final UnifiedCacheModuleKey FS_VIEW_MODULE_KEY =
            UnifiedCacheModuleKey.of(HudiEngineCache.ENGINE_TYPE, "fs-view");
    private static final UnifiedCacheModuleKey META_CLIENT_MODULE_KEY =
            UnifiedCacheModuleKey.of(HudiEngineCache.ENGINE_TYPE, "meta-client");

    private final HudiCachedPartitionProcessor partitionProcessor;
    private final HudiCachedFsViewProcessor fsViewProcessor;
    private final HudiCachedMetaClientProcessor metaClientProcessor;

    private HudiCatalogCache(HudiCachedPartitionProcessor partitionProcessor,
            HudiCachedFsViewProcessor fsViewProcessor,
            HudiCachedMetaClientProcessor metaClientProcessor) {
        this.partitionProcessor = Objects.requireNonNull(partitionProcessor, "partitionProcessor cannot be null");
        this.fsViewProcessor = Objects.requireNonNull(fsViewProcessor, "fsViewProcessor cannot be null");
        this.metaClientProcessor = Objects.requireNonNull(metaClientProcessor, "metaClientProcessor cannot be null");
    }

    public static HudiCatalogCache fromCatalog(ExternalCatalog catalog, ExecutorService executor) {
        Objects.requireNonNull(catalog, "catalog cannot be null");
        Objects.requireNonNull(executor, "executor cannot be null");
        if (!(catalog instanceof HMSExternalCatalog)) {
            throw new IllegalArgumentException("Hudi only supports hive(or compatible) catalog now");
        }
        HudiCachedPartitionProcessor partitionProcessor = new HudiCachedPartitionProcessor(catalog.getId(), executor,
                resolveCacheSpec(catalog, PARTITION_MODULE_KEY));
        HudiCachedFsViewProcessor fsViewProcessor = new HudiCachedFsViewProcessor(executor,
                resolveCacheSpec(catalog, FS_VIEW_MODULE_KEY));
        HudiCachedMetaClientProcessor metaClientProcessor = new HudiCachedMetaClientProcessor(executor,
                resolveCacheSpec(catalog, META_CLIENT_MODULE_KEY));
        return new HudiCatalogCache(partitionProcessor, fsViewProcessor, metaClientProcessor);
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

    public void cleanUp() {
        partitionProcessor.cleanUp();
        fsViewProcessor.cleanUp();
        metaClientProcessor.cleanUp();
    }

    private static CacheSpec resolveCacheSpec(ExternalCatalog catalog, UnifiedCacheModuleKey moduleKey) {
        return moduleKey.toCacheSpec(catalog.getProperties(),
                true, Config.external_cache_expire_time_seconds_after_access, Config.max_external_table_cache_num);
    }
}
