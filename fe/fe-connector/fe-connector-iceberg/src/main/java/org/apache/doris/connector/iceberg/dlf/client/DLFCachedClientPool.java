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

package org.apache.doris.connector.iceberg.dlf.client;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Catalog-scoped cache of DLF client pools. */
public class DLFCachedClientPool implements ClientPool<IMetaStoreClient, TException>, AutoCloseable {

    private Cache<String, DLFClientPool> clientPoolCache;
    private final Configuration conf;
    private final String catalogId;
    private final int clientPoolSize;
    private final long evictionInterval;

    public DLFCachedClientPool(Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        // DLF clients are configuration-bound; preserving the catalog id in the cache key prevents cross-catalog reuse.
        this.catalogId = conf.get(DataLakeConfig.CATALOG_ID, "");
        this.clientPoolSize = PropertyUtil.propertyAsInt(properties, CatalogProperties.CLIENT_POOL_SIZE,
                CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval = PropertyUtil.propertyAsLong(properties,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        initializeClientPoolCache();
    }

    private void initializeClientPoolCache() {
        clientPoolCache = Caffeine.newBuilder()
                .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                .executor(Runnable::run)
                .removalListener((key, value, cause) -> ((DLFClientPool) value).close())
                .build();
    }

    protected DLFClientPool clientPool() {
        return clientPoolCache.get(catalogId, key -> new DLFClientPool(clientPoolSize, conf));
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
        return clientPool().run(action);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
            throws TException, InterruptedException {
        return clientPool().run(action, retry);
    }

    @Override
    public void close() {
        // Synchronous removal guarantees all configuration-bound clients are released before catalog close returns.
        clientPoolCache.invalidateAll();
        clientPoolCache.cleanUp();
    }
}
