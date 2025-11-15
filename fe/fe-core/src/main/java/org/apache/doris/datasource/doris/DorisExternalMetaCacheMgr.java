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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class DorisExternalMetaCacheMgr {
    private final LoadingCache<Long, ImmutableMap<Long, Backend>> backendsCache;

    public DorisExternalMetaCacheMgr(ExecutorService executor) {
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        backendsCache = cacheFactory.buildCache(key -> loadBackends(key), null, executor);
    }

    private ImmutableMap<Long, Backend> loadBackends(Long catalogId) {
        RemoteDorisExternalCatalog catalog = (RemoteDorisExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(catalogId);
        return catalog.loadBackends();
    }

    public void removeCache(long catalogId) {
        backendsCache.invalidate(catalogId);
    }

    public void invalidateCatalogCache(long catalogId) {
        backendsCache.invalidate(catalogId);
    }

    public ImmutableMap<Long, Backend> getBackends(long catalogId) {
        ImmutableMap<Long, Backend> backends = backendsCache.get(catalogId);
        if (backends == null) {
            return ImmutableMap.of();
        }
        return backends;
    }
}
