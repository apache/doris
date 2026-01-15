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
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class DorisExternalMetaCacheMgr {
    private static final Logger LOG = LogManager.getLogger(DorisExternalMetaCacheMgr.class);
    private final LoadingCache<Long, ImmutableMap<Long, Backend>> backendsCache;

    public DorisExternalMetaCacheMgr(ExecutorService executor) {
        CacheFactory cacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                20,
                true,
                null);
        backendsCache = cacheFactory.buildCache(key -> loadBackends(key), executor);
    }

    private ImmutableMap<Long, Backend> loadBackends(Long catalogId) {
        RemoteDorisExternalCatalog catalog = (RemoteDorisExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(catalogId);
        List<Backend> backends = catalog.getFeServiceClient().listBackends();
        if (LOG.isDebugEnabled()) {
            List<String> names = backends.stream().map(b -> b.getAddress()).collect(Collectors.toList());
            LOG.debug("load backends:{} from:{}", String.join(",", names), catalog.getName());
        }
        Map<Long, Backend> backendMap = Maps.newHashMap();
        backends.forEach(backend -> backendMap.put(backend.getId(), backend));
        return ImmutableMap.copyOf(backendMap);
    }

    public void removeCache(long catalogId) {
        backendsCache.invalidate(catalogId);
    }

    public void invalidateBackendCache(long catalogId) {
        backendsCache.invalidate(catalogId);
    }

    public void invalidateCatalogCache(long catalogId) {
        invalidateBackendCache(catalogId);
    }

    public ImmutableMap<Long, Backend> getBackends(long catalogId) {
        ImmutableMap<Long, Backend> backends = backendsCache.get(catalogId);
        if (backends == null) {
            return ImmutableMap.of();
        }
        return backends;
    }
}
