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

package org.apache.doris.datasource.metacache;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.ScopedMetaCache.CacheMetrics;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Catalog scoped entry container.
 */
final class CatalogMetaCacheRuntime {
    private final CatalogMetaCache owner = new CatalogMetaCache();
    private final Map<String, MetaCache<?, ?>> entries = new ConcurrentHashMap<>();

    CatalogMetaCache owner() {
        return owner;
    }

    MetaCache<?, ?> get(String entryName) {
        return entries.get(entryName);
    }

    void put(String entryName, MetaCache<?, ?> entry) {
        entries.put(Objects.requireNonNull(entryName, "entryName"), Objects.requireNonNull(entry, "entry"));
    }

    Map<String, MetaCacheEntryStats> stats() {
        Map<String, MetaCacheEntryStats> result = Maps.newHashMap();
        entries.forEach((name, entry) -> result.put(name, stats(entry)));
        return result;
    }

    void invalidateAll() {
        owner.invalidateCatalog();
    }

    void close() {
        owner.close();
    }

    private MetaCacheEntryStats stats(MetaCache<?, ?> entry) {
        CacheSpec spec = entry.cacheSpec();
        CacheMetrics metrics = entry.metrics();
        long requests = metrics.getRequestCount();
        long loads = metrics.getLoadSuccessCount() + metrics.getLoadFailureCount();
        return new MetaCacheEntryStats(
                spec.isEnable(),
                entry.isEnabled(),
                entry.isAutoRefresh(),
                spec.getTtlSecond(),
                spec.getCapacity(),
                entry.size(),
                requests,
                metrics.getHitCount(),
                metrics.getMissCount(),
                requests == 0L ? 0D : (double) metrics.getHitCount() / requests,
                metrics.getLoadSuccessCount(),
                metrics.getLoadFailureCount(),
                metrics.getTotalLoadTimeNanos(),
                loads == 0L ? 0D : (double) metrics.getTotalLoadTimeNanos() / loads,
                metrics.getEvictionCount(),
                metrics.getInvalidateCount(),
                metrics.getLastLoadSuccessTimeMs(),
                metrics.getLastLoadFailureTimeMs(),
                metrics.getLastError());
    }
}
