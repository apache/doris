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

package org.apache.doris.datasource.iceberg.cache;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.metacache.CacheSpec;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A lightweight manifest cache that stores parsed DataFile/DeleteFile lists per manifest.
 */
public class IcebergManifestCache {
    private static final Logger LOG = LogManager.getLogger(IcebergManifestCache.class);

    private final LoadingCache<ManifestCacheKey, ManifestCacheValue> cache;
    private final ConcurrentHashMap<String, Set<String>> tableManifestPathIndex = new ConcurrentHashMap<>();

    public IcebergManifestCache(long capacity, long ttlSec) {
        CacheFactory cacheFactory = new CacheFactory(
                CacheSpec.toExpireAfterAccess(ttlSec),
                java.util.OptionalLong.empty(),
                capacity,
                true,
                null);
        cache = cacheFactory.buildCache(new CacheLoader<ManifestCacheKey, ManifestCacheValue>() {
            @Override
            public ManifestCacheValue load(ManifestCacheKey key) {
                throw new CacheException("Manifest cache loader should be provided explicitly for key %s", null, key);
            }
        });
    }

    public ManifestCacheValue get(ManifestCacheKey key, Callable<ManifestCacheValue> loader) {
        try {
            return cache.get(key, ignored -> {
                try {
                    return loader.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new CacheException("Failed to load manifest cache for key %s", e, key);
        }
    }

    public Optional<ManifestCacheValue> peek(ManifestCacheKey key) {
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    public void registerManifestPath(String tableName, String path) {
        String nonNullTableName = Objects.requireNonNull(tableName, "tableName cannot be null");
        String nonNullPath = Objects.requireNonNull(path, "path cannot be null");
        tableManifestPathIndex.computeIfAbsent(nonNullTableName, ignored -> ConcurrentHashMap.newKeySet())
                .add(nonNullPath);
    }

    public void invalidateByPath(String path) {
        String nonNullPath = Objects.requireNonNull(path, "path cannot be null");
        cache.invalidate(buildKey(nonNullPath));
        tableManifestPathIndex.values().forEach(paths -> paths.remove(nonNullPath));
    }

    public void invalidateByTable(String tableName) {
        Set<String> paths = tableManifestPathIndex.remove(
                Objects.requireNonNull(tableName, "tableName cannot be null"));
        if (paths == null || paths.isEmpty()) {
            return;
        }
        for (String path : paths) {
            cache.invalidate(buildKey(path));
        }
    }

    public void invalidateByTablePrefix(String tablePrefix) {
        String nonNullTablePrefix = Objects.requireNonNull(tablePrefix, "tablePrefix cannot be null");
        for (String tableName : new ArrayList<>(tableManifestPathIndex.keySet())) {
            if (tableName.startsWith(nonNullTablePrefix)) {
                invalidateByTable(tableName);
            }
        }
    }

    public void invalidateAll() {
        cache.invalidateAll();
        tableManifestPathIndex.clear();
    }

    public ManifestCacheKey buildKey(String path) {
        return new ManifestCacheKey(path);
    }

    public CacheStats getStats() {
        return cache.stats();
    }

    public long getEstimatedSize() {
        return cache.estimatedSize();
    }
}
