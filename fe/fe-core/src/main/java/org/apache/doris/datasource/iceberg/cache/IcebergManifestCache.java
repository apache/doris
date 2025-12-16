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

import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * A lightweight manifest cache that stores parsed DataFile/DeleteFile lists per manifest.
 */
public class IcebergManifestCache {
    private static final Logger LOG = LogManager.getLogger(IcebergManifestCache.class);

    private final LoadingCache<ManifestCacheKey, ManifestCacheValue> cache;

    public IcebergManifestCache() {
        long capacityInBytes = Config.iceberg_manifest_cache_capacity_mb * 1024L * 1024L;
        Weigher<ManifestCacheKey, ManifestCacheValue> weigher = (key, value) -> {
            long weight = Optional.ofNullable(value).map(ManifestCacheValue::getWeightBytes).orElse(0L);
            if (weight > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) weight;
        };
        Caffeine<ManifestCacheKey, ManifestCacheValue> builder = Caffeine.newBuilder()
                .maximumWeight(capacityInBytes)
                .weigher(weigher);
        if (Config.iceberg_manifest_cache_ttl_ms > 0) {
            builder = builder.expireAfterAccess(Duration.ofMillis(Config.iceberg_manifest_cache_ttl_ms));
        }
        cache = builder.build(new CacheLoader<ManifestCacheKey, ManifestCacheValue>() {
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

    public void invalidateByPath(String path) {
        cache.asMap().keySet().stream()
                .filter(key -> key.getPath().equals(path))
                .forEach(cache::invalidate);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public ManifestCacheKey buildKey(String path) {
        return new ManifestCacheKey(path);
    }
}
