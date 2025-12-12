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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Cache for Iceberg Manifest files.
 *
 * This cache stores DataFile and DeleteFile objects from Manifest files
 * to avoid repeated I/O when querying the same Manifest files.
 */
public class IcebergManifestCache {
    private static final Logger LOG = LogManager.getLogger(IcebergManifestCache.class);

    // DataFile cache: Key is Manifest file path, Value is DataFile set
    private final Cache<String, Set<DataFile>> dataFileCache;

    // DeleteFile cache: Key is Manifest file path, Value is DeleteFile set
    private final Cache<String, Set<DeleteFile>> deleteFileCache;

    // Table to manifest mapping, for faster cache invalidation
    private final ConcurrentHashMap<String, Set<String>> tableToManifestMap;

    public IcebergManifestCache(ExecutorService executor) {
        if (!Config.enable_iceberg_manifest_cache) {
            this.dataFileCache = null;
            this.deleteFileCache = null;
            this.tableToManifestMap = null;
            LOG.info("Iceberg manifest cache is disabled.");
            return;
        }

        this.tableToManifestMap = new ConcurrentHashMap<>();

        // Calculate cache size limits (based on JVM max memory ratio)
        long maxMemory = Runtime.getRuntime().maxMemory();
        long dataFileCacheSize = Math.round(maxMemory * Config.iceberg_data_file_cache_memory_usage_ratio);
        long deleteFileCacheSize = Math.round(maxMemory * Config.iceberg_delete_file_cache_memory_usage_ratio);

        // Initialize DataFile cache
        this.dataFileCache = Caffeine
                .newBuilder()
                .executor(executor)
                .expireAfterWrite(Config.iceberg_manifest_cache_ttl_sec, TimeUnit.SECONDS)
                .weigher((String key, Set<DataFile> files) -> {
                    long size = estimateSize(key);
                    if (files != null && !files.isEmpty()) {
                        size += estimateDataFileSize(files.iterator().next()) * files.size();
                    }
                    return (int) Math.min(size, Integer.MAX_VALUE);
                })
                .maximumWeight(dataFileCacheSize)
                .recordStats()
                .build();

        // Initialize DeleteFile cache
        this.deleteFileCache = Caffeine
                .newBuilder()
                .executor(executor)
                .expireAfterWrite(Config.iceberg_manifest_cache_ttl_sec, TimeUnit.SECONDS)
                .weigher((String key, Set<DeleteFile> files) -> {
                    long size = estimateSize(key);
                    if (files != null && !files.isEmpty()) {
                        size += estimateDeleteFileSize(files.iterator().next()) * files.size();
                    }
                    return (int) Math.min(size, Integer.MAX_VALUE);
                })
                .maximumWeight(deleteFileCacheSize)
                .recordStats()
                .build();
    }

    /**
     * Get DataFiles from cache for a manifest path
     */
    public Set<DataFile> getDataFiles(String manifestPath) {
        if (dataFileCache == null) {
            return null;
        }
        return dataFileCache.getIfPresent(manifestPath);
    }

    /**
     * Get DeleteFiles from cache for a manifest path
     */
    public Set<DeleteFile> getDeleteFiles(String manifestPath) {
        if (deleteFileCache == null) {
            return null;
        }
        return deleteFileCache.getIfPresent(manifestPath);
    }

    /**
     * Put DataFiles into cache
     */
    public void putDataFiles(String manifestPath, Set<DataFile> dataFiles) {
        if (dataFileCache == null || dataFiles == null || dataFiles.isEmpty()) {
            return;
        }
        dataFileCache.put(manifestPath, Sets.newHashSet(dataFiles));
    }

    /**
     * Put DeleteFiles into cache
     */
    public void putDeleteFiles(String manifestPath, Set<DeleteFile> deleteFiles) {
        if (deleteFileCache == null || deleteFiles == null || deleteFiles.isEmpty()) {
            return;
        }
        deleteFileCache.put(manifestPath, Sets.newHashSet(deleteFiles));
    }

    /**
     * Prepare cache for a manifest path (register table mapping)
     */
    public void prepareCache(String manifestPath, String tableName) {
        tableToManifestMap.computeIfAbsent(tableName, k -> ConcurrentHashMap.newKeySet()).add(manifestPath);
    }

    /**
     * Invalidate cache for a table
     */
    public void invalidateTableCache(String tableName) {
        if (dataFileCache == null || deleteFileCache == null) {
            return;
        }

        Set<String> manifestPaths = tableToManifestMap.remove(tableName);
        if (manifestPaths == null || manifestPaths.isEmpty()) {
            return;
        }

        for (String manifestPath : manifestPaths) {
            dataFileCache.invalidate(manifestPath);
            deleteFileCache.invalidate(manifestPath);
        }
    }

    /**
     * Estimate size of a string key
     */
    private long estimateSize(String key) {
        return key != null ? key.length() * 2L : 0;
    }

    /**
     * Estimate size of a DataFile
     */
    private long estimateDataFileSize(DataFile dataFile) {
        long size = 0;
        if (dataFile.path() != null) {
            size += dataFile.path().toString().length() * 2L;
        }
        size += 200; // Base object overhead
        if (Config.iceberg_data_file_cache_with_metrics && dataFile.valueCounts() != null) {
            size += dataFile.valueCounts().size() * 8L;
        }
        return size;
    }

    /**
     * Estimate size of a DeleteFile
     */
    private long estimateDeleteFileSize(DeleteFile deleteFile) {
        long size = 0;
        if (deleteFile.path() != null) {
            size += deleteFile.path().toString().length() * 2L;
        }
        size += 150; // Base object overhead
        return size;
    }

    /**
     * Get cache statistics
     */
    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> stats = new HashMap<>();

        if (dataFileCache != null) {
            stats.put("data_file_cache", ExternalMetaCacheMgr.getCacheStats(
                    dataFileCache.stats(), dataFileCache.estimatedSize()));
        }

        if (deleteFileCache != null) {
            stats.put("delete_file_cache", ExternalMetaCacheMgr.getCacheStats(
                    deleteFileCache.stats(), deleteFileCache.estimatedSize()));
        }

        return stats;
    }
}
