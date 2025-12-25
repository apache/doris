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

import org.apache.doris.datasource.CacheException;

import com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Helper to load manifest content and populate the manifest cache.
 */
public class IcebergManifestCacheLoader {
    private static final Logger LOG = LogManager.getLogger(IcebergManifestCacheLoader.class);

    private IcebergManifestCacheLoader() {
    }

    public static ManifestCacheValue loadDataFilesWithCache(IcebergManifestCache cache, ManifestFile manifest,
            Table table) {
        return loadDataFilesWithCache(cache, manifest, table, null);
    }

    public static ManifestCacheValue loadDataFilesWithCache(IcebergManifestCache cache, ManifestFile manifest,
            Table table, Consumer<Boolean> cacheHitRecorder) {
        return loadWithCache(cache, manifest, cacheHitRecorder, () -> loadDataFiles(manifest, table));
    }

    public static ManifestCacheValue loadDeleteFilesWithCache(IcebergManifestCache cache, ManifestFile manifest,
            Table table) {
        return loadDeleteFilesWithCache(cache, manifest, table, null);
    }

    public static ManifestCacheValue loadDeleteFilesWithCache(IcebergManifestCache cache, ManifestFile manifest,
            Table table, Consumer<Boolean> cacheHitRecorder) {
        return loadWithCache(cache, manifest, cacheHitRecorder, () -> loadDeleteFiles(manifest, table));
    }

    private static ManifestCacheValue loadDataFiles(ManifestFile manifest, Table table) {
        List<DataFile> dataFiles = Lists.newArrayList();
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
            // ManifestReader implements CloseableIterable<DataFile>, iterate directly
            for (DataFile dataFile : reader) {
                dataFiles.add(dataFile.copy());
            }
        } catch (IOException e) {
            LOG.warn("Failed to read data manifest {}", manifest.path(), e);
            throw new CacheException("Failed to read data manifest %s", e, manifest.path());
        }
        return ManifestCacheValue.forDataFiles(dataFiles);
    }

    private static ManifestCacheValue loadDeleteFiles(ManifestFile manifest, Table table) {
        List<DeleteFile> deleteFiles = Lists.newArrayList();
        try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, table.io(),
                table.specs())) {
            // ManifestReader implements CloseableIterable<DeleteFile>, iterate directly
            for (DeleteFile deleteFile : reader) {
                deleteFiles.add(deleteFile.copy());
            }
        } catch (IOException e) {
            LOG.warn("Failed to read delete manifest {}", manifest.path(), e);
            throw new CacheException("Failed to read delete manifest %s", e, manifest.path());
        }
        return ManifestCacheValue.forDeleteFiles(deleteFiles);
    }

    private static ManifestCacheValue loadWithCache(IcebergManifestCache cache, ManifestFile manifest,
            Consumer<Boolean> cacheHitRecorder, Loader loader) {
        ManifestCacheKey key = buildKey(cache, manifest);
        Optional<ManifestCacheValue> cached = cache.peek(key);
        boolean cacheHit = cached.isPresent();
        if (cacheHitRecorder != null) {
            cacheHitRecorder.accept(cacheHit);
        }
        if (cacheHit) {
            return cached.get();
        }
        return cache.get(key, loader::load);
    }

    @FunctionalInterface
    private interface Loader {
        ManifestCacheValue load();
    }

    private static ManifestCacheKey buildKey(IcebergManifestCache cache, ManifestFile manifest) {
        // Iceberg manifest files are immutable, so path uniquely identifies a manifest
        return cache.buildKey(manifest.path());
    }
}
