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

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;

import java.util.function.Consumer;

/**
 * Helper to load manifest content and populate the manifest cache.
 */
public class IcebergManifestCacheLoader {
    private IcebergManifestCacheLoader() {
    }

    public static ManifestCacheValue loadDataFilesWithCache(IcebergExternalMetaCache cache, ExternalTable dorisTable,
            ManifestFile manifest, Table table) {
        return loadDataFilesWithCache(cache, dorisTable, manifest, table, null);
    }

    public static ManifestCacheValue loadDataFilesWithCache(IcebergExternalMetaCache cache, ExternalTable dorisTable,
            ManifestFile manifest, Table table, Consumer<Boolean> cacheHitRecorder) {
        return cache.getManifestCacheValue(dorisTable, manifest, table, cacheHitRecorder);
    }

    public static ManifestCacheValue loadDeleteFilesWithCache(IcebergExternalMetaCache cache,
            ExternalTable dorisTable, ManifestFile manifest, Table table) {
        return loadDeleteFilesWithCache(cache, dorisTable, manifest, table, null);
    }

    public static ManifestCacheValue loadDeleteFilesWithCache(IcebergExternalMetaCache cache,
            ExternalTable dorisTable, ManifestFile manifest, Table table, Consumer<Boolean> cacheHitRecorder) {
        return cache.getManifestCacheValue(dorisTable, manifest, table, cacheHitRecorder);
    }
}
