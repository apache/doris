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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.MetaCacheSizeEstimator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

import java.util.Collections;
import java.util.List;

/**
 * Cached manifest payload containing the parsed files of one iceberg manifest.
 *
 * <p>Ported verbatim from the legacy fe-core {@code org.apache.doris.datasource.iceberg.cache.ManifestCacheValue}
 * (references only iceberg-SDK types, so no fe-core dependency). A DATA manifest yields data files; a DELETES
 * manifest yields delete files (T08, mirrors {@link IcebergManifestCache}).
 */
public class ManifestCacheValue {
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;
    private final MetaCacheSizeEstimate sizeEstimate;

    private ManifestCacheValue(List<DataFile> dataFiles, List<DeleteFile> deleteFiles, boolean estimateWeight) {
        this.dataFiles = dataFiles == null ? Collections.emptyList() : dataFiles;
        this.deleteFiles = deleteFiles == null ? Collections.emptyList() : deleteFiles;
        this.sizeEstimate = estimateWeight
                ? MetaCacheSizeEstimator.estimateSafely("iceberg_manifest_estimator_failure",
                        () -> MetaCacheSizeEstimate.complete(
                                IcebergCacheSizeEstimator.estimateManifestValue(this)))
                : MetaCacheSizeEstimate.complete(0L);
    }

    public static ManifestCacheValue forDataFiles(List<DataFile> dataFiles) {
        return forDataFiles(dataFiles, false);
    }

    static ManifestCacheValue forDataFiles(List<DataFile> dataFiles, boolean estimateWeight) {
        return new ManifestCacheValue(dataFiles, Collections.emptyList(), estimateWeight);
    }

    public static ManifestCacheValue forDeleteFiles(List<DeleteFile> deleteFiles) {
        return forDeleteFiles(deleteFiles, false);
    }

    static ManifestCacheValue forDeleteFiles(List<DeleteFile> deleteFiles, boolean estimateWeight) {
        return new ManifestCacheValue(Collections.emptyList(), deleteFiles, estimateWeight);
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public List<DeleteFile> getDeleteFiles() {
        return deleteFiles;
    }

    MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate;
    }
}
