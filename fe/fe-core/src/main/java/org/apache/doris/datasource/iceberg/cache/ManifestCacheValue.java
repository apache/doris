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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;

import java.util.Collections;
import java.util.List;

/**
 * Cached manifest payload containing parsed files and an estimated weight.
 */
public class ManifestCacheValue {
    private final ManifestFile manifestFile;
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;
    private final long weightBytes;

    private ManifestCacheValue(ManifestFile manifestFile, List<DataFile> dataFiles,
            List<DeleteFile> deleteFiles, long weightBytes) {
        this.manifestFile = manifestFile;
        this.dataFiles = dataFiles == null ? Collections.emptyList() : dataFiles;
        this.deleteFiles = deleteFiles == null ? Collections.emptyList() : deleteFiles;
        this.weightBytes = weightBytes;
    }

    public static ManifestCacheValue forDataFiles(ManifestFile manifestFile, List<DataFile> dataFiles) {
        return new ManifestCacheValue(manifestFile, dataFiles, Collections.emptyList(),
                estimateWeight(dataFiles, Collections.emptyList()));
    }

    public static ManifestCacheValue forDeleteFiles(ManifestFile manifestFile, List<DeleteFile> deleteFiles) {
        return new ManifestCacheValue(manifestFile, Collections.emptyList(), deleteFiles,
                estimateWeight(Collections.emptyList(), deleteFiles));
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public List<DeleteFile> getDeleteFiles() {
        return deleteFiles;
    }

    public long getWeightBytes() {
        return weightBytes;
    }

    public ManifestFile getManifestFile() {
        return manifestFile;
    }

    private static long estimateWeight(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        // A coarse weight estimation based on path lengths and fixed object overhead.
        long total = 0;
        for (DataFile file : dataFiles) {
            total += 128L; // base object overhead
            if (file != null && file.path() != null) {
                total += file.path().toString().length();
            }
        }
        for (DeleteFile file : deleteFiles) {
            total += 128L;
            if (file != null && file.path() != null) {
                total += file.path().toString().length();
            }
        }
        return total;
    }
}
