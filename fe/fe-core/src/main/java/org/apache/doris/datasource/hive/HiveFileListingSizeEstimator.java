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

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheKey;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HiveFileStatus;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import org.apache.hadoop.fs.BlockLocation;

import java.io.IOException;

/** Conservative incremental formula, avoiding object-graph walks and per-file path rescans. */
final class HiveFileListingSizeEstimator {
    private HiveFileListingSizeEstimator() {
    }

    static long baseBytes(FileCacheKey key) {
        long bytes = 256L + stringBytes(key.getLocation()) + stringBytes(key.getInputFormat());
        for (String value : key.getPartitionValues()) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, 16L + 2L * stringBytes(value));
        }
        return bytes;
    }

    static long fileBytes(HiveFileStatus file) throws org.apache.doris.fs.FileSystemIOException {
        try {
            return estimateFileBytes(file);
        } catch (IOException e) {
            throw new org.apache.doris.fs.FileSystemIOException("Failed to size file block locations", e);
        }
    }

    private static long estimateFileBytes(HiveFileStatus file) throws IOException {
        // LocationPath retains schema, normalized path and fsIdentifier; storage properties are shared.
        long bytes = 256L + stringBytes(file.getPath().getSchema())
                + stringBytes(file.getPath().getNormalizedLocation()) + stringBytes(file.getPath().getFsIdentifier());
        BlockLocation[] blocks = file.getBlockLocations();
        if (blocks == null) {
            return bytes;
        }
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.estimatedObjectArrayBytes(blocks.length));
        for (BlockLocation block : blocks) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, 160L);
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, stringsBytes(block.getNames()));
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, stringsBytes(block.getHosts()));
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, stringsBytes(block.getTopologyPaths()));
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, stringsBytes(block.getCachedHosts()));
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, stringsBytes(block.getStorageIds()));
            if (block.getStorageTypes() != null) {
                bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                        MetaCacheWeightUtils.estimatedObjectArrayBytes(block.getStorageTypes().length));
            }
        }
        return bytes;
    }

    private static long stringsBytes(String[] values) {
        if (values == null) {
            return 0L;
        }
        long bytes = MetaCacheWeightUtils.estimatedObjectArrayBytes(values.length);
        for (String value : values) {
            bytes = MetaCacheWeightUtils.saturatedAdd(bytes, stringBytes(value));
        }
        return bytes;
    }

    // UTF-16 upper bound is constant-time and also covers non-compact String JVM configurations.
    private static long stringBytes(String value) {
        return value == null ? 0L : 48L + 2L * value.length();
    }
}
