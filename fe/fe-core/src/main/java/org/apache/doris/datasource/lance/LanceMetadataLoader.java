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

package org.apache.doris.datasource.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Loads one fixed Lance dataset snapshot through the Lance Java SDK. */
public final class LanceMetadataLoader {
    private static final long ALLOCATOR_LIMIT = 256L * 1024 * 1024;
    private static final long METADATA_CACHE_SIZE = 64L * 1024 * 1024;

    private LanceMetadataLoader() {
    }

    /**
     * Load metadata for a directly-addressed dataset, using Doris backend storage properties.
     * This overload owns a short-lived allocator and is suitable for an S3 TVF.
     */
    public static LanceTableMetadata load(String datasetUri, Map<String, String> backendStorageOptions)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            return load(datasetUri, LanceStorageOptions.forJavaSdk(backendStorageOptions),
                    backendStorageOptions, allocator);
        }
    }

    /**
     * Load metadata with an allocator and Java storage options already owned by a catalog.
     * Schema, version and fragments are all read from the same opened dataset snapshot.
     */
    public static LanceTableMetadata load(String datasetUri, Map<String, String> javaStorageOptions,
            Map<String, String> backendStorageOptions, BufferAllocator allocator) throws Exception {
        return load(datasetUri, javaStorageOptions, backendStorageOptions, OptionalLong.empty(), allocator);
    }

    /** Load metadata from an explicitly selected Lance version. */
    public static LanceTableMetadata load(String datasetUri, Map<String, String> javaStorageOptions,
            Map<String, String> backendStorageOptions, long version, BufferAllocator allocator) throws Exception {
        return load(datasetUri, javaStorageOptions, backendStorageOptions, OptionalLong.of(version), allocator);
    }

    /** Resolve the latest Lance version whose commit time does not exceed the requested timestamp. */
    public static long resolveVersionAtOrBefore(String datasetUri, Map<String, String> javaStorageOptions,
            long timestampMillis, BufferAllocator allocator) throws Exception {
        ReadOptions readOptions = readOptions(javaStorageOptions, OptionalLong.empty());
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(readOptions).build()) {
            return LanceSnapshotResolver.versionAtOrBefore(dataset.listVersions(), timestampMillis);
        }
    }

    private static LanceTableMetadata load(String datasetUri, Map<String, String> javaStorageOptions,
            Map<String, String> backendStorageOptions, OptionalLong version,
            BufferAllocator allocator) throws Exception {
        ReadOptions readOptions = readOptions(javaStorageOptions, version);
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(readOptions).build()) {
            long resolvedVersion = dataset.version();
            List<LanceTableMetadata.LanceFragmentInfo> fragments = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                fragments.add(new LanceTableMetadata.LanceFragmentInfo(
                        fragment.getId(), fragment.metadata().getNumRows()));
            }
            return new LanceTableMetadata(datasetUri, resolvedVersion, dataset.getSchema(), fragments,
                    backendStorageOptions);
        }
    }

    private static ReadOptions readOptions(Map<String, String> javaStorageOptions, OptionalLong version) {
        ReadOptions.Builder builder = new ReadOptions.Builder()
                .setStorageOptions(javaStorageOptions)
                .setIndexCacheSizeBytes(0)
                .setMetadataCacheSizeBytes(METADATA_CACHE_SIZE);
        if (version.isPresent()) {
            builder.setVersion(version.getAsLong());
        }
        return builder.build();
    }
}
