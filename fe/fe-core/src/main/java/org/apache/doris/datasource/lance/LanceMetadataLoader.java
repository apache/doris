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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Loads one fixed Lance dataset snapshot through the Lance Java SDK. */
public final class LanceMetadataLoader {
    private static final long ALLOCATOR_LIMIT = 256L * 1024 * 1024;

    private LanceMetadataLoader() {
    }

    /**
     * Loads the latest metadata for a directly addressed dataset and owns a short-lived allocator.
     *
     * <p>Called by {@link org.apache.doris.tablefunction.S3TableValuedFunction} when reading a
     * Lance dataset through an S3 TVF.
     */
    public static LanceTableMetadata loadLatestForTvf(
            String datasetUri, Map<String, String> backendProperties)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            return loadLatest(datasetUri, LanceStorageOptions.toLanceOptions(backendProperties), allocator);
        }
    }

    /**
     * Loads the latest metadata with the allocator and storage options owned by a catalog.
     *
     * <p>Called by
     * {@link LanceExternalCatalog#loadTableMetadata(String, String, java.util.Optional)} when no
     * time-travel version is requested. Schema, version, and fragments are read from the same
     * opened dataset snapshot.
     */
    public static LanceTableMetadata loadLatest(String datasetUri, Map<String, String> lanceStorageOptions,
            BufferAllocator allocator) throws Exception {
        return loadInternal(datasetUri, lanceStorageOptions, OptionalLong.empty(), allocator);
    }

    /**
     * Loads metadata from an explicitly selected Lance version.
     *
     * <p>Called by
     * {@link LanceExternalCatalog#loadTableMetadata(String, String, java.util.Optional)} for both
     * {@code FOR VERSION AS OF} and the version resolved from {@code FOR TIME AS OF}.
     */
    public static LanceTableMetadata loadVersion(String datasetUri, Map<String, String> lanceStorageOptions,
            long version, BufferAllocator allocator) throws Exception {
        return loadInternal(datasetUri, lanceStorageOptions, OptionalLong.of(version), allocator);
    }

    /** Shared implementation for the latest-version and explicit-version public entry points. */
    private static LanceTableMetadata loadInternal(String datasetUri, Map<String, String> lanceStorageOptions,
            OptionalLong version, BufferAllocator allocator) throws Exception {
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(LanceReadOptions.build(lanceStorageOptions, version)).build()) {
            long resolvedVersion = dataset.version();
            List<LanceTableMetadata.LanceFragmentInfo> fragments = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                fragments.add(new LanceTableMetadata.LanceFragmentInfo(
                        fragment.getId(), fragment.metadata().getNumRows(),
                        fragment.metadata().getPhysicalRows()));
            }
            return new LanceTableMetadata(datasetUri, resolvedVersion, dataset.getSchema(), fragments,
                    lanceStorageOptions);
        }
    }
}
