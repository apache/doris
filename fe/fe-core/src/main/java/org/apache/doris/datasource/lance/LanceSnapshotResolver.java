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
import org.lance.Dataset;
import org.lance.Version;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Resolves Doris time-travel selectors to immutable Lance version IDs. */
final class LanceSnapshotResolver {
    private LanceSnapshotResolver() {
    }

    static long parseVersion(String value) {
        final long version;
        try {
            version = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Lance FOR VERSION AS OF requires a numeric version, but was '" + value + "'", e);
        }
        if (version <= 0) {
            throw new IllegalArgumentException(
                    "Lance FOR VERSION AS OF requires a positive version, but was " + version);
        }
        return version;
    }

    /**
     * Gets the latest Lance version whose commit time does not exceed the requested timestamp.
     *
     * <p>Called by
     * {@link LanceExternalCatalog#loadTableMetadata(String, String, java.util.Optional)} to resolve
     * a {@code FOR TIME AS OF} clause before loading the selected metadata snapshot.
     */
    static long getVersionAtOrBefore(String datasetUri, Map<String, String> javaStorageOptions,
            long timestampMillis, BufferAllocator allocator) throws Exception {
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(LanceReadOptions.build(javaStorageOptions, OptionalLong.empty())).build()) {
            return versionAtOrBefore(dataset.listVersions(), timestampMillis);
        }
    }

    /** Selects a version from the version list fetched by {@link #getVersionAtOrBefore}. */
    static long versionAtOrBefore(List<Version> versions, long timestampMillis) {
        Instant requestedTime = Instant.ofEpochMilli(timestampMillis);
        return versions.stream()
                .filter(version -> !version.getDataTime().toInstant().isAfter(requestedTime))
                .max(Comparator.comparing((Version version) -> version.getDataTime().toInstant())
                        .thenComparingLong(Version::getId))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Lance dataset has no version at or before timestamp " + timestampMillis))
                .getId();
    }
}
