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

package org.apache.doris.datasource.lance.metadata;

import org.lance.ReadOptions;
import org.lance.Session;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** Builds consistently configured SDK read options for Lance dataset metadata access. */
public final class LanceReadOptions {
    private static final long METADATA_CACHE_SIZE = 64L * 1024 * 1024;

    private LanceReadOptions() {
    }

    /** Independent index inspection and S3 TVF reads use short-lived caches. */
    public static ReadOptions forIndependentRead(Map<String, String> storageOptions, OptionalLong version) {
        return baseOptions(storageOptions, version)
                .setIndexCacheSizeBytes(0).setMetadataCacheSizeBytes(METADATA_CACHE_SIZE).build();
    }

    /** Capacities belong to the catalog Session, not to each Dataset opened with it. */
    public static ReadOptions forSharedSession(
            Map<String, String> storageOptions, OptionalLong version, Session session) {
        return baseOptions(storageOptions, version).setSession(Objects.requireNonNull(session, "session")).build();
    }

    private static ReadOptions.Builder baseOptions(Map<String, String> storageOptions, OptionalLong version) {
        ReadOptions.Builder builder = new ReadOptions.Builder().setStorageOptions(storageOptions);
        if (version.isPresent()) {
            builder.setVersion(version.getAsLong());
        }
        return builder;
    }
}
