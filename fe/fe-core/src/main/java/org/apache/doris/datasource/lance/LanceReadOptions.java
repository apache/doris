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

import org.lance.ReadOptions;

import java.util.Map;
import java.util.OptionalLong;

/** Builds consistently configured SDK read options for Lance dataset metadata access. */
final class LanceReadOptions {
    private static final long METADATA_CACHE_SIZE = 64L * 1024 * 1024;

    private LanceReadOptions() {
    }

    /** Called by metadata loading and timestamp-based snapshot resolution. */
    static ReadOptions build(Map<String, String> javaStorageOptions, OptionalLong version) {
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
