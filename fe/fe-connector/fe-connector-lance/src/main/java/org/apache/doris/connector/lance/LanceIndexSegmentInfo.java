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

package org.apache.doris.connector.lance;

import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Immutable metadata for one physical segment of a logical Lance index. */
final class LanceIndexSegmentInfo {
    private final UUID uuid;
    private final String indexName;
    private final List<Integer> fieldIds;
    private final List<Long> fragmentIds;
    private final IndexType indexType;

    LanceIndexSegmentInfo(UUID uuid, String indexName, List<Integer> fieldIds, List<Long> fragmentIds,
            IndexType indexType) {
        this.uuid = uuid;
        this.indexName = indexName;
        this.fieldIds = Collections.unmodifiableList(new ArrayList<>(fieldIds));
        this.fragmentIds = fragmentIds == null
                ? null : Collections.unmodifiableList(new ArrayList<>(fragmentIds));
        this.indexType = Objects.requireNonNull(indexType, "indexType must not be null");
    }

    UUID getUuid() {
        return uuid;
    }

    String getIndexName() {
        return indexName;
    }

    List<Integer> getFieldIds() {
        return fieldIds;
    }

    /**
     * Returns the fragment bitmap recorded in the manifest.
     *
     * <p>Index segments without a fragment bitmap have unknown coverage. Callers must not infer
     * coverage from the segment's dataset version in that case.
     */
    Optional<List<Long>> getFragmentIds() {
        return Optional.ofNullable(fragmentIds);
    }

    IndexType getIndexType() {
        return indexType;
    }
}
