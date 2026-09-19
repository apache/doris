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

import org.apache.doris.datasource.lance.index.LanceIndexSegmentGroup;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

/** Immutable metadata resolved from one already-fixed Lance dataset version. */
public class LanceTableMetadata {
    public enum IndexMetadataState {
        /** BASIC reads intentionally omit both field IDs and index metadata. */
        NOT_LOADED,
        /** Field IDs and index metadata were read successfully; the index list may be empty. */
        LOADED,
        /** Index discovery succeeded but the SDK could not convert the schema to Lance field IDs. */
        FIELD_IDS_UNAVAILABLE;

        public boolean canPlanIndexSegments() {
            return this == LOADED;
        }
    }

    private final LanceTableAccess access;
    private final IndexMetadataState indexMetadataState;
    private final long version;
    private final Schema schema;
    private final List<LanceFragmentInfo> fragments;
    private final Map<String, Integer> lanceFieldIds;
    private final List<LanceIndexSegmentGroup> indexes;

    /** Creates a BASIC snapshot; an empty index list here means indexes were not requested. */
    public static LanceTableMetadata createBasicSnapshot(LanceTableAccess access, long version,
            Schema schema, List<LanceFragmentInfo> fragments) {
        return new LanceTableMetadata(access, version, schema, fragments,
                Collections.emptyMap(), Collections.emptyList(), IndexMetadataState.NOT_LOADED);
    }

    /** Creates a snapshot with loaded indexes, including a valid empty index list. */
    public static LanceTableMetadata createSnapshotWithIndexes(LanceTableAccess access, long version,
            Schema schema, List<LanceFragmentInfo> fragments,
            Map<String, Integer> lanceFieldIds, List<LanceIndexSegmentInfo> indexSegments) {
        return new LanceTableMetadata(access, version, schema, fragments,
                lanceFieldIds, indexSegments, IndexMetadataState.LOADED);
    }

    /** Keeps discovered indexes when the SDK cannot map schema fields to Lance IDs. */
    public static LanceTableMetadata createSnapshotWithUnavailableFieldIds(LanceTableAccess access, long version,
            Schema schema, List<LanceFragmentInfo> fragments, List<LanceIndexSegmentInfo> indexSegments) {
        return new LanceTableMetadata(access, version, schema, fragments,
                Collections.emptyMap(), indexSegments, IndexMetadataState.FIELD_IDS_UNAVAILABLE);
    }

    private LanceTableMetadata(LanceTableAccess access, long version, Schema schema,
            List<LanceFragmentInfo> fragments, Map<String, Integer> lanceFieldIds,
            List<LanceIndexSegmentInfo> indexSegments,
            IndexMetadataState indexMetadataState) {
        this.access = Objects.requireNonNull(access, "access");
        this.indexMetadataState = indexMetadataState;
        this.version = version;
        this.schema = schema;
        this.fragments = Collections.unmodifiableList(new ArrayList<>(fragments));
        this.lanceFieldIds = Collections.unmodifiableMap(new HashMap<>(lanceFieldIds));
        this.indexes = LanceIndexSegmentGroup.groupByName(indexSegments);
    }

    public String getDatasetUri() {
        return access.getDatasetUri();
    }

    public long getVersion() {
        return version;
    }

    public Schema getSchema() {
        return schema;
    }

    public List<LanceFragmentInfo> getFragments() {
        return fragments;
    }

    public List<LanceIndexSegmentGroup> getIndexes() {
        return indexes;
    }

    public OptionalInt getLanceFieldId(String fieldName) {
        Integer fieldId = lanceFieldIds.get(fieldName);
        return fieldId == null ? OptionalInt.empty() : OptionalInt.of(fieldId);
    }

    /** Lance object-store options, understood as-is by both the FE SDK and lance-c. */
    public Map<String, String> getLanceStorageOptions() {
        return access.getStorageOptions();
    }

    public IndexMetadataState getIndexMetadataState() {
        return indexMetadataState;
    }

    public long getRowCount() {
        return fragments.stream().mapToLong(LanceFragmentInfo::getRowCount).sum();
    }
}
