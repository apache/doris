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

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Immutable metadata resolved from one already-fixed Lance dataset version. */
public class LanceTableMetadata {
    private final String datasetUri;
    private final long version;
    private final Schema schema;
    private final List<LanceFragmentInfo> fragments;
    private final Map<String, Integer> lanceFieldIds;
    private final List<LanceIndexSegmentInfo> indexSegments;
    private final Map<String, String> lanceStorageOptions;

    public static LanceTableMetadata withoutIndexSegments(String datasetUri, long version,
            Schema schema, List<LanceFragmentInfo> fragments,
            Map<String, String> lanceStorageOptions) {
        return new LanceTableMetadata(datasetUri, version, schema, fragments,
                Collections.emptyMap(), Collections.emptyList(), lanceStorageOptions);
    }

    public static LanceTableMetadata withIndexSegments(String datasetUri, long version,
            Schema schema, List<LanceFragmentInfo> fragments,
            Map<String, Integer> lanceFieldIds, List<LanceIndexSegmentInfo> indexSegments,
            Map<String, String> lanceStorageOptions) {
        return new LanceTableMetadata(datasetUri, version, schema, fragments,
                lanceFieldIds, indexSegments, lanceStorageOptions);
    }

    private LanceTableMetadata(String datasetUri, long version, Schema schema,
            List<LanceFragmentInfo> fragments, Map<String, Integer> lanceFieldIds,
            List<LanceIndexSegmentInfo> indexSegments, Map<String, String> lanceStorageOptions) {
        this.datasetUri = datasetUri;
        this.version = version;
        this.schema = schema;
        this.fragments = Collections.unmodifiableList(new ArrayList<>(fragments));
        this.lanceFieldIds = Collections.unmodifiableMap(new HashMap<>(lanceFieldIds));
        this.indexSegments = Collections.unmodifiableList(new ArrayList<>(indexSegments));
        this.lanceStorageOptions = Collections.unmodifiableMap(new HashMap<>(lanceStorageOptions));
    }

    public String getDatasetUri() {
        return datasetUri;
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

    public List<LanceIndexSegmentInfo> getIndexSegments() {
        return indexSegments;
    }

    public OptionalInt getLanceFieldId(String fieldName) {
        Integer fieldId = lanceFieldIds.get(fieldName);
        return fieldId == null ? OptionalInt.empty() : OptionalInt.of(fieldId);
    }

    /** Lance object-store options, understood as-is by both the FE SDK and lance-c. */
    public Map<String, String> getLanceStorageOptions() {
        return lanceStorageOptions;
    }

    public long getRowCount() {
        return fragments.stream().mapToLong(LanceFragmentInfo::getRowCount).sum();
    }
}
