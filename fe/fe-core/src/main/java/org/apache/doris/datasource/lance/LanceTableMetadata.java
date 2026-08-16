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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Immutable metadata resolved from one already-fixed Lance dataset version. */
public class LanceTableMetadata {
    private final String datasetUri;
    private final long version;
    private final Schema schema;
    private final List<LanceFragmentInfo> fragments;
    private final Map<String, String> lanceStorageOptions;

    public LanceTableMetadata(String datasetUri, long version, Schema schema,
            List<LanceFragmentInfo> fragments, Map<String, String> lanceStorageOptions) {
        this.datasetUri = datasetUri;
        this.version = version;
        this.schema = schema;
        this.fragments = Collections.unmodifiableList(fragments);
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

    /** Lance object-store options for this dataset, understood as-is by both the FE SDK and lance-c. */
    public Map<String, String> getLanceStorageOptions() {
        return lanceStorageOptions;
    }

    public long getRowCount() {
        return fragments.stream().mapToLong(LanceFragmentInfo::getRowCount).sum();
    }

    public static class LanceFragmentInfo {
        private final long id;
        private final long rowCount;
        private final long physicalRows;

        public LanceFragmentInfo(long id, long rowCount, long physicalRows) {
            this.id = id;
            this.rowCount = rowCount;
            this.physicalRows = physicalRows;
        }

        public long getId() {
            return id;
        }

        /** Logical rows after deletions, used for row-count statistics. */
        public long getRowCount() {
            return rowCount;
        }

        /**
         * Physical rows on disk before deletions. The pinned BE legacy reader reads and merges
         * physical batches before applying the deletion vector, so scan work scales with this
         * value rather than the post-deletion row count. Used for split scheduling weight.
         */
        public long getPhysicalRows() {
            return physicalRows;
        }
    }
}
