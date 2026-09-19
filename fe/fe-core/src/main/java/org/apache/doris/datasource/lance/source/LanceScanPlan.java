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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExternalSearchRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Fixed planning decisions; the scheduler may still attach backend information to each Split. */
final class LanceScanPlan {
    enum SearchKind {
        /** Ordinary SELECT, including scalar filters and metadata COUNT. */
        NORMAL,
        /** Vector candidates merged by a Doris TopN above the scan. */
        VECTOR,
        /** FTS candidates restricted to committed inverted-index coverage. */
        FULL_TEXT;

        boolean isExternalSearch() {
            return this != NORMAL;
        }

        /** A TVF must carry exactly one search request; NORMAL has no search request. */
        static SearchKind fromSearchRequest(TExternalSearchRequest request) {
            if (request == null || !request.isSetSearchQuery()) {
                throw new IllegalArgumentException("Lance external search request requires search_query");
            }
            boolean hasVector = request.getSearchQuery().isSetVectorSearch();
            boolean hasFullText = request.getSearchQuery().isSetFullTextSearch();
            if (hasVector == hasFullText) {
                throw new IllegalArgumentException("Lance external search query must set exactly one search kind");
            }
            return hasVector ? VECTOR : FULL_TEXT;
        }
    }

    /** Explains FE segment selection, not whether the BE may automatically use a Lance index. */
    enum VectorIndexStatus {
        /** No vector planning result exists yet (also used by ordinary and FTS scans). */
        NOT_PLANNED,
        /** The request explicitly sets use_index=false. */
        DISABLED,
        /** No discoverable vector index matches the requested field. */
        NO_MATCH,
        /** The selected index has a different or unknown metric. */
        METRIC_MISMATCH,
        /** At least one segment has no fragment coverage metadata. */
        UNKNOWN_COVERAGE,
        /** All segment coverage lies outside the selected snapshot's visible fragments. */
        NO_VISIBLE_COVERAGE,
        /** FE produced segment splits, possibly with additional uncovered-fragment splits. */
        USED
    }

    final VectorIndexStatus vectorIndexStatus;
    private final List<Split> splits;
    final long version;
    final int fragmentCount;
    final int fragmentsPerSplit;
    final int indexSegmentCount;
    final int indexedFragmentCount;
    final int unindexedFragmentCount;
    final String scalarIndexName;

    LanceScanPlan(List<Split> splits, long version, int fragments, int fragmentsPerSplit,
            int indexSegments, int indexFragments, int unindexedFragments, String scalarIndexName,
            VectorIndexStatus vectorIndexStatus) {
        this.vectorIndexStatus = vectorIndexStatus;
        this.splits = Collections.unmodifiableList(new ArrayList<>(splits));
        this.version = version;
        this.fragmentCount = fragments;
        this.fragmentsPerSplit = fragmentsPerSplit;
        this.indexSegmentCount = indexSegments;
        this.indexedFragmentCount = indexFragments;
        this.unindexedFragmentCount = unindexedFragments;
        this.scalarIndexName = scalarIndexName;
    }

    static LanceScanPlan empty() {
        return new LanceScanPlan(Collections.emptyList(), -1, 0, 0, 0, 0, 0, null, VectorIndexStatus.NOT_PLANNED);
    }

    /** Backend assignment shuffles its input list; keep that mutation outside the stored plan. */
    List<Split> createSchedulingSplits() {
        return new ArrayList<>(splits);
    }
}
