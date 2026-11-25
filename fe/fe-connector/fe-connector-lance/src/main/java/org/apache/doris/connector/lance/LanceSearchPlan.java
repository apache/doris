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

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TVectorSearchParams;

import java.util.List;
import java.util.Map;

/**
 * How a vector or full-text search splits the fragments of its version, and what EXPLAIN says about it: the
 * physical index segments it searches, one range each, and the fragments no such segment covers, which a vector
 * search scans exactly (a flat search), one range each.
 */
final class LanceSearchPlan {
    static final String PROP_SEARCH_TYPE = "lance.search_type";
    static final String PROP_SEARCH_COLUMN = "lance.search_column";
    static final String PROP_VECTOR_INDEX_STATUS = "lance.vector_index_status";
    static final String PROP_QUERY_PARALLELISM = "lance.query_parallelism";
    static final String PROP_DISTANCE_RANGE = "lance.distance_range";
    static final String PROP_METRIC = "lance.metric";
    static final String PROP_FTS_COVERAGE_MODE = "lance.fts_coverage_mode";
    static final String PROP_FTS_QUERY_TYPE = "lance.fts_query_type";
    static final String PROP_FTS_MATCH_OPERATOR = "lance.fts_match_operator";
    static final String PROP_FTS_MAX_FUZZY_DISTANCE = "lance.fts_max_fuzzy_distance";
    static final String PROP_FTS_PHRASE_SLOP = "lance.fts_phrase_slop";
    static final String PROP_SEARCH_FRAGMENTS = "lance.search_fragments";
    static final String PROP_SEARCH_UNINDEXED_FRAGMENTS = "lance.search_unindexed_fragments";
    static final String PROP_SEARCH_INDEX_SEGMENTS = "lance.search_index_segments";
    static final String PROP_SEARCH_INDEX_FRAGMENTS = "lance.search_index_fragments";

    private static final String VECTOR = "VECTOR";
    private static final String FULL_TEXT = "FULL_TEXT";

    /**
     * Why a vector search did or did not search an index. The BE searches an index only through the segments
     * planned here, so a vector search under any status but {@link #USED} is a flat search.
     */
    enum VectorIndexStatus {
        /** Not a vector search. */
        NOT_PLANNED,
        /** The search sets use_index=false. */
        DISABLED,
        /** No vector index covers the searched column. */
        NO_MATCH,
        /** The index was built with a different metric, or its details name none. */
        METRIC_MISMATCH,
        /** The index search cannot evaluate this distance range exactly. */
        DISTANCE_RANGE_FALLBACK,
        /** A segment has no fragment coverage metadata. */
        UNKNOWN_COVERAGE,
        /** No segment covers a fragment of the version searched. */
        NO_VISIBLE_COVERAGE,
        /** The index segments were planned, possibly with ranges for the fragments they do not cover. */
        USED
    }

    private final LanceSplitBuilder splits;
    private final VectorIndexStatus vectorIndexStatus;
    private final int fragmentCount;
    private final int indexSegmentCount;
    private final int indexedFragmentCount;

    LanceSearchPlan(LanceSplitBuilder splits, VectorIndexStatus vectorIndexStatus, int fragmentCount,
            int indexSegmentCount, int indexedFragmentCount) {
        this.splits = splits;
        this.vectorIndexStatus = vectorIndexStatus;
        this.fragmentCount = fragmentCount;
        this.indexSegmentCount = indexSegmentCount;
        this.indexedFragmentCount = indexedFragmentCount;
    }

    VectorIndexStatus getVectorIndexStatus() {
        return vectorIndexStatus;
    }

    int getIndexSegmentCount() {
        return indexSegmentCount;
    }

    int getIndexedFragmentCount() {
        return indexedFragmentCount;
    }

    int getUnindexedFragmentCount() {
        return fragmentCount - indexedFragmentCount;
    }

    /** The ranges; each asks for its own candidates, so none is bounded by a LIMIT. */
    List<ConnectorScanRange> ranges() {
        return splits.buildRanges(-1);
    }

    /** Records what EXPLAIN shows for the search, which {@link #appendExplain} reads back. */
    void addExplainProperties(Map<String, String> props, TExternalSearchRequest request) {
        if (request.getSearchQuery().isSetVectorSearch()) {
            TVectorSearchParams vector = request.getSearchQuery().getVectorSearch();
            props.put(PROP_SEARCH_TYPE, VECTOR);
            props.put(PROP_VECTOR_INDEX_STATUS, vectorIndexStatus.name());
            props.put(PROP_SEARCH_COLUMN, vector.getColumn());
            if (request.isSetVectorSearchOptions() && request.getVectorSearchOptions().isSetQueryParallelism()) {
                props.put(PROP_QUERY_PARALLELISM,
                        String.valueOf(request.getVectorSearchOptions().getQueryParallelism()));
            }
            if (vector.isSetDistanceLowerBound() || vector.isSetDistanceUpperBound()) {
                props.put(PROP_DISTANCE_RANGE, "["
                        + (vector.isSetDistanceLowerBound() ? String.valueOf(vector.getDistanceLowerBound()) : "-inf")
                        + ", "
                        + (vector.isSetDistanceUpperBound() ? String.valueOf(vector.getDistanceUpperBound()) : "+inf")
                        + ")");
            }
            props.put(PROP_METRIC, LanceVectorQuery.metricName(vector.getMetric()));
        } else {
            TFullTextSearchParams fullText = request.getSearchQuery().getFullTextSearch();
            props.put(PROP_SEARCH_TYPE, FULL_TEXT);
            props.put(PROP_SEARCH_COLUMN, fullText.getColumn());
            props.put(PROP_FTS_COVERAGE_MODE, String.valueOf(fullText.getCoverageMode()));
            props.put(PROP_FTS_QUERY_TYPE, String.valueOf(fullText.getQueryType()));
            if (fullText.getQueryType() == TFtsQueryType.MATCH) {
                props.put(PROP_FTS_MATCH_OPERATOR, String.valueOf(fullText.getMatchOperator()));
                props.put(PROP_FTS_MAX_FUZZY_DISTANCE, String.valueOf(fullText.getMaxFuzzyDistance()));
            } else {
                props.put(PROP_FTS_PHRASE_SLOP, String.valueOf(fullText.getPhraseSlop()));
            }
        }
        props.put(PROP_SEARCH_FRAGMENTS, String.valueOf(fragmentCount));
        props.put(PROP_SEARCH_UNINDEXED_FRAGMENTS, String.valueOf(getUnindexedFragmentCount()));
        props.put(PROP_SEARCH_INDEX_SEGMENTS, String.valueOf(indexSegmentCount));
        props.put(PROP_SEARCH_INDEX_FRAGMENTS, String.valueOf(indexedFragmentCount));
    }

    /** Whether the scan properties describe a search, which {@link #appendExplain} then prints. */
    static boolean isSearch(Map<String, String> nodeProperties) {
        return nodeProperties.containsKey(PROP_SEARCH_TYPE);
    }

    /** The lines branch-4.x printed for a search; {@code lanceVersion} is the version the search reads. */
    static void appendExplain(StringBuilder output, String prefix, Map<String, String> nodeProperties,
            String version) {
        String searchType = nodeProperties.get(PROP_SEARCH_TYPE);
        output.append(prefix).append("externalSearchType=").append(searchType).append("\n");
        if (VECTOR.equals(searchType)) {
            output.append(prefix).append("lanceVectorIndexStatus=")
                    .append(nodeProperties.get(PROP_VECTOR_INDEX_STATUS)).append("\n");
            output.append(prefix).append("lanceVectorColumn=").append(nodeProperties.get(PROP_SEARCH_COLUMN))
                    .append("\n");
            appendIfPresent(output, prefix, "lanceQueryParallelism", nodeProperties.get(PROP_QUERY_PARALLELISM));
            appendIfPresent(output, prefix, "lanceDistanceRange", nodeProperties.get(PROP_DISTANCE_RANGE));
            output.append(prefix).append("lanceMetric=").append(nodeProperties.get(PROP_METRIC)).append("\n");
        } else {
            output.append(prefix).append("lanceFullTextColumn=").append(nodeProperties.get(PROP_SEARCH_COLUMN))
                    .append("\n");
            output.append(prefix).append("lanceFtsCoverageMode=")
                    .append(nodeProperties.get(PROP_FTS_COVERAGE_MODE)).append("\n");
            output.append(prefix).append("lanceFtsQueryType=").append(nodeProperties.get(PROP_FTS_QUERY_TYPE))
                    .append("\n");
            appendIfPresent(output, prefix, "lanceFtsMatchOperator", nodeProperties.get(PROP_FTS_MATCH_OPERATOR));
            appendIfPresent(output, prefix, "lanceFtsMaxFuzzyDistance",
                    nodeProperties.get(PROP_FTS_MAX_FUZZY_DISTANCE));
            appendIfPresent(output, prefix, "lanceFtsPhraseSlop", nodeProperties.get(PROP_FTS_PHRASE_SLOP));
        }
        output.append(prefix).append("lanceVersion=").append(version).append("\n");
        output.append(prefix).append("lanceSearchFragments=").append(nodeProperties.get(PROP_SEARCH_FRAGMENTS))
                .append("\n");
        output.append(prefix).append("lanceSearchUnindexedFragments=")
                .append(nodeProperties.get(PROP_SEARCH_UNINDEXED_FRAGMENTS)).append("\n");
        output.append(prefix).append("lanceSearchIndexSegments=")
                .append(nodeProperties.get(PROP_SEARCH_INDEX_SEGMENTS)).append("\n");
        output.append(prefix).append("lanceSearchIndexFragments=")
                .append(nodeProperties.get(PROP_SEARCH_INDEX_FRAGMENTS)).append("\n");
    }

    private static void appendIfPresent(StringBuilder output, String prefix, String name, String value) {
        if (value != null) {
            output.append(prefix).append(name).append("=").append(value).append("\n");
        }
    }
}
