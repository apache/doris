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

import java.util.List;
import java.util.Map;

/** How an ordinary scan groups the fragments of its version, and what EXPLAIN says about it. */
final class LanceScanPlan {
    static final String PROP_FRAGMENTS_PER_SPLIT = "lance.fragments_per_split";
    static final String PROP_GROUPING_INDEX = "lance.grouping_index";
    static final String PROP_GROUPING_INDEX_SEGMENTS = "lance.grouping_index_segments";
    static final String PROP_GROUPING_INDEXED_FRAGMENTS = "lance.grouping_indexed_fragments";
    static final String PROP_GROUPING_UNINDEXED_FRAGMENTS = "lance.grouping_unindexed_fragments";

    private final LanceSplitBuilder splits;
    private final int fragmentCount;
    private final int fragmentsPerSplit;
    /** The scalar index whose segments the plan reads, or null. */
    private final String scalarIndexName;
    private final int indexSegmentCount;
    private final int indexedFragmentCount;
    private final int unindexedFragmentCount;

    LanceScanPlan(LanceSplitBuilder splits, int fragmentCount, int fragmentsPerSplit, String scalarIndexName,
            int indexSegmentCount, int indexedFragmentCount, int unindexedFragmentCount) {
        this.splits = splits;
        this.fragmentCount = fragmentCount;
        this.fragmentsPerSplit = fragmentsPerSplit;
        this.scalarIndexName = scalarIndexName;
        this.indexSegmentCount = indexSegmentCount;
        this.indexedFragmentCount = indexedFragmentCount;
        this.unindexedFragmentCount = unindexedFragmentCount;
    }

    int getFragmentCount() {
        return fragmentCount;
    }

    /** The ranges, each bounded by {@code limit} (-1 for none). */
    List<ConnectorScanRange> ranges(long limit) {
        return splits.buildRanges(limit);
    }

    /** Records the grouping for {@link #appendExplain}, which only sees the scan's node properties. */
    void addExplainProperties(Map<String, String> props) {
        if (fragmentsPerSplit > 0) {
            props.put(PROP_FRAGMENTS_PER_SPLIT, String.valueOf(fragmentsPerSplit));
        } else if (scalarIndexName != null) {
            props.put(PROP_GROUPING_INDEX, scalarIndexName);
            props.put(PROP_GROUPING_INDEX_SEGMENTS, String.valueOf(indexSegmentCount));
            props.put(PROP_GROUPING_INDEXED_FRAGMENTS, String.valueOf(indexedFragmentCount));
            props.put(PROP_GROUPING_UNINDEXED_FRAGMENTS, String.valueOf(unindexedFragmentCount));
        }
    }

    static void appendExplain(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        String fragmentsPerSplit = nodeProperties.get(PROP_FRAGMENTS_PER_SPLIT);
        String index = nodeProperties.get(PROP_GROUPING_INDEX);
        if (fragmentsPerSplit != null) {
            output.append(prefix).append("lanceFragmentGrouping=DEBUG\n");
            output.append(prefix).append("lanceFragmentsPerSplit=").append(fragmentsPerSplit).append("\n");
        } else if (index == null) {
            output.append(prefix).append("lanceFragmentGrouping=FRAGMENT\n");
        } else {
            output.append(prefix).append("lanceFragmentGrouping=INDEX_SEGMENT\n");
            output.append(prefix).append("lanceScalarIndexScan=SEGMENT\n");
            output.append(prefix).append("lanceGroupingIndex=").append(index).append("\n");
            output.append(prefix).append("lanceGroupingIndexSegments=")
                    .append(nodeProperties.get(PROP_GROUPING_INDEX_SEGMENTS)).append("\n");
            output.append(prefix).append("lanceGroupingIndexedFragments=")
                    .append(nodeProperties.get(PROP_GROUPING_INDEXED_FRAGMENTS)).append("\n");
            output.append(prefix).append("lanceGroupingUnindexedFragments=")
                    .append(nodeProperties.get(PROP_GROUPING_UNINDEXED_FRAGMENTS)).append("\n");
        }
    }
}
