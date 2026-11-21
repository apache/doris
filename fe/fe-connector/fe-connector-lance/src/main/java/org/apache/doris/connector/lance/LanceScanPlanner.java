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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Plans which fragments each range of an ordinary Lance scan reads, from metadata alone. Planning is
 * deterministic, so the scan properties (EXPLAIN) and the range list of one statement agree.
 */
final class LanceScanPlanner {
    /** Below this many rows one COUNT(*) carrier is enough; above it each fragment gets one. */
    static final long COUNT_WITH_PARALLEL_SPLITS = 10000;

    private LanceScanPlanner() {
    }

    /**
     * Groups the fragments of {@code metadata}'s version: {@code fragmentsPerSplit} at a time when it is
     * positive (a debugging override), else one range per scalar-index segment that can drive the pushed
     * filter plus one range per fragment no such segment covers.
     *
     * @param indexMetadata read only when an index segment could be planned: a positive
     *         {@code fragmentsPerSplit} or a scan with nothing pushed never reads it
     */
    static LanceScanPlan plan(LanceTableMetadata metadata, LancePredicateConverter converter,
            List<ConnectorExpression> pushedConjuncts, int fragmentsPerSplit,
            Supplier<LanceIndexMetadata> indexMetadata) {
        if (fragmentsPerSplit < 0) {
            throw new DorisConnectorException("lance_fragments_per_split must be non-negative");
        }
        Map<Long, LanceFragmentInfo> visibleFragments = visibleFragments(metadata);
        int fragmentCount = visibleFragments.size();
        if (fragmentsPerSplit > 0) {
            // Keep the debug grouping exact, even when it produces fewer splits than BEs.
            LanceSplitBuilder splits = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0);
            splits.addUncoveredFragments(visibleFragments.values(), fragmentsPerSplit, false);
            return new LanceScanPlan(splits, fragmentCount, fragmentsPerSplit, null, 0, 0, 0);
        }
        LanceScalarIndexPlanner.Plan scalarIndexPlan = pushedConjuncts.isEmpty() ? null
                : LanceScalarIndexPlanner.plan(metadata, indexMetadata.get(), converter, pushedConjuncts,
                        visibleFragments);
        LanceSplitBuilder splits;
        int indexSegments = 0;
        int indexedFragments = 0;
        if (scalarIndexPlan != null) {
            splits = scalarIndexPlan.splits;
            indexSegments = splits.splitCount();
            indexedFragments = splits.indexSegmentFragmentCount();
        } else {
            splits = new LanceSplitBuilder(metadata.getDatasetUri(), metadata.getVersion(), 0);
        }
        // Uncovered fragments must not repeat the selected segment's search. When FE
        // could not select a segment, retain native selection unless the filter itself
        // is known to cause expensive repeated searches (a complement or oversized tree).
        splits.addUncoveredFragments(visibleFragments.values(), 1, scalarIndexPlan != null
                || LanceScalarIndexPlanner.shouldDisableFragmentIndex(pushedConjuncts));
        return new LanceScanPlan(splits, fragmentCount, 0,
                scalarIndexPlan == null ? null : scalarIndexPlan.indexName, indexSegments, indexedFragments,
                scalarIndexPlan == null ? 0 : fragmentCount - indexedFragments);
    }

    /**
     * COUNT(*) / COUNT(1) with no filter, answered from the manifest's row counts. Each carrier holds a disjoint
     * group of fragments and their logical rows, so a BE that cannot serve the count scans an equivalent pinned
     * snapshot instead. A large count gets one carrier per fragment to keep the scan parallel; a small one, or a
     * version without fragments, gets one carrier.
     */
    static List<ConnectorScanRange> countRanges(LanceTableMetadata metadata) {
        long rowCount = metadata.getRowCount();
        List<LanceFragmentInfo> fragments = new ArrayList<>(visibleFragments(metadata).values());
        List<List<LanceFragmentInfo>> groups = new ArrayList<>();
        if (rowCount >= COUNT_WITH_PARALLEL_SPLITS) {
            for (LanceFragmentInfo fragment : fragments) {
                groups.add(Collections.singletonList(fragment));
            }
        } else {
            groups.add(fragments);
        }
        long targetSplitSize = 1;
        for (List<LanceFragmentInfo> group : groups) {
            targetSplitSize = Math.max(targetSplitSize, physicalRows(group));
        }
        List<ConnectorScanRange> ranges = new ArrayList<>(groups.size());
        for (List<LanceFragmentInfo> group : groups) {
            List<Long> fragmentIds = new ArrayList<>(group.size());
            long logicalRows = 0;
            for (LanceFragmentInfo fragment : group) {
                fragmentIds.add(fragment.getId());
                logicalRows += fragment.getRowCount();
            }
            ranges.add(LanceScanRange.metadataCount(metadata.getDatasetUri(), metadata.getVersion(), fragmentIds,
                    logicalRows, rowCount, physicalRows(group), targetSplitSize));
        }
        return ranges;
    }

    private static long physicalRows(List<LanceFragmentInfo> fragments) {
        long physicalRows = 0;
        for (LanceFragmentInfo fragment : fragments) {
            physicalRows += fragment.getSchedulingWeight();
        }
        return Math.max(physicalRows, 1);
    }

    private static Map<Long, LanceFragmentInfo> visibleFragments(LanceTableMetadata metadata) {
        Map<Long, LanceFragmentInfo> visible = new LinkedHashMap<>();
        for (LanceFragmentInfo fragment : metadata.getFragments()) {
            if (visible.put(fragment.getId(), fragment) != null) {
                throw new DorisConnectorException("Duplicate Lance fragment id " + fragment.getId()
                        + " at dataset version " + metadata.getVersion());
            }
        }
        return visible;
    }
}
