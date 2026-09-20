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

import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.spi.Split;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Builds splits from index-segment coverage and optional uncovered fragments. */
public final class LanceSplitBuilder {
    private final String datasetUri;
    private final long version;
    private final List<LanceSplit> splits;
    private final Set<Long> indexSegmentFragmentIds = new HashSet<>();

    LanceSplitBuilder(String datasetUri, long version, int expectedIndexSegments) {
        this.datasetUri = datasetUri;
        this.version = version;
        this.splits = new ArrayList<>(expectedIndexSegments);
    }

    /** Builds ordinary fixed-snapshot fragment scans without selecting index segments. */
    public static List<Split> buildFragmentSplits(String datasetUri, long version,
            Iterable<LanceFragmentInfo> fragments, int fragmentsPerSplit) {
        LanceSplitBuilder builder = new LanceSplitBuilder(datasetUri, version, 0);
        builder.addUncoveredFragments(fragments, fragmentsPerSplit);
        return builder.buildSplits();
    }

    boolean isEmpty() {
        return splits.isEmpty();
    }

    int splitCount() {
        return splits.size();
    }

    int indexSegmentFragmentCount() {
        return indexSegmentFragmentIds.size();
    }

    boolean isCoveredByIndexSegment(long fragmentId) {
        return indexSegmentFragmentIds.contains(fragmentId);
    }

    void addIndexSegmentSplit(UUID indexSegmentUuid,
            List<Long> fragmentIds, long physicalRows) {
        indexSegmentFragmentIds.addAll(fragmentIds);
        splits.add(LanceSplit.scanIndexSegment(
                datasetUri, version, indexSegmentUuid, fragmentIds, physicalRows));
    }

    // Manifest order and row-based split weights are shared by ordinary scans and fallbacks.
    // An empty index coverage set groups all fragments; vector fallbacks use a group size of 1.
    void addUncoveredFragments(Iterable<LanceFragmentInfo> fragments, int fragmentsPerSplit) {
        addUncoveredFragments(fragments, fragmentsPerSplit, false);
    }

    void addUncoveredFragments(Iterable<LanceFragmentInfo> fragments, int fragmentsPerSplit,
            boolean disableScalarIndex) {
        if (fragmentsPerSplit < 1) {
            throw new IllegalArgumentException("fragmentsPerSplit must be positive");
        }
        List<Long> fragmentIds = new ArrayList<>();
        long physicalRows = 0;
        for (LanceFragmentInfo fragment : fragments) {
            if (isCoveredByIndexSegment(fragment.getId())) {
                continue;
            }
            fragmentIds.add(fragment.getId());
            physicalRows += fragment.getSchedulingWeight();
            if (fragmentIds.size() == fragmentsPerSplit) {
                addFragmentGroup(fragmentIds, physicalRows, disableScalarIndex);
                fragmentIds.clear();
                physicalRows = 0;
            }
        }
        if (!fragmentIds.isEmpty()) {
            addFragmentGroup(fragmentIds, physicalRows, disableScalarIndex);
        }
    }

    private void addFragmentGroup(List<Long> fragmentIds, long physicalRows, boolean disableScalarIndex) {
        splits.add(disableScalarIndex
                ? LanceSplit.scanFragmentsWithoutScalarIndex(datasetUri, version, fragmentIds, physicalRows)
                : LanceSplit.scanFragments(datasetUri, version, fragmentIds, physicalRows));
    }

    List<Split> buildSplits() {
        // Keep scheduling weights proportional to the largest split in this plan.
        long maxPhysicalRows = splits.stream().mapToLong(LanceSplit::getSelfSplitWeight).max().orElse(1);
        for (LanceSplit split : splits) {
            split.setTargetSplitSize(maxPhysicalRows);
        }
        return new ArrayList<>(splits);
    }
}
