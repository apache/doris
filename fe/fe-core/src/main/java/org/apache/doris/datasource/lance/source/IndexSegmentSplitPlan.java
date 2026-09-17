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

import org.apache.doris.datasource.lance.LanceFragmentInfo;
import org.apache.doris.spi.Split;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Builds splits from index-segment coverage and optional uncovered fragments. */
final class IndexSegmentSplitPlan {
    private final String datasetUri;
    private final long version;
    private final List<LanceSplit> splits;
    private final Set<Long> indexSegmentFragmentIds = new HashSet<>();

    IndexSegmentSplitPlan(String datasetUri, long version, int expectedIndexSegments) {
        this.datasetUri = datasetUri;
        this.version = version;
        this.splits = new ArrayList<>(expectedIndexSegments);
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
        splits.add(LanceSplit.forIndexSegment(
                datasetUri, version, indexSegmentUuid, fragmentIds, physicalRows));
    }

    // Manifest order and row-based split weights are shared by ordinary scans and fallbacks.
    // An empty index coverage set groups all fragments; vector fallbacks use a group size of 1.
    void addUncoveredFragments(Iterable<LanceFragmentInfo> fragments, int fragmentsPerSplit) {
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
            physicalRows += Math.max(fragment.getPhysicalRows(), 1);
            if (fragmentIds.size() == fragmentsPerSplit) {
                addFragmentGroup(fragmentIds, physicalRows);
                fragmentIds.clear();
                physicalRows = 0;
            }
        }
        if (!fragmentIds.isEmpty()) {
            addFragmentGroup(fragmentIds, physicalRows);
        }
    }

    private void addFragmentGroup(List<Long> fragmentIds, long physicalRows) {
        splits.add(LanceSplit.forFragments(datasetUri, version, fragmentIds, physicalRows));
    }

    /** Subdivides fragment-only groups for available backends; assigned segments stay intact. */
    List<Split> buildFragmentSplits(int numBackends, Map<Long, LanceFragmentInfo> visibleFragments) {
        int targetSplits = Math.min(numBackends, visibleFragments.size());
        if (splits.size() >= targetSplits) {
            return buildSplits();
        }
        int[] groupCounts = new int[splits.size()];
        Arrays.fill(groupCounts, 1);
        for (int count = splits.size(); count < targetSplits; count++) {
            int largestGroup = -1;
            double largestWeight = -1;
            for (int i = 0; i < splits.size(); i++) {
                LanceSplit split = splits.get(i);
                // Never duplicate a search segment UUID or assign a fragment to two splits.
                if (split.hasIndexSegmentUuids() || groupCounts[i] >= split.getFragmentIds().size()) {
                    continue;
                }
                double weight = (double) split.getSelfSplitWeight() / groupCounts[i];
                if (weight > largestWeight) {
                    largestGroup = i;
                    largestWeight = weight;
                }
            }
            if (largestGroup < 0) {
                break;
            }
            groupCounts[largestGroup]++;
        }
        List<LanceSplit> originalSplits = new ArrayList<>(splits);
        splits.clear();
        for (int i = 0; i < originalSplits.size(); i++) {
            LanceSplit split = originalSplits.get(i);
            if (groupCounts[i] == 1) {
                splits.add(split);
            } else {
                subdivideFragmentGroup(split, groupCounts[i], visibleFragments);
            }
        }
        return buildSplits();
    }

    private void subdivideFragmentGroup(LanceSplit split, int groups,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        List<Long> fragments = split.getFragmentIds();
        long remainingRows = split.getSelfSplitWeight();
        int start = 0;
        for (int remainingGroups = groups; remainingGroups > 0; remainingGroups--) {
            long targetRows = remainingRows / remainingGroups;
            long physicalRows = 0;
            int end = start;
            // Retain fragment order and leave at least one fragment for each remaining group.
            int lastEnd = fragments.size() - (remainingGroups - 1);
            do {
                physicalRows += Math.max(visibleFragments.get(fragments.get(end++)).getPhysicalRows(), 1);
            } while (end < lastEnd && physicalRows < targetRows);
            addFragmentGroup(fragments.subList(start, end), physicalRows);
            start = end;
            remainingRows -= physicalRows;
        }
    }

    List<Split> buildSplits() {
        // Recompute after subdivision; the old largest group's weight is no longer applicable.
        long maxPhysicalRows = splits.stream().mapToLong(LanceSplit::getSelfSplitWeight).max().orElse(1);
        for (LanceSplit split : splits) {
            split.setTargetSplitSize(maxPhysicalRows);
        }
        return new ArrayList<>(splits);
    }
}
