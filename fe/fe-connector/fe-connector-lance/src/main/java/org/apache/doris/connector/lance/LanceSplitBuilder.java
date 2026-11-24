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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Builds the ranges of one plan from index-segment coverage and the fragments no segment covers. */
final class LanceSplitBuilder {
    private final String datasetUri;
    private final long version;
    private final List<Group> groups;
    private final Set<Long> indexSegmentFragmentIds = new HashSet<>();

    LanceSplitBuilder(String datasetUri, long version, int expectedIndexSegments) {
        this.datasetUri = datasetUri;
        this.version = version;
        this.groups = new ArrayList<>(expectedIndexSegments);
    }

    boolean isEmpty() {
        return groups.isEmpty();
    }

    int splitCount() {
        return groups.size();
    }

    int indexSegmentFragmentCount() {
        return indexSegmentFragmentIds.size();
    }

    boolean isCoveredByIndexSegment(long fragmentId) {
        return indexSegmentFragmentIds.contains(fragmentId);
    }

    void addIndexSegmentSplit(UUID indexSegmentUuid, List<Long> fragmentIds, long physicalRows) {
        indexSegmentFragmentIds.addAll(fragmentIds);
        groups.add(new Group(indexSegmentUuid, fragmentIds, physicalRows, false));
    }

    /**
     * Groups the fragments no index segment covers, {@code fragmentsPerSplit} at a time in manifest order;
     * {@code disableScalarIndex} keeps BE from searching a scalar index on them.
     */
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
                groups.add(new Group(null, fragmentIds, physicalRows, disableScalarIndex));
                fragmentIds = new ArrayList<>();
                physicalRows = 0;
            }
        }
        if (!fragmentIds.isEmpty()) {
            groups.add(new Group(null, fragmentIds, physicalRows, disableScalarIndex));
        }
    }

    /** The ranges, each bounded by {@code limit} (-1 for none); weights are relative to the largest range. */
    List<ConnectorScanRange> buildRanges(long limit) {
        long targetSplitSize = groups.stream().mapToLong(group -> group.physicalRows).max().orElse(1);
        List<ConnectorScanRange> ranges = new ArrayList<>(groups.size());
        for (Group group : groups) {
            ranges.add(group.indexSegmentUuid == null
                    ? LanceScanRange.fragments(datasetUri, version, group.fragmentIds, group.physicalRows,
                            targetSplitSize, group.scalarIndexDisabled, limit)
                    : LanceScanRange.indexSegment(datasetUri, version, group.indexSegmentUuid, group.fragmentIds,
                            group.physicalRows, targetSplitSize, limit));
        }
        return ranges;
    }

    private static final class Group {
        private final UUID indexSegmentUuid;
        private final List<Long> fragmentIds;
        private final long physicalRows;
        private final boolean scalarIndexDisabled;

        private Group(UUID indexSegmentUuid, List<Long> fragmentIds, long physicalRows,
                boolean scalarIndexDisabled) {
            this.indexSegmentUuid = indexSegmentUuid;
            this.fragmentIds = fragmentIds;
            this.physicalRows = Math.max(physicalRows, 1);
            this.scalarIndexDisabled = scalarIndexDisabled;
        }
    }
}
