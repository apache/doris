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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Builds vector-search splits from physical Lance index segments and unindexed fragments. */
final class IndexSegmentSplitPlan {
    private final String datasetUri;
    private final long version;
    private final List<Split> splits;
    private final Set<Long> indexSegmentFragmentIds = new HashSet<>();
    private long maxPhysicalRows = 1;

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

    boolean isCoveredByIndexSegment(long fragmentId) {
        return indexSegmentFragmentIds.contains(fragmentId);
    }

    void addIndexSegmentSplit(UUID indexSegmentUuid,
            List<Long> fragmentIds, long physicalRows) {
        indexSegmentFragmentIds.addAll(fragmentIds);
        addSplit(LanceSplit.forIndexSegment(
                datasetUri, version, indexSegmentUuid, fragmentIds, physicalRows), physicalRows);
    }

    void addUnindexedFragmentSplit(LanceFragmentInfo fragment) {
        long physicalRows = Math.max(fragment.getPhysicalRows(), 1);
        addSplit(LanceSplit.forFragment(
                datasetUri, version, fragment.getId(), physicalRows), physicalRows);
    }

    List<Split> buildSplits() {
        for (Split split : splits) {
            split.setTargetSplitSize(maxPhysicalRows);
        }
        return splits;
    }

    private void addSplit(LanceSplit split, long physicalRows) {
        splits.add(split);
        maxPhysicalRows = Math.max(maxPhysicalRows, physicalRows);
    }
}
