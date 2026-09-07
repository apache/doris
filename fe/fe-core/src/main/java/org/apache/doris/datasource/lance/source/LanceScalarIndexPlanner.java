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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.datasource.lance.LanceFragmentInfo;
import org.apache.doris.datasource.lance.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.LanceTableMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Groups fragments using scalar-index coverage. Index search planning remains entirely in Lance. */
final class LanceScalarIndexPlanner {
    static final class Plan {
        final String indexName;
        final IndexSegmentSplitPlan splits;
        private final long coveredRows;

        Plan(String indexName, IndexSegmentSplitPlan splits, long coveredRows) {
            this.indexName = indexName;
            this.splits = splits;
            this.coveredRows = coveredRows;
        }
    }

    static Plan plan(LanceTableMetadata metadata, List<Expr> pushedConjuncts,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        if (metadata.getVersion() <= 0) {
            return null;
        }
        Set<Integer> filterFields = collectFilterFields(metadata, pushedConjuncts);
        if (filterFields.isEmpty()) {
            return null;
        }
        // Group all segments of each logical index before checking coverage. Name order
        // provides a stable winner when multiple indices cover the same number of rows.
        Map<String, List<LanceIndexSegmentInfo>> indices = metadata.getIndexSegments().stream()
                .collect(Collectors.groupingBy(LanceIndexSegmentInfo::getIndexName,
                        TreeMap::new, Collectors.toList()));
        Plan selected = null;
        for (List<LanceIndexSegmentInfo> segments : indices.values()) {
            // The loader copies logical-index field metadata into every segment. An index
            // is a candidate when any of its fields occurs in a pushed filter.
            // Non-vector indices include INVERTED (FTS); this selects grouping, not search semantics.
            LanceIndexSegmentInfo index = segments.get(0);
            if (index.isVectorIndex() || Collections.disjoint(index.getFieldIds(), filterFields)) {
                continue;
            }
            Plan candidate = groupFragments(metadata, segments, visibleFragments);
            if (candidate != null && (selected == null || candidate.coveredRows > selected.coveredRows)) {
                selected = candidate;
            }
        }
        return selected;
    }

    private static Set<Integer> collectFilterFields(LanceTableMetadata metadata, List<Expr> pushedConjuncts) {
        Set<SlotRef> slots = new HashSet<>();
        pushedConjuncts.forEach(expr -> expr.collect(SlotRef.class, slots));
        Set<Integer> fields = new HashSet<>();
        for (SlotRef slot : slots) {
            metadata.getLanceFieldId(slot.getColumnName()).ifPresent(fields::add);
        }
        return fields;
    }

    private static Plan groupFragments(LanceTableMetadata metadata, List<LanceIndexSegmentInfo> segments,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        IndexSegmentSplitPlan splits = new IndexSegmentSplitPlan(
                metadata.getDatasetUri(), metadata.getVersion(), segments.size());
        Set<Long> coveredFragments = new HashSet<>();
        long coveredRows = 0;
        for (LanceIndexSegmentInfo segment : segments) {
            if (!segment.getFragmentIds().isPresent()) {
                return null;
            }
            List<Long> fragments = new ArrayList<>();
            long physicalRows = 0;
            for (Long fragmentId : segment.getFragmentIds().get()) {
                LanceFragmentInfo fragment = visibleFragments.get(fragmentId);
                if (fragment == null) {
                    continue;
                }
                // A visible fragment must have one output owner. Reject overlapping
                // coverage for this candidate and let the caller try another index.
                if (!coveredFragments.add(fragmentId)) {
                    return null;
                }
                fragments.add(fragmentId);
                physicalRows += Math.max(fragment.getPhysicalRows(), 1);
                coveredRows += Math.max(fragment.getPhysicalRows(), 0);
            }
            if (!fragments.isEmpty()) {
                splits.addIndexSegmentFragmentGroup(fragments, physicalRows);
            }
        }
        return splits.isEmpty() ? null : new Plan(segments.get(0).getIndexName(), splits, coveredRows);
    }
}
