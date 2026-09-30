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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentGroup;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;

import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Assigns one BTree/Bitmap/LabelList segment and a disjoint fragment domain to each ordinary scan task. */
final class LanceScalarIndexPlanner {
    static final class Plan {
        final String indexName;
        final LanceSplitBuilder splits;
        private final long coveredRows;

        Plan(String indexName, LanceSplitBuilder splits, long coveredRows) {
            this.indexName = indexName;
            this.splits = splits;
            this.coveredRows = coveredRows;
        }
    }

    static Plan plan(LanceTableMetadata metadata, List<Expr> pushedConjuncts,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        if (metadata.getVersion() <= 0
                || !metadata.getIndexMetadataState().canPlanIndexSegments()) {
            return null;
        }
        Set<Integer> filterFields = collectFilterFields(metadata, pushedConjuncts);
        if (filterFields.isEmpty()) {
            return null;
        }
        // Metadata already groups physical segments by logical index. Name order
        // provides a stable winner when multiple indices cover the same number of rows.
        List<LanceIndexSegmentGroup> indices = new ArrayList<>(metadata.getIndexes());
        indices.sort(java.util.Comparator.comparing(LanceIndexSegmentGroup::getName));
        boolean splitComplementByFragment = pushedConjuncts.stream()
                .anyMatch(LanceScalarIndexPlanner::containsComplement);
        Plan selected = null;
        for (LanceIndexSegmentGroup logicalIndex : indices) {
            List<LanceIndexSegmentInfo> segments = logicalIndex.getSegments();
            // Segment-scoped scans support one top-level key in BTree/Bitmap/LabelList indices. Lance
            // performs the final typed driver selection and falls back within the same domain.
            LanceIndexSegmentInfo index = segments.get(0);
            if ((index.getIndexType() != IndexType.BTREE && index.getIndexType() != IndexType.BITMAP
                    && index.getIndexType() != IndexType.LABEL_LIST)
                    || index.getFieldIds().size() != 1 || !filterFields.contains(index.getFieldIds().get(0))) {
                continue;
            }
            Plan candidate = groupFragments(metadata, segments, visibleFragments,
                    splitComplementByFragment);
            if (candidate != null && (selected == null || candidate.coveredRows > selected.coveredRows)) {
                selected = candidate;
            }
        }
        return selected;
    }

    private static Set<Integer> collectFilterFields(LanceTableMetadata metadata, List<Expr> pushedConjuncts) {
        Set<Integer> fields = new HashSet<>();
        for (Expr expr : pushedConjuncts) {
            fields.addAll(collectDriverFields(metadata, expr, false));
        }
        return fields;
    }

    private static Set<Integer> collectDriverFields(LanceTableMetadata metadata, Expr expr, boolean complete) {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate.Operator op = ((CompoundPredicate) expr).getOp();
            if (op == CompoundPredicate.Operator.NOT) {
                // NOT cannot complement a pruned AND: that would discard valid matches.
                return collectDriverFields(metadata, expr.getChild(0), true);
            }
            Set<Integer> left = collectDriverFields(metadata, expr.getChild(0), complete);
            Set<Integer> right = collectDriverFields(metadata, expr.getChild(1), complete);
            if (op == CompoundPredicate.Operator.AND && !complete) {
                left.addAll(right);
            } else {
                // One selected index must supply candidates for both OR branches, and for
                // every leaf of a negated subtree. lance-c rechecks the complete predicate.
                left.retainAll(right);
            }
            return left;
        }
        Set<SlotRef> slots = new HashSet<>();
        expr.collect(SlotRef.class, slots);
        Set<Integer> fields = new HashSet<>();
        for (SlotRef slot : slots) {
            metadata.getLanceFieldId(slot.getColumnName()).ifPresent(fields::add);
        }
        return fields;
    }

    private static boolean containsComplement(Expr expr) {
        if ((expr instanceof CompoundPredicate
                && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.NOT)
                || (expr instanceof InPredicate && ((InPredicate) expr).isNotIn())
                || (expr instanceof BinaryPredicate
                && ((BinaryPredicate) expr).getOp() == BinaryPredicate.Operator.NE)
                || (expr instanceof IsNullPredicate && ((IsNullPredicate) expr).isNotNull())) {
            return true;
        }
        return expr.getChildren().stream().anyMatch(LanceScalarIndexPlanner::containsComplement);
    }

    private static Plan groupFragments(LanceTableMetadata metadata, List<LanceIndexSegmentInfo> segments,
            Map<Long, LanceFragmentInfo> visibleFragments, boolean splitComplementByFragment) {
        LanceSplitBuilder splits = new LanceSplitBuilder(
                metadata.getDatasetUri(), metadata.getVersion(), segments.size());
        Set<Long> coveredFragments = new HashSet<>();
        long coveredRows = 0;
        for (LanceIndexSegmentInfo segment : segments) {
            if (!segment.getFragmentIds().isPresent()
                    || segment.getIndexType() != segments.get(0).getIndexType()
                    || !segment.getFieldIds().equals(segments.get(0).getFieldIds())) {
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
                physicalRows += fragment.getSchedulingWeight();
                coveredRows += Math.max(fragment.getPhysicalRows(), 0);
            }
            if (!fragments.isEmpty()) {
                // Complement selectivity is unknown. Preserve fragment-level scheduling so a
                // broad NOT cannot funnel all covered data through one BE. Each task retains
                // the same segment UUID but owns a disjoint domain, including on fallback.
                if (splitComplementByFragment) {
                    for (Long fragmentId : fragments) {
                        splits.addIndexSegmentSplit(segment.getUuid(), java.util.Collections.singletonList(fragmentId),
                                visibleFragments.get(fragmentId).getSchedulingWeight());
                    }
                } else {
                    splits.addIndexSegmentSplit(segment.getUuid(), fragments, physicalRows);
                }
            }
        }
        return splits.isEmpty() ? null : new Plan(segments.get(0).getIndexName(), splits, coveredRows);
    }
}
