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

import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentGroup;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;

import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Assigns one BTree/Bitmap/LabelList segment and a disjoint fragment domain to each ordinary scan task. */
final class LanceScalarIndexPlanner {
    // Match the pinned lance-c scoped-expression bounds. Exceeding either bound makes
    // native planning scan the whole domain, so do not coalesce fragments in that case.
    private static final int MAX_EXPRESSION_NODES = 128;
    private static final int MAX_EXPRESSION_DEPTH = 32;

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
        if (exceedsExpressionBudget(pushedConjuncts)) {
            return null;
        }
        // Metadata already groups physical segments by logical index. Name order
        // provides a stable winner when multiple indices cover the same number of rows.
        List<LanceIndexSegmentGroup> indices = new ArrayList<>(metadata.getIndexes());
        indices.sort(java.util.Comparator.comparing(LanceIndexSegmentGroup::getName));
        // Native chooses the first matching parser, not the index with the widest
        // coverage. Without its dispatch order, leave competing logical indexes to native.
        Set<Integer> indexedFields = new HashSet<>();
        Set<Integer> ambiguousFields = new HashSet<>();
        for (LanceIndexSegmentGroup index : indices) {
            for (Integer field : index.getSegments().get(0).getFieldIds()) {
                if (!indexedFields.add(field)) {
                    ambiguousFields.add(field);
                }
            }
        }
        Plan selected = null;
        Map<IndexType, Set<Integer>> fieldsByIndexType = new HashMap<>();
        for (LanceIndexSegmentGroup logicalIndex : indices) {
            List<LanceIndexSegmentInfo> segments = logicalIndex.getSegments();
            // Only coalesce a segment when a positive, indexable necessary condition
            // exists. Broad complements keep fragment parallelism without repeating a
            // segment-wide index search once per fragment.
            LanceIndexSegmentInfo index = segments.get(0);
            if ((index.getIndexType() != IndexType.BTREE && index.getIndexType() != IndexType.BITMAP
                    && index.getIndexType() != IndexType.LABEL_LIST)
                    || index.getFieldIds().size() != 1
                    || ambiguousFields.contains(index.getFieldIds().get(0))
                    || !fieldsByIndexType.computeIfAbsent(index.getIndexType(),
                            type -> collectFilterFields(metadata, pushedConjuncts, type))
                            .contains(index.getFieldIds().get(0))) {
                continue;
            }
            Plan candidate = groupFragments(metadata, segments, visibleFragments);
            if (candidate != null && (selected == null || candidate.coveredRows > selected.coveredRows)) {
                selected = candidate;
            }
        }
        return selected;
    }

    private static Set<Integer> collectFilterFields(LanceTableMetadata metadata,
            List<Expr> pushedConjuncts, IndexType indexType) {
        Set<Integer> fields = new HashSet<>();
        for (Expr expr : pushedConjuncts) {
            fields.addAll(collectDriverFields(metadata, expr, indexType, false));
        }
        return fields;
    }

    private static Set<Integer> collectDriverFields(LanceTableMetadata metadata, Expr expr,
            IndexType indexType, boolean requireExact) {
        Set<Integer> fields = new HashSet<>();
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate.Operator op = ((CompoundPredicate) expr).getOp();
            if (op == CompoundPredicate.Operator.NOT) {
                return fields;
            }
            boolean exactBranches = requireExact || op == CompoundPredicate.Operator.OR;
            Set<Integer> left = collectDriverFields(metadata, expr.getChild(0), indexType, exactBranches);
            Set<Integer> right = collectDriverFields(metadata, expr.getChild(1), indexType, exactBranches);
            if (op == CompoundPredicate.Operator.AND) {
                // A refine-only conjunct anywhere below OR makes native reject the
                // union, even if its sibling could independently drive this index.
                if (requireExact && (left.isEmpty() || right.isEmpty())) {
                    return fields;
                }
                left.addAll(right);
            } else {
                // Sharing a slot is insufficient: both OR branches must actually be
                // indexable. A suffix LIKE, for example, cannot supply BTree candidates.
                left.retainAll(right);
            }
            return left;
        }
        if (!isPositiveIndexLeaf(expr, indexType, requireExact)) {
            return fields;
        }
        Set<SlotRef> slots = new HashSet<>();
        expr.collect(SlotRef.class, slots);
        for (SlotRef slot : slots) {
            metadata.getLanceFieldId(slot.getColumnName()).ifPresent(fields::add);
        }
        return fields;
    }

    private static boolean isPositiveIndexLeaf(Expr expr, IndexType indexType, boolean requireExact) {
        if (indexType == IndexType.LABEL_LIST) {
            if (!(expr instanceof FunctionCallExpr) || expr.getChildren().size() != 2) {
                return false;
            }
            String name = ((FunctionCallExpr) expr).getFnName().getFunction();
            if ("array_contains".equalsIgnoreCase(name)) {
                return expr.getChild(0) instanceof SlotRef && expr.getChild(1) instanceof LiteralExpr;
            }
            return "arrays_overlap".equalsIgnoreCase(name) && overlapSize(expr) > 0;
        }
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();
            return (op == BinaryPredicate.Operator.EQ || op == BinaryPredicate.Operator.GT
                    || op == BinaryPredicate.Operator.GE || op == BinaryPredicate.Operator.LT
                    || op == BinaryPredicate.Operator.LE)
                    && ((expr.getChild(0) instanceof SlotRef && expr.getChild(1) instanceof LiteralExpr)
                    || (expr.getChild(1) instanceof SlotRef && expr.getChild(0) instanceof LiteralExpr));
        }
        if (expr instanceof InPredicate) {
            return !((InPredicate) expr).isNotIn() && expr.getChild(0) instanceof SlotRef;
        }
        if (expr instanceof IsNullPredicate) {
            return !((IsNullPredicate) expr).isNotNull() && expr.getChild(0) instanceof SlotRef;
        }
        // Bitmap has no prefix-query support. A refined LIKE can drive an AND,
        // but native's OR planner rejects branches that need a residual recheck.
        if (indexType != IndexType.BTREE || expr.getChildren().size() != 2
                || !(expr.getChild(0) instanceof SlotRef) || !(expr.getChild(1) instanceof StringLiteral)) {
            return false;
        }
        String name = expr instanceof FunctionCallExpr
                ? ((FunctionCallExpr) expr).getFnName().getFunction() : "";
        String prefix = ((StringLiteral) expr.getChild(1)).getStringValue();
        if ("starts_with".equalsIgnoreCase(name)) {
            // Unlike LIKE, every character in starts_with is literal, including % and _.
            return !prefix.isEmpty();
        }
        boolean like = (expr instanceof LikePredicate && ((LikePredicate) expr).getOp() == LikePredicate.Operator.LIKE)
                || "like".equalsIgnoreCase(name);
        if (!like || prefix.isEmpty() || prefix.indexOf('\\') >= 0) {
            return false;
        }
        for (int i = 0; i < prefix.length(); i++) {
            char character = prefix.charAt(i);
            if (character == '%' || character == '_') {
                return i > 0 && (!requireExact || (character == '%' && i == prefix.length() - 1));
            }
        }
        return true;
    }

    private static int overlapSize(Expr expr) {
        if (!(expr instanceof FunctionCallExpr) || expr.getChildren().size() != 2
                || !"arrays_overlap".equalsIgnoreCase(((FunctionCallExpr) expr).getFnName().getFunction())) {
            return 0;
        }
        for (int i = 0; i < 2; i++) {
            if (expr.getChild(i) instanceof ArrayLiteral && expr.getChild(1 - i) instanceof SlotRef) {
                return expr.getChild(i).getChildren().size();
            }
        }
        return 0;
    }

    static boolean shouldDisableFragmentIndex(List<Expr> pushedConjuncts) {
        if (pushedConjuncts.isEmpty()) {
            return false;
        }
        // Missing metadata or an ambiguous index name is not evidence against native
        // index use. Disable only known expensive shapes, independent of FE discovery.
        return exceedsExpressionBudget(pushedConjuncts)
                || pushedConjuncts.stream().noneMatch(LanceScalarIndexPlanner::hasPositivePredicate);
    }

    private static boolean hasPositivePredicate(Expr expr) {
        if (expr instanceof CompoundPredicate) {
            return ((CompoundPredicate) expr).getOp() != CompoundPredicate.Operator.NOT
                    && expr.getChildren().stream().anyMatch(LanceScalarIndexPlanner::hasPositivePredicate);
        }
        if (expr instanceof BinaryPredicate) {
            return ((BinaryPredicate) expr).getOp() != BinaryPredicate.Operator.NE;
        }
        if (expr instanceof InPredicate) {
            return !((InPredicate) expr).isNotIn();
        }
        if (expr instanceof IsNullPredicate) {
            return !((IsNullPredicate) expr).isNotNull();
        }
        return true;
    }

    private static boolean exceedsExpressionBudget(List<Expr> conjuncts) {
        return conjunctionNodes(conjuncts, 0, conjuncts.size(), 0) > MAX_EXPRESSION_NODES;
    }

    private static int conjunctionNodes(List<Expr> conjuncts, int begin, int end, int depth) {
        if (begin == end) {
            return 0;
        }
        if (end - begin == 1) {
            return expressionNodes(conjuncts.get(begin), depth);
        }
        // The converter emits n-ary and:bool; DataFusion splits its arguments in
        // halves. Preserve each leaf's actual depth instead of assuming a left-deep AND.
        int middle = begin + (end - begin) / 2;
        return 1 + conjunctionNodes(conjuncts, begin, middle, depth + 1)
                + conjunctionNodes(conjuncts, middle, end, depth + 1);
    }

    private static int expressionNodes(Expr expr, int depth) {
        if (depth > MAX_EXPRESSION_DEPTH) {
            return MAX_EXPRESSION_NODES + 1;
        }
        if (expr instanceof CompoundPredicate) {
            int nodes = 1;
            for (Expr child : expr.getChildren()) {
                nodes += expressionNodes(child, depth + 1);
                if (nodes > MAX_EXPRESSION_NODES) {
                    break;
                }
            }
            return nodes;
        }
        int labels = overlapSize(expr);
        // The converter emits a balanced OR tree: N memberships require 2*N-1 nodes.
        if (labels > 0) {
            int levels = 32 - Integer.numberOfLeadingZeros(labels - 1);
            if (labels > MAX_EXPRESSION_NODES / 2 || depth + levels > MAX_EXPRESSION_DEPTH) {
                return MAX_EXPRESSION_NODES + 1;
            }
            return 2 * labels - 1;
        }
        if (expr instanceof InPredicate) {
            InPredicate in = (InPredicate) expr;
            int values = in.getInElementNum();
            // DataFusion 54 expands up to three values into a left-deep OR (or
            // AND of negated equalities for NOT IN) before the native budget check.
            if (values > 0 && values <= 3) {
                int negation = in.isNotIn() ? 1 : 0;
                if (depth + values - 1 + negation > MAX_EXPRESSION_DEPTH) {
                    return MAX_EXPRESSION_NODES + 1;
                }
                return (2 + negation) * values - 1;
            }
            return in.isNotIn() ? 2 : 1;
        }
        return 1;
    }

    private static Plan groupFragments(LanceTableMetadata metadata, List<LanceIndexSegmentInfo> segments,
            Map<Long, LanceFragmentInfo> visibleFragments) {
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
                splits.addIndexSegmentSplit(segment.getUuid(), fragments, physicalRows);
            }
        }
        return splits.isEmpty() ? null : new Plan(segments.get(0).getIndexName(), splits, coveredRows);
    }
}
