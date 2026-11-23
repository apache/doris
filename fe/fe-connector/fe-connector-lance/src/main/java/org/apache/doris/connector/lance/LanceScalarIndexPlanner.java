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

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
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

    private LanceScalarIndexPlanner() {
    }

    /**
     * The index whose segments can each drive the pushed predicate over a disjoint set of fragments, covering
     * the most rows; null when there is none.
     *
     * @param converter the converter that pushed the conjuncts, which resolves their column names to Lance
     *         fields as it did for the Substrait filter
     */
    static Plan plan(LanceTableMetadata metadata, LanceIndexMetadata indexMetadata,
            LancePredicateConverter converter, List<ConnectorExpression> pushedConjuncts,
            Map<Long, LanceFragmentInfo> visibleFragments) {
        if (!indexMetadata.canPlanIndexSegments() || exceedsExpressionBudget(pushedConjuncts)) {
            return null;
        }
        // Metadata already groups physical segments by logical index. Name order
        // provides a stable winner when multiple indices cover the same number of rows.
        List<LanceIndexSegmentGroup> indices = new ArrayList<>(indexMetadata.getIndexes());
        indices.sort(Comparator.comparing(LanceIndexSegmentGroup::getName));
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
        FieldIds fieldIds = new FieldIds(indexMetadata, converter);
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
                            type -> collectFilterFields(fieldIds, pushedConjuncts, type))
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

    private static Set<Integer> collectFilterFields(FieldIds fieldIds, List<ConnectorExpression> pushedConjuncts,
            IndexType indexType) {
        Set<Integer> fields = new HashSet<>();
        for (ConnectorExpression expr : pushedConjuncts) {
            fields.addAll(collectDriverFields(fieldIds, expr, indexType, false));
        }
        return fields;
    }

    private static Set<Integer> collectDriverFields(FieldIds fieldIds, ConnectorExpression expr,
            IndexType indexType, boolean requireExact) {
        Set<Integer> fields = new HashSet<>();
        if (expr instanceof ConnectorNot) {
            return fields;
        }
        if (expr instanceof ConnectorAnd) {
            // A refine-only conjunct anywhere below OR makes native reject the
            // union, even if its sibling could independently drive this index.
            for (ConnectorExpression conjunct : ((ConnectorAnd) expr).getConjuncts()) {
                Set<Integer> conjunctFields = collectDriverFields(fieldIds, conjunct, indexType, requireExact);
                if (requireExact && conjunctFields.isEmpty()) {
                    return new HashSet<>();
                }
                fields.addAll(conjunctFields);
            }
            return fields;
        }
        if (expr instanceof ConnectorOr) {
            // Sharing a column is insufficient: every OR branch must actually be
            // indexable. A suffix LIKE, for example, cannot supply BTree candidates.
            List<ConnectorExpression> disjuncts = ((ConnectorOr) expr).getDisjuncts();
            fields.addAll(collectDriverFields(fieldIds, disjuncts.get(0), indexType, true));
            for (int i = 1; i < disjuncts.size(); i++) {
                fields.retainAll(collectDriverFields(fieldIds, disjuncts.get(i), indexType, true));
            }
            return fields;
        }
        if (!isPositiveIndexLeaf(expr, indexType, requireExact)) {
            return fields;
        }
        for (ConnectorExpression child : expr.getChildren()) {
            if (child instanceof ConnectorColumnRef) {
                fieldIds.of(child).ifPresent(fields::add);
            }
        }
        return fields;
    }

    private static boolean isPositiveIndexLeaf(ConnectorExpression expr, IndexType indexType,
            boolean requireExact) {
        if (indexType == IndexType.LABEL_LIST) {
            if (!(expr instanceof ConnectorFunctionCall) || expr.getChildren().size() != 2) {
                return false;
            }
            String name = ((ConnectorFunctionCall) expr).getFunctionName();
            if ("array_contains".equalsIgnoreCase(name)) {
                return expr.getChildren().get(0) instanceof ConnectorColumnRef
                        && expr.getChildren().get(1) instanceof ConnectorLiteral;
            }
            return "arrays_overlap".equalsIgnoreCase(name) && overlapSize(expr) > 0;
        }
        if (expr instanceof ConnectorComparison) {
            ConnectorComparison.Operator op = ((ConnectorComparison) expr).getOperator();
            return op == ConnectorComparison.Operator.EQ || op == ConnectorComparison.Operator.GT
                    || op == ConnectorComparison.Operator.GE || op == ConnectorComparison.Operator.LT
                    || op == ConnectorComparison.Operator.LE;
        }
        if (expr instanceof ConnectorIn) {
            return !((ConnectorIn) expr).isNegated();
        }
        if (expr instanceof ConnectorIsNull) {
            return !((ConnectorIsNull) expr).isNegated();
        }
        // Bitmap has no prefix-query support. A refined LIKE can drive an AND,
        // but native's OR planner rejects branches that need a residual recheck.
        if (indexType != IndexType.BTREE || expr.getChildren().size() != 2
                || !(expr.getChildren().get(1) instanceof ConnectorLiteral)) {
            return false;
        }
        Object prefixValue = ((ConnectorLiteral) expr.getChildren().get(1)).getValue();
        if (!(prefixValue instanceof String)) {
            return false;
        }
        String prefix = (String) prefixValue;
        if (expr instanceof ConnectorFunctionCall
                && "starts_with".equalsIgnoreCase(((ConnectorFunctionCall) expr).getFunctionName())) {
            // Unlike LIKE, every character in starts_with is literal, including % and _.
            return !prefix.isEmpty();
        }
        if (!(expr instanceof ConnectorLike) || prefix.isEmpty() || prefix.indexOf('\\') >= 0) {
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

    private static int overlapSize(ConnectorExpression expr) {
        if (!(expr instanceof ConnectorFunctionCall) || expr.getChildren().size() != 2
                || !"arrays_overlap".equalsIgnoreCase(((ConnectorFunctionCall) expr).getFunctionName())) {
            return 0;
        }
        for (int i = 0; i < 2; i++) {
            if (expr.getChildren().get(1 - i) instanceof ConnectorColumnRef) {
                int size = LancePredicateConverter.arrayElements(expr.getChildren().get(i))
                        .map(List::size).orElse(0);
                if (size > 0) {
                    return size;
                }
            }
        }
        return 0;
    }

    /**
     * Whether ranges that get no index segment should read without a scalar index: the pushed filter is known to
     * make BE repeat an expensive index search on every range (a broad complement or an oversized tree).
     * Missing metadata or an ambiguous index name is not evidence against native index use.
     */
    static boolean shouldDisableFragmentIndex(List<ConnectorExpression> pushedConjuncts) {
        if (pushedConjuncts.isEmpty()) {
            return false;
        }
        return exceedsExpressionBudget(pushedConjuncts)
                || pushedConjuncts.stream().noneMatch(LanceScalarIndexPlanner::hasPositivePredicate);
    }

    private static boolean hasPositivePredicate(ConnectorExpression expr) {
        if (expr instanceof ConnectorNot) {
            return false;
        }
        if (expr instanceof ConnectorAnd || expr instanceof ConnectorOr) {
            return expr.getChildren().stream().anyMatch(LanceScalarIndexPlanner::hasPositivePredicate);
        }
        if (expr instanceof ConnectorComparison) {
            return ((ConnectorComparison) expr).getOperator() != ConnectorComparison.Operator.NE;
        }
        if (expr instanceof ConnectorIn) {
            return !((ConnectorIn) expr).isNegated();
        }
        if (expr instanceof ConnectorIsNull) {
            return !((ConnectorIsNull) expr).isNegated();
        }
        return true;
    }

    private static boolean exceedsExpressionBudget(List<ConnectorExpression> conjuncts) {
        return balancedNodes(conjuncts, 0, conjuncts.size(), 0) > MAX_EXPRESSION_NODES;
    }

    // The converter emits n-ary and:bool / or:bool; DataFusion splits their arguments in
    // halves. Preserve each leaf's actual depth instead of assuming a left-deep tree.
    private static int balancedNodes(List<ConnectorExpression> operands, int begin, int end, int depth) {
        if (begin == end) {
            return 0;
        }
        if (end - begin == 1) {
            return expressionNodes(operands.get(begin), depth);
        }
        int middle = begin + (end - begin) / 2;
        return 1 + balancedNodes(operands, begin, middle, depth + 1)
                + balancedNodes(operands, middle, end, depth + 1);
    }

    private static int expressionNodes(ConnectorExpression expr, int depth) {
        if (depth > MAX_EXPRESSION_DEPTH) {
            return MAX_EXPRESSION_NODES + 1;
        }
        if (expr instanceof ConnectorAnd || expr instanceof ConnectorOr) {
            List<ConnectorExpression> operands = expr.getChildren();
            return balancedNodes(operands, 0, operands.size(), depth);
        }
        if (expr instanceof ConnectorNot) {
            return 1 + expressionNodes(((ConnectorNot) expr).getOperand(), depth + 1);
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
        if (expr instanceof ConnectorIn) {
            ConnectorIn in = (ConnectorIn) expr;
            int values = in.getInList().size();
            // DataFusion 54 expands up to three values into a left-deep OR (or
            // AND of negated equalities for NOT IN) before the native budget check.
            if (values > 0 && values <= 3) {
                int negation = in.isNegated() ? 1 : 0;
                if (depth + values - 1 + negation > MAX_EXPRESSION_DEPTH) {
                    return MAX_EXPRESSION_NODES + 1;
                }
                return (2 + negation) * values - 1;
            }
            return in.isNegated() ? 2 : 1;
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

    /** The Lance field id of a column reference, resolved to its field as the converter resolved it. */
    private static final class FieldIds {
        private final LanceIndexMetadata indexMetadata;
        private final LancePredicateConverter converter;

        private FieldIds(LanceIndexMetadata indexMetadata, LancePredicateConverter converter) {
            this.indexMetadata = indexMetadata;
            this.converter = converter;
        }

        private OptionalInt of(ConnectorExpression column) {
            return converter.resolveFieldName(column)
                    .map(indexMetadata::getLanceFieldId)
                    .orElse(OptionalInt.empty());
        }
    }
}
