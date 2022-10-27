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

package org.apache.doris.nereids.rules.mv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for selecting materialized index rules.
 */
public abstract class AbstractSelectMaterializedIndexRule {
    protected boolean shouldSelectIndex(LogicalOlapScan scan) {
        switch (scan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
            case DUP_KEYS:
                return !scan.isIndexSelected();
            default:
                return false;
        }
    }

    /**
     * 1. indexes have all the required columns.
     * 2. find matching key prefix most.
     * 3. sort by row count, column count and index id.
     */
    protected List<Long> filterAndOrder(
            Stream<MaterializedIndex> candidates,
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            List<Expression> predicates) {

        OlapTable table = scan.getTable();
        // Scan slot exprId -> slot name
        Map<ExprId, String> exprIdToName = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(NamedExpression::getExprId, NamedExpression::getName));

        // get required column names in metadata.
        Set<String> requiredColumnNames = requiredScanOutput
                .stream()
                .map(slot -> exprIdToName.get(slot.getExprId()))
                .collect(Collectors.toSet());

        // 1. filter index contains all the required columns by column name.
        List<MaterializedIndex> containAllRequiredColumns = candidates
                .filter(index -> table.getSchemaByIndexId(index.getId(), true)
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toSet())
                        .containsAll(requiredColumnNames)
                ).collect(Collectors.toList());

        // 2. find matching key prefix most.
        List<MaterializedIndex> matchingKeyPrefixMost = matchPrefixMost(scan, containAllRequiredColumns, predicates,
                exprIdToName);

        List<Long> partitionIds = scan.getSelectedPartitionIds();
        // 3. sort by row count, column count and index id.
        return matchingKeyPrefixMost.stream()
                .map(MaterializedIndex::getId)
                .sorted(Comparator
                        // compare by row count
                        .comparing(rid -> partitionIds.stream()
                                .mapToLong(pid -> table.getPartition(pid).getIndex((Long) rid).getRowCount())
                                .sum())
                        // compare by column count
                        .thenComparing(rid -> table.getSchemaByIndexId((Long) rid).size())
                        // compare by index id
                        .thenComparing(rid -> (Long) rid))
                .collect(Collectors.toList());
    }

    protected List<MaterializedIndex> matchPrefixMost(
            LogicalOlapScan scan,
            List<MaterializedIndex> candidate,
            List<Expression> predicates,
            Map<ExprId, String> exprIdToName) {
        Map<Boolean, Set<String>> split = filterCanUsePrefixIndexAndSplitByEquality(predicates, exprIdToName);
        Set<String> equalColNames = split.getOrDefault(true, ImmutableSet.of());
        Set<String> nonEqualColNames = split.getOrDefault(false, ImmutableSet.of());

        if (!(equalColNames.isEmpty() && nonEqualColNames.isEmpty())) {
            List<MaterializedIndex> matchingResult = matchKeyPrefixMost(scan.getTable(), candidate,
                    equalColNames, nonEqualColNames);
            return matchingResult.isEmpty() ? candidate : matchingResult;
        } else {
            return candidate;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Split conjuncts into equal-to and non-equal-to.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Filter the input conjuncts those can use prefix and split into 2 groups: is equal-to or non-equal-to predicate
     * when comparing the key column.
     */
    private Map<Boolean, Set<String>> filterCanUsePrefixIndexAndSplitByEquality(
            List<Expression> conjunct, Map<ExprId, String> exprIdToColName) {
        return conjunct.stream()
                .map(expr -> PredicateChecker.canUsePrefixIndex(expr, exprIdToColName))
                .filter(result -> !result.equals(PrefixIndexCheckResult.FAILURE))
                .collect(Collectors.groupingBy(
                        result -> result.type == ResultType.SUCCESS_EQUAL,
                        Collectors.mapping(result -> result.colName, Collectors.toSet())
                ));
    }

    private enum ResultType {
        FAILURE,
        SUCCESS_EQUAL,
        SUCCESS_NON_EQUAL,
    }

    private static class PrefixIndexCheckResult {
        public static final PrefixIndexCheckResult FAILURE = new PrefixIndexCheckResult(null, ResultType.FAILURE);
        private final String colName;
        private final ResultType type;

        private PrefixIndexCheckResult(String colName, ResultType result) {
            this.colName = colName;
            this.type = result;
        }

        public static PrefixIndexCheckResult createEqual(String name) {
            return new PrefixIndexCheckResult(name, ResultType.SUCCESS_EQUAL);
        }

        public static PrefixIndexCheckResult createNonEqual(String name) {
            return new PrefixIndexCheckResult(name, ResultType.SUCCESS_NON_EQUAL);
        }
    }

    /**
     * Check if an expression could prefix key index.
     */
    private static class PredicateChecker extends ExpressionVisitor<PrefixIndexCheckResult, Map<ExprId, String>> {
        private static final PredicateChecker INSTANCE = new PredicateChecker();

        private PredicateChecker() {
        }

        public static PrefixIndexCheckResult canUsePrefixIndex(Expression expression,
                Map<ExprId, String> exprIdToName) {
            return expression.accept(INSTANCE, exprIdToName);
        }

        @Override
        public PrefixIndexCheckResult visit(Expression expr, Map<ExprId, String> context) {
            return PrefixIndexCheckResult.FAILURE;
        }

        @Override
        public PrefixIndexCheckResult visitInPredicate(InPredicate in, Map<ExprId, String> context) {
            Optional<ExprId> slotOrCastOnSlot = ExpressionUtils.isSlotOrCastOnSlot(in.getCompareExpr());
            if (slotOrCastOnSlot.isPresent() && in.getOptions().stream().allMatch(Literal.class::isInstance)) {
                return PrefixIndexCheckResult.createEqual(context.get(slotOrCastOnSlot.get()));
            } else {
                return PrefixIndexCheckResult.FAILURE;
            }
        }

        @Override
        public PrefixIndexCheckResult visitComparisonPredicate(ComparisonPredicate cp, Map<ExprId, String> context) {
            if (cp instanceof EqualTo || cp instanceof NullSafeEqual) {
                return check(cp, context, PrefixIndexCheckResult::createEqual);
            } else {
                return check(cp, context, PrefixIndexCheckResult::createNonEqual);
            }
        }

        private PrefixIndexCheckResult check(ComparisonPredicate cp, Map<ExprId, String> exprIdToColumnName,
                Function<String, PrefixIndexCheckResult> resultMapper) {
            return check(cp).map(exprId -> resultMapper.apply(exprIdToColumnName.get(exprId)))
                    .orElse(PrefixIndexCheckResult.FAILURE);
        }

        private Optional<ExprId> check(ComparisonPredicate cp) {
            Optional<ExprId> exprId = check(cp.left(), cp.right());
            return exprId.isPresent() ? exprId : check(cp.right(), cp.left());
        }

        private Optional<ExprId> check(Expression maybeSlot, Expression maybeConst) {
            Optional<ExprId> exprIdOpt = ExpressionUtils.isSlotOrCastOnSlot(maybeSlot);
            return exprIdOpt.isPresent() && maybeConst.isConstant() ? exprIdOpt : Optional.empty();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Matching key prefix
    ///////////////////////////////////////////////////////////////////////////
    private List<MaterializedIndex> matchKeyPrefixMost(
            OlapTable table,
            List<MaterializedIndex> indexes,
            Set<String> equalColumns,
            Set<String> nonEqualColumns) {
        TreeMap<Integer, List<MaterializedIndex>> collect = indexes.stream()
                .collect(Collectors.toMap(
                        index -> indexKeyPrefixMatchCount(table, index, equalColumns, nonEqualColumns),
                        Lists::newArrayList,
                        (l1, l2) -> {
                            l1.addAll(l2);
                            return l1;
                        },
                        TreeMap::new)
                );
        return collect.descendingMap().firstEntry().getValue();
    }

    private int indexKeyPrefixMatchCount(
            OlapTable table,
            MaterializedIndex index,
            Set<String> equalColNames,
            Set<String> nonEqualColNames) {
        int matchCount = 0;
        for (Column column : table.getSchemaByIndexId(index.getId())) {
            if (equalColNames.contains(column.getName())) {
                matchCount++;
            } else if (nonEqualColNames.contains(column.getName())) {
                // Unequivalence predicate's columns can match only first column in index.
                matchCount++;
                break;
            } else {
                break;
            }
        }
        return matchCount;
    }
}
