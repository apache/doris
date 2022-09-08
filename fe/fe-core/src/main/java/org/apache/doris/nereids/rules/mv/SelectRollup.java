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
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Select rollup.
 */
@Developing
public class SelectRollup implements RewriteRuleFactory {
    ///////////////////////////////////////////////////////////////////////////
    // All the patterns
    ///////////////////////////////////////////////////////////////////////////
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // only agg above scan
                // Aggregate(Scan)
                logicalAggregate(logicalOlapScan().when(LogicalOlapScan::shouldSelectRollup)).then(agg -> {
                    LogicalOlapScan scan = agg.child();
                    return agg.withChildren(
                            scan.withCandidateIndexIds(
                                    selectCandidateRollupIds(
                                            scan,
                                            agg.getInputSlots(),
                                            ImmutableList.of(),
                                            extractAggFunctionAndReplaceSlot(agg, Optional.empty())
                                    )
                            )
                    );
                }).toRule(RuleType.ROLLUP_AGG_SCAN),

                // filter could push down scan.
                // Aggregate(Filter(Scan))
                logicalAggregate(logicalFilter(logicalOlapScan().when(LogicalOlapScan::shouldSelectRollup)))
                        .then(agg -> {
                            LogicalFilter<LogicalOlapScan> filter = agg.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();
                            return agg.withChildren(filter.withChildren(
                                    scan.withCandidateIndexIds(
                                            selectCandidateRollupIds(
                                                    scan,
                                                    requiredSlots,
                                                    filter.getConjuncts(),
                                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty())
                                            )
                                    )
                            ));
                        }).toRule(RuleType.ROLLUP_AGG_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(logicalOlapScan().when(LogicalOlapScan::shouldSelectRollup)))
                        .then(agg -> {
                            LogicalProject<LogicalOlapScan> project = agg.child();
                            LogicalOlapScan scan = project.child();
                            return agg.withChildren(
                                    project.withChildren(
                                            scan.withCandidateIndexIds(
                                                    selectCandidateRollupIds(
                                                            scan,
                                                            project.getInputSlots(),
                                                            ImmutableList.of(),
                                                            extractAggFunctionAndReplaceSlot(agg,
                                                                    Optional.of(project))
                                                    )
                                            )
                                    )
                            );
                        }).toRule(RuleType.ROLLUP_AGG_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Project(Filter(Scan)))
                logicalAggregate(logicalProject(logicalFilter(logicalOlapScan()
                        .when(LogicalOlapScan::shouldSelectRollup)))).then(agg -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return agg.withChildren(project.withChildren(filter.withChildren(
                                    scan.withCandidateIndexIds(selectCandidateRollupIds(
                                                    scan,
                                                    agg.getInputSlots(),
                                                    filter.getConjuncts(),
                                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project))
                                            )
                                    )
                            )));
                        }).toRule(RuleType.ROLLUP_AGG_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(logicalOlapScan()
                        .when(LogicalOlapScan::shouldSelectRollup)))).then(agg -> {
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = agg.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            return agg.withChildren(filter.withChildren(project.withChildren(
                                    scan.withCandidateIndexIds(selectCandidateRollupIds(
                                                    scan,
                                                    project.getInputSlots(),
                                                    ImmutableList.of(),
                                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project))
                                            )
                                    )
                            )));
                        }).toRule(RuleType.ROLLUP_AGG_FILTER_PROJECT_SCAN)
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    // Main entrance of select rollup
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Select candidate rollup ids.
     * <p>
     * todo: 0. turn off pre agg, checking input aggregate functions and group by expressions, etc.
     * 1. rollup contains all the required output slots.
     * 2. match the most prefix index if pushdown predicates present.
     */
    private List<Long> selectCandidateRollupIds(
            LogicalOlapScan olapScan,
            Set<Slot> requiredScanOutput,
            List<Expression> predicates,
            // not used now, reserved for checking aggregate function type match.
            List<AggregateFunction> aggregateFunctions) {
        Preconditions.checkArgument(Sets.newHashSet(olapScan.getOutput()).containsAll(requiredScanOutput),
                "Scan's output should contains all the input required scan output.");

        OlapTable table = olapScan.getTable();

        // Scan slot exprId -> slot name
        Map<ExprId, String> exprIdToName = olapScan.getOutput()
                .stream()
                .collect(Collectors.toMap(NamedExpression::getExprId, NamedExpression::getName));

        // get required column names in metadata.
        Set<String> requiredColumnNames = requiredScanOutput
                .stream()
                .map(slot -> exprIdToName.get(slot.getExprId()))
                .collect(Collectors.toSet());

        // 1. filter rollup contains all the required columns by column name.
        List<MaterializedIndex> containAllRequiredColumns = table.getVisibleIndex().stream()
                .filter(rollup -> table.getSchemaByIndexId(rollup.getId(), true)
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toSet())
                        .containsAll(requiredColumnNames)
                ).collect(Collectors.toList());

        Map<Boolean, Set<String>> split = split(predicates, exprIdToName);
        Set<String> equalColNames = split.getOrDefault(true, ImmutableSet.of());
        Set<String> nonEqualColNames = split.getOrDefault(false, ImmutableSet.of());

        // 2. find matching key prefix most.
        List<MaterializedIndex> matchingKeyPrefixMost;
        if (!(equalColNames.isEmpty() && nonEqualColNames.isEmpty())) {
            List<MaterializedIndex> matchingResult = matchKeyPrefixMost(table, containAllRequiredColumns,
                    equalColNames, nonEqualColNames);
            matchingKeyPrefixMost = matchingResult.isEmpty() ? containAllRequiredColumns : matchingResult;
        } else {
            matchingKeyPrefixMost = containAllRequiredColumns;
        }

        List<Long> partitionIds = olapScan.getSelectedPartitionIds();
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
                        // compare by rollup index id
                        .thenComparing(rid -> (Long) rid))
                .collect(Collectors.toList());
    }

    ///////////////////////////////////////////////////////////////////////////
    // Matching key prefix
    ///////////////////////////////////////////////////////////////////////////
    private List<MaterializedIndex> matchKeyPrefixMost(
            OlapTable table,
            List<MaterializedIndex> rollups,
            Set<String> equalColumns,
            Set<String> nonEqualColumns) {
        TreeMap<Integer, List<MaterializedIndex>> collect = rollups.stream()
                .collect(Collectors.toMap(
                        rollup -> rollupKeyPrefixMatchCount(table, rollup, equalColumns, nonEqualColumns),
                        Lists::newArrayList,
                        (l1, l2) -> {
                            l1.addAll(l2);
                            return l1;
                        },
                        TreeMap::new)
                );
        return collect.descendingMap().firstEntry().getValue();
    }

    private int rollupKeyPrefixMatchCount(
            OlapTable table,
            MaterializedIndex rollup,
            Set<String> equalColNames,
            Set<String> nonEqualColNames) {
        int matchCount = 0;
        for (Column column : table.getSchemaByIndexId(rollup.getId())) {
            if (equalColNames.contains(column.getName())) {
                matchCount++;
            } else if (nonEqualColNames.contains(column.getName())) {
                // Unequivalence predicate's columns can match only first column in rollup.
                matchCount++;
                break;
            } else {
                break;
            }
        }
        return matchCount;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Split conjuncts into equal-to and non-equal-to.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Split conjuncts input equal-to and non-equal-to.
     */
    private Map<Boolean, Set<String>> split(List<Expression> conjunct, Map<ExprId, String> exprIdToColName) {
        return conjunct.stream()
                .map(expr -> PredicateChecker.canUsePrefixIndex(expr, exprIdToColName))
                .filter(result -> !result.equals(CheckResult.FAILURE))
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

    private static class CheckResult {
        public static final CheckResult FAILURE = new CheckResult(null, ResultType.FAILURE);
        private final String colName;
        private final ResultType type;

        private CheckResult(String colName, ResultType result) {
            this.colName = colName;
            this.type = result;
        }

        public static CheckResult createEqual(String name) {
            return new CheckResult(name, ResultType.SUCCESS_EQUAL);
        }

        public static CheckResult createNonEqual(String name) {
            return new CheckResult(name, ResultType.SUCCESS_NON_EQUAL);
        }
    }

    /**
     * Check if an expression could prefix key index.
     */
    private static class PredicateChecker extends ExpressionVisitor<CheckResult, Map<ExprId, String>> {
        private static final PredicateChecker INSTANCE = new PredicateChecker();

        private PredicateChecker() {
        }

        public static CheckResult canUsePrefixIndex(Expression expression, Map<ExprId, String> exprIdToName) {
            return expression.accept(INSTANCE, exprIdToName);
        }

        @Override
        public CheckResult visit(Expression expr, Map<ExprId, String> context) {
            return CheckResult.FAILURE;
        }

        @Override
        public CheckResult visitInPredicate(InPredicate in, Map<ExprId, String> context) {
            Optional<ExprId> slotOrCastOnSlot = ExpressionUtils.isSlotOrCastOnSlot(in.getCompareExpr());
            if (slotOrCastOnSlot.isPresent() && in.getOptions().stream().allMatch(Literal.class::isInstance)) {
                return CheckResult.createEqual(context.get(slotOrCastOnSlot.get()));
            } else {
                return CheckResult.FAILURE;
            }
        }

        @Override
        public CheckResult visitComparisonPredicate(ComparisonPredicate cp, Map<ExprId, String> context) {
            if (cp instanceof EqualTo || cp instanceof NullSafeEqual) {
                return check(cp, context, CheckResult::createEqual);
            } else {
                return check(cp, context, CheckResult::createNonEqual);
            }
        }

        private CheckResult check(ComparisonPredicate cp, Map<ExprId, String> exprIdToColumnName,
                Function<String, CheckResult> resultMapper) {
            return check(cp).map(exprId -> resultMapper.apply(exprIdToColumnName.get(exprId)))
                    .orElse(CheckResult.FAILURE);
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

    /**
     * Do aggregate function extraction and replace aggregate function's input slots by underlying project.
     * <p>
     * 1. extract aggregate functions in aggregate plan.
     * <p>
     * 2. replace aggregate function's input slot by underlying project expression if project is present.
     * <p>
     * For example:
     * <pre>
     * input arguments:
     * agg: Aggregate(sum(v) as sum_value)
     * underlying project: Project(a + b as v)
     *
     * output:
     * sum(a + b)
     * </pre>
     */
    private List<AggregateFunction> extractAggFunctionAndReplaceSlot(
            LogicalAggregate<?> agg,
            Optional<LogicalProject<?>> project) {
        Optional<Map<Slot, Expression>> slotToProducerOpt = project.map(Project::getSlotToProducer);
        return agg.getOutputExpressions().stream()
                // extract aggregate functions.
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                // replace aggregate function's input slot by its producing expression.
                .map(expr -> slotToProducerOpt.map(slotToExpressions
                                -> (AggregateFunction) ExpressionUtils.replace(expr, slotToExpressions))
                        .orElse(expr)
                )
                .collect(Collectors.toList());
    }
}
