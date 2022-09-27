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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Select rollup index when aggregate is present.
 */
@Developing
public class SelectRollupWithAggregate implements RewriteRuleFactory {
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
                    Pair<PreAggStatus, List<Long>> result = selectCandidateRollupIds(
                            scan,
                            agg.getInputSlots(),
                            ImmutableList.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            agg.getGroupByExpressions());
                    return agg.withChildren(
                            scan.withMaterializedIndexSelected(result.key(), result.value())
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

                            Pair<PreAggStatus, List<Long>> result = selectCandidateRollupIds(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    agg.getGroupByExpressions()
                            );
                            return agg.withChildren(filter.withChildren(
                                    scan.withMaterializedIndexSelected(result.key(), result.value())
                            ));
                        }).toRule(RuleType.ROLLUP_AGG_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(logicalOlapScan().when(LogicalOlapScan::shouldSelectRollup)))
                        .then(agg -> {
                            LogicalProject<LogicalOlapScan> project = agg.child();
                            LogicalOlapScan scan = project.child();
                            Pair<PreAggStatus, List<Long>> result = selectCandidateRollupIds(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableList.of(),
                                    extractAggFunctionAndReplaceSlot(agg,
                                            Optional.of(project)),
                                    agg.getGroupByExpressions()
                            );
                            return agg.withChildren(
                                    project.withChildren(
                                            scan.withMaterializedIndexSelected(result.key(), result.value())
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
                            Pair<PreAggStatus, List<Long>> result = selectCandidateRollupIds(
                                    scan,
                                    agg.getInputSlots(),
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );
                            return agg.withChildren(project.withChildren(filter.withChildren(
                                    scan.withMaterializedIndexSelected(result.key(), result.value())
                            )));
                        }).toRule(RuleType.ROLLUP_AGG_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(logicalOlapScan()
                        .when(LogicalOlapScan::shouldSelectRollup)))).then(agg -> {
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = agg.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            Pair<PreAggStatus, List<Long>> result = selectCandidateRollupIds(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableList.of(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );
                            return agg.withChildren(filter.withChildren(project.withChildren(
                                    scan.withMaterializedIndexSelected(result.key(), result.value())
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
     * 0. turn off pre agg, checking input aggregate functions and group by expressions, etc.
     * 1. rollup contains all the required output slots.
     * 2. match the most prefix index if pushdown predicates present.
     * 3. sort the result matching rollup index ids.
     */
    private Pair<PreAggStatus, List<Long>> selectCandidateRollupIds(
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            List<Expression> predicates,
            // not used now, reserved for checking aggregate function type match.
            List<AggregateFunction> aggregateFunctions,
            List<Expression> groupingExprs) {
        Preconditions.checkArgument(Sets.newHashSet(scan.getOutput()).containsAll(requiredScanOutput),
                String.format("Scan's output (%s) should contains all the input required scan output (%s).",
                        scan.getOutput(), requiredScanOutput));

        // 0. maybe turn off pre agg.
        PreAggStatus preAggStatus = checkPreAggStatus(scan, predicates, aggregateFunctions, groupingExprs);
        if (preAggStatus.isOff()) {
            // return early if pre agg status if off.
            return Pair.of(preAggStatus, ImmutableList.of(scan.getTable().getBaseIndexId()));
        }

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

        // 1. filter rollup contains all the required columns by column name.
        List<MaterializedIndex> containAllRequiredColumns = table.getVisibleIndex().stream()
                .filter(rollup -> table.getSchemaByIndexId(rollup.getId(), true)
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toSet())
                        .containsAll(requiredColumnNames)
                ).collect(Collectors.toList());

        Map<Boolean, Set<String>> split = filterCanUsePrefixIndexAndSplitByEquality(predicates, exprIdToName);
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

        List<Long> partitionIds = scan.getSelectedPartitionIds();
        // 3. sort by row count, column count and index id.
        List<Long> sortedIndexId = matchingKeyPrefixMost.stream()
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
        return Pair.of(preAggStatus, sortedIndexId);
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
        Optional<Map<Slot, Expression>> slotToProducerOpt = project.map(Project::getAliasToProducer);
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

    ///////////////////////////////////////////////////////////////////////////
    // Set pre-aggregation status.
    ///////////////////////////////////////////////////////////////////////////
    private PreAggStatus checkPreAggStatus(
            LogicalOlapScan olapScan,
            List<Expression> predicates,
            List<AggregateFunction> aggregateFuncs,
            List<Expression> groupingExprs) {
        CheckContext checkContext = new CheckContext(olapScan);
        return checkAggregateFunctions(aggregateFuncs, checkContext)
                .offOrElse(() -> checkGroupingExprs(groupingExprs, checkContext))
                .offOrElse(() -> checkPredicates(predicates, checkContext));
    }

    /**
     * Check pre agg status according to aggregate functions.
     */
    private PreAggStatus checkAggregateFunctions(
            List<AggregateFunction> aggregateFuncs,
            CheckContext checkContext) {
        return aggregateFuncs.stream()
                .map(f -> AggregateFunctionChecker.INSTANCE.check(f, checkContext))
                .filter(PreAggStatus::isOff)
                .findAny()
                .orElse(PreAggStatus.on());
    }

    // TODO: support all the aggregate function types in storage engine.
    private static class AggregateFunctionChecker extends ExpressionVisitor<PreAggStatus, CheckContext> {

        public static final AggregateFunctionChecker INSTANCE = new AggregateFunctionChecker();

        public PreAggStatus check(AggregateFunction aggFun, CheckContext ctx) {
            return aggFun.accept(INSTANCE, ctx);
        }

        @Override
        public PreAggStatus visit(Expression expr, CheckContext context) {
            return PreAggStatus.off(String.format("%s is not aggregate function.", expr.toSql()));
        }

        @Override
        public PreAggStatus visitAggregateFunction(AggregateFunction aggregateFunction, CheckContext context) {
            return PreAggStatus.off(String.format("Aggregate %s function is not supported in storage engine.",
                    aggregateFunction.getName()));
        }

        @Override
        public PreAggStatus visitMax(Max max, CheckContext context) {
            return checkAggFunc(max, AggregateType.MAX, extractSlotId(max.child()), context, true);
        }

        @Override
        public PreAggStatus visitMin(Min min, CheckContext context) {
            return checkAggFunc(min, AggregateType.MIN, extractSlotId(min.child()), context, true);
        }

        @Override
        public PreAggStatus visitSum(Sum sum, CheckContext context) {
            return checkAggFunc(sum, AggregateType.SUM, extractSlotId(sum.child()), context, false);
        }

        private PreAggStatus checkAggFunc(
                AggregateFunction aggFunc,
                AggregateType matchingAggType,
                Optional<ExprId> exprIdOpt,
                CheckContext ctx,
                boolean canUseKeyColumn) {
            return exprIdOpt.map(exprId -> {
                if (ctx.exprIdToKeyColumn.containsKey(exprId)) {
                    if (canUseKeyColumn) {
                        return PreAggStatus.on();
                    } else {
                        Column column = ctx.exprIdToKeyColumn.get(exprId);
                        return PreAggStatus.off(String.format("Aggregate function %s contains key column %s.",
                                aggFunc.toSql(), column.getName()));
                    }
                } else if (ctx.exprIdToValueColumn.containsKey(exprId)) {
                    AggregateType aggType = ctx.exprIdToValueColumn.get(exprId).getAggregationType();
                    if (aggType == matchingAggType) {
                        return PreAggStatus.on();
                    } else {
                        return PreAggStatus.off(String.format("Aggregate operator don't match, aggregate function: %s"
                                + ", column aggregate type: %s", aggFunc.toSql(), aggType));
                    }
                } else {
                    return PreAggStatus.off(String.format("Slot(%s) in %s is neither key column nor value column.",
                            exprId, aggFunc.toSql()));
                }
            }).orElse(PreAggStatus.off(String.format("Input of aggregate function %s should be slot or cast on slot.",
                    aggFunc.toSql())));
        }

        // TODO: support more type of expressions, such as case when.
        private Optional<ExprId> extractSlotId(Expression expr) {
            return ExpressionUtils.isSlotOrCastOnSlot(expr);
        }
    }

    private static class CheckContext {
        public final Map<ExprId, Column> exprIdToKeyColumn;
        public final Map<ExprId, Column> exprIdToValueColumn;

        public CheckContext(LogicalOlapScan scan) {
            // map<is_key, map<column_name, column>>
            Map<Boolean, Map<String, Column>> nameToColumnGroupingByIsKey = scan.getTable().getBaseSchema()
                    .stream()
                    .collect(Collectors.groupingBy(
                            Column::isKey,
                            Collectors.mapping(Function.identity(),
                                    Collectors.toMap(Column::getName, Function.identity())
                            )
                    ));
            Map<String, Column> keyNameToColumn = nameToColumnGroupingByIsKey.get(true);
            Map<String, Column> valueNameToColumn = nameToColumnGroupingByIsKey.get(false);
            Map<String, ExprId> nameToExprId = scan.getOutput()
                    .stream()
                    .collect(Collectors.toMap(
                            NamedExpression::getName,
                            NamedExpression::getExprId)
                    );
            this.exprIdToKeyColumn = keyNameToColumn.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> nameToExprId.get(e.getKey()),
                            Entry::getValue)
                    );
            this.exprIdToValueColumn = valueNameToColumn.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> nameToExprId.get(e.getKey()),
                            Entry::getValue)
                    );
        }
    }

    /**
     * Grouping expressions should not have value type columns.
     */
    private PreAggStatus checkGroupingExprs(
            List<Expression> groupingExprs,
            CheckContext checkContext) {
        return checkHasNoValueTypeColumn(groupingExprs, checkContext,
                "Grouping expression %s contains value column %s");
    }

    /**
     * Predicates should not have value type columns.
     */
    private PreAggStatus checkPredicates(
            List<Expression> predicates,
            CheckContext checkContext) {
        return checkHasNoValueTypeColumn(predicates, checkContext,
                "Predicate %s contains value column %s");
    }

    /**
     * Check the input expressions have no referenced slot to underlying value type column.
     */
    private PreAggStatus checkHasNoValueTypeColumn(List<Expression> exprs, CheckContext ctx, String errorMsg) {
        Map<ExprId, Column> exprIdToValueColumn = ctx.exprIdToValueColumn;
        return exprs.stream()
                .map(expr -> expr.getInputSlots()
                        .stream()
                        .filter(slot -> exprIdToValueColumn.containsKey(slot.getExprId()))
                        .findAny()
                        .map(slot -> Pair.of(expr, exprIdToValueColumn.get(slot.getExprId())))
                )
                .filter(Optional::isPresent)
                .findAny()
                .orElse(Optional.empty())
                .map(exprToColumn -> PreAggStatus.off(String.format(errorMsg,
                        exprToColumn.key().toSql(), exprToColumn.value().getName())))
                .orElse(PreAggStatus.on());
    }
}
