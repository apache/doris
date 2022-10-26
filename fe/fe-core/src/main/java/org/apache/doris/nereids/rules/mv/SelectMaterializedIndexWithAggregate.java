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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Select materialized index, i.e., both for rollup and materialized view when aggregate is present.
 * TODO: optimize queries with aggregate not on top of scan directly, e.g., aggregate -> join -> scan
 *   to use materialized index.
 */
@Developing
public class SelectMaterializedIndexWithAggregate extends AbstractSelectMaterializedIndexRule
        implements RewriteRuleFactory {
    ///////////////////////////////////////////////////////////////////////////
    // All the patterns
    ///////////////////////////////////////////////////////////////////////////
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // only agg above scan
                // Aggregate(Scan)
                logicalAggregate(logicalOlapScan().when(this::shouldSelectIndex)).then(agg -> {
                    LogicalOlapScan scan = agg.child();
                    Pair<PreAggStatus, List<Long>> result = select(
                            scan,
                            agg.getInputSlots(),
                            ImmutableList.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            agg.getGroupByExpressions());
                    return agg.withChildren(
                            scan.withMaterializedIndexSelected(result.key(), result.value())
                    );
                }).toRule(RuleType.MATERIALIZED_INDEX_AGG_SCAN),

                // filter could push down scan.
                // Aggregate(Filter(Scan))
                logicalAggregate(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(agg -> {
                            LogicalFilter<LogicalOlapScan> filter = agg.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();

                            Pair<PreAggStatus, List<Long>> result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    agg.getGroupByExpressions()
                            );
                            return agg.withChildren(filter.withChildren(
                                    scan.withMaterializedIndexSelected(result.key(), result.value())
                            ));
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(agg -> {
                            LogicalProject<LogicalOlapScan> project = agg.child();
                            LogicalOlapScan scan = project.child();
                            Pair<PreAggStatus, List<Long>> result = select(
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
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Project(Filter(Scan)))
                logicalAggregate(logicalProject(logicalFilter(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).then(agg -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            Set<Slot> requiredSlots = Stream.concat(
                                    project.getInputSlots().stream(), filter.getInputSlots().stream())
                                    .collect(Collectors.toSet());
                            Pair<PreAggStatus, List<Long>> result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );
                            return agg.withChildren(project.withChildren(filter.withChildren(
                                    scan.withMaterializedIndexSelected(result.key(), result.value())
                            )));
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).then(agg -> {
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = agg.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            Pair<PreAggStatus, List<Long>> result = select(
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
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_PROJECT_SCAN)
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    // Main entrance of select materialized index.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Select materialized index ids.
     * <p>
     * 1. find candidate indexes by pre-agg status: checking input aggregate functions and group by expressions
     * and pushdown predicates.
     * 2. filter and order the candidate indexes.
     */
    private Pair<PreAggStatus, List<Long>> select(
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            List<Expression> predicates,
            List<AggregateFunction> aggregateFunctions,
            List<Expression> groupingExprs) {
        Preconditions.checkArgument(scan.getOutputSet().containsAll(requiredScanOutput),
                String.format("Scan's output (%s) should contains all the input required scan output (%s).",
                        scan.getOutput(), requiredScanOutput));

        OlapTable table = scan.getTable();

        // 0. check pre-aggregation status.
        final PreAggStatus preAggStatus;
        final Stream<MaterializedIndex> checkPreAggResult;
        switch (table.getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                // Check pre-aggregation status by base index for aggregate-keys and unique-keys OLAP table.
                preAggStatus = checkPreAggStatus(scan, table.getBaseIndexId(), predicates,
                        aggregateFunctions, groupingExprs);
                if (preAggStatus.isOff()) {
                    // return early if pre agg status if off.
                    return Pair.of(preAggStatus, ImmutableList.of(scan.getTable().getBaseIndexId()));
                }
                checkPreAggResult = table.getVisibleIndex().stream();
                break;
            case DUP_KEYS:
                Map<Boolean, List<MaterializedIndex>> indexesGroupByIsBaseOrNot = table.getVisibleIndex()
                        .stream()
                        .collect(Collectors.groupingBy(index -> index.getId() == table.getBaseIndexId()));

                // Duplicate-keys table could use base index and indexes that pre-aggregation status is on.
                checkPreAggResult = Stream.concat(
                        indexesGroupByIsBaseOrNot.get(true).stream(),
                        indexesGroupByIsBaseOrNot.getOrDefault(false, ImmutableList.of())
                                .stream()
                                .filter(index -> checkPreAggStatus(scan, index.getId(), predicates,
                                        aggregateFunctions, groupingExprs).isOn())
                );

                // Pre-aggregation is set to `on` by default for duplicate-keys table.
                preAggStatus = PreAggStatus.on();
                break;
            default:
                throw new RuntimeException("Not supported keys type: " + table.getKeysType());
        }

        List<Long> sortedIndexId = filterAndOrder(checkPreAggResult, scan, requiredScanOutput, predicates);
        return Pair.of(preAggStatus, sortedIndexId);
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
            long indexId,
            List<Expression> predicates,
            List<AggregateFunction> aggregateFuncs,
            List<Expression> groupingExprs) {
        CheckContext checkContext = new CheckContext(olapScan, indexId);
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

        // TODO: select count(xxx) for duplicated-keys table.
        @Override
        public PreAggStatus visitCount(Count count, CheckContext context) {
            // Now count(distinct key_column) is only supported for aggregate-keys and unique-keys OLAP table.
            if (count.isDistinct()) {
                Optional<ExprId> exprIdOpt = extractSlotId(count.child(0));
                if (exprIdOpt.isPresent() && context.exprIdToKeyColumn.containsKey(exprIdOpt.get())) {
                    return PreAggStatus.on();
                }
            }
            return PreAggStatus.off(String.format(
                    "Count distinct is only valid for key columns, but meet %s.", count.toSql()));
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

        public CheckContext(LogicalOlapScan scan, long indexId) {
            // map<is_key, map<column_name, column>>
            Map<Boolean, Map<String, Column>> nameToColumnGroupingByIsKey
                    = scan.getTable().getSchemaByIndexId(indexId)
                    .stream()
                    .collect(Collectors.groupingBy(
                            Column::isKey,
                            Collectors.toMap(Column::getName, Function.identity())
                    ));
            Map<String, Column> keyNameToColumn = nameToColumnGroupingByIsKey.get(true);
            Map<String, Column> valueNameToColumn = nameToColumnGroupingByIsKey.getOrDefault(false, ImmutableMap.of());
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
        return disablePreAggIfContainsAnyValueColumn(groupingExprs, checkContext,
                "Grouping expression %s contains value column %s");
    }

    /**
     * Predicates should not have value type columns.
     */
    private PreAggStatus checkPredicates(
            List<Expression> predicates,
            CheckContext checkContext) {
        return disablePreAggIfContainsAnyValueColumn(predicates, checkContext,
                "Predicate %s contains value column %s");
    }

    /**
     * Check the input expressions have no referenced slot to underlying value type column.
     */
    private PreAggStatus disablePreAggIfContainsAnyValueColumn(List<Expression> exprs, CheckContext ctx,
            String errorMsg) {
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
