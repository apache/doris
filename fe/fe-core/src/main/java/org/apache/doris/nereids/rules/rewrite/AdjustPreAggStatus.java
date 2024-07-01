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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AdjustPreAggStatus
 */
@Developing
public class AdjustPreAggStatus implements RewriteRuleFactory {
    ///////////////////////////////////////////////////////////////////////////
    // All the patterns
    ///////////////////////////////////////////////////////////////////////////
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Aggregate(Scan)
                logicalAggregate(logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet))
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalOlapScan> agg = ctx.root;
                            LogicalOlapScan scan = agg.child();
                            PreAggStatus preAggStatus = checkKeysType(scan);
                            if (preAggStatus == PreAggStatus.unset()) {
                                List<AggregateFunction> aggregateFunctions =
                                        extractAggFunctionAndReplaceSlot(agg, Optional.empty());
                                List<Expression> groupByExpressions = agg.getGroupByExpressions();
                                Set<Expression> predicates = ImmutableSet.of();
                                preAggStatus = checkPreAggStatus(scan, predicates,
                                        aggregateFunctions, groupByExpressions);
                            }
                            return agg.withChildren(scan.withPreAggStatus(preAggStatus));
                        }).toRule(RuleType.PREAGG_STATUS_AGG_SCAN),

                // Aggregate(Filter(Scan))
                logicalAggregate(
                        logicalFilter(logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg = ctx.root;
                                    LogicalFilter<LogicalOlapScan> filter = agg.child();
                                    LogicalOlapScan scan = filter.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.empty());
                                        List<Expression> groupByExpressions =
                                                agg.getGroupByExpressions();
                                        Set<Expression> predicates = filter.getConjuncts();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(filter
                                            .withChildren(scan.withPreAggStatus(preAggStatus)));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_FILTER_SCAN),

                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalProject<LogicalOlapScan>> agg =
                                            ctx.root;
                                    LogicalProject<LogicalOlapScan> project = agg.child();
                                    LogicalOlapScan scan = project.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg,
                                                        Optional.of(project));
                                        List<Expression> groupByExpressions =
                                                ExpressionUtils.replace(agg.getGroupByExpressions(),
                                                        project.getAliasToProducer());
                                        Set<Expression> predicates = ImmutableSet.of();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(project
                                            .withChildren(scan.withPreAggStatus(preAggStatus)));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_PROJECT_SCAN),

                // Aggregate(Project(Filter(Scan)))
                logicalAggregate(logicalProject(logicalFilter(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet))))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalProject<LogicalFilter<LogicalOlapScan>>> agg = ctx.root;
                                    LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                                    LogicalFilter<LogicalOlapScan> filter = project.child();
                                    LogicalOlapScan scan = filter.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.of(project));
                                        List<Expression> groupByExpressions =
                                                ExpressionUtils.replace(agg.getGroupByExpressions(),
                                                        project.getAliasToProducer());
                                        Set<Expression> predicates = filter.getConjuncts();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(project.withChildren(filter
                                            .withChildren(scan.withPreAggStatus(preAggStatus))));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_PROJECT_FILTER_SCAN),

                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet))))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalFilter<LogicalProject<LogicalOlapScan>>> agg = ctx.root;
                                    LogicalFilter<LogicalProject<LogicalOlapScan>> filter =
                                            agg.child();
                                    LogicalProject<LogicalOlapScan> project = filter.child();
                                    LogicalOlapScan scan = project.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.of(project));
                                        List<Expression> groupByExpressions =
                                                ExpressionUtils.replace(agg.getGroupByExpressions(),
                                                        project.getAliasToProducer());
                                        Set<Expression> predicates = ExpressionUtils.replace(
                                                filter.getConjuncts(), project.getAliasToProducer());
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(filter.withChildren(project
                                            .withChildren(scan.withPreAggStatus(preAggStatus))));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_FILTER_PROJECT_SCAN),

                // Aggregate(Repeat(Scan))
                logicalAggregate(
                        logicalRepeat(logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalRepeat<LogicalOlapScan>> agg = ctx.root;
                                    LogicalRepeat<LogicalOlapScan> repeat = agg.child();
                                    LogicalOlapScan scan = repeat.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.empty());
                                        List<Expression> groupByExpressions = nonVirtualGroupByExprs(agg);
                                        Set<Expression> predicates = ImmutableSet.of();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(repeat
                                            .withChildren(scan.withPreAggStatus(preAggStatus)));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_REPEAT_SCAN),

                // Aggregate(Repeat(Filter(Scan)))
                logicalAggregate(logicalRepeat(logicalFilter(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet))))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalRepeat<LogicalFilter<LogicalOlapScan>>> agg = ctx.root;
                                    LogicalRepeat<LogicalFilter<LogicalOlapScan>> repeat = agg.child();
                                    LogicalFilter<LogicalOlapScan> filter = repeat.child();
                                    LogicalOlapScan scan = filter.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.empty());
                                        List<Expression> groupByExpressions =
                                                nonVirtualGroupByExprs(agg);
                                        Set<Expression> predicates = filter.getConjuncts();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(repeat.withChildren(filter
                                            .withChildren(scan.withPreAggStatus(preAggStatus))));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_REPEAT_FILTER_SCAN),

                // Aggregate(Repeat(Project(Scan)))
                logicalAggregate(logicalRepeat(logicalProject(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet))))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalRepeat<LogicalProject<LogicalOlapScan>>> agg = ctx.root;
                                    LogicalRepeat<LogicalProject<LogicalOlapScan>> repeat = agg.child();
                                    LogicalProject<LogicalOlapScan> project = repeat.child();
                                    LogicalOlapScan scan = project.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.empty());
                                        List<Expression> groupByExpressions =
                                                ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                                        project.getAliasToProducer());
                                        Set<Expression> predicates = ImmutableSet.of();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(repeat.withChildren(project
                                            .withChildren(scan.withPreAggStatus(preAggStatus))));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_REPEAT_PROJECT_SCAN),

                // Aggregate(Repeat(Project(Filter(Scan))))
                logicalAggregate(logicalRepeat(logicalProject(logicalFilter(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)))))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalRepeat<LogicalProject<LogicalFilter<LogicalOlapScan>>>> agg
                                            = ctx.root;
                                    LogicalRepeat<LogicalProject<LogicalFilter<LogicalOlapScan>>> repeat = agg.child();
                                    LogicalProject<LogicalFilter<LogicalOlapScan>> project = repeat.child();
                                    LogicalFilter<LogicalOlapScan> filter = project.child();
                                    LogicalOlapScan scan = filter.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.empty());
                                        List<Expression> groupByExpressions =
                                                ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                                        project.getAliasToProducer());
                                        Set<Expression> predicates = filter.getConjuncts();
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(repeat
                                            .withChildren(project.withChildren(filter.withChildren(
                                                    scan.withPreAggStatus(preAggStatus)))));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_REPEAT_PROJECT_FILTER_SCAN),

                // Aggregate(Repeat(Filter(Project(Scan))))
                logicalAggregate(logicalRepeat(logicalFilter(logicalProject(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)))))
                                .thenApply(ctx -> {
                                    LogicalAggregate<LogicalRepeat<LogicalFilter<LogicalProject<LogicalOlapScan>>>> agg
                                            = ctx.root;
                                    LogicalRepeat<LogicalFilter<LogicalProject<LogicalOlapScan>>> repeat = agg.child();
                                    LogicalFilter<LogicalProject<LogicalOlapScan>> filter = repeat.child();
                                    LogicalProject<LogicalOlapScan> project = filter.child();
                                    LogicalOlapScan scan = project.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions =
                                                extractAggFunctionAndReplaceSlot(agg, Optional.of(project));
                                        List<Expression> groupByExpressions =
                                                ExpressionUtils.replace(nonVirtualGroupByExprs(agg),
                                                        project.getAliasToProducer());
                                        Set<Expression> predicates = ExpressionUtils.replace(
                                                filter.getConjuncts(), project.getAliasToProducer());
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return agg.withChildren(repeat
                                            .withChildren(filter.withChildren(project.withChildren(
                                                    scan.withPreAggStatus(preAggStatus)))));
                                }).toRule(RuleType.PREAGG_STATUS_AGG_REPEAT_FILTER_PROJECT_SCAN),

                // Filter(Project(Scan))
                logicalFilter(logicalProject(
                        logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)))
                                .thenApply(ctx -> {
                                    LogicalFilter<LogicalProject<LogicalOlapScan>> filter = ctx.root;
                                    LogicalProject<LogicalOlapScan> project = filter.child();
                                    LogicalOlapScan scan = project.child();
                                    PreAggStatus preAggStatus = checkKeysType(scan);
                                    if (preAggStatus == PreAggStatus.unset()) {
                                        List<AggregateFunction> aggregateFunctions = ImmutableList.of();
                                        List<Expression> groupByExpressions = ImmutableList.of();
                                        Set<Expression> predicates = ExpressionUtils.replace(
                                                filter.getConjuncts(), project.getAliasToProducer());
                                        preAggStatus = checkPreAggStatus(scan, predicates,
                                                aggregateFunctions, groupByExpressions);
                                    }
                                    return filter.withChildren(project
                                            .withChildren(scan.withPreAggStatus(preAggStatus)));
                                }).toRule(RuleType.PREAGG_STATUS_FILTER_PROJECT_SCAN),

                // Filter(Scan)
                logicalFilter(logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet))
                        .thenApply(ctx -> {
                            LogicalFilter<LogicalOlapScan> filter = ctx.root;
                            LogicalOlapScan scan = filter.child();
                            PreAggStatus preAggStatus = checkKeysType(scan);
                            if (preAggStatus == PreAggStatus.unset()) {
                                List<AggregateFunction> aggregateFunctions = ImmutableList.of();
                                List<Expression> groupByExpressions = ImmutableList.of();
                                Set<Expression> predicates = filter.getConjuncts();
                                preAggStatus = checkPreAggStatus(scan, predicates,
                                        aggregateFunctions, groupByExpressions);
                            }
                            return filter.withChildren(scan.withPreAggStatus(preAggStatus));
                        }).toRule(RuleType.PREAGG_STATUS_FILTER_SCAN),

                // only scan.
                logicalOlapScan().when(LogicalOlapScan::isPreAggStatusUnSet)
                        .thenApply(ctx -> {
                            LogicalOlapScan scan = ctx.root;
                            PreAggStatus preAggStatus = checkKeysType(scan);
                            if (preAggStatus == PreAggStatus.unset()) {
                                List<AggregateFunction> aggregateFunctions = ImmutableList.of();
                                List<Expression> groupByExpressions = ImmutableList.of();
                                Set<Expression> predicates = ImmutableSet.of();
                                preAggStatus = checkPreAggStatus(scan, predicates,
                                        aggregateFunctions, groupByExpressions);
                            }
                            return scan.withPreAggStatus(preAggStatus);
                        }).toRule(RuleType.PREAGG_STATUS_SCAN));
    }

    ///////////////////////////////////////////////////////////////////////////
    // Set pre-aggregation status.
    ///////////////////////////////////////////////////////////////////////////

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
    private List<AggregateFunction> extractAggFunctionAndReplaceSlot(LogicalAggregate<?> agg,
            Optional<LogicalProject<?>> project) {
        Optional<Map<Slot, Expression>> slotToProducerOpt =
                project.map(Project::getAliasToProducer);
        return agg.getOutputExpressions().stream()
                // extract aggregate functions.
                .flatMap(e -> e.<AggregateFunction>collect(AggregateFunction.class::isInstance)
                        .stream())
                // replace aggregate function's input slot by its producing expression.
                .map(expr -> slotToProducerOpt
                        .map(slotToExpressions -> (AggregateFunction) ExpressionUtils.replace(expr,
                                slotToExpressions))
                        .orElse(expr))
                .collect(Collectors.toList());
    }

    private PreAggStatus checkKeysType(LogicalOlapScan olapScan) {
        long selectIndexId = olapScan.getSelectedIndexId();
        MaterializedIndexMeta meta = olapScan.getTable().getIndexMetaByIndexId(selectIndexId);
        if (meta.getKeysType() == KeysType.DUP_KEYS || (meta.getKeysType() == KeysType.UNIQUE_KEYS
                && olapScan.getTable().getEnableUniqueKeyMergeOnWrite())) {
            return PreAggStatus.on();
        } else {
            return PreAggStatus.unset();
        }
    }

    private PreAggStatus checkPreAggStatus(LogicalOlapScan olapScan, Set<Expression> predicates,
            List<AggregateFunction> aggregateFuncs, List<Expression> groupingExprs) {
        Set<Slot> outputSlots = olapScan.getOutputSet();
        Pair<Set<SlotReference>, Set<SlotReference>> splittedSlots = splitSlots(outputSlots);
        Set<SlotReference> keySlots = splittedSlots.first;
        Set<SlotReference> valueSlots = splittedSlots.second;
        Preconditions.checkState(outputSlots.size() == keySlots.size() + valueSlots.size(),
                "output slots contains no key or value slots");

        Set<Slot> groupingExprsInputSlots = ExpressionUtils.getInputSlotSet(groupingExprs);
        if (groupingExprsInputSlots.retainAll(keySlots)) {
            return PreAggStatus
                    .off(String.format("Grouping expression %s contains non-key column %s",
                            groupingExprs, groupingExprsInputSlots));
        }

        Set<Slot> predicateInputSlots = ExpressionUtils.getInputSlotSet(predicates);
        if (predicateInputSlots.retainAll(keySlots)) {
            return PreAggStatus.off(String.format("Predicate %s contains non-key column %s",
                    predicates, predicateInputSlots));
        }

        return checkAggregateFunctions(aggregateFuncs, groupingExprsInputSlots);
    }

    private Pair<Set<SlotReference>, Set<SlotReference>> splitSlots(Set<Slot> slots) {
        Set<SlotReference> keySlots = Sets.newHashSetWithExpectedSize(slots.size());
        Set<SlotReference> valueSlots = Sets.newHashSetWithExpectedSize(slots.size());
        for (Slot slot : slots) {
            if (slot instanceof SlotReference && ((SlotReference) slot).getColumn().isPresent()) {
                if (((SlotReference) slot).getColumn().get().isKey()) {
                    keySlots.add((SlotReference) slot);
                } else {
                    valueSlots.add((SlotReference) slot);
                }
            }
        }
        return Pair.of(keySlots, valueSlots);
    }

    private static Expression removeCast(Expression expression) {
        while (expression instanceof Cast) {
            expression = ((Cast) expression).child();
        }
        return expression;
    }

    private PreAggStatus checkAggWithKeyAndValueSlots(AggregateFunction aggFunc,
            Set<SlotReference> keySlots, Set<SlotReference> valueSlots) {
        Expression child = aggFunc.child(0);
        List<Expression> conditionExps = new ArrayList<>();
        List<Expression> returnExps = new ArrayList<>();

        // ignore cast
        while (child instanceof Cast) {
            if (!((Cast) child).getDataType().isNumericType()) {
                return PreAggStatus.off(String.format("%s is not numeric CAST.", child.toSql()));
            }
            child = child.child(0);
        }
        // step 1: extract all condition exprs and return exprs
        if (child instanceof If) {
            conditionExps.add(child.child(0));
            returnExps.add(removeCast(child.child(1)));
            returnExps.add(removeCast(child.child(2)));
        } else if (child instanceof CaseWhen) {
            CaseWhen caseWhen = (CaseWhen) child;
            // WHEN THEN
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                conditionExps.add(whenClause.getOperand());
                returnExps.add(removeCast(whenClause.getResult()));
            }
            // ELSE
            returnExps.add(removeCast(caseWhen.getDefaultValue().orElse(new NullLiteral())));
        } else {
            // currently, only IF and CASE WHEN are supported
            returnExps.add(removeCast(child));
        }

        // step 2: check condition expressions
        Set<Slot> inputSlots = ExpressionUtils.getInputSlotSet(conditionExps);
        inputSlots.retainAll(valueSlots);
        if (!inputSlots.isEmpty()) {
            return PreAggStatus
                    .off(String.format("some columns in condition %s is not key.", conditionExps));
        }

        return KeyAndValueSlotsAggChecker.INSTANCE.check(aggFunc, returnExps);
    }

    private PreAggStatus checkAggregateFunctions(List<AggregateFunction> aggregateFuncs,
            Set<Slot> groupingExprsInputSlots) {
        PreAggStatus preAggStatus = aggregateFuncs.isEmpty() && groupingExprsInputSlots.isEmpty()
                ? PreAggStatus.off("No aggregate on scan.")
                : PreAggStatus.on();
        for (AggregateFunction aggFunc : aggregateFuncs) {
            if (aggFunc.children().isEmpty()) {
                preAggStatus = PreAggStatus.off(
                        String.format("can't turn preAgg on for aggregate function %s", aggFunc));
            } else if (aggFunc.children().size() == 1 && aggFunc.child(0) instanceof Slot) {
                Slot aggSlot = (Slot) aggFunc.child(0);
                if (aggSlot instanceof SlotReference
                        && ((SlotReference) aggSlot).getColumn().isPresent()) {
                    if (((SlotReference) aggSlot).getColumn().get().isKey()) {
                        preAggStatus = OneKeySlotAggChecker.INSTANCE.check(aggFunc);
                    } else {
                        preAggStatus = OneValueSlotAggChecker.INSTANCE.check(aggFunc,
                                ((SlotReference) aggSlot).getColumn().get().getAggregationType());
                    }
                } else {
                    preAggStatus = PreAggStatus.off(
                            String.format("aggregate function %s use unknown slot %s from scan",
                                    aggFunc, aggSlot));
                }
            } else {
                Set<Slot> aggSlots = aggFunc.getInputSlots();
                Pair<Set<SlotReference>, Set<SlotReference>> splitSlots = splitSlots(aggSlots);
                preAggStatus =
                        checkAggWithKeyAndValueSlots(aggFunc, splitSlots.first, splitSlots.second);
            }
            if (preAggStatus.isOff()) {
                return preAggStatus;
            }
        }
        return preAggStatus;
    }

    private List<Expression> nonVirtualGroupByExprs(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
                .filter(expr -> !(expr instanceof VirtualSlotReference))
                .collect(ImmutableList.toImmutableList());
    }

    private static class OneValueSlotAggChecker
            extends ExpressionVisitor<PreAggStatus, AggregateType> {
        public static final OneValueSlotAggChecker INSTANCE = new OneValueSlotAggChecker();

        public PreAggStatus check(AggregateFunction aggFun, AggregateType aggregateType) {
            return aggFun.accept(INSTANCE, aggregateType);
        }

        @Override
        public PreAggStatus visit(Expression expr, AggregateType aggregateType) {
            return PreAggStatus.off(String.format("%s is not aggregate function.", expr.toSql()));
        }

        @Override
        public PreAggStatus visitAggregateFunction(AggregateFunction aggregateFunction,
                AggregateType aggregateType) {
            return PreAggStatus
                    .off(String.format("%s is not supported.", aggregateFunction.toSql()));
        }

        @Override
        public PreAggStatus visitMax(Max max, AggregateType aggregateType) {
            if (aggregateType == AggregateType.MAX && !max.isDistinct()) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus
                        .off(String.format("%s is not match agg mode %s or has distinct param",
                                max.toSql(), aggregateType));
            }
        }

        @Override
        public PreAggStatus visitMin(Min min, AggregateType aggregateType) {
            if (aggregateType == AggregateType.MIN && !min.isDistinct()) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus
                        .off(String.format("%s is not match agg mode %s or has distinct param",
                                min.toSql(), aggregateType));
            }
        }

        @Override
        public PreAggStatus visitSum(Sum sum, AggregateType aggregateType) {
            if (aggregateType == AggregateType.SUM && !sum.isDistinct()) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus
                        .off(String.format("%s is not match agg mode %s or has distinct param",
                                sum.toSql(), aggregateType));
            }
        }

        @Override
        public PreAggStatus visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount,
                AggregateType aggregateType) {
            if (aggregateType == AggregateType.BITMAP_UNION) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid bitmap_union_count: " + bitmapUnionCount.toSql());
            }
        }

        @Override
        public PreAggStatus visitBitmapUnion(BitmapUnion bitmapUnion, AggregateType aggregateType) {
            if (aggregateType == AggregateType.BITMAP_UNION) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid bitmapUnion: " + bitmapUnion.toSql());
            }
        }

        @Override
        public PreAggStatus visitHllUnionAgg(HllUnionAgg hllUnionAgg, AggregateType aggregateType) {
            if (aggregateType == AggregateType.HLL_UNION) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid hllUnionAgg: " + hllUnionAgg.toSql());
            }
        }

        @Override
        public PreAggStatus visitHllUnion(HllUnion hllUnion, AggregateType aggregateType) {
            if (aggregateType == AggregateType.HLL_UNION) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off("invalid hllUnion: " + hllUnion.toSql());
            }
        }
    }

    private static class OneKeySlotAggChecker extends ExpressionVisitor<PreAggStatus, Void> {
        public static final OneKeySlotAggChecker INSTANCE = new OneKeySlotAggChecker();

        public PreAggStatus check(AggregateFunction aggFun) {
            return aggFun.accept(INSTANCE, null);
        }

        @Override
        public PreAggStatus visit(Expression expr, Void context) {
            return PreAggStatus.off(String.format("%s is not aggregate function.", expr.toSql()));
        }

        @Override
        public PreAggStatus visitAggregateFunction(AggregateFunction aggregateFunction,
                Void context) {
            return PreAggStatus.off(String.format("Aggregate function %s contains key column %s",
                    aggregateFunction.toSql(), aggregateFunction.child(0).toSql()));
        }

        @Override
        public PreAggStatus visitMax(Max max, Void context) {
            return PreAggStatus.on();
        }

        @Override
        public PreAggStatus visitMin(Min min, Void context) {
            return PreAggStatus.on();
        }

        @Override
        public PreAggStatus visitCount(Count count, Void context) {
            if (count.isDistinct()) {
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off(String.format("%s is not distinct.", count.toSql()));
            }
        }
    }

    private static class KeyAndValueSlotsAggChecker
            extends ExpressionVisitor<PreAggStatus, List<Expression>> {
        public static final KeyAndValueSlotsAggChecker INSTANCE = new KeyAndValueSlotsAggChecker();

        public PreAggStatus check(AggregateFunction aggFun, List<Expression> returnValues) {
            return aggFun.accept(INSTANCE, returnValues);
        }

        @Override
        public PreAggStatus visit(Expression expr, List<Expression> returnValues) {
            return PreAggStatus.off(String.format("%s is not aggregate function.", expr.toSql()));
        }

        @Override
        public PreAggStatus visitAggregateFunction(AggregateFunction aggregateFunction,
                List<Expression> returnValues) {
            return PreAggStatus
                    .off(String.format("%s is not supported.", aggregateFunction.toSql()));
        }

        @Override
        public PreAggStatus visitSum(Sum sum, List<Expression> returnValues) {
            for (Expression value : returnValues) {
                if (!(isAggTypeMatched(value, AggregateType.SUM) || value.isZeroLiteral()
                        || value.isNullLiteral())) {
                    return PreAggStatus.off(String.format("%s is not supported.", sum.toSql()));
                }
            }
            return PreAggStatus.on();
        }

        @Override
        public PreAggStatus visitMax(Max max, List<Expression> returnValues) {
            for (Expression value : returnValues) {
                if (!(isAggTypeMatched(value, AggregateType.MAX) || isKeySlot(value)
                        || value.isNullLiteral())) {
                    return PreAggStatus.off(String.format("%s is not supported.", max.toSql()));
                }
            }
            return PreAggStatus.on();
        }

        @Override
        public PreAggStatus visitMin(Min min, List<Expression> returnValues) {
            for (Expression value : returnValues) {
                if (!(isAggTypeMatched(value, AggregateType.MIN) || isKeySlot(value)
                        || value.isNullLiteral())) {
                    return PreAggStatus.off(String.format("%s is not supported.", min.toSql()));
                }
            }
            return PreAggStatus.on();
        }

        @Override
        public PreAggStatus visitCount(Count count, List<Expression> returnValues) {
            if (count.isDistinct()) {
                for (Expression value : returnValues) {
                    if (!(isKeySlot(value) || value.isZeroLiteral() || value.isNullLiteral())) {
                        return PreAggStatus
                                .off(String.format("%s is not supported.", count.toSql()));
                    }
                }
                return PreAggStatus.on();
            } else {
                return PreAggStatus.off(String.format("%s is not supported.", count.toSql()));
            }
        }

        private boolean isKeySlot(Expression expression) {
            return expression instanceof SlotReference
                    && ((SlotReference) expression).getColumn().isPresent()
                    && ((SlotReference) expression).getColumn().get().isKey();
        }

        private boolean isAggTypeMatched(Expression expression, AggregateType aggregateType) {
            return expression instanceof SlotReference
                    && ((SlotReference) expression).getColumn().isPresent()
                    && ((SlotReference) expression).getColumn().get()
                            .getAggregationType() == aggregateType;
        }
    }
}
