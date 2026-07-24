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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
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
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * SetPreAggStatus
 * bottom-up tranverse the plan tree and collect required info into PreAggInfoContext
 * when get to the bottom LogicalOlapScan node, we set the preagg status using info in PreAggInfoContext
 */
public class SetPreAggStatus extends DefaultPlanRewriter<Stack<SetPreAggStatus.PreAggInfoContext>>
        implements CustomRewriter {
    private static final Logger LOG = LogManager.getLogger(SetPreAggStatus.class);
    private Map<RelationId, PreAggInfoContext> olapScanPreAggContexts = new HashMap<>();

    /**
     * PreAggInfoContext
     */
    public static class PreAggInfoContext {
        private List<Expression> filterConjuncts = new ArrayList<>();
        private List<Expression> joinConjuncts = new ArrayList<>();
        private List<Expression> groupByExpresssions = new ArrayList<>();
        private List<Expression> groupingScalarFunctionExpresssions = new ArrayList<>();
        private Set<AggregateFunction> aggregateFunctions = new HashSet<>();
        private Set<RelationId> olapScanIds = new HashSet<>();

        private Map<Slot, Expression> replaceMap = new HashMap<>();

        private void setReplaceMap(Map<Slot, Expression> newReplaceMap) {
            // merge instead of replace: sibling projects under a join share one
            // PreAggInfoContext, and a full replacement would lose mappings from
            // the sibling. merge keeps all entries; new entries shadow old ones
            // by putAll semantics so a chain of projects still resolves correctly.
            //
            // Before merging, resolve the new aliases' producers through the
            // existing replaceMap so that upper-layer aliases reference base table
            // columns directly instead of intermediate computed aliases.
            Map<Slot, Expression> merged = new HashMap<>(this.replaceMap);
            for (Map.Entry<Slot, Expression> entry : newReplaceMap.entrySet()) {
                Expression resolvedProducer = ExpressionUtils.replace(
                        entry.getValue(), this.replaceMap);
                merged.put(entry.getKey(), resolvedProducer);
            }
            this.replaceMap = merged;
        }

        private void addRelationId(RelationId id) {
            olapScanIds.add(id);
        }

        private void addJoinInfo(LogicalJoin logicalJoin) {
            joinConjuncts.addAll(logicalJoin.getExpressions());
            joinConjuncts = Lists.newArrayList(ExpressionUtils.replace(joinConjuncts, replaceMap));
        }

        private void addFilterConjuncts(List<Expression> conjuncts) {
            filterConjuncts.addAll(conjuncts);
            filterConjuncts = Lists.newArrayList(ExpressionUtils.replace(filterConjuncts, replaceMap));
        }

        private void addGroupByExpresssions(List<Expression> expressions) {
            groupByExpresssions.addAll(expressions);
            groupByExpresssions.removeAll(groupingScalarFunctionExpresssions);
            groupByExpresssions = Lists.newArrayList(ExpressionUtils.replace(groupByExpresssions, replaceMap));
        }

        private void addGroupingScalarFunctionExpresssions(List<Expression> expressions) {
            groupingScalarFunctionExpresssions.addAll(expressions);
        }

        private void addGroupingScalarFunctionExpresssion(Expression expression) {
            groupingScalarFunctionExpresssions.add(expression);
        }

        private void addAggregateFunctions(Set<AggregateFunction> functions) {
            aggregateFunctions.addAll(functions);
            Set<AggregateFunction> newAggregateFunctions = Sets.newHashSet();
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                newAggregateFunctions
                        .add((AggregateFunction) ExpressionUtils.replace(aggregateFunction, replaceMap));
            }
            aggregateFunctions = newAggregateFunctions;
        }
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        Plan newPlan = plan.accept(this, new Stack<>());
        return newPlan.accept(SetOlapScanPreAgg.INSTANCE, olapScanPreAggContexts);
    }

    @Override
    public Plan visit(Plan plan, Stack<PreAggInfoContext> context) {
        // push null sentinel to stop preagg collection for children reached via generic visitor,
        // while keeping the aggregate's own frame intact on the stack
        context.push(null);
        Plan newPlan = super.visit(plan, context);
        context.pop();
        return newPlan;
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan logicalOlapScan, Stack<PreAggInfoContext> context) {
        if (logicalOlapScan.isPreAggStatusUnSet()) {
            long selectIndexId = logicalOlapScan.getSelectedIndexId();
            MaterializedIndexMeta meta = logicalOlapScan.getTable().getIndexMetaByIndexId(selectIndexId);
            if (meta.getKeysType() == KeysType.DUP_KEYS || (meta.getKeysType() == KeysType.UNIQUE_KEYS
                    && logicalOlapScan.getTable().getEnableUniqueKeyMergeOnWrite())
                    || (meta.getKeysType() == KeysType.UNIQUE_KEYS
                        && logicalOlapScan.getTable().isMorTable()
                        && ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable().isReadMorAsDupEnabled(
                            logicalOlapScan.getTable().getQualifiedDbName(),
                            logicalOlapScan.getTable().getName()))) {
                return logicalOlapScan.withPreAggStatus(PreAggStatus.on());
            } else {
                if (!context.empty() && context.peek() != null) {
                    context.peek().addRelationId(logicalOlapScan.getRelationId());
                }
                return logicalOlapScan;
            }
        } else {
            return logicalOlapScan;
        }
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> logicalFilter, Stack<PreAggInfoContext> context) {
        LogicalFilter plan = (LogicalFilter) super.visit(logicalFilter, context);
        if (!context.empty() && context.peek() != null) {
            context.peek().addFilterConjuncts(plan.getExpressions());
        }
        return plan;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> logicalJoin,
            Stack<PreAggInfoContext> context) {
        LogicalJoin plan = (LogicalJoin) super.visit(logicalJoin, context);
        if (!context.empty() && context.peek() != null) {
            context.peek().addJoinInfo(plan);
        }
        return plan;
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> logicalProject,
            Stack<PreAggInfoContext> context) {
        LogicalProject plan = (LogicalProject) super.visit(logicalProject, context);
        if (!context.empty() && context.peek() != null) {
            context.peek().setReplaceMap(plan.getAliasToProducer());
        }
        return plan;
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> logicalAggregate,
            Stack<PreAggInfoContext> context) {
        PreAggInfoContext preAggInfoContext = new PreAggInfoContext();
        context.push(preAggInfoContext);
        Plan plan = super.visit(logicalAggregate, context);
        PreAggInfoContext popped = context.pop();
        if (popped != preAggInfoContext) {
            if (SessionVariable.isFeDebug()) {
                Preconditions.checkState(popped == preAggInfoContext,
                        "PreAggInfoContext stack mismatch in visitLogicalAggregate");
            } else {
                LOG.warn("PreAggInfoContext stack mismatch in visitLogicalAggregate: "
                        + "expected {} but got {}. Skipping preagg for this aggregate.",
                        preAggInfoContext, popped);
                return plan;
            }
        }
        popped.addAggregateFunctions(logicalAggregate.getAggregateFunctions());
        popped.addGroupByExpresssions(logicalAggregate.getGroupByExpressions());
        for (RelationId id : popped.olapScanIds) {
            olapScanPreAggContexts.put(id, popped);
        }
        return plan;
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Stack<PreAggInfoContext> context) {
        repeat = (LogicalRepeat<? extends Plan>) super.visit(repeat, context);
        if (!context.isEmpty() && context.peek() != null) {
            context.peek().addGroupingScalarFunctionExpresssion(repeat.getGroupingId().get());
            context.peek().addGroupingScalarFunctionExpresssions(
                    repeat.getOutputExpressions().stream()
                            .filter(e -> e.containsType(GroupingScalarFunction.class))
                            .map(e -> e.toSlot())
                            .collect(Collectors.toList()));
        }
        return repeat;
    }

    private static class SetOlapScanPreAgg extends DefaultPlanRewriter<Map<RelationId, PreAggInfoContext>> {
        private static SetOlapScanPreAgg INSTANCE = new SetOlapScanPreAgg();

        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Map<RelationId, PreAggInfoContext> context) {
            if (olapScan.isPreAggStatusUnSet()) {
                PreAggStatus preAggStatus = PreAggStatus.off("No valid aggregate on scan.");
                PreAggInfoContext preAggInfoContext = context.get(olapScan.getRelationId());
                if (preAggInfoContext != null) {
                    preAggStatus = createPreAggStatus(olapScan, preAggInfoContext);
                }
                return olapScan.withPreAggStatus(preAggStatus);
            } else {
                return olapScan;
            }
        }

        private PreAggStatus createPreAggStatus(LogicalOlapScan logicalOlapScan, PreAggInfoContext context) {
            List<Expression> filterConjuncts = context.filterConjuncts;
            List<Expression> joinConjuncts = context.joinConjuncts;
            Set<AggregateFunction> aggregateFuncs = context.aggregateFunctions;
            List<Expression> groupingExprs = context.groupByExpresssions;
            Set<Slot> outputSlots = logicalOlapScan.getOutputSet();
            Pair<Set<SlotReference>, Set<SlotReference>> splittedSlots = splitKeyValueSlots(outputSlots);
            Set<SlotReference> keySlots = splittedSlots.first;
            Set<SlotReference> valueSlots = splittedSlots.second;
            Preconditions.checkState(outputSlots.size() == keySlots.size() + valueSlots.size(),
                    "output slots contains no key or value slots");

            Set<Slot> groupingExprsInputSlots = ExpressionUtils.getInputSlotSet(groupingExprs);
            if (!Sets.intersection(groupingExprsInputSlots, valueSlots).isEmpty()) {
                return PreAggStatus
                        .off(String.format("Grouping expression %s contains non-key column %s",
                                groupingExprs, groupingExprsInputSlots));
            }

            Set<Slot> filterInputSlots = ExpressionUtils.getInputSlotSet(filterConjuncts);
            if (!Sets.intersection(filterInputSlots, valueSlots).isEmpty()) {
                return PreAggStatus.off(String.format("Filter conjuncts %s contains non-key column %s",
                        filterConjuncts, filterInputSlots));
            }

            Set<Slot> joinInputSlots = ExpressionUtils.getInputSlotSet(joinConjuncts);
            if (!Sets.intersection(joinInputSlots, valueSlots).isEmpty()) {
                return PreAggStatus.off(String.format("Join conjuncts %s contains non-key column %s",
                        joinConjuncts, joinInputSlots));
            }
            Set<AggregateFunction> candidateAggFuncs = Sets.newHashSet();
            for (AggregateFunction aggregateFunction : aggregateFuncs) {
                if (!Sets.intersection(aggregateFunction.getInputSlots(), outputSlots).isEmpty()) {
                    candidateAggFuncs.add(aggregateFunction);
                } else {
                    if (!(aggregateFunction instanceof Max || aggregateFunction instanceof Min
                            || (aggregateFunction instanceof Count && aggregateFunction.isDistinct()))) {
                        return PreAggStatus.off(
                                String.format("can't turn preAgg on because aggregate function %s in other table",
                                        aggregateFunction));
                    }
                }
            }

            Set<Slot> candidateGroupByInputSlots = Sets.newHashSet();
            candidateGroupByInputSlots.addAll(groupingExprsInputSlots);
            candidateGroupByInputSlots.retainAll(outputSlots);
            if (candidateAggFuncs.isEmpty() && candidateGroupByInputSlots.isEmpty()) {
                return !aggregateFuncs.isEmpty() || !groupingExprs.isEmpty() ? PreAggStatus.on()
                        : PreAggStatus.off("No aggregate on scan.");
            } else {
                return checkAggregateFunctions(candidateAggFuncs, candidateGroupByInputSlots);
            }
        }

        private PreAggStatus checkAggregateFunctions(Set<AggregateFunction> aggregateFuncs,
                Set<Slot> groupingExprsInputSlots) {
            if (aggregateFuncs.isEmpty() && groupingExprsInputSlots.isEmpty()) {
                return PreAggStatus.off("No aggregate on scan.");
            }
            PreAggStatus preAggStatus = PreAggStatus.on();
            for (AggregateFunction aggFunc : aggregateFuncs) {
                Set<Slot> aggSlots = aggFunc.getInputSlots();
                if (aggSlots.isEmpty()) {
                    preAggStatus = PreAggStatus.off(
                            String.format("can't turn preAgg on for aggregate function %s", aggFunc));
                } else {
                    Pair<Set<SlotReference>, Set<SlotReference>> splitSlots = splitKeyValueSlots(aggSlots);
                    if (splitSlots.first.isEmpty()) {
                        // only value slots
                        if (aggFunc.children().size() == 1 && aggFunc.child(0) instanceof SlotReference) {
                            SlotReference slotRef = (SlotReference) aggFunc.child(0);
                            if (slotRef.getOriginalColumn().isPresent()) {
                                preAggStatus = OneValueSlotAggChecker.INSTANCE.check(aggFunc,
                                        slotRef.getOriginalColumn().get().getAggregationType());
                            } else {
                                preAggStatus = PreAggStatus.off(
                                        String.format("can't turn preAgg on for aggregate function %s", aggFunc));
                            }
                        } else {
                            preAggStatus = PreAggStatus.off(
                                    String.format("can't turn preAgg on for aggregate function %s", aggFunc));
                        }
                    } else if (splitSlots.second.isEmpty()) {
                        // only key slots
                        // Volatile expressions (e.g., random()) must be rejected here:
                        // with pre-agg ON they would be evaluated per partial row instead of
                        // per merged logical row, changing DISTINCT/MAX/MIN semantics.
                        if (aggFunc.containsVolatileExpression()) {
                            preAggStatus = PreAggStatus.off(
                                    String.format("can't turn preAgg on for aggregate function %s", aggFunc));
                        } else {
                            preAggStatus = KeySlotAggChecker.INSTANCE.check(aggFunc);
                        }
                    } else {
                        // checkAggWithKeyAndValueSlots only inspects child(0) for IF/CaseWhen patterns.
                        // For multi-argument aggregate functions, child(0) inspection is insufficient
                        // as later arguments may contain value columns that are not validated.
                        if (aggFunc.children().size() > 1) {
                            preAggStatus = PreAggStatus.off(
                                    String.format("can't turn preAgg on for aggregate function %s", aggFunc));
                        } else {
                            preAggStatus = checkAggWithKeyAndValueSlots(aggFunc, splitSlots.first, splitSlots.second);
                        }
                    }
                }
                if (preAggStatus.isOff()) {
                    return preAggStatus;
                }
            }
            return preAggStatus;
        }

        private Pair<Set<SlotReference>, Set<SlotReference>> splitKeyValueSlots(Set<Slot> slots) {
            Set<SlotReference> keySlots = com.google.common.collect.Sets.newHashSetWithExpectedSize(slots.size());
            Set<SlotReference> valueSlots = com.google.common.collect.Sets.newHashSetWithExpectedSize(slots.size());
            for (Slot slot : slots) {
                if (slot instanceof SlotReference && ((SlotReference) slot).getOriginalColumn().isPresent()) {
                    if (((SlotReference) slot).getOriginalColumn().get().isKey()) {
                        keySlots.add((SlotReference) slot);
                    } else {
                        valueSlots.add((SlotReference) slot);
                    }
                }
            }
            return Pair.of(keySlots, valueSlots);
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
            if (conditionExps.isEmpty()) {
                return PreAggStatus.off(
                        String.format("can't turn preAgg on for aggregate function %s", aggFunc));
            }

            // step 2: check condition expressions
            Set<Slot> inputSlots = ExpressionUtils.getInputSlotSet(conditionExps);
            if (!keySlots.containsAll(inputSlots)) {
                return PreAggStatus
                        .off(String.format("some columns in condition %s is not key.", conditionExps));
            }

            // step 3: reject volatile expressions in conditions — they would be evaluated
            // per partial row with pre-agg ON, changing the aggregation result.
            if (conditionExps.stream().anyMatch(Expression::containsVolatileExpression)) {
                return PreAggStatus.off(
                        String.format("condition %s contains volatile expression, "
                                + "can't turn preAgg on for aggregate function %s",
                                conditionExps, aggFunc));
            }

            return KeyAndValueSlotsAggChecker.INSTANCE.check(aggFunc, returnExps);
        }

        private static Expression removeCast(Expression expression) {
            while (expression instanceof Cast) {
                expression = ((Cast) expression).child();
            }
            return expression;
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

        private static class KeySlotAggChecker extends ExpressionVisitor<PreAggStatus, Void> {
            public static final KeySlotAggChecker INSTANCE = new KeySlotAggChecker();

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
                if (aggregateFunction.isDistinct()) {
                    return PreAggStatus.on();
                } else {
                    return PreAggStatus.off(String.format("%s is not distinct.", aggregateFunction.toSql()));
                }
            }

            @Override
            public PreAggStatus visitMax(Max max, Void context) {
                return PreAggStatus.on();
            }

            @Override
            public PreAggStatus visitMin(Min min, Void context) {
                return PreAggStatus.on();
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
                        && ((SlotReference) expression).getOriginalColumn().isPresent()
                        && ((SlotReference) expression).getOriginalColumn().get().isKey();
            }

            private boolean isAggTypeMatched(Expression expression, AggregateType aggregateType) {
                return expression instanceof SlotReference
                        && ((SlotReference) expression).getOriginalColumn().isPresent()
                        && ((SlotReference) expression).getOriginalColumn().get()
                                .getAggregationType() == aggregateType;
            }
        }
    }
}
