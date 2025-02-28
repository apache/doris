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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * SetPreAggStatus
 */
public class SetPreAggStatus extends DefaultPlanRewriter<Stack<SetPreAggStatus.PreAggValidateContext>>
        implements CustomRewriter {

    /**
     * PreAggValidateContext
     */
    public static class PreAggValidateContext {
        public Set<Slot> outerJoinNullableSideSlots = new HashSet<>();
        public List<Expression> filterConjuncts = new ArrayList<>();
        public List<Expression> joinConjuncts = new ArrayList<>();
        public List<Expression> groupByExpresssions = new ArrayList<>();
        public Set<AggregateFunction> aggregateFunctions = new HashSet<>();

        void addJoinInfo(LogicalJoin logicalJoin) {
            joinConjuncts.addAll(logicalJoin.getExpressions());
            JoinType joinType = logicalJoin.getJoinType();
            if (joinType.isOuterJoin()) {
                if (outerJoinNullableSideSlots == null) {
                    outerJoinNullableSideSlots = Sets.newHashSet();
                }
                switch (joinType) {
                    case LEFT_OUTER_JOIN:
                        outerJoinNullableSideSlots.addAll(logicalJoin.right().getOutput());
                        break;
                    case RIGHT_OUTER_JOIN:
                        outerJoinNullableSideSlots.addAll(logicalJoin.left().getOutput());
                        break;
                    case FULL_OUTER_JOIN:
                        outerJoinNullableSideSlots.addAll(logicalJoin.getOutput());
                        break;
                    default:
                        break;
                }
            }
        }

        void addFilterConjuncts(List<Expression> conjuncts) {
            filterConjuncts.addAll(conjuncts);
        }

        void addGroupByExpresssions(List<Expression> expressions) {
            groupByExpresssions.addAll(expressions);
        }

        void addAggregateFunctions(Set<AggregateFunction> functions) {
            aggregateFunctions.addAll(functions);
        }

        void replaceExpressions(Map<Slot, Expression> replaceMap) {
            if (!filterConjuncts.isEmpty()) {
                filterConjuncts = Lists.newArrayList(ExpressionUtils.replace(filterConjuncts, replaceMap));
            }
            if (!joinConjuncts.isEmpty()) {
                joinConjuncts = Lists.newArrayList(ExpressionUtils.replace(joinConjuncts, replaceMap));
            }
            if (!groupByExpresssions.isEmpty()) {
                groupByExpresssions = Lists.newArrayList(ExpressionUtils.replace(groupByExpresssions, replaceMap));
            }
            if (!outerJoinNullableSideSlots.isEmpty()) {
                Set<Slot> newOuterJoinNullableSideSlots = Sets.newHashSet();
                for (Slot slot : outerJoinNullableSideSlots) {
                    Expression expr = ExpressionUtils.replace(slot, replaceMap);
                    if (expr instanceof Slot) {
                        newOuterJoinNullableSideSlots.add((Slot) expr);
                    } else {
                        newOuterJoinNullableSideSlots.add(slot);
                    }
                }
                outerJoinNullableSideSlots = newOuterJoinNullableSideSlots;
            }
            if (!aggregateFunctions.isEmpty()) {
                Set<AggregateFunction> newAggregateFunctions = Sets.newHashSet();
                for (AggregateFunction aggregateFunction : aggregateFunctions) {
                    newAggregateFunctions
                            .add((AggregateFunction) ExpressionUtils.replace(aggregateFunction, replaceMap));
                }
                aggregateFunctions = newAggregateFunctions;
            }
        }
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, new Stack<>());
    }

    @Override
    public Plan visit(Plan plan, Stack<PreAggValidateContext> context) {
        context.clear();
        return super.visit(plan, context);
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan logicalOlapScan, Stack<PreAggValidateContext> context) {
        if (logicalOlapScan.isPreAggStatusUnSet()) {
            long selectIndexId = logicalOlapScan.getSelectedIndexId();
            MaterializedIndexMeta meta = logicalOlapScan.getTable().getIndexMetaByIndexId(selectIndexId);
            if (meta.getKeysType() == KeysType.DUP_KEYS || (meta.getKeysType() == KeysType.UNIQUE_KEYS
                    && logicalOlapScan.getTable().getEnableUniqueKeyMergeOnWrite())) {
                return logicalOlapScan.withPreAggStatus(PreAggStatus.on());
            } else {
                if (!context.isEmpty()) {
                    return logicalOlapScan.withPreAggStatus(createPreAggStatus(logicalOlapScan, context.peek()));
                } else {
                    return logicalOlapScan.withPreAggStatus(PreAggStatus.off("No valid aggregate on scan."));
                }
            }
        } else {
            return logicalOlapScan;
        }
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> logicalFilter, Stack<PreAggValidateContext> context) {
        if (!context.empty()) {
            context.peek().addFilterConjuncts(logicalFilter.getExpressions());
        }
        return super.visit(logicalFilter, context);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> logicalJoin,
            Stack<PreAggValidateContext> context) {
        if (logicalJoin.getJoinType().isOuterJoin()) {
            // TODO how to handle outer join?
            context.clear();
        }
        int size = context.size();
        if (size != 0) {
            context.peek().addJoinInfo(logicalJoin);
        }
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(logicalJoin.arity());
        boolean hasNewChildren = false;
        for (Plan child : logicalJoin.children()) {
            Plan newChild = child.accept(this, context);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
            while (context.size() > size) {
                context.pop();
            }
        }

        if (hasNewChildren) {
            return logicalJoin.withChildren(newChildren.build());
        } else {
            return logicalJoin;
        }
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> logicalProject,
            Stack<PreAggValidateContext> context) {
        if (!context.empty()) {
            context.peek().replaceExpressions(logicalProject.getAliasToProducer());
        }
        return super.visit(logicalProject, context);
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> logicalAggregate,
            Stack<PreAggValidateContext> context) {
        PreAggValidateContext validateContext = new PreAggValidateContext();
        validateContext.addAggregateFunctions(logicalAggregate.getAggregateFunctions());
        validateContext.addGroupByExpresssions(nonVirtualGroupByExprs(logicalAggregate));
        context.push(validateContext);
        return super.visit(logicalAggregate, context);
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> logicalRepeat, Stack<PreAggValidateContext> context) {
        return super.visit(logicalRepeat, context);
    }

    private PreAggStatus createPreAggStatus(LogicalOlapScan logicalOlapScan, PreAggValidateContext context) {
        List<Expression> filterConjuncts = context.filterConjuncts;
        List<Expression> joinConjuncts = context.joinConjuncts;
        Set<AggregateFunction> aggregateFuncs = context.aggregateFunctions;
        List<Expression> groupingExprs = context.groupByExpresssions;
        Set<Slot> outputSlots = logicalOlapScan.getOutputSet();
        Pair<Set<SlotReference>, Set<SlotReference>> splittedSlots = splitSlots(outputSlots);
        Set<SlotReference> keySlots = splittedSlots.first;
        Set<SlotReference> valueSlots = splittedSlots.second;
        Preconditions.checkState(outputSlots.size() == keySlots.size() + valueSlots.size(),
                "output slots contains no key or value slots");

        Set<Slot> groupingExprsInputSlots = ExpressionUtils.getInputSlotSet(groupingExprs);
        Set<Slot> filterInputSlots = ExpressionUtils.getInputSlotSet(filterConjuncts);
        Set<Slot> joinInputSlots = ExpressionUtils.getInputSlotSet(joinConjuncts);
        Set<Slot> aggFunctionInputSlots = ExpressionUtils.getInputSlotSet(aggregateFuncs);

        Set<Slot> allUsedSlots = new HashSet<>();
        allUsedSlots.addAll(groupingExprsInputSlots);
        allUsedSlots.addAll(filterInputSlots);
        allUsedSlots.addAll(joinInputSlots);
        allUsedSlots.addAll(aggFunctionInputSlots);

        allUsedSlots.retainAll(context.outerJoinNullableSideSlots);
        if (!allUsedSlots.isEmpty()) {
            return PreAggStatus.off("can not turn on pre agg for outer join");
        }

        Set<Slot> tmpInputSlots = Sets.newHashSet();
        tmpInputSlots.addAll(groupingExprsInputSlots);
        tmpInputSlots.retainAll(valueSlots);
        if (!tmpInputSlots.isEmpty()) {
            return PreAggStatus
                    .off(String.format("Grouping expression %s contains non-key column %s",
                            groupingExprs, tmpInputSlots));
        }

        filterInputSlots.retainAll(valueSlots);
        if (!filterInputSlots.isEmpty()) {
            return PreAggStatus.off(String.format("Filter conjuncts %s contains non-key column %s",
                    filterConjuncts, filterInputSlots));
        }

        joinInputSlots.retainAll(valueSlots);
        if (!joinInputSlots.isEmpty()) {
            return PreAggStatus.off(String.format("Join conjuncts %s contains non-key column %s",
                    joinConjuncts, joinInputSlots));
        }
        for (AggregateFunction aggregateFunction : aggregateFuncs) {
            if (!outputSlots.containsAll(aggregateFunction.getInputSlots())) {
                return PreAggStatus.off(String.format("agg function is invalid %s",
                        aggregateFunction.toSql()));
            }
        }

        Set<Slot> candidateGroupByInputSlots = Sets.newHashSet();
        candidateGroupByInputSlots.addAll(groupingExprsInputSlots);
        candidateGroupByInputSlots.retainAll(outputSlots);

        return checkAggregateFunctions(aggregateFuncs, candidateGroupByInputSlots);
    }

    private PreAggStatus checkAggregateFunctions(Set<AggregateFunction> aggregateFuncs,
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
                preAggStatus = checkAggWithKeyAndValueSlots(aggFunc, splitSlots.first, splitSlots.second);
            }
            if (preAggStatus.isOff()) {
                return preAggStatus;
            }
        }
        return preAggStatus;
    }

    private Pair<Set<SlotReference>, Set<SlotReference>> splitSlots(Set<Slot> slots) {
        Set<SlotReference> keySlots = com.google.common.collect.Sets.newHashSetWithExpectedSize(slots.size());
        Set<SlotReference> valueSlots = com.google.common.collect.Sets.newHashSetWithExpectedSize(slots.size());
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

    private List<Expression> nonVirtualGroupByExprs(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
                .filter(expr -> !(expr instanceof VirtualSlotReference))
                .collect(ImmutableList.toImmutableList());
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
