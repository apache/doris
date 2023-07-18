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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * because some rule could change output's nullable.
 * So, we need add a rule to adjust all expression's nullable attribute after rewrite.
 */
public class AdjustNullable extends DefaultPlanRewriter<Void> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, null);
    }

    @Override
    public Plan visit(Plan plan, Void context) {
        LogicalPlan logicalPlan = (LogicalPlan) super.visit(plan, context);
        return logicalPlan.recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        aggregate = (LogicalAggregate<? extends Plan>) super.visit(aggregate, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(aggregate);
        List<NamedExpression> newOutputs
                = updateExpressions(aggregate.getOutputExpressions(), exprIdSlotMap);
        List<Expression> newGroupExpressions
                = updateExpressions(aggregate.getGroupByExpressions(), exprIdSlotMap);
        return aggregate.withGroupByAndOutput(newGroupExpressions, newOutputs);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        filter = (LogicalFilter<? extends Plan>) super.visit(filter, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(filter);
        Set<Expression> conjuncts = updateExpressions(filter.getConjuncts(), exprIdSlotMap);
        return filter.withConjuncts(conjuncts).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Void context) {
        generate = (LogicalGenerate<? extends Plan>) super.visit(generate, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(generate);
        List<Function> newGenerators = updateExpressions(generate.getGenerators(), exprIdSlotMap);
        return generate.withGenerators(newGenerators).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(join);
        List<Expression> hashConjuncts = updateExpressions(join.getHashJoinConjuncts(), exprIdSlotMap);
        // because other join compute on join's output on be, so we need to change slot to join's output
        exprIdSlotMap = join.getOutputSet().stream()
                .collect(Collectors.toMap(NamedExpression::getExprId, s -> s));
        List<Expression> otherConjuncts = updateExpressions(join.getOtherJoinConjuncts(), exprIdSlotMap);
        return join.withJoinConjuncts(hashConjuncts, otherConjuncts).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        project = (LogicalProject<? extends Plan>) super.visit(project, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(project);
        List<NamedExpression> newProjects = updateExpressions(project.getProjects(), exprIdSlotMap);
        return project.withProjects(newProjects);
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void context) {
        repeat = (LogicalRepeat<? extends Plan>) super.visit(repeat, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(repeat);
        Set<Expression> flattenGroupingSetExpr = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));
        List<NamedExpression> newOutputs = Lists.newArrayList();
        for (NamedExpression output : repeat.getOutputExpressions()) {
            if (flattenGroupingSetExpr.contains(output)) {
                newOutputs.add(output);
            } else {
                newOutputs.add(updateExpression(output, exprIdSlotMap));
            }
        }
        return repeat.withGroupSetsAndOutput(repeat.getGroupingSets(), newOutputs).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalSetOperation(LogicalSetOperation setOperation, Void context) {
        setOperation = (LogicalSetOperation) super.visit(setOperation, context);
        if (setOperation.children().isEmpty()) {
            return setOperation;
        }
        List<Boolean> inputNullable = setOperation.child(0).getOutput().stream()
                .map(ExpressionTrait::nullable).collect(Collectors.toList());
        for (int i = 1; i < setOperation.arity(); i++) {
            List<Slot> childOutput = setOperation.getChildOutput(i);
            for (int j = 0; j < childOutput.size(); j++) {
                if (childOutput.get(j).nullable()) {
                    inputNullable.set(j, true);
                }
            }
        }
        List<NamedExpression> outputs = setOperation.getOutputs();
        List<NamedExpression> newOutputs = Lists.newArrayListWithCapacity(outputs.size());
        for (int i = 0; i < inputNullable.size(); i++) {
            NamedExpression ne = outputs.get(i);
            Slot slot = ne instanceof Alias ? (Slot) ((Alias) ne).child() : (Slot) ne;
            if (inputNullable.get(i)) {
                slot = slot.withNullable(true);
            }
            newOutputs.add(ne instanceof Alias ? (NamedExpression) ne.withChildren(slot) : slot);
        }
        return setOperation.withNewOutputs(newOutputs).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
        sort = (LogicalSort<? extends Plan>) super.visit(sort, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(sort);
        List<OrderKey> newKeys = sort.getOrderKeys().stream()
                .map(old -> old.withExpression(updateExpression(old.getExpr(), exprIdSlotMap)))
                .collect(ImmutableList.toImmutableList());
        return sort.withOrderKeys(newKeys).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
        topN = (LogicalTopN<? extends Plan>) super.visit(topN, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(topN);
        List<OrderKey> newKeys = topN.getOrderKeys().stream()
                .map(old -> old.withExpression(updateExpression(old.getExpr(), exprIdSlotMap)))
                .collect(ImmutableList.toImmutableList());
        return topN.withOrderKeys(newKeys).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, Void context) {
        window = (LogicalWindow<? extends Plan>) super.visit(window, context);
        Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(window);
        List<NamedExpression> windowExpressions =
                updateExpressions(window.getWindowExpressions(), exprIdSlotMap);
        return window.withExpression(windowExpressions, window.child());
    }

    private <T extends Expression> T updateExpression(T input, Map<ExprId, Slot> exprIdSlotMap) {
        return (T) input.rewriteDownShortCircuit(e -> e.accept(SlotReferenceReplacer.INSTANCE, exprIdSlotMap));
    }

    private <T extends Expression> List<T> updateExpressions(List<T> inputs, Map<ExprId, Slot> exprIdSlotMap) {
        return inputs.stream().map(i -> updateExpression(i, exprIdSlotMap)).collect(ImmutableList.toImmutableList());
    }

    private <T extends Expression> Set<T> updateExpressions(Set<T> inputs, Map<ExprId, Slot> exprIdSlotMap) {
        return inputs.stream().map(i -> updateExpression(i, exprIdSlotMap)).collect(ImmutableSet.toImmutableSet());
    }

    private Map<ExprId, Slot> collectChildrenOutputMap(LogicalPlan plan) {
        return plan.children().stream()
                .map(Plan::getOutputSet)
                .flatMap(Set::stream)
                .collect(Collectors.toMap(NamedExpression::getExprId, s -> s));
    }

    private static class SlotReferenceReplacer extends DefaultExpressionRewriter<Map<ExprId, Slot>> {
        public static SlotReferenceReplacer INSTANCE = new SlotReferenceReplacer();

        @Override
        public Expression visitSlotReference(SlotReference slotReference, Map<ExprId, Slot> context) {
            if (context.containsKey(slotReference.getExprId())) {
                return slotReference.withNullable(context.get(slotReference.getExprId()).nullable());
            } else {
                return slotReference;
            }
        }
    }
}
