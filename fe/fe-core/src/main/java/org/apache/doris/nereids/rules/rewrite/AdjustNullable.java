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
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * because some rule could change output's nullable.
 * So, we need add a rule to adjust all expression's nullable attribute after rewrite.
 */
public class AdjustNullable extends DefaultPlanRewriter<Map<ExprId, Slot>> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, Maps.newHashMap());
    }

    @Override
    public Plan visit(Plan plan, Map<ExprId, Slot> replaceMap) {
        LogicalPlan logicalPlan = (LogicalPlan) super.visit(plan, replaceMap);
        logicalPlan = logicalPlan.recomputeLogicalProperties();
        logicalPlan.getOutputSet().forEach(s -> replaceMap.put(s.getExprId(), s));
        return logicalPlan;
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> logicalSink, Map<ExprId, Slot> replaceMap) {
        logicalSink = (LogicalSink<? extends Plan>) super.visit(logicalSink, replaceMap);
        List<NamedExpression> newOutputExprs = updateExpressions(logicalSink.getOutputExprs(), replaceMap);
        return logicalSink.withOutputExprs(newOutputExprs);
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Map<ExprId, Slot> replaceMap) {
        aggregate = (LogicalAggregate<? extends Plan>) super.visit(aggregate, replaceMap);
        List<NamedExpression> newOutputs
                = updateExpressions(aggregate.getOutputExpressions(), replaceMap);
        List<Expression> newGroupExpressions
                = updateExpressions(aggregate.getGroupByExpressions(), replaceMap);
        newOutputs.forEach(o -> replaceMap.put(o.getExprId(), o.toSlot()));
        return aggregate.withGroupByAndOutput(newGroupExpressions, newOutputs);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Map<ExprId, Slot> replaceMap) {
        filter = (LogicalFilter<? extends Plan>) super.visit(filter, replaceMap);
        Set<Expression> conjuncts = updateExpressions(filter.getConjuncts(), replaceMap);
        return filter.withConjuncts(conjuncts).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Map<ExprId, Slot> replaceMap) {
        generate = (LogicalGenerate<? extends Plan>) super.visit(generate, replaceMap);
        List<Function> newGenerators = updateExpressions(generate.getGenerators(), replaceMap);
        Plan newGenerate = generate.withGenerators(newGenerators).recomputeLogicalProperties();
        newGenerate.getOutputSet().forEach(o -> replaceMap.put(o.getExprId(), o));
        return newGenerate;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Map<ExprId, Slot> replaceMap) {
        join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, replaceMap);
        List<Expression> hashConjuncts = updateExpressions(join.getHashJoinConjuncts(), replaceMap);
        List<Expression> markConjuncts;
        if (hashConjuncts.isEmpty()) {
            // if hashConjuncts is empty, mark join conjuncts may used to build hash table
            // so need call updateExpressions for mark join conjuncts before adjust nullable by output slot
            markConjuncts = updateExpressions(join.getMarkJoinConjuncts(), replaceMap);
        } else {
            markConjuncts = null;
        }
        join.getOutputSet().forEach(o -> replaceMap.put(o.getExprId(), o));
        if (markConjuncts == null) {
            // hashConjuncts is not empty, mark join conjuncts are processed like other join conjuncts
            Preconditions.checkState(!hashConjuncts.isEmpty(), "hash conjuncts should not be empty");
            markConjuncts = updateExpressions(join.getMarkJoinConjuncts(), replaceMap);
        }
        List<Expression> otherConjuncts = updateExpressions(join.getOtherJoinConjuncts(), replaceMap);
        return join.withJoinConjuncts(hashConjuncts, otherConjuncts, markConjuncts,
                    join.getJoinReorderContext()).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Map<ExprId, Slot> replaceMap) {
        project = (LogicalProject<? extends Plan>) super.visit(project, replaceMap);
        List<NamedExpression> newProjects = updateExpressions(project.getProjects(), replaceMap);
        newProjects.forEach(p -> replaceMap.put(p.getExprId(), p.toSlot()));
        return project.withProjects(newProjects);
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Map<ExprId, Slot> replaceMap) {
        repeat = (LogicalRepeat<? extends Plan>) super.visit(repeat, replaceMap);
        Set<Expression> flattenGroupingSetExpr = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));
        List<NamedExpression> newOutputs = Lists.newArrayList();
        for (NamedExpression output : repeat.getOutputExpressions()) {
            if (flattenGroupingSetExpr.contains(output)) {
                newOutputs.add(output);
            } else {
                newOutputs.add(updateExpression(output, replaceMap));
            }
        }
        newOutputs.forEach(o -> replaceMap.put(o.getExprId(), o.toSlot()));
        return repeat.withGroupSetsAndOutput(repeat.getGroupingSets(), newOutputs).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalSetOperation(LogicalSetOperation setOperation, Map<ExprId, Slot> replaceMap) {
        setOperation = (LogicalSetOperation) super.visit(setOperation, replaceMap);
        ImmutableList.Builder<List<SlotReference>> newChildrenOutputs = ImmutableList.builder();
        List<Boolean> inputNullable = null;
        if (!setOperation.children().isEmpty()) {
            inputNullable = setOperation.getRegularChildOutput(0).stream()
                    .map(ExpressionTrait::nullable).collect(Collectors.toList());
            for (int i = 0; i < setOperation.arity(); i++) {
                List<Slot> childOutput = setOperation.child(i).getOutput();
                List<SlotReference> setChildOutput = setOperation.getRegularChildOutput(i);
                ImmutableList.Builder<SlotReference> newChildOutputs = ImmutableList.builder();
                for (int j = 0; j < setChildOutput.size(); j++) {
                    for (Slot slot : childOutput) {
                        if (slot.getExprId().equals(setChildOutput.get(j).getExprId())) {
                            inputNullable.set(j, slot.nullable() || inputNullable.get(j));
                            newChildOutputs.add((SlotReference) slot);
                            break;
                        }
                    }
                }
                newChildrenOutputs.add(newChildOutputs.build());
            }
        }
        if (setOperation instanceof LogicalUnion) {
            LogicalUnion logicalUnion = (LogicalUnion) setOperation;
            if (!logicalUnion.getConstantExprsList().isEmpty() && setOperation.children().isEmpty()) {
                int outputSize = logicalUnion.getConstantExprsList().get(0).size();
                // create the inputNullable list and fill it with all FALSE values
                inputNullable = Lists.newArrayListWithCapacity(outputSize);
                for (int i = 0; i < outputSize; i++) {
                    inputNullable.add(false);
                }
            }
            for (List<NamedExpression> constantExprs : logicalUnion.getConstantExprsList()) {
                for (int j = 0; j < constantExprs.size(); j++) {
                    inputNullable.set(j, inputNullable.get(j) || constantExprs.get(j).nullable());
                }
            }
        }
        if (inputNullable == null) {
            // this is a fail-safe
            // means there is no children and having no getConstantExprsList
            // no way to update the nullable flag, so just do nothing
            return setOperation;
        }
        List<NamedExpression> outputs = setOperation.getOutputs();
        List<NamedExpression> newOutputs = Lists.newArrayListWithCapacity(outputs.size());
        for (int i = 0; i < inputNullable.size(); i++) {
            NamedExpression ne = outputs.get(i);
            Slot slot = ne instanceof Alias ? (Slot) ((Alias) ne).child() : (Slot) ne;
            slot = slot.withNullable(inputNullable.get(i));
            newOutputs.add(ne instanceof Alias ? (NamedExpression) ne.withChildren(slot) : slot);
        }
        newOutputs.forEach(o -> replaceMap.put(o.getExprId(), o.toSlot()));
        return setOperation.withNewOutputs(newOutputs)
                .withChildrenAndTheirOutputs(setOperation.children(), newChildrenOutputs.build())
                .recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Map<ExprId, Slot> replaceMap) {
        sort = (LogicalSort<? extends Plan>) super.visit(sort, replaceMap);
        List<OrderKey> newKeys = sort.getOrderKeys().stream()
                .map(old -> old.withExpression(updateExpression(old.getExpr(), replaceMap)))
                .collect(ImmutableList.toImmutableList());
        return sort.withOrderKeys(newKeys).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Map<ExprId, Slot> replaceMap) {
        topN = (LogicalTopN<? extends Plan>) super.visit(topN, replaceMap);
        List<OrderKey> newKeys = topN.getOrderKeys().stream()
                .map(old -> old.withExpression(updateExpression(old.getExpr(), replaceMap)))
                .collect(ImmutableList.toImmutableList());
        return topN.withOrderKeys(newKeys).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, Map<ExprId, Slot> replaceMap) {
        window = (LogicalWindow<? extends Plan>) super.visit(window, replaceMap);
        List<NamedExpression> windowExpressions =
                updateExpressions(window.getWindowExpressions(), replaceMap);
        windowExpressions.forEach(w -> replaceMap.put(w.getExprId(), w.toSlot()));
        return window.withExpressionsAndChild(windowExpressions, window.child());
    }

    @Override
    public Plan visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN,
            Map<ExprId, Slot> replaceMap) {
        partitionTopN = (LogicalPartitionTopN<? extends Plan>) super.visit(partitionTopN, replaceMap);
        List<Expression> partitionKeys = updateExpressions(partitionTopN.getPartitionKeys(), replaceMap);
        List<OrderExpression> orderKeys = updateExpressions(partitionTopN.getOrderKeys(), replaceMap);
        return partitionTopN.withPartitionKeysAndOrderKeys(partitionKeys, orderKeys);
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Map<ExprId, Slot> replaceMap) {
        Map<Slot, Slot> consumerToProducerOutputMap = new LinkedHashMap<>();
        Map<Slot, Slot> producerToConsumerOutputMap = new LinkedHashMap<>();
        for (Slot producerOutputSlot : cteConsumer.getConsumerToProducerOutputMap().values()) {
            Slot newProducerOutputSlot = updateExpression(producerOutputSlot, replaceMap);
            Slot newConsumerOutputSlot = cteConsumer.getProducerToConsumerOutputMap().get(producerOutputSlot)
                    .withNullable(newProducerOutputSlot.nullable());
            producerToConsumerOutputMap.put(newProducerOutputSlot, newConsumerOutputSlot);
            consumerToProducerOutputMap.put(newConsumerOutputSlot, newProducerOutputSlot);
            replaceMap.put(newConsumerOutputSlot.getExprId(), newConsumerOutputSlot);
        }
        return cteConsumer.withTwoMaps(consumerToProducerOutputMap, producerToConsumerOutputMap);
    }

    private <T extends Expression> T updateExpression(T input, Map<ExprId, Slot> replaceMap) {
        return (T) input.rewriteDownShortCircuit(e -> e.accept(SlotReferenceReplacer.INSTANCE, replaceMap));
    }

    private <T extends Expression> List<T> updateExpressions(List<T> inputs, Map<ExprId, Slot> replaceMap) {
        ImmutableList.Builder<T> result = ImmutableList.builderWithExpectedSize(inputs.size());
        for (T input : inputs) {
            result.add(updateExpression(input, replaceMap));
        }
        return result.build();
    }

    private <T extends Expression> Set<T> updateExpressions(Set<T> inputs, Map<ExprId, Slot> replaceMap) {
        ImmutableSet.Builder<T> result = ImmutableSet.builderWithExpectedSize(inputs.size());
        for (T input : inputs) {
            result.add(updateExpression(input, replaceMap));
        }
        return result.build();
    }

    private static class SlotReferenceReplacer extends DefaultExpressionRewriter<Map<ExprId, Slot>> {
        public static SlotReferenceReplacer INSTANCE = new SlotReferenceReplacer();

        @Override
        public Expression visitSlotReference(SlotReference slotReference, Map<ExprId, Slot> context) {
            if (context.containsKey(slotReference.getExprId())) {
                Slot slot = context.get(slotReference.getExprId());
                if (slot.getDataType().isAggStateType()) {
                    // we must replace data type, because nested type and agg state contains nullable of their children.
                    // TODO: remove if statement after we ensure be constant folding do not change expr type at all.
                    return slotReference.withNullableAndDataType(slot.nullable(), slot.getDataType());
                } else {
                    return slotReference.withNullable(slot.nullable());
                }
            } else {
                return slotReference;
            }
        }
    }
}
