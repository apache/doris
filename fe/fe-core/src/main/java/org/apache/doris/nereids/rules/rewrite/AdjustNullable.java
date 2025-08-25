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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
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
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * because some rule could change output's nullable.
 * So, we need add a rule to adjust all expression's nullable attribute after rewrite.
 */
public class AdjustNullable extends DefaultPlanRewriter<Map<ExprId, Slot>> implements CustomRewriter {

    private static final Logger LOG = LogManager.getLogger(AdjustNullable.class);

    private final boolean isAnalyzedPhase;

    public AdjustNullable(boolean isAnalyzedPhase) {
        this.isAnalyzedPhase = isAnalyzedPhase;
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, Maps.newHashMap());
    }

    @Override
    public Plan visit(Plan plan, Map<ExprId, Slot> replaceMap) {
        LogicalPlan logicalPlan = (LogicalPlan) super.visit(plan, replaceMap);
        logicalPlan = logicalPlan.recomputeLogicalProperties();
        for (Slot slot : logicalPlan.getOutput()) {
            replaceMap.put(slot.getExprId(), slot);
        }
        return logicalPlan;
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> logicalSink, Map<ExprId, Slot> replaceMap) {
        logicalSink = (LogicalSink<? extends Plan>) super.visit(logicalSink, replaceMap);
        Optional<List<NamedExpression>> newOutputExprs
                = updateExpressions(logicalSink.getOutputExprs(), replaceMap, true);
        if (!newOutputExprs.isPresent()) {
            return logicalSink;
        } else {
            return logicalSink.withOutputExprs(newOutputExprs.get());
        }
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Map<ExprId, Slot> replaceMap) {
        aggregate = (LogicalAggregate<? extends Plan>) super.visit(aggregate, replaceMap);
        Optional<List<NamedExpression>> newOutputs
                = updateExpressions(aggregate.getOutputExpressions(), replaceMap, true);
        Optional<List<Expression>> newGroupBy = updateExpressions(aggregate.getGroupByExpressions(), replaceMap, true);
        for (NamedExpression newOutput : newOutputs.orElse(aggregate.getOutputExpressions())) {
            replaceMap.put(newOutput.getExprId(), newOutput.toSlot());
        }
        if (!newOutputs.isPresent() && !newGroupBy.isPresent()) {
            return aggregate;
        }
        return aggregate.withGroupByAndOutput(
                newGroupBy.orElse(newGroupBy.orElse(aggregate.getGroupByExpressions())),
                newOutputs.orElse(newOutputs.orElse(aggregate.getOutputs()))
        );
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Map<ExprId, Slot> replaceMap) {
        filter = (LogicalFilter<? extends Plan>) super.visit(filter, replaceMap);
        Optional<Set<Expression>> conjuncts = updateExpressions(filter.getConjuncts(), replaceMap, true);
        if (!conjuncts.isPresent()) {
            return filter;
        }
        return filter.withConjunctsAndChild(conjuncts.get(), filter.child());
    }

    @Override
    public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Map<ExprId, Slot> replaceMap) {
        generate = (LogicalGenerate<? extends Plan>) super.visit(generate, replaceMap);
        Optional<List<Function>> newGenerators = updateExpressions(generate.getGenerators(), replaceMap, true);
        Plan newGenerate = generate;
        if (newGenerators.isPresent()) {
            newGenerate = generate.withGenerators(newGenerators.get()).recomputeLogicalProperties();
        }
        for (Slot slot : newGenerate.getOutput()) {
            replaceMap.put(slot.getExprId(), slot);
        }
        return newGenerate;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Map<ExprId, Slot> replaceMap) {
        join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, replaceMap);
        Optional<List<Expression>> hashConjuncts = updateExpressions(join.getHashJoinConjuncts(), replaceMap, true);
        Optional<List<Expression>> markConjuncts = Optional.empty();
        boolean hadUpdatedMarkConjuncts = false;
        if (isAnalyzedPhase || join.getHashJoinConjuncts().isEmpty()) {
            // if hashConjuncts is empty, mark join conjuncts may use to build hash table
            // so need call updateExpressions for mark join conjuncts before adjust nullable by output slot
            markConjuncts = updateExpressions(join.getMarkJoinConjuncts(), replaceMap, true);
            hadUpdatedMarkConjuncts = true;
        }
        // in fact, otherConjuncts shouldn't use join output nullable attribute,
        // it should use left and right tables' origin nullable attribute.
        // but for history reason, BE use join output nullable attribute for evaluating the other conditions.
        // so here, we make a difference:
        // 1) when at analyzed phase, still update other conjuncts without using join output nullables.
        //    then later at rewrite phase, the join conditions may push down, and the push down condition with proper
        //    nullable attribute.
        // 2) when at the end of rewrite phase, update other conjuncts with join output nullables.
        //    Just change it to be consistent with BE.
        Optional<List<Expression>> otherConjuncts = Optional.empty();
        if (isAnalyzedPhase) {
            otherConjuncts = updateExpressions(join.getOtherJoinConjuncts(), replaceMap, true);
        }
        for (Slot slot : join.getOutput()) {
            replaceMap.put(slot.getExprId(), slot);
        }
        if (!hadUpdatedMarkConjuncts) {
            markConjuncts = updateExpressions(join.getMarkJoinConjuncts(), replaceMap, false);
        }
        if (!isAnalyzedPhase) {
            otherConjuncts = updateExpressions(join.getOtherJoinConjuncts(), replaceMap, false);
        }
        if (!hashConjuncts.isPresent() && !markConjuncts.isPresent() && !otherConjuncts.isPresent()) {
            return join;
        }
        return join.withJoinConjuncts(
                hashConjuncts.orElse(join.getHashJoinConjuncts()),
                otherConjuncts.orElse(join.getOtherJoinConjuncts()),
                markConjuncts.orElse(join.getMarkJoinConjuncts()),
                join.getJoinReorderContext()
        ).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Map<ExprId, Slot> replaceMap) {
        project = (LogicalProject<? extends Plan>) super.visit(project, replaceMap);
        Optional<List<NamedExpression>> newProjects = updateExpressions(project.getProjects(), replaceMap, true);
        for (NamedExpression newProject : newProjects.orElse(project.getProjects())) {
            replaceMap.put(newProject.getExprId(), newProject.toSlot());
        }
        if (!newProjects.isPresent()) {
            return project;
        }
        return project.withProjects(newProjects.get());
    }

    @Override
    public Plan visitLogicalApply(LogicalApply<? extends Plan, ? extends Plan> apply, Map<ExprId, Slot> replaceMap) {
        apply = (LogicalApply<? extends Plan, ? extends Plan>) super.visit(apply, replaceMap);
        Optional<Expression> newCompareExpr = updateExpression(apply.getCompareExpr(), replaceMap, true);
        Optional<Expression> newTypeCoercionExpr = updateExpression(apply.getTypeCoercionExpr(), replaceMap, true);
        Optional<List<Slot>> newCorrelationSlot = updateExpressions(apply.getCorrelationSlot(), replaceMap, true);
        Optional<Expression> newCorrelationFilter = updateExpression(apply.getCorrelationFilter(), replaceMap, true);
        Optional<MarkJoinSlotReference> newMarkJoinSlotReference =
                updateExpression(apply.getMarkJoinSlotReference(), replaceMap, true);

        for (Slot slot : apply.getOutput()) {
            replaceMap.put(slot.getExprId(), slot);
        }
        if (!newCompareExpr.isPresent() && !newTypeCoercionExpr.isPresent() && !newCorrelationSlot.isPresent()
                && !newCorrelationFilter.isPresent() && !newMarkJoinSlotReference.isPresent()) {
            return apply;
        }

        return new LogicalApply<>(
                newCorrelationSlot.orElse(apply.getCorrelationSlot()),
                apply.getSubqueryType(),
                apply.isNot(),
                newCompareExpr.isPresent() ? newCompareExpr : apply.getCompareExpr(),
                newTypeCoercionExpr.isPresent() ? newTypeCoercionExpr : apply.getTypeCoercionExpr(),
                newCorrelationFilter.isPresent() ? newCorrelationFilter : apply.getCorrelationFilter(),
                newMarkJoinSlotReference.isPresent() ? newMarkJoinSlotReference : apply.getMarkJoinSlotReference(),
                apply.isNeedAddSubOutputToProjects(),
                apply.isMarkJoinSlotNotNull(),
                apply.left(),
                apply.right());
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Map<ExprId, Slot> replaceMap) {
        repeat = (LogicalRepeat<? extends Plan>) super.visit(repeat, replaceMap);
        Set<Expression> flattenGroupingSetExpr = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));
        List<NamedExpression> newOutputs = Lists.newArrayList();
        for (NamedExpression output : repeat.getOutputExpressions()) {
            NamedExpression newOutput;
            if (flattenGroupingSetExpr.contains(output)) {
                newOutput = output;
            } else {
                newOutput = updateExpression(output, replaceMap, true).orElse(output);
            }
            newOutputs.add(newOutput);
            replaceMap.put(newOutput.getExprId(), newOutput.toSlot());
        }
        return repeat.withGroupSetsAndOutput(repeat.getGroupingSets(), newOutputs).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalSetOperation(LogicalSetOperation setOperation, Map<ExprId, Slot> replaceMap) {
        setOperation = (LogicalSetOperation) super.visit(setOperation, replaceMap);
        ImmutableList.Builder<List<SlotReference>> newChildrenOutputs = ImmutableList.builder();
        List<Boolean> inputNullable = null;
        if (!setOperation.children().isEmpty()) {
            inputNullable = Lists.newArrayListWithCapacity(setOperation.getOutputs().size());
            for (int i = 0; i < setOperation.getOutputs().size(); i++) {
                inputNullable.add(false);
            }
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
            NamedExpression newOutput = ne instanceof Alias ? (NamedExpression) ne.withChildren(slot) : slot;
            newOutputs.add(newOutput);
            replaceMap.put(newOutput.getExprId(), newOutput.toSlot());
        }
        return setOperation.withNewOutputs(newOutputs)
                .withChildrenAndTheirOutputs(setOperation.children(), newChildrenOutputs.build())
                .recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Map<ExprId, Slot> replaceMap) {
        sort = (LogicalSort<? extends Plan>) super.visit(sort, replaceMap);

        boolean changed = false;
        ImmutableList.Builder<OrderKey> newOrderKeys = ImmutableList.builder();
        for (OrderKey orderKey : sort.getOrderKeys()) {
            Optional<Expression> newOrderKey = updateExpression(orderKey.getExpr(), replaceMap, true);
            if (!newOrderKey.isPresent()) {
                newOrderKeys.add(orderKey);
            } else {
                changed = true;
                newOrderKeys.add(orderKey.withExpression(newOrderKey.get()));
            }
        }
        if (!changed) {
            return sort;
        }
        return sort.withOrderKeysAndChild(newOrderKeys.build(), sort.child());
    }

    @Override
    public Plan visitLogicalTopN(LogicalTopN<? extends Plan> topN, Map<ExprId, Slot> replaceMap) {
        topN = (LogicalTopN<? extends Plan>) super.visit(topN, replaceMap);

        boolean changed = false;
        ImmutableList.Builder<OrderKey> newOrderKeys = ImmutableList.builder();
        for (OrderKey orderKey : topN.getOrderKeys()) {
            Optional<Expression> newOrderKey = updateExpression(orderKey.getExpr(), replaceMap, true);
            if (!newOrderKey.isPresent()) {
                newOrderKeys.add(orderKey);
            } else {
                changed = true;
                newOrderKeys.add(orderKey.withExpression(newOrderKey.get()));
            }
        }
        if (!changed) {
            return topN;
        }
        return topN.withOrderKeys(newOrderKeys.build()).recomputeLogicalProperties();
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, Map<ExprId, Slot> replaceMap) {
        window = (LogicalWindow<? extends Plan>) super.visit(window, replaceMap);
        Optional<List<NamedExpression>> windowExpressions =
                updateExpressions(window.getWindowExpressions(), replaceMap, true);
        for (NamedExpression w : windowExpressions.orElse(window.getWindowExpressions())) {
            replaceMap.put(w.getExprId(), w.toSlot());
        }
        if (!windowExpressions.isPresent()) {
            return window;
        }
        return window.withExpressionsAndChild(windowExpressions.get(), window.child());
    }

    @Override
    public Plan visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN,
            Map<ExprId, Slot> replaceMap) {
        partitionTopN = (LogicalPartitionTopN<? extends Plan>) super.visit(partitionTopN, replaceMap);
        Optional<List<Expression>> partitionKeys
                = updateExpressions(partitionTopN.getPartitionKeys(), replaceMap, true);
        Optional<List<OrderExpression>> orderKeys = updateExpressions(partitionTopN.getOrderKeys(), replaceMap, true);
        if (!partitionKeys.isPresent() && !orderKeys.isPresent()) {
            return partitionTopN;
        }
        return partitionTopN.withPartitionKeysAndOrderKeys(
                partitionKeys.orElse(partitionTopN.getPartitionKeys()), orderKeys.orElse(partitionTopN.getOrderKeys())
        );
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Map<ExprId, Slot> replaceMap) {
        Map<Slot, Slot> consumerToProducerOutputMap = new LinkedHashMap<>();
        Multimap<Slot, Slot> producerToConsumerOutputMap = LinkedHashMultimap.create();
        for (Slot producerOutputSlot : cteConsumer.getConsumerToProducerOutputMap().values()) {
            Optional<Slot> newProducerOutputSlot = updateExpression(producerOutputSlot, replaceMap, true);
            for (Slot consumerOutputSlot : cteConsumer.getProducerToConsumerOutputMap().get(producerOutputSlot)) {
                Slot slot = newProducerOutputSlot.orElse(producerOutputSlot);
                Slot newConsumerOutputSlot = consumerOutputSlot.withNullable(slot.nullable());
                producerToConsumerOutputMap.put(slot, newConsumerOutputSlot);
                consumerToProducerOutputMap.put(newConsumerOutputSlot, slot);
                replaceMap.put(newConsumerOutputSlot.getExprId(), newConsumerOutputSlot);
            }
        }
        return cteConsumer.withTwoMaps(consumerToProducerOutputMap, producerToConsumerOutputMap);
    }

    private <T extends Expression> Optional<T> updateExpression(Optional<T> input,
            Map<ExprId, Slot> replaceMap, boolean debugCheck) {
        return input.isPresent() ? updateExpression(input.get(), replaceMap, debugCheck) : Optional.empty();
    }

    private <T extends Expression> Optional<T> updateExpression(T input,
            Map<ExprId, Slot> replaceMap, boolean debugCheck) {
        AtomicBoolean changed = new AtomicBoolean(false);
        Expression replaced = doUpdateExpression(changed, input, replaceMap, debugCheck, isAnalyzedPhase);
        return changed.get() ? Optional.of((T) replaced) : Optional.empty();
    }

    /**
     * Adjust nullable attribute of slot reference in expression.
     */
    public static Expression doUpdateExpression(AtomicBoolean changed, Expression input,
            Map<ExprId, Slot> replaceMap, boolean debugCheck, boolean isAnalyzedPhase) {
        return input.rewriteDownShortCircuit(e -> {
            if (e instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) e;
                Slot newSlotReference = slotReference;
                Slot replacedSlot = replaceMap.get(slotReference.getExprId());
                if (replacedSlot != null) {
                    if (replacedSlot.getDataType().isAggStateType()) {
                        if (slotReference.nullable() != replacedSlot.nullable()
                                || !slotReference.getDataType().equals(replacedSlot.getDataType())) {
                            // we must replace data type, because nested type and agg state contains nullable
                            // of their children.
                            // TODO: remove if statement after we ensure be constant folding do not change
                            //  expr type at all.
                            changed.set(true);
                            newSlotReference = slotReference.withNullableAndDataType(
                                    replacedSlot.nullable(), replacedSlot.getDataType());
                        }
                    } else if (slotReference.nullable() != replacedSlot.nullable()) {
                        changed.set(true);
                        newSlotReference = slotReference.withNullable(replacedSlot.nullable());
                    }
                }
                // for join other conditions, debugCheck = false, for other case, debugCheck is always true.
                // Because join other condition use join output's nullable attribute, outer join may check fail.
                // At analyzed phase, the slot reference nullable may change, for example, NormalRepeat may adjust some
                // slot reference to nullable, after this rule, node above repeat need adjust.
                // so analyzed phase don't assert not-nullable -> nullable, otherwise adjust plan above
                // repeat may check fail.
                if (!slotReference.nullable() && newSlotReference.nullable()
                        && !isAnalyzedPhase && debugCheck && ConnectContext.get() != null) {
                    if (ConnectContext.get().getSessionVariable().feDebug) {
                        throw new AnalysisException("AdjustNullable convert slot " + slotReference
                                + " from not-nullable to nullable. You can disable check by set fe_debug = false.");
                    } else {
                        LOG.warn("adjust nullable convert slot '" + slotReference
                                + "' from not-nullable to nullable for query "
                                + DebugUtil.printId(ConnectContext.get().queryId()));
                    }
                }
                return newSlotReference;
            } else {
                return e;
            }
        });
    }

    private <T extends Expression> Optional<List<T>> updateExpressions(List<T> inputs,
            Map<ExprId, Slot> replaceMap, boolean debugCheck) {
        ImmutableList.Builder<T> result = ImmutableList.builderWithExpectedSize(inputs.size());
        boolean changed = false;
        for (T input : inputs) {
            Optional<T> newInput = updateExpression(input, replaceMap, debugCheck);
            changed |= newInput.isPresent();
            result.add(newInput.orElse(input));
        }
        return changed ? Optional.of(result.build()) : Optional.empty();
    }

    private <T extends Expression> Optional<Set<T>> updateExpressions(Set<T> inputs,
            Map<ExprId, Slot> replaceMap, boolean debugCheck) {
        boolean changed = false;
        ImmutableSet.Builder<T> result = ImmutableSet.builderWithExpectedSize(inputs.size());
        for (T input : inputs) {
            Optional<T> newInput = updateExpression(input, replaceMap, debugCheck);
            changed |= newInput.isPresent();
            result.add(newInput.orElse(input));
        }
        return changed ? Optional.of(result.build()) : Optional.empty();
    }
}
