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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

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
public class AdjustNullable implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.ADJUST_NULLABLE_ON_AGGREGATE.build(logicalAggregate().then(aggregate -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(aggregate);
                    List<NamedExpression> newOutputs
                            = updateExpressions(aggregate.getOutputExpressions(), exprIdSlotMap);
                    List<Expression> newGroupExpressions
                            = updateExpressions(aggregate.getGroupByExpressions(), exprIdSlotMap);
                    return aggregate.withGroupByAndOutput(newGroupExpressions, newOutputs).recomputeLogicalProperties();
                })),
                RuleType.ADJUST_NULLABLE_ON_ASSERT_NUM_ROWS.build(logicalAssertNumRows().then(
                        LogicalPlan::recomputeLogicalProperties)),
                RuleType.ADJUST_NULLABLE_ON_FILTER.build(logicalFilter().then(filter -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(filter);
                    Set<Expression> conjuncts = updateExpressions(filter.getConjuncts(), exprIdSlotMap);
                    return new LogicalFilter<>(conjuncts, filter.child());
                })),
                RuleType.ADJUST_NULLABLE_ON_GENERATE.build(logicalGenerate().then(generate -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(generate);
                    List<Function> newGenerators = updateExpressions(generate.getGenerators(), exprIdSlotMap);
                    return generate.withGenerators(newGenerators).recomputeLogicalProperties();
                })),
                RuleType.ADJUST_NULLABLE_ON_JOIN.build(logicalJoin().then(join -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(join);
                    List<Expression> hashConjuncts = updateExpressions(join.getHashJoinConjuncts(), exprIdSlotMap);
                    List<Expression> otherConjuncts = updateExpressions(join.getOtherJoinConjuncts(), exprIdSlotMap);
                    return join.withJoinConjuncts(hashConjuncts, otherConjuncts).recomputeLogicalProperties();
                })),
                RuleType.ADJUST_NULLABLE_ON_LIMIT.build(logicalLimit().then(LogicalPlan::recomputeLogicalProperties)),
                RuleType.ADJUST_NULLABLE_ON_PROJECT.build(logicalProject().then(project -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(project);
                    List<NamedExpression> newProjects = updateExpressions(project.getProjects(), exprIdSlotMap);
                    return project.withProjects(newProjects);
                })),
                RuleType.ADJUST_NULLABLE_ON_REPEAT.build(logicalRepeat().then(repeat -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(repeat);
                    List<NamedExpression> newOutputs = updateExpressions(repeat.getOutputExpressions(), exprIdSlotMap);
                    List<List<Expression>> newGroupingSets = repeat.getGroupingSets().stream()
                            .map(l -> updateExpressions(l, exprIdSlotMap))
                            .collect(ImmutableList.toImmutableList());
                    return repeat.withGroupSetsAndOutput(newGroupingSets, newOutputs).recomputeLogicalProperties();
                })),
                RuleType.ADJUST_NULLABLE_ON_SET_OPERATION.build(logicalSetOperation().then(setOperation -> {
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
                        SlotReference slotReference = (SlotReference) outputs.get(i);
                        if (inputNullable.get(i)) {
                            slotReference = slotReference.withNullable(true);
                        }
                        newOutputs.add(slotReference);
                    }
                    return setOperation.withNewOutputs(newOutputs).recomputeLogicalProperties();
                })),
                RuleType.ADJUST_NULLABLE_ON_SORT.build(logicalSort().then(sort -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(sort);
                    List<OrderKey> newKeys = sort.getOrderKeys().stream()
                            .map(old -> old.withExpression(updateExpression(old.getExpr(), exprIdSlotMap)))
                            .collect(ImmutableList.toImmutableList());
                    return sort.withOrderKeys(newKeys).recomputeLogicalProperties();
                })),
                RuleType.ADJUST_NULLABLE_ON_TOP_N.build(logicalTopN().then(topN -> {
                    Map<ExprId, Slot> exprIdSlotMap = collectChildrenOutputMap(topN);
                    List<OrderKey> newKeys = topN.getOrderKeys().stream()
                            .map(old -> old.withExpression(updateExpression(old.getExpr(), exprIdSlotMap)))
                            .collect(ImmutableList.toImmutableList());
                    return topN.withOrderKeys(newKeys).recomputeLogicalProperties();
                }))
        );
    }

    private <T extends Expression> T updateExpression(T input, Map<ExprId, Slot> exprIdSlotMap) {
        return (T) input.rewriteDownShortCircuit(e -> SlotReferenceReplacer.INSTANCE.visit(e, exprIdSlotMap));
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
            return context.getOrDefault(slotReference.getExprId(), slotReference);
        }
    }
}
