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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LogicalAggregate  (groupByExpr=[c1#13], outputExpr=[c1#13, count(c1#13) AS `count(c1)`#15])
 *  +--LogicalUnion (outputs=[c1#13], regularChildrenOutputs=[[c1#9], [a#4], [a#7]])
 *    |--child1 (output = [[c1#9]])
 *    |--child2 (output = [[a#4]])
 *    +--child3 (output = [[a#7]])
 * transform to:
 * LogicalAggregate (groupByExpr=[c1#13], outputExpr=[c1#13, sum0(count(c1)#19) AS `count(c1)`#15])
 *  +--LogicalUnion (outputs=[c1#13, count(c1)#19], regularChildrenOutputs=[[c1#9, count(c1)#16],
 *   [a#4, count(a)#17], [a#7, count(a)#18]])
 *    |--LogicalAggregate (groupByExpr=[c1#9], outputExpr=[c1#9, count(c1#9) AS `count(c1)`#16])
 *    |  +--child1
 *    |--LogicalAggregate (groupByExpr=[a#4], outputExpr=[a#4, count(a#4) AS `count(a)`#17])
 *    |  +--child2
 *    +--LogicalAggregate (groupByExpr=[a#7], outputExpr=[a#7, count(a#7) AS `count(a)`#18]]
 *      +--child3
 */
public class PushCountIntoUnionAll implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalAggregate(logicalUnion().when(this::checkUnion))
                .when(this::checkAgg)
                .then(this::doPush)
                .toRule(RuleType.PUSH_COUNT_INTO_UNION_ALL),
                logicalAggregate(logicalProject(logicalUnion().when(this::checkUnion)))
                .when(this::checkAgg)
                .when(this::checkProjectUseless)
                .then(this::removeProjectAndPush)
                .toRule(RuleType.PUSH_COUNT_INTO_UNION_ALL)
                );
    }

    private Plan doPush(LogicalAggregate<LogicalUnion> agg) {
        LogicalUnion logicalUnion = agg.child();
        List<Slot> outputs = logicalUnion.getOutput();
        Map<Slot, Integer> replaceMap = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            replaceMap.put(outputs.get(i), i);
        }
        int childSize = logicalUnion.children().size();
        List<Expression> upperGroupByExpressions = agg.getGroupByExpressions();
        List<NamedExpression> upperOutputExpressions = agg.getOutputExpressions();
        Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(childSize);
        Builder<List<SlotReference>> childrenOutputs = ImmutableList.builderWithExpectedSize(childSize);
        // create the pushed down LogicalAggregate
        List<List<SlotReference>> childSlots = logicalUnion.getRegularChildrenOutputs();
        for (int i = 0; i < childSize; i++) {
            List<SlotReference> childOutputs = childSlots.get(i);
            List<Expression> groupByExpressions = replaceExpressionByUnionAll(upperGroupByExpressions, replaceMap,
                    childOutputs);
            List<NamedExpression> outputExpressions = replaceExpressionByUnionAll(upperOutputExpressions, replaceMap,
                    childOutputs);
            Plan child = logicalUnion.children().get(i);
            LogicalAggregate<Plan> logicalAggregate = new LogicalAggregate<>(groupByExpressions, outputExpressions,
                    child);
            newChildren.add(logicalAggregate);
            childrenOutputs.add((List<SlotReference>) (List) logicalAggregate.getOutput());
        }

        // create the new LogicalUnion
        LogicalSetOperation newLogicalUnion = logicalUnion.withChildrenAndTheirOutputs(newChildren.build(),
                childrenOutputs.build());
        List<NamedExpression> newLogicalUnionOutputs = Lists.newArrayList();
        for (NamedExpression ce : upperOutputExpressions) {
            if (ce instanceof Alias) {
                newLogicalUnionOutputs.add(new SlotReference(ce.getName(), ce.getDataType(), ce.nullable()));
            } else if (ce instanceof SlotReference) {
                newLogicalUnionOutputs.add(ce);
            } else {
                return logicalUnion;
            }
        }
        newLogicalUnion = newLogicalUnion.withNewOutputs(newLogicalUnionOutputs);

        // The count in the upper agg is converted to sum0, and the alias id and name of the count remain unchanged.
        Builder<NamedExpression> newUpperOutputExpressions = ImmutableList.builderWithExpectedSize(
                upperOutputExpressions.size());
        for (int i = 0; i < upperOutputExpressions.size(); i++) {
            NamedExpression sum0Child = newLogicalUnionOutputs.get(i);
            Expression rewrittenExpression = upperOutputExpressions.get(i).rewriteDownShortCircuit(expr -> {
                if (expr instanceof Alias && ((Alias) expr).child() instanceof Count) {
                    Alias alias = ((Alias) expr);
                    return new Alias(alias.getExprId(), new Sum0(sum0Child), alias.getName());
                }
                return expr;
            });
            newUpperOutputExpressions.add((NamedExpression) rewrittenExpression);
        }
        return agg.withAggOutputChild(newUpperOutputExpressions.build(), newLogicalUnion);
    }

    private <E extends Expression> List<E> replaceExpressionByUnionAll(List<E> expressions,
            Map<Slot, Integer> replaceMap, List<? extends Slot> childOutputs) {
        // Traverse expressions. If a slot in replaceMap appears, replace it with childOutputs[replaceMap[slot]]
        return ExpressionUtils.rewriteDownShortCircuit(expressions, expr -> {
            if (expr instanceof Alias && ((Alias) expr).child() instanceof Count) {
                Count cnt = (Count) ((Alias) expr).child();
                if (cnt.isCountStar()) {
                    return new Alias(new Count());
                } else {
                    Expression newCntChild = cnt.child(0).rewriteDownShortCircuit(e -> {
                        if (e instanceof SlotReference && replaceMap.containsKey(e)) {
                            return childOutputs.get(replaceMap.get(e));
                        }
                        return e;
                    });
                    return new Alias(new Count(newCntChild));
                }
            } else if (expr instanceof SlotReference && replaceMap.containsKey(expr)) {
                return childOutputs.get(replaceMap.get(expr));
            }
            return expr;
        });
    }

    private boolean checkAgg(LogicalAggregate aggregate) {
        Set<Count> res = ExpressionUtils.collect(aggregate.getOutputExpressions(), expr -> expr instanceof Count);
        if (res.isEmpty()) {
            return false;
        }
        return !hasUnsuportedAggFunc(aggregate);
    }

    private boolean checkProjectUseless(LogicalAggregate<LogicalProject<LogicalUnion>> agg) {
        LogicalProject<LogicalUnion> project = agg.child();
        if (project.getProjects().size() != 1) {
            return false;
        }
        if (!(project.getProjects().get(0) instanceof Alias)) {
            return false;
        }
        Alias alias = (Alias) project.getProjects().get(0);
        if (!alias.child(0).equals(new TinyIntLiteral((byte) 1))) {
            return false;
        }
        List<NamedExpression> aggOutputs = agg.getOutputExpressions();
        Slot slot = project.getOutput().get(0);
        if (ExpressionUtils.anyMatch(aggOutputs, expr -> expr.equals(slot))) {
            return false;
        }
        return true;
    }

    private Plan removeProjectAndPush(LogicalAggregate<LogicalProject<LogicalUnion>> agg) {
        Plan afterRemove = agg.withChildren(agg.child().child());
        return doPush((LogicalAggregate<LogicalUnion>) afterRemove);
    }

    private boolean hasUnsuportedAggFunc(LogicalAggregate aggregate) {
        // only support count, not suport sum,min... and not support count(distinct)
        return ExpressionUtils.deapAnyMatch(aggregate.getOutputExpressions(), expr -> {
            if (expr instanceof AggregateFunction) {
                if (!(expr instanceof Count)) {
                    return true;
                } else {
                    return ((Count) expr).isDistinct();
                }
            } else {
                return false;
            }
        });
    }

    private boolean checkUnion(LogicalUnion union) {
        if (union.getQualifier() != Qualifier.ALL) {
            return false;
        }
        if (union.children() == null || union.children().isEmpty()) {
            return false;
        }
        if (!union.getConstantExprsList().isEmpty()) {
            return false;
        }
        return true;
    }
}
