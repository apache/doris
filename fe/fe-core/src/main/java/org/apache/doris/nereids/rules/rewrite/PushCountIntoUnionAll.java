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
// copied from https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/PluginManager.java

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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** PushCountIntoUnionAll */
public class PushCountIntoUnionAll extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalUnion().when(this::checkoutUnion))
                .whenNot(this::hasUnsuportedAggFunc)
                .then(this::doPush)
                .toRule(RuleType.PUSH_COUNT_INTO_UNION_ALL);
    }

    private Plan doPush(LogicalAggregate<LogicalUnion> agg) {
        LogicalUnion logicalUnion = agg.child();
        List<Slot> outputs = logicalUnion.getOutput();
        Map<Slot, Integer> mmap = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            mmap.put(outputs.get(i), i);
        }
        int childSize = logicalUnion.children().size();
        List<Expression> upperGroupByExpressions = agg.getGroupByExpressions();
        List<NamedExpression> upperOutputExpressions = agg.getOutputExpressions();
        Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(childSize);
        Builder<List<SlotReference>> childrenOutputs = ImmutableList.builderWithExpectedSize(childSize);
        // create the pushed down LogicalAggregate
        for (int i = 0; i < childSize; i++) {
            Plan child = logicalUnion.children().get(i);
            List<Slot> childOutputs = child.getOutput();
            List<Expression> groupByExpressions = replaceExpressionByUnionAll(upperGroupByExpressions, mmap,
                    childOutputs);
            List<NamedExpression> outputExpressions = replaceExpressionByUnionAll(upperOutputExpressions, mmap,
                    childOutputs);
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
            Map<Slot, Integer> mmap, List<Slot> childOutputs) {
        // 遍历expressions，如果出现了mmap中的slot，那么替换为childOutputs[mmap[slot]]
        return ExpressionUtils.rewriteDownShortCircuit(expressions, expr -> {
            if (expr instanceof Alias && ((Alias) expr).child() instanceof Count) {
                Count cnt = (Count) ((Alias) expr).child();
                if (cnt.isCountStar()) {
                    return new Alias(new Count());
                } else {
                    Expression newCntChild = cnt.child(0).rewriteDownShortCircuit(e -> {
                        if (e instanceof SlotReference && mmap.containsKey(e)) {
                            return childOutputs.get(mmap.get(e));
                        }
                        return e;
                    });
                    return new Alias(new Count(newCntChild));
                }
            } else if (expr instanceof SlotReference && mmap.containsKey(expr)) {
                return childOutputs.get(mmap.get(expr));
            }
            return expr;
        });
    }

    private boolean hasUnsuportedAggFunc(LogicalAggregate aggregate) {
        // 如果有不是count的aggfunc，或者有count distinct 都不支持这个规则
        return ExpressionUtils.deapAnyMatch(aggregate.getOutputExpressions(), expr -> {
            if (expr instanceof AggregateFunction) {
                return !(expr instanceof Count) || ((Count) expr).isDistinct();
            } else {
                return false;
            }
        });
    }

    private boolean checkoutUnion(LogicalUnion union) {
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
