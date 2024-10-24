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
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * join (l_orderkey > n_nationkey + n_regionkey)
 *    +----scan(lineItem)
 *    +----scan(nation)
 * =>
 * join(l_orderkey > x)
 *    +----scan(lineItem)
 *    +----project(n_nationkey + n_regionkey as x)
 *         +----scan(nation)
 */
public class ProjectOtherJoinConditionForNestedLoopJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getHashJoinConjuncts().isEmpty()
                        && !join.isMarkJoin()
                        && !join.getOtherJoinConjuncts().isEmpty())
                .whenNot(join -> ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable()
                        .allowedRuntimeFilterType(TRuntimeFilterType.BITMAP))
                .then(join -> {
                    List<Expression> otherConjuncts = join.getOtherJoinConjuncts();
                    List<Expression> newOtherConjuncts = new ArrayList<>();
                    Set<Slot> leftSlots = join.child(0).getOutputSet();
                    Set<Slot> rightSlots = join.child(1).getOutputSet();
                    ReplacerContext ctx = new ReplacerContext(leftSlots, rightSlots);
                    for (Expression conj : otherConjuncts) {
                        Expression newConj = conj.accept(AliasReplacer.INSTANCE, ctx);
                        newOtherConjuncts.add(newConj);
                    }
                    boolean changed = !ctx.leftAlias.isEmpty() || !ctx.rightAlias.isEmpty();
                    if (changed) {
                        Plan left = join.left();
                        if (!ctx.leftAlias.isEmpty()) {
                            List<NamedExpression> newProjects = Lists.newArrayList(left.getOutput());
                            newProjects.addAll(ctx.leftAlias);
                            left = new LogicalProject<>(newProjects, left);
                        }
                        Plan right = join.right();
                        if (!ctx.rightAlias.isEmpty()) {
                            List<NamedExpression> newProjects = Lists.newArrayList(right.getOutput());
                            newProjects.addAll(ctx.rightAlias);
                            right = new LogicalProject<>(newProjects, right);
                        }
                        return join.withJoinConjuncts(join.getHashJoinConjuncts(),
                                newOtherConjuncts, join.getJoinReorderContext())
                                .withChildren(ImmutableList.of(left, right));
                    }
                    return null;
                }
        ).toRule(RuleType.PROJECT_OTHER_JOIN_CONDITION);
    }

    private static class ReplacerContext {
        HashMap<Expression, Alias> aliasMap = new HashMap<>();
        Set<Slot> leftSlots;
        Set<Slot> rightSlots;
        Set<Alias> leftAlias = new HashSet<>();
        Set<Alias> rightAlias = new HashSet<>();

        public ReplacerContext(Set<Slot> leftSlots, Set<Slot> rightSlots) {
            this.leftSlots = leftSlots;
            this.rightSlots = rightSlots;
        }

    }

    private static class AliasReplacer extends DefaultExpressionRewriter<ReplacerContext> {
        public static AliasReplacer INSTANCE = new AliasReplacer();

        @Override
        public Expression visit(Expression expression, ReplacerContext ctx) {
            Set<Slot> input = expression.getInputSlots();
            if (input.isEmpty() || expression instanceof Slot) {
                return expression;
            }
            if (ctx.leftSlots.containsAll(input)) {
                Alias alias = ctx.aliasMap.computeIfAbsent(expression, o -> new Alias(o));
                ctx.leftAlias.add(alias);
                return alias.toSlot();
            } else if (ctx.rightSlots.containsAll(input)) {
                Alias alias = ctx.aliasMap.computeIfAbsent(expression, o -> new Alias(o));
                ctx.rightAlias.add(alias);
                return alias.toSlot();
            } else {
                return super.visit(expression, ctx);
            }
        }

    }
}
