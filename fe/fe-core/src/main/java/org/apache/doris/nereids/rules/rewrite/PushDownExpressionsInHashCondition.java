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
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * push down expression which is not slot reference
 */
public class PushDownExpressionsInHashCondition extends OneRewriteRuleFactory {
    /*
     * rewrite example:
     *       join(t1.a + 1 = t2.b + 2)                            join(c = d)
     *             /       \                                         /    \
     *            /         \                                       /      \
     *           /           \             ====>                   /        \
     *          /             \                                   /          \
     *  olapScan(t1)     olapScan(t2)            project(t1.a + 1 as c)  project(t2.b + 2 as d)
     *                                                        |                   |
     *                                                        |                   |
     *                                                        |                   |
     *                                                        |                   |
     *                                                     olapScan(t1)        olapScan(t2)
     *TODO: now t1.a + t2.a = t1.b is not in hashJoinConjuncts. The rule will not handle it.
     */
    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getHashJoinConjuncts().stream().anyMatch(equalTo ->
                        equalTo.children().stream().anyMatch(e -> !(e instanceof Slot))))
                .then(PushDownExpressionsInHashCondition::pushDownHashExpression)
                .toRule(RuleType.PUSH_DOWN_EXPRESSIONS_IN_HASH_CONDITIONS);
    }

    /**
     * push down complex expression in hash condition
     */
    public static LogicalJoin<? extends Plan, ? extends Plan> pushDownHashExpression(
            LogicalJoin<? extends Plan, ? extends Plan> join) {
        Set<NamedExpression> leftProjectExprs = Sets.newHashSet();
        Set<NamedExpression> rightProjectExprs = Sets.newHashSet();
        Map<Expression, NamedExpression> exprReplaceMap = Maps.newHashMap();
        join.getHashJoinConjuncts().forEach(conjunct -> {
            Preconditions.checkArgument(conjunct instanceof EqualPredicate);
            // sometimes: t1 join t2 on t2.a + 1 = t1.a + 2, so check the situation, but actually it
            // doesn't swap the two sides.
            conjunct = JoinUtils.swapEqualToForChildrenOrder((EqualPredicate) conjunct, join.left().getOutputSet());
            generateReplaceMapAndProjectExprs(conjunct.child(0), exprReplaceMap, leftProjectExprs);
            generateReplaceMapAndProjectExprs(conjunct.child(1), exprReplaceMap, rightProjectExprs);
        });

        // add other conjuncts used slots to project exprs
        Set<ExprId> leftExprIdSet = join.left().getOutputExprIdSet();
        join.getOtherJoinConjuncts().stream().flatMap(conjunct ->
                conjunct.getInputSlots().stream()
        ).forEach(slot -> {
            if (leftExprIdSet.contains(slot.getExprId())) {
                // belong to left child
                leftProjectExprs.add(slot);
            } else {
                // belong to right child
                rightProjectExprs.add(slot);
            }
        });

        // add mark conjuncts used slots to project exprs
        join.getMarkJoinConjuncts().stream().flatMap(conjunct ->
                conjunct.getInputSlots().stream()
        ).forEach(slot -> {
            if (leftExprIdSet.contains(slot.getExprId())) {
                // belong to left child
                leftProjectExprs.add(slot);
            } else {
                // belong to right child
                rightProjectExprs.add(slot);
            }
        });

        List<Expression> newHashConjuncts = join.getHashJoinConjuncts().stream()
                .map(equalTo -> equalTo.withChildren(equalTo.children()
                        .stream().map(expr -> exprReplaceMap.get(expr).toSlot())
                        .collect(ImmutableList.toImmutableList())))
                .collect(ImmutableList.toImmutableList());
        return join.withHashJoinConjunctsAndChildren(
                newHashConjuncts,
                createChildProjectPlan(join.left(), join, leftProjectExprs),
                createChildProjectPlan(join.right(), join, rightProjectExprs), join.getJoinReorderContext());

    }

    private static LogicalProject createChildProjectPlan(Plan plan, LogicalJoin join,
            Set<NamedExpression> conditionUsedExprs) {
        Set<NamedExpression> intersectionSlots = Sets.newHashSet(plan.getOutputSet());
        intersectionSlots.retainAll(join.getOutputSet());
        intersectionSlots.addAll(conditionUsedExprs);
        return new LogicalProject(intersectionSlots.stream()
                .collect(ImmutableList.toImmutableList()), plan);
    }

    private static void generateReplaceMapAndProjectExprs(Expression expr, Map<Expression, NamedExpression> replaceMap,
            Set<NamedExpression> projects) {
        if (expr instanceof SlotReference) {
            projects.add((SlotReference) expr);
            replaceMap.put(expr, (SlotReference) expr);
        } else {
            Alias alias = new Alias(expr, "expr_" + expr.toSql());
            if (replaceMap.putIfAbsent(expr, alias.toSlot()) == null) {
                projects.add(alias);
            }
        }
    }
}
