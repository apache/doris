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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Join extract OR expression from case when / if / nullif expressions.
 *
 * 1. extract conditions for one side, latter can push down the one side condition:
 *
 *    t1 join t2 on not (case when t1.a = 1 then t2.a else t2.b) + t2.b + t2.c > 10)
 *    =>
 *    t1 join t2 on not (case when t1.a = 1 then t2.a else t2.b end) + t2.b + t2.c > 10)
 *                  AND (not (t2.a + t2.b + t2.c > 10) or not (t2.b + t2.b + t2.c > 10))
 *
 *
 * 2. extract conditions for both sides, which use for OR EXPANSION rule:
 *    the OR EXPANSION is an OR expression which all its disjuncts are hash join conditions.
 *
 *    t1 join t2 on (case when t1.a = 1 then t2.a else t2.b end) = t1.a + t1.b
 *    =>
 *    t1 join t2 on (case when t1.a = 1 then t2.a else t2.b end) = t1.a + t1.b
 *                AND (t2.a = t1.a + t1.b or t2.b = t1.a + t1.b)

 * Notice we don't extract more than one case when like expressions.
 * because it may generate expressions with combinatorial explosion.
 *
 * (((case c1 then p1 else p2 end) + (case when d1 then q1 else q2 end))) + a  > 10
 * =>
 * (p1 + q1 + a > 10)
 *     or (p1 + q2 + a > 10)
 *     or (p2 + q1 + a > 10)
 *     or (p2 + q2 + a > 10)
 *
 * so we only extract at most one case when like expression for each condition.
 */
public class JoinExtractOrFromCaseWhen implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalJoin()
                .when(this::needRewrite)
                .then(this::rewrite)
                .toRule(RuleType.JOIN_EXTRACT_OR_FROM_CASE_WHEN));
    }

    private boolean needRewrite(LogicalJoin<?, ?> join) {
        Set<Slot> leftSlots = join.left().getOutputSet();
        Set<Slot> rightSlots = join.right().getOutputSet();
        for (Expression expr : join.getOtherJoinConjuncts()) {
            if (isConditionNeedRewrite(expr, leftSlots, rightSlots)) {
                return true;
            }
        }
        return false;
    }

    // 1. expr contains slots from both sides;
    private boolean isConditionNeedRewrite(Expression expr, Set<Slot> leftSlots, Set<Slot> rightSlots) {
        return getExtractChildIndexAndOtherChildSlotFromLeft(expr, leftSlots, rightSlots).isPresent();
    }

    private Plan rewrite(LogicalJoin<? extends Plan, ? extends Plan> join) {
        Set<Expression> newOtherConditions = Sets.newLinkedHashSetWithExpectedSize(join.getOtherJoinConjuncts().size());
        newOtherConditions.addAll(join.getOtherJoinConjuncts());
        int oldCondSize = newOtherConditions.size();
        boolean extractHashCondition = OrExpansion.INSTANCE.needRewriteJoin(join);
        List<Expression> orExpandConds = Lists.newArrayList();
        for (Expression expr : join.getOtherJoinConjuncts()) {
            tryAddOrExpansionHashCondition(orExpandConds, expr, join);
        }
        for (Expression expr : join.getOtherJoinConjuncts()) {
            extractExpression(join, expr, extractHashCondition, newOtherConditions, orExpandConds);
        }
        if (!orExpandConds.isEmpty()) {
            newOtherConditions.addAll(orExpandConds);
        }
        if (newOtherConditions.size() == oldCondSize) {
            return join;
        } else {
            return join.withJoinConjuncts(join.getHashJoinConjuncts(), ImmutableList.copyOf(newOtherConditions),
                    join.getJoinReorderContext());
        }
    }

    private void extractExpression(LogicalJoin<? extends Plan, ? extends Plan> join, Expression expr,
            boolean extractHashCondition, Set<Expression> conditions, List<Expression> orExpandConds) {
        Set<Slot> leftSlots = join.left().getOutputSet();
        Set<Slot> rightSlots = join.right().getOutputSet();
        Optional<Pair<Integer, Boolean>> extractOpt
                = getExtractChildIndexAndOtherChildSlotFromLeft(expr, leftSlots, rightSlots);
        if (!extractOpt.isPresent()) {
            return;
        }

        int extractChildIndex = extractOpt.get().first;
        Boolean otherChildrenFromLeft = extractOpt.get().second;
        if (otherChildrenFromLeft == null) {
            doExtractExpression(expr, extractChildIndex, true, leftSlots, rightSlots)
                    .ifPresent(conditions::add);
            doExtractExpression(expr, extractChildIndex, false, leftSlots, rightSlots)
                    .ifPresent(conditions::add);
        } else {
            doExtractExpression(expr, extractChildIndex, otherChildrenFromLeft, leftSlots, rightSlots)
                    .ifPresent(conditions::add);
            if (expr instanceof EqualPredicate && extractHashCondition) {
                doExtractExpression(expr, extractChildIndex, !otherChildrenFromLeft, leftSlots, rightSlots)
                        .ifPresent(cond -> tryAddOrExpansionHashCondition(orExpandConds, cond, join));
            }
        }
    }

    // Or Expansion only use one condition, so we keep the one with least disjunctions.
    private void tryAddOrExpansionHashCondition(List<Expression> orExpandConds,
            Expression condition, LogicalJoin<? extends Plan, ? extends Plan> join) {
        // Or Expansion only works for all the disjunctions are equal predicates
        if (!JoinUtils.extractExpressionForHashTable(
                join.left().getOutput(), join.right().getOutput(), ExpressionUtils.extractDisjunction(condition)
                ).second.isEmpty()) {
            return;
        }

        if (orExpandConds.isEmpty()) {
            orExpandConds.add(condition);
        } else {
            int childNum = condition instanceof Or ? condition.children().size() : 1;
            int otherChildNum = orExpandConds.get(0) instanceof Or ? orExpandConds.get(0).children().size() : 1;
            if (childNum < otherChildNum) {
                orExpandConds.clear();
                orExpandConds.add(condition);
            }
        }
    }

    // one child contains both side slots, other children contains only one side slots.
    private Optional<Pair<Integer, Boolean>> getExtractChildIndexAndOtherChildSlotFromLeft(Expression expr,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        if (expr.containsUniqueFunction()) {
            return Optional.empty();
        }
        int extractChildIndex = -1;
        Boolean otherChildSlotFromLeft = null;
        for (int i = 0; i < expr.children().size(); i++) {
            Expression child = expr.child(i);
            Set<Slot> childSlots = child.getInputSlots();
            if (childSlots.isEmpty()) {
                continue;
            }
            boolean containsLeft = !Collections.disjoint(childSlots, leftSlots);
            boolean containsRight = !Collections.disjoint(childSlots, rightSlots);
            if (containsLeft && containsRight) {
                if (extractChildIndex != -1 || !ExpressionUtils.containsCaseWhenLikeType(child)) {
                    // more than one child contains both side slots
                    return Optional.empty();
                }
                extractChildIndex = i;
            } else if (containsLeft) {
                if (otherChildSlotFromLeft == null) {
                    otherChildSlotFromLeft = true;
                } else if (!otherChildSlotFromLeft) {
                    // one child from left, another child from right
                    return Optional.empty();
                }
            } else if (containsRight) {
                if (otherChildSlotFromLeft == null) {
                    otherChildSlotFromLeft = false;
                } else if (otherChildSlotFromLeft) {
                    // one child from left, another child from right
                    return Optional.empty();
                }
            } else {
                // should not be here
                return Optional.empty();
            }
        }

        if (extractChildIndex == -1) {
            return Optional.empty();
        }

        return Optional.of(Pair.of(extractChildIndex, otherChildSlotFromLeft));
    }

    private Optional<Expression> doExtractExpression(Expression expr, int extractChildIndex, boolean childSlotFromLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Expression target = expr.child(extractChildIndex);
        Optional<List<Expression>> expandTargetOpt = tryExtractCaseWhen(
                target, childSlotFromLeft, leftSlots, rightSlots);
        if (!expandTargetOpt.isPresent()) {
            return Optional.empty();
        }

        List<Expression> expandTargetExpressions = expandTargetOpt.get();
        if (expandTargetExpressions.size() <= 1) {
            return Optional.empty();
        }

        List<Expression> newChildren = Lists.newArrayList(expr.children());
        List<Expression> disjuncts = Lists.newArrayListWithExpectedSize(expandTargetExpressions.size());
        for (Expression expandTargetExpr : expandTargetExpressions) {
            newChildren.set(extractChildIndex, expandTargetExpr);
            disjuncts.add(expr.withChildren(newChildren));
        }

        Expression result = ExpressionUtils.or(disjuncts);
        if (result.getInputSlots().isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(ExpressionUtils.or(result));
    }

    private Optional<List<Expression>> tryExtractCaseWhen(Expression expr, boolean childSlotFromLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        if (isSlotsEmptyOrFrom(expr, childSlotFromLeft, leftSlots, rightSlots)) {
            return Optional.of(ImmutableList.of(expr));
        }

        Optional<List<Expression>> caseWhenLikeResults = ExpressionUtils.getCaseWhenLikeBranchResults(expr);
        if (caseWhenLikeResults.isPresent()) {
            for (Expression branchResult : caseWhenLikeResults.get()) {
                if (!isSlotsEmptyOrFrom(branchResult, childSlotFromLeft, leftSlots, rightSlots)) {
                    return Optional.empty();
                }
            }
            return caseWhenLikeResults;
        }

        if (!ExpressionUtils.containsCaseWhenLikeType(expr)) {
            return Optional.empty();
        }

        int expandChildIndex = -1;
        List<Expression> expandChildExpressions = null;
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(expr.children().size());
        for (int i = 0; i < expr.children().size(); i++) {
            Expression child = expr.child(i);
            Optional<List<Expression>> childExtractedOpt = tryExtractCaseWhen(
                    child, childSlotFromLeft, leftSlots, rightSlots);
            if (!childExtractedOpt.isPresent()) {
                return Optional.empty();
            }
            List<Expression> childExtracted = childExtractedOpt.get();
            if (childExtracted.size() == 1) {
                Expression newChild = childExtracted.get(0);
                newChildren.add(newChild);
            } else {
                // more than one child to expand
                if (expandChildIndex != -1) {
                    return Optional.empty();
                }
                expandChildIndex = i;
                expandChildExpressions = childExtracted;
                // will replace the child later, add a placeholder first
                newChildren.add(child);
            }
        }
        if (expandChildIndex == -1) {
            return Optional.empty();
        }
        List<Expression> resultExpressions = Lists.newArrayListWithExpectedSize(expandChildExpressions.size());
        for (Expression expandChildExpr : expandChildExpressions) {
            newChildren.set(expandChildIndex, expandChildExpr);
            Expression newExpr = expr.withChildren(newChildren);
            resultExpressions.add(newExpr);
        }
        return Optional.of(resultExpressions);
    }

    private boolean isSlotsEmptyOrFrom(Expression expr, boolean slotFromLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Set<Slot> exprSlots = expr.getInputSlots();
        if (slotFromLeft) {
            return Collections.disjoint(exprSlots, rightSlots);
        } else {
            return Collections.disjoint(exprSlots, leftSlots);
        }
    }

}
