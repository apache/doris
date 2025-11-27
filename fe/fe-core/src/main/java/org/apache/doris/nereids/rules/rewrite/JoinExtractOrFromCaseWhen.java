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
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
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
import java.util.concurrent.atomic.AtomicReference;

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

    private enum SlotFrom {
        FROM_LEFT_SIDE_ONLY,
        FROM_RIGHT_SIDE_ONLY,
        FROM_BOTH_SIDE,
        FROM_NONE,
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalJoin()
                .when(this::needRewrite)
                .thenApply(ctx -> rewrite(ctx.root, new ExpressionRewriteContext(ctx.root, ctx.cascadesContext)))
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
        if (expr.containsUniqueFunction()) {
            return false;
        }
        Set<Slot> exprSlots = expr.getInputSlots();
        // all slots are from one side, no need to process it.
        if (Collections.disjoint(exprSlots, leftSlots) || Collections.disjoint(exprSlots, rightSlots)
                || !ExpressionUtils.containsCaseWhenLikeType(expr)) {
            return false;
        }
        // can not rewrite none case when child
        return getNoneCaseWhenChildSlotFrom(expr, leftSlots, rightSlots) != SlotFrom.FROM_BOTH_SIDE;
    }

    private Plan rewrite(LogicalJoin<? extends Plan, ? extends Plan> join, ExpressionRewriteContext context) {
        Set<Expression> newOtherConditions = Sets.newLinkedHashSetWithExpectedSize(join.getOtherJoinConjuncts().size());
        newOtherConditions.addAll(join.getOtherJoinConjuncts());
        int oldCondSize = newOtherConditions.size();
        boolean extractOrExpansionCondition = OrExpansion.INSTANCE.needRewriteJoin(join);
        AtomicReference<Pair<Expression, Integer>> leastOrExpandCondRef = new AtomicReference<>();
        for (Expression expr : join.getOtherJoinConjuncts()) {
            tryAddOrExpansionHashCondition(expr, join, context, leastOrExpandCondRef);
        }
        for (Expression expr : join.getOtherJoinConjuncts()) {
            extractExpression(expr, context, join, extractOrExpansionCondition, newOtherConditions, leastOrExpandCondRef);
        }
        if (leastOrExpandCondRef.get() != null) {
            newOtherConditions.add(leastOrExpandCondRef.get().first);
        }
        if (newOtherConditions.size() == oldCondSize) {
            return join;
        } else {
            return join.withJoinConjuncts(join.getHashJoinConjuncts(), ImmutableList.copyOf(newOtherConditions),
                    join.getJoinReorderContext());
        }
    }

    private void extractExpression(Expression expr, ExpressionRewriteContext context,
            LogicalJoin<? extends Plan, ? extends Plan> join, boolean extractOrExpansionCondition,
            Set<Expression> conditions, AtomicReference<Pair<Expression, Integer>> leastOrExpandCondRef) {
        Set<Slot> leftSlots = join.left().getOutputSet();
        Set<Slot> rightSlots = join.right().getOutputSet();
        if (!isConditionNeedRewrite(expr, leftSlots, rightSlots)) {
            return;
        }
        Boolean otherChildrenFromLeft = null;
        switch (getNoneCaseWhenChildSlotFrom(expr, leftSlots, rightSlots)) {
            case FROM_BOTH_SIDE: {
                return;
            }
            case FROM_LEFT_SIDE_ONLY: {
                otherChildrenFromLeft = true;
                break;
            }
            case FROM_RIGHT_SIDE_ONLY: {
                otherChildrenFromLeft = false;
                break;
            }
        }

        List<SlotFrom> childrenSlotFrom = Lists.newArrayListWithExpectedSize(expr.children().size());
        for (Expression child : expr.children()) {
            childrenSlotFrom.add(getExpressionSlotFrom(child, leftSlots, rightSlots));
        }
        for (int i = 0; i < expr.children().size(); i++) {
            if (!ExpressionUtils.containsCaseWhenLikeType(expr.child(i))) {
                continue;
            }
            if (otherChildrenFromLeft == null) {
                // extract expression for left side
                doExtractExpression(expr, i, true, true, childrenSlotFrom, leftSlots, rightSlots)
                        .ifPresent(conditions::add);
                // extract expression for right side
                doExtractExpression(expr, i, false, false, childrenSlotFrom, leftSlots, rightSlots)
                        .ifPresent(conditions::add);
            } else {
                // extract expression for one side, all child slots need from the same side
                doExtractExpression(expr, i, otherChildrenFromLeft, otherChildrenFromLeft, childrenSlotFrom, leftSlots, rightSlots)
                        .ifPresent(conditions::add);
                if (expr instanceof EqualPredicate && extractOrExpansionCondition) {
                    // extract expression for hash condition only when the expr is equal predicate
                    doExtractExpression(expr, i, !otherChildrenFromLeft, otherChildrenFromLeft, childrenSlotFrom,
                            leftSlots, rightSlots).ifPresent(
                                    cond -> tryAddOrExpansionHashCondition(cond, join, context, leastOrExpandCondRef));
                }
            }
        }
    }

    // Or Expansion only use one condition, so we keep the one with least disjunctions.
    private void tryAddOrExpansionHashCondition(Expression condition, LogicalJoin<? extends Plan, ? extends Plan> join,
            ExpressionRewriteContext context, AtomicReference<Pair<Expression, Integer>> leastOrExpandCondRef) {
        // Or Expansion only works for all the disjunctions are equal predicates
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(condition);
        List<Expression> remainOtherConditions = JoinUtils.extractExpressionForHashTable(
                join.left().getOutput(), join.right().getOutput(), disjunctions).second;
        int hashCondLen = disjunctions.size() - remainOtherConditions.size();
        // no hash condition extracted, all are other conditions
        if (hashCondLen == 0) {
            return;
        }

        for (Expression expr : remainOtherConditions) {
            // for case when t1.a > t2.x then t1.a when t1.b > t2.y then t1. b else null end = t2.x
            // then will extract E1 = (t1.a = t2.x or t1.b = t2.x or null = t2.x)
            // but E1 can not use as OR Expansion condition, because null = t2.x is not a valid hash join condition.
            // but after we fold null = t2.x to null, latter expression simplifier can simplify E1
            // to (t1.a = t2.x or t1.b = t2.x), then it becomes a valid OR Expansion condition.
            Expression foldExpr = FoldConstantRuleOnFE.evaluate(expr, context);
            if (!foldExpr.isLiteral()) {
                return;
            }
            // foldExpr should be NULL / TRUE /FALSE,  later expression simplifier can handle them.
        }

        Pair<Expression, Integer> leastOrExpandCond = leastOrExpandCondRef.get();
        int oldHashCondLen = leastOrExpandCond == null ? -1 : leastOrExpandCond.second;
        if (oldHashCondLen == -1 || hashCondLen < oldHashCondLen) {
            leastOrExpandCondRef.set(Pair.of(condition, hashCondLen));
        }
    }

    private SlotFrom getNoneCaseWhenChildSlotFrom(Expression expr, Set<Slot> leftSlots, Set<Slot> rightSlots) {
        SlotFrom mergeSlotFrom = SlotFrom.FROM_NONE;
        for (int i = 0; i < expr.children().size(); i++) {
            Expression child = expr.child(i);
            if (ExpressionUtils.containsCaseWhenLikeType(child)) {
                continue;
            }
            SlotFrom childSlotFrom = getExpressionSlotFrom(child, leftSlots, rightSlots);
            switch (childSlotFrom) {
                case FROM_LEFT_SIDE_ONLY: {
                    if (mergeSlotFrom == SlotFrom.FROM_RIGHT_SIDE_ONLY) {
                        return SlotFrom.FROM_BOTH_SIDE;
                    }
                    mergeSlotFrom = childSlotFrom;
                    break;
                }
                case FROM_RIGHT_SIDE_ONLY: {
                    if (mergeSlotFrom == SlotFrom.FROM_LEFT_SIDE_ONLY) {
                        return SlotFrom.FROM_BOTH_SIDE;
                    }
                    mergeSlotFrom = childSlotFrom;
                    break;
                }
                case FROM_BOTH_SIDE: {
                    return childSlotFrom;
                }
            }
        }

        return mergeSlotFrom;
    }

    // extract case when expression from `expr`'s child at `extractChildIndex`.
    // after extraction, all slots of this child at `extractChildIndex`
    // are from one side indicated by `extractChildSlotFromLeft`.
    // for expr's other children, their slot from need met the otherChildSlotFromLeft
    private Optional<Expression> doExtractExpression(Expression expr, int extractChildIndex,
            boolean extractChildSlotFromLeft, boolean otherChildSlotFromLeft, List<SlotFrom> childrenSlotFrom,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        for (int i = 0; i < expr.children().size(); i++) {
            // we only rewrite extractChildIndex,
            // so for other child, it must need the requirement of `otherChildSlotFromLeft`
            if (i != extractChildIndex) {
                // use childrenSlotFrom to avoid call Collection.disjoint too many, but maybe we can delete it.
                SlotFrom slotFrom = childrenSlotFrom.get(i);
                if (slotFrom == SlotFrom.FROM_BOTH_SIDE
                        || (otherChildSlotFromLeft && slotFrom == SlotFrom.FROM_RIGHT_SIDE_ONLY)
                        || (!otherChildSlotFromLeft && slotFrom == SlotFrom.FROM_LEFT_SIDE_ONLY)) {
                    return Optional.empty();
                }
            }
        }

        Expression expandChild = expr.child(extractChildIndex);
        Optional<List<Expression>> resultOpt = tryExtractCaseWhen(
                expandChild, extractChildSlotFromLeft, leftSlots, rightSlots);
        if (!resultOpt.isPresent()) {
            return Optional.empty();
        }

        List<Expression> expandTargetExpressions = resultOpt.get();
        if (expandTargetExpressions.size() <= 1) {
            // if size = 1, then it don't expand, should be just the expr itself.
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

    // try to extract case when like expressions from expr.
    // after extraction, all slots in expr are from one side indicated by `slotFromLeft`.
    // if `expr`'s all slots are already from `slotFromLeft`, return expr itself, no need handle with its children.
    // otherwise will recurse its children to extract case when like expressions.
    private Optional<List<Expression>> tryExtractCaseWhen(Expression expr, boolean slotFromLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        if (isAllSlotsFromLeftSide(expr, slotFromLeft, leftSlots, rightSlots)) {
            return Optional.of(ImmutableList.of(expr));
        }

        if (!ExpressionUtils.containsCaseWhenLikeType(expr)) {
            return Optional.empty();
        }

        // process case when like expression.
        Optional<List<Expression>> caseWhenLikeResults = ExpressionUtils.getCaseWhenLikeBranchResults(expr);
        if (caseWhenLikeResults.isPresent()) {
            for (Expression branchResult : caseWhenLikeResults.get()) {
                if (!isAllSlotsFromLeftSide(branchResult, slotFromLeft, leftSlots, rightSlots)) {
                    return Optional.empty();
                }
            }
            return caseWhenLikeResults;
        }

        int expandChildIndex = -1;
        List<Expression> expandChildExpressions = null;
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(expr.children().size());
        for (int i = 0; i < expr.children().size(); i++) {
            Expression child = expr.child(i);
            Optional<List<Expression>> childExtractedOpt = tryExtractCaseWhen(
                    child, slotFromLeft, leftSlots, rightSlots);
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

    // check whether all slots in expr are from one side, allow empty slots.
    private boolean isAllSlotsFromLeftSide(Expression expr, boolean slotFromLeft,
            Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Set<Slot> exprSlots = expr.getInputSlots();
        if (slotFromLeft) {
            // no slots from right
            return Collections.disjoint(exprSlots, rightSlots);
        } else {
            // no slots from left
            return Collections.disjoint(exprSlots, leftSlots);
        }
    }

    private SlotFrom getExpressionSlotFrom(Expression expr, Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Set<Slot> exprSlots = expr.getInputSlots();
        boolean containsLeft = !Collections.disjoint(exprSlots, leftSlots);
        boolean containsRight = !Collections.disjoint(exprSlots, rightSlots);
        if (containsLeft && containsRight) {
            return SlotFrom.FROM_BOTH_SIDE;
        } else if (containsLeft) {
            return SlotFrom.FROM_LEFT_SIDE_ONLY;
        } else if (containsRight) {
            return SlotFrom.FROM_RIGHT_SIDE_ONLY;
        } else {
            return SlotFrom.FROM_NONE;
        }
    }

}
