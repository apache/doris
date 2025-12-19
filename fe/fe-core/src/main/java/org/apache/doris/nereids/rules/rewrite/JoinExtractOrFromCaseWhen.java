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

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalJoin()
                .when(this::needRewrite)
                .thenApply(ctx -> rewrite(ctx.root, new ExpressionRewriteContext(ctx.root, ctx.cascadesContext)))
                .toRule(RuleType.JOIN_EXTRACT_OR_FROM_CASE_WHEN));
    }

    private boolean needRewrite(LogicalJoin<Plan, Plan> join) {
        if (PushDownJoinOtherCondition.needRewrite(join) || OrExpansion.INSTANCE.needRewriteJoin(join)) {
            Set<Slot> leftSlots = join.left().getOutputSet();
            Set<Slot> rightSlots = join.right().getOutputSet();
            for (Expression expr : join.getOtherJoinConjuncts()) {
                if (isConditionNeedRewrite(expr, leftSlots, rightSlots)) {
                    return true;
                }
            }
        }
        if (!join.isMarkJoin()) {
            Set<Slot> leftSlots = join.left().getOutputSet();
            Set<Slot> rightSlots = join.right().getOutputSet();
            for (Expression expr : join.getHashJoinConjuncts()) {
                if (isConditionNeedRewrite(expr, leftSlots, rightSlots)) {
                    return true;
                }
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
        return true;
    }

    private Plan rewrite(LogicalJoin<Plan, Plan> join, ExpressionRewriteContext context) {
        Set<Expression> newOtherConditions = Sets.newLinkedHashSetWithExpectedSize(join.getOtherJoinConjuncts().size());
        newOtherConditions.addAll(join.getOtherJoinConjuncts());
        int oldCondSize = newOtherConditions.size();
        AtomicReference<Pair<Expression, Integer>> leastOrExpandCondRef = new AtomicReference<>();
        for (Expression expr : join.getOtherJoinConjuncts()) {
            tryAddOrExpansionHashCondition(expr, join, context, leastOrExpandCondRef);
        }
        for (Expression expr : join.getOtherJoinConjuncts()) {
            extractExpression(expr, context, join, newOtherConditions, leastOrExpandCondRef);
        }
        if (!join.isMarkJoin()) {
            // Notice: if join's hash conditions is not empty, then OrExpansion.needRewriteJoin will return fail
            // so it will not extract OrExpansion from the hash condition, it only extract one side condition
            // for hash condition:  if(t1.a > 10, 1, 100) = if(t2.x > 10, 2, 200),
            // we can still extract two one-side condition:
            // 1) if(t1.a > 10, 1, 100) = 2 or if(t1.a > 10, 1, 100) = 200
            // 2) if(t2.x > 10, 2, 200) = 1 or if(t2.x > 10, 2, 200) = 100
            for (Expression expr : join.getHashJoinConjuncts()) {
                extractExpression(expr, context, join, newOtherConditions, leastOrExpandCondRef);
            }
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
            LogicalJoin<Plan, Plan> join, Set<Expression> newOtherConditions,
            AtomicReference<Pair<Expression, Integer>> leastOrExpandCondRef) {
        Set<Slot> leftSlots = join.left().getOutputSet();
        Set<Slot> rightSlots = join.right().getOutputSet();
        if (!isConditionNeedRewrite(expr, leftSlots, rightSlots)) {
            return;
        }
        List<Integer> containsLeftSlotChildIndexes = Lists.newArrayList();
        List<Integer> containsRightSlotChildIndexes = Lists.newArrayList();
        for (int i = 0; i < expr.children().size(); i++) {
            Set<Slot> childSlots = expr.child(i).getInputSlots();
            if (!Collections.disjoint(childSlots, leftSlots)) {
                containsLeftSlotChildIndexes.add(i);
            }
            if (!Collections.disjoint(childSlots, rightSlots)) {
                containsRightSlotChildIndexes.add(i);
            }
        }
        // all slots are from one side, no need handle
        if (containsLeftSlotChildIndexes.isEmpty() || containsRightSlotChildIndexes.isEmpty()) {
            return;
        }
        boolean canPushDownOther = PushDownJoinOtherCondition.needRewrite(join);
        boolean extractedLeftSideCond = canPushDownOther && PushDownJoinOtherCondition.PUSH_DOWN_LEFT_VALID_TYPE
                .contains(join.getJoinType());
        boolean extractedRightSideCond = canPushDownOther && PushDownJoinOtherCondition.PUSH_DOWN_RIGHT_VALID_TYPE
                .contains(join.getJoinType());
        boolean extractOrExpansionCond = OrExpansion.INSTANCE.needRewriteJoin(join);
        // eliminate all the right slots of all children, but we rewrite at most 1 case when expression,
        // so require contains right slot child num not exceeds 1.
        if (extractedLeftSideCond && containsRightSlotChildIndexes.size() == 1) {
            doExtractExpression(expr, containsRightSlotChildIndexes.get(0), true, leftSlots, rightSlots)
                    .ifPresent(newOtherConditions::add);
        }
        // eliminate all the left slots of all children, but we rewrite at most 1 case when expression,
        // so require contains left slot child num not exceeds 1.
        if (extractedRightSideCond && containsLeftSlotChildIndexes.size() == 1) {
            doExtractExpression(expr, containsLeftSlotChildIndexes.get(0), false, leftSlots, rightSlots)
                    .ifPresent(newOtherConditions::add);
        }
        if (extractOrExpansionCond && expr instanceof EqualPredicate) {
            Optional<Expression> orExpansionExpr = Optional.empty();
            if (containsLeftSlotChildIndexes.size() == 1 && containsRightSlotChildIndexes.size() == 2) {
                // equal's two children all contain right slots, while only one child contains left slot,
                // then we eliminate the right slot from the child which it contains left slots.
                orExpansionExpr = doExtractExpression(
                        expr, containsLeftSlotChildIndexes.get(0), true, leftSlots, rightSlots);
            } else if (containsLeftSlotChildIndexes.size() == 2 && containsRightSlotChildIndexes.size() == 1) {
                // equal's two children all contain left slots, while one child contains left slot,
                // then we eliminate the left slot from the child which it contains right slots.
                orExpansionExpr = doExtractExpression(
                        expr, containsRightSlotChildIndexes.get(0), false, leftSlots, rightSlots);
            }
            orExpansionExpr.ifPresent(
                    cond -> tryAddOrExpansionHashCondition(cond, join, context, leastOrExpandCondRef));
        }
    }

    // Or Expansion only use one condition, so we keep the one with least disjunctions.
    private void tryAddOrExpansionHashCondition(Expression condition, LogicalJoin<Plan, Plan> join,
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

    // extract case when expression from `expr`'s child C at `extractChildIndex`.
    // after extraction, new C will contain only slots from one side indicated by `extractedChildSlotFromLeft`,
    // but new C can allow no contains any slots.
    private Optional<Expression> doExtractExpression(Expression expr, int extractChildIndex,
            boolean extractChildSlotFromLeft, Set<Slot> leftSlots, Set<Slot> rightSlots) {
        Expression expandChild = expr.child(extractChildIndex);
        Optional<List<Expression>> resultOpt = tryExtractCaseWhen(
                expandChild, extractChildSlotFromLeft, leftSlots, rightSlots);
        if (!resultOpt.isPresent()) {
            return Optional.empty();
        }

        List<Expression> expandTargetExpressions = resultOpt.get();
        if (expandTargetExpressions.size() <= 1) {
            // if size = 1, then C don't expand, should be just the expr itself.
            return Optional.empty();
        }

        List<Expression> newChildren = Lists.newArrayList(expr.children());
        List<Expression> disjuncts = Lists.newArrayListWithExpectedSize(expandTargetExpressions.size());
        for (Expression expandTargetExpr : expandTargetExpressions) {
            newChildren.set(extractChildIndex, expandTargetExpr);
            disjuncts.add(expr.withChildren(newChildren));
        }

        return Optional.of(ExpressionUtils.or(disjuncts));
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

    // check whether all slots in expr are from one side, allow contains no slots.
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
}
