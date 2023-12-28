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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * https://blogs.oracle.com/optimizer/post/optimizer-transformations-or-expansion
 * NLJ (cond1 or cond2)              UnionAll
 *                      =>         /        \
 *                           HJ(cond1) HJ(cond2 and !cond1)
 */
public class OrExpansion extends OneExplorationRuleFactory {
    public static final OrExpansion INSTANCE = new OrExpansion();
    public static final ImmutableSet<JoinType> supportJoinType = new ImmutableSet
            .Builder<JoinType>()
            .add(JoinType.INNER_JOIN)
            .add(JoinType.LEFT_ANTI_JOIN)
            .add(JoinType.LEFT_OUTER_JOIN)
            .add(JoinType.FULL_OUTER_JOIN)
            .build();

    @Override
    public Rule build() {
        return logicalJoin(any(), any()).when(JoinUtils::shouldNestedLoopJoin)
                .when(join -> supportJoinType.contains(join.getJoinType())
                        && ConnectContext.get().getSessionVariable().getEnablePipelineEngine())
                .thenApply(ctx -> {
                    LogicalJoin<? extends Plan, ? extends Plan> join = ctx.root;
                    Preconditions.checkArgument(join.getHashJoinConjuncts().isEmpty(),
                            "Only Expansion nest loop join without hashCond");

                    //1. Try to split or conditions
                    Pair<List<Expression>, List<Expression>> hashOtherConditions = splitOrCondition(join);
                    if (hashOtherConditions == null || hashOtherConditions.first.size() <= 1) {
                        return join;
                    }

                    //2. Construct CTE with the children
                    LogicalCTEProducer<? extends Plan> leftProducer = new LogicalCTEProducer<>(
                            ctx.statementContext.getNextCTEId(), join.left());
                    LogicalCTEProducer<? extends Plan> rightProducer = new LogicalCTEProducer<>(
                            ctx.statementContext.getNextCTEId(), join.right());
                    List<Plan> joins = new ArrayList<>();

                    // 3. Expand join to hash join with CTE
                    if (join.getJoinType().isInnerJoin()) {
                        joins.addAll(expandInnerJoin(ctx.cascadesContext, hashOtherConditions,
                                join, leftProducer, rightProducer));
                    } else if (join.getJoinType().isOuterJoin()) {
                        // left outer join = inner join union left anti join
                        joins.addAll(expandInnerJoin(ctx.cascadesContext, hashOtherConditions,
                                join, leftProducer, rightProducer));
                        joins.add(expandLeftAntiJoin(ctx.cascadesContext,
                                hashOtherConditions, join, leftProducer, rightProducer));
                        if (join.getJoinType().equals(JoinType.FULL_OUTER_JOIN)) {
                            // full outer join = inner join union left anti join union right anti join
                            joins.add(expandLeftAntiJoin(ctx.cascadesContext,
                                    hashOtherConditions, join, rightProducer, leftProducer));
                        }
                    } else if (join.getJoinType().equals(JoinType.LEFT_ANTI_JOIN)) {
                        joins.add(expandLeftAntiJoin(ctx.cascadesContext,
                                hashOtherConditions, join, leftProducer, rightProducer));
                    } else {
                        throw new RuntimeException("or-expansion is not supported for " + join);
                    }

                    //4. union all joins and construct LogicalCTEAnchor with CTEs
                    List<List<SlotReference>> childrenOutputs = joins.stream()
                            .map(j -> j.getOutput().stream()
                                    .map(SlotReference.class::cast)
                                    .collect(ImmutableList.toImmutableList()))
                            .collect(ImmutableList.toImmutableList());
                    LogicalUnion union = new LogicalUnion(Qualifier.ALL, new ArrayList<>(join.getOutput()),
                            childrenOutputs, ImmutableList.of(), false, joins);
                    LogicalCTEAnchor<? extends Plan, ? extends Plan> intermediateAnchor = new LogicalCTEAnchor<>(
                            rightProducer.getCteId(), rightProducer, union);
                    return new LogicalCTEAnchor<Plan, Plan>(leftProducer.getCteId(), leftProducer, intermediateAnchor);
                }).toRule(RuleType.OR_EXPANSION);
    }

    // try to find a condition that can be split into hash conditions
    // If that conditions exist, return split disjunctions, and other
    // conditions without the or condition, otherwise return null.
    private @Nullable Pair<List<Expression>, List<Expression>> splitOrCondition(
            LogicalJoin<? extends Plan, ? extends Plan> join) {
        List<Expression> otherConditions = new ArrayList<>(join.getOtherJoinConjuncts());
        for (Expression expr : otherConditions) {
            List<Expression> disjunctions = ExpressionUtils.extractDisjunction(expr);
            Pair<List<Expression>, List<Expression>> hashOtherCond = JoinUtils.extractExpressionForHashTable(
                    join.left().getOutput(), join.right().getOutput(), disjunctions);
            if (hashOtherCond.second.isEmpty()) {
                otherConditions.remove(expr);
                return Pair.of(disjunctions, otherConditions);
            }
        }
        return null;
    }

    // expand Anti Join:
    // Left Anti join cond1 or cond2           Left Anti join cond1
    // /                      \                /               \
    //left                   right   ===>  Anti join cond2    CTERight2
    //                                      /      \
    //                                  CTELeft CTERight1
    private Plan expandLeftAntiJoin(CascadesContext ctx,
            Pair<List<Expression>, List<Expression>> hashOtherConditions,
            LogicalJoin<? extends Plan, ? extends Plan> originJoin,
            LogicalCTEProducer<? extends Plan> leftProducer,
            LogicalCTEProducer<? extends org.apache.doris.nereids.trees.plans.Plan> rightProducer) {
        LogicalCTEConsumer left = new LogicalCTEConsumer(ctx.getStatementContext().getNextRelationId(),
                leftProducer.getCteId(), "", leftProducer);
        LogicalCTEConsumer right = new LogicalCTEConsumer(ctx.getStatementContext().getNextRelationId(),
                rightProducer.getCteId(), "", rightProducer);
        ctx.putCTEIdToConsumer(left);
        ctx.putCTEIdToConsumer(right);

        Map<Slot, Slot> replaced = new HashMap<>(left.getProducerToConsumerOutputMap());
        replaced.putAll(right.getProducerToConsumerOutputMap());
        List<Expression> disjunctions = hashOtherConditions.first;
        List<Expression> otherConditions = hashOtherConditions.second;
        otherConditions = otherConditions.stream()
                .map(e -> e.rewriteUp(s -> replaced.containsKey(s) ? replaced.get(s) : s)).collect(Collectors.toList());

        Expression hashCond = disjunctions.get(0);
        hashCond = hashCond.rewriteUp(s -> replaced.containsKey(s) ? replaced.get(s) : s);
        Plan newPlan = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN, Lists.newArrayList(hashCond),
                otherConditions, originJoin.getHint(),
                originJoin.getMarkJoinSlotReference(), left, right);
        if (hashCond.children().stream().anyMatch(e -> !(e instanceof Slot))) {
            Plan normalizedPlan = PushDownExpressionsInHashCondition.pushDownHashExpression(
                    (LogicalJoin<? extends Plan, ? extends Plan>) newPlan);
            newPlan = new LogicalProject<>(new ArrayList<>(newPlan.getOutput()), normalizedPlan);
        }

        for (int i = 1; i < disjunctions.size(); i++) {
            hashCond = disjunctions.get(i);
            LogicalCTEConsumer newRight = new LogicalCTEConsumer(
                    ctx.getStatementContext().getNextRelationId(), rightProducer.getCteId(), "", rightProducer);
            ctx.putCTEIdToConsumer(newRight);
            Map<Slot, Slot> newReplaced = new HashMap<>(left.getProducerToConsumerOutputMap());
            newReplaced.putAll(newRight.getProducerToConsumerOutputMap());
            hashCond = hashCond.rewriteUp(s -> newReplaced.containsKey(s) ? newReplaced.get(s) : s);
            newPlan = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN, Lists.newArrayList(hashCond),
                    new ArrayList<>(), originJoin.getHint(),
                    originJoin.getMarkJoinSlotReference(), newPlan, newRight);
            if (hashCond.children().stream().anyMatch(e -> !(e instanceof Slot))) {
                newPlan = PushDownExpressionsInHashCondition.pushDownHashExpression(
                        (LogicalJoin<? extends Plan, ? extends Plan>) newPlan);
            }
        }

        Plan finalNewPlan = newPlan;
        List<NamedExpression> projects = originJoin.getOutput().stream()
                .map(replaced::get)
                .map(s -> finalNewPlan.getOutputSet().contains(s) ? s :
                        new Alias(new NullLiteral(s.getDataType()), s.getName()))
                .collect(Collectors.toList());
        return new LogicalProject<>(projects, newPlan);
    }

    // expand Inner Join:
    // Inner join cond1 or cond2                   UnionAll
    // /                     \                /               \
    //left                   right   ===>  Inner join cond1   Inner join cond1 and !cond2
    //                                      /      \          /       \
    //                                  CTELeft1 CTERight1  CTELeft2  CTERight2
    private List<Plan> expandInnerJoin(CascadesContext ctx, Pair<List<Expression>,
            List<Expression>> hashOtherConditions,
            LogicalJoin<? extends Plan, ? extends Plan> join, LogicalCTEProducer<? extends Plan> leftProducer,
            LogicalCTEProducer<? extends Plan> rightProducer) {
        List<Expression> disjunctions = hashOtherConditions.first;
        List<Expression> otherConditions = hashOtherConditions.second;
        // For null values, equalTo and not equalTo both return false
        // To avoid it, we always return true when there is null
        List<Expression> notExprs = disjunctions.stream()
                .map(e -> ExpressionUtils.or(new Not(e), new IsNull(e)))
                .collect(ImmutableList.toImmutableList());
        List<Plan> joins = Lists.newArrayList();

        for (int hashCondIdx = 0; hashCondIdx < disjunctions.size(); hashCondIdx++) {
            // extract hash conditions and other condition
            Pair<List<Expression>, List<Expression>> pair = extractHashAndOtherConditions(hashCondIdx, disjunctions,
                    notExprs);
            pair.second.addAll(otherConditions);

            LogicalCTEConsumer left = new LogicalCTEConsumer(ctx.getStatementContext().getNextRelationId(),
                    leftProducer.getCteId(), "", leftProducer);
            LogicalCTEConsumer right = new LogicalCTEConsumer(ctx.getStatementContext().getNextRelationId(),
                    rightProducer.getCteId(), "", rightProducer);
            ctx.putCTEIdToConsumer(left);
            ctx.putCTEIdToConsumer(right);

            //rewrite conjuncts to replace the old slots with CTE slots
            Map<Slot, Slot> replaced = new HashMap<>(left.getProducerToConsumerOutputMap());
            replaced.putAll(right.getProducerToConsumerOutputMap());
            List<Expression> hashCond = pair.first.stream()
                    .map(e -> e.rewriteUp(s -> replaced.containsKey(s) ? replaced.get(s) : s))
                    .collect(Collectors.toList());
            List<Expression> otherCond = pair.second.stream()
                    .map(e -> e.rewriteUp(s -> replaced.containsKey(s) ? replaced.get(s) : s))
                    .collect(Collectors.toList());

            LogicalJoin<? extends Plan, ? extends Plan> newJoin = new LogicalJoin<>(
                    JoinType.INNER_JOIN, hashCond, otherCond, join.getHint(),
                    join.getMarkJoinSlotReference(), left, right);
            if (newJoin.getHashJoinConjuncts().stream()
                    .anyMatch(equalTo -> equalTo.children().stream().anyMatch(e -> !(e instanceof Slot)))) {
                Plan plan = PushDownExpressionsInHashCondition.pushDownHashExpression(newJoin);
                plan = new LogicalProject<>(new ArrayList<>(newJoin.getOutput()), plan);
                joins.add(plan);
            } else {
                joins.add(newJoin);
            }
        }
        return joins;
    }

    // join(a or b or c) = join(a) union join(b) union join(c)
    //                   = join(a) union all (join b and !a) union all join(c and !b and !a)
    // return hashConditions and otherConditions
    private Pair<List<Expression>, List<Expression>> extractHashAndOtherConditions(int hashCondIdx,
            List<Expression> equal, List<Expression> not) {
        List<Expression> others = new ArrayList<>();
        for (int i = 0; i < hashCondIdx; i++) {
            others.add(not.get(i));
        }
        return Pair.of(Lists.newArrayList(equal.get(hashCondIdx)), others);
    }
}
