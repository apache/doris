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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.PushdownExpressionsInHashCondition;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * https://blogs.oracle.com/optimizer/post/optimizer-transformations-or-expansion
 * NLJ (cond1 or cond2)              UnionAll
 *                      =>         /        \
 *                           HJ(cond1) HJ(cond2 and !cond1)
 */
public class OrExpansion extends OneExplorationRuleFactory {
    public static final OrExpansion INSTANCE = new OrExpansion();

    @Override
    public Rule build() {
        return logicalJoin()
                .when(JoinUtils::shouldNestedLoopJoin)
                .when(join -> join.getJoinType().isInnerJoin())
                .thenApply(ctx -> {
                    LogicalJoin<? extends Plan, ? extends Plan> join = ctx.root;
                    Preconditions.checkArgument(join.getHashJoinConjuncts().isEmpty(),
                            "Only Expansion nest loop join without hashCond");
                    List<Expression> disjunctions = null;
                    List<Expression> otherConditions = Lists.newArrayList(join.getOtherJoinConjuncts());

                    // We pick the first or condition that can be split to EqualTo expressions.
                    for (Expression expr : otherConditions) {
                        Pair<List<Expression>, List<Expression>> pair = expandExpr(expr, join);
                        if (pair.second.isEmpty()) {
                            disjunctions = pair.first;
                            otherConditions.remove(expr);
                            break;
                        }
                    }
                    // If there is non-EqualTo expression, it means there is nlj child
                    // Therefore refuse this case
                    if (disjunctions == null) {
                        return join;
                    }
                    //Construct CTE with the children
                    LogicalCTEProducer<? extends Plan> leftProducer = new LogicalCTEProducer<>(
                            ctx.statementContext.getNextCTEId(), join.left());
                    LogicalCTEProducer<? extends Plan> rightProducer = new LogicalCTEProducer<>(
                            ctx.statementContext.getNextCTEId(), join.right());
                    // expand join to hash join with CTE
                    List<Plan> joins = expandJoin(ctx.statementContext, disjunctions, otherConditions, join,
                            leftProducer,
                            rightProducer);

                    LogicalUnion union = new LogicalUnion(Qualifier.ALL, new ArrayList<>(join.getOutput()),
                            ImmutableList.of(),
                            false, joins);
                    LogicalCTEAnchor<? extends Plan, ? extends Plan> intermediateAnchor = new LogicalCTEAnchor<>(
                            rightProducer.getCteId(), rightProducer, union);
                    return new LogicalCTEAnchor<Plan, Plan>(leftProducer.getCteId(), leftProducer, intermediateAnchor);
                }).toRule(RuleType.OR_EXPANSION);
    }

    // extract disjunctions for this otherExpr and divide them into HashCond and OtherCond
    // return hash conditions and other conditions
    private Pair<List<Expression>, List<Expression>> expandExpr(Expression otherExpr,
            LogicalJoin<? extends Plan, ? extends Plan> join) {
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(otherExpr);
        return JoinUtils.extractExpressionForHashTable(join.left().getOutput(), join.right().getOutput(), disjunctions);
    }

    private List<Plan> expandJoin(StatementContext ctx, List<Expression> disjunctions, List<Expression> otherConditions,
            LogicalJoin<? extends Plan, ? extends Plan> join, LogicalCTEProducer<? extends Plan> leftProducer,
            LogicalCTEProducer<? extends Plan> rightProducer) {
        List<Expression> notExprs = disjunctions.stream().map(Not::new).collect(Collectors.toList());
        List<Plan> joins = Lists.newArrayList();

        for (int hashCondIdx = 0; hashCondIdx < disjunctions.size(); hashCondIdx++) {
            // extract hash conditions and other condition
            Pair<List<Expression>, List<Expression>> pair =
                    extractHashAndOtherConditions(hashCondIdx, disjunctions, notExprs);
            pair.second.addAll(otherConditions);

            LogicalCTEConsumer left = new LogicalCTEConsumer(ctx.getNextRelationId(), leftProducer.getCteId(), "",
                    leftProducer);
            LogicalCTEConsumer right = new LogicalCTEConsumer(ctx.getNextRelationId(), rightProducer.getCteId(), "",
                    rightProducer);

            //rewrite conjuncts to replace the old slots with CTE slots
            Map<Slot, Slot> replaced = new HashMap<>(left.getProducerToConsumerOutputMap());
            replaced.putAll(right.getProducerToConsumerOutputMap());
            List<Expression> hashCond = pair.first.stream()
                    .map(e -> e.rewriteUp(s -> replaced.containsKey(s) ? replaced.get(s) : s))
                    .collect(Collectors.toList());
            List<Expression> otherCond = pair.second.stream()
                    .map(e -> e.rewriteUp(s -> replaced.containsKey(s) ? replaced.get(s) : s))
                    .collect(Collectors.toList());

            LogicalJoin<? extends Plan, ? extends Plan> newJoin = join.withJoinConjuncts(hashCond, otherCond)
                    .withChildren(Lists.newArrayList(left, right));
            if (newJoin.getHashJoinConjuncts().stream().anyMatch(equalTo ->
                    equalTo.children().stream().anyMatch(e -> !(e instanceof Slot)))) {
                Plan plan = PushdownExpressionsInHashCondition.pushDownHashExpression(newJoin);
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
