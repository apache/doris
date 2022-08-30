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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;

/**
 * Common function for JoinExchange
 */
class JoinExchangeHelper {
    /*
     *        topJoin                      newTopJoin
     *        /      \                      /      \
     *   leftJoin  rightJoin   -->   newLeftJoin newRightJoin
     *    /    \    /    \            /    \        /    \
     *   A      B  C      D          A      C      B      D
     */

    private final LogicalJoin<? extends Plan, ? extends Plan> topJoin;
    private final LogicalJoin<GroupPlan, GroupPlan> leftJoin;
    private final LogicalJoin<GroupPlan, GroupPlan> rightJoin;
    private final GroupPlan a;
    private final GroupPlan b;
    private final GroupPlan c;
    private final GroupPlan d;
    private final List<SlotReference> aOutput;
    private final List<SlotReference> bOutput;
    private final List<SlotReference> cOutput;
    private final List<SlotReference> dOutput;


    private final List<NamedExpression> allProjects = Lists.newArrayList();

    private final List<Expression> allHashJoinConjuncts = Lists.newArrayList();
    private final List<Expression> allNonHashJoinConjuncts = Lists.newArrayList();

    private final List<Expression> newLeftHashJoinConjuncts = Lists.newArrayList();
    private final List<Expression> newLeftNonHashJoinConjuncts = Lists.newArrayList();

    private final List<Expression> newRightHashJoinConjuncts = Lists.newArrayList();
    private final List<Expression> newRightNonHashJoinConjuncts = Lists.newArrayList();

    private final List<Expression> newTopHashJoinConjuncts = Lists.newArrayList();
    private final List<Expression> newTopNonHashJoinConjuncts = Lists.newArrayList();

    /**
     * init.
     */
    public JoinExchangeHelper(LogicalJoin<? extends Plan, ? extends Plan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> leftJoin, LogicalJoin<GroupPlan, GroupPlan> rightJoin) {
        this.topJoin = topJoin;
        this.leftJoin = leftJoin;
        this.rightJoin = rightJoin;
        a = leftJoin.left();
        b = leftJoin.right();
        c = rightJoin.left();
        d = rightJoin.right();
        aOutput = Utils.getOutputSlotReference(a);
        bOutput = Utils.getOutputSlotReference(b);
        cOutput = Utils.getOutputSlotReference(c);
        dOutput = Utils.getOutputSlotReference(d);

        Preconditions.checkArgument(!topJoin.getHashJoinConjuncts().isEmpty(), "topJoin hashJoinConjuncts must exist.");
        Preconditions.checkArgument(!leftJoin.getHashJoinConjuncts().isEmpty(),
                "leftJoin hashJoinConjuncts must exist.");
        Preconditions.checkArgument(!rightJoin.getHashJoinConjuncts().isEmpty(),
                "leftJoin hashJoinConjuncts must exist.");

        allHashJoinConjuncts.addAll(topJoin.getHashJoinConjuncts());
        allHashJoinConjuncts.addAll(leftJoin.getHashJoinConjuncts());
        allHashJoinConjuncts.addAll(rightJoin.getHashJoinConjuncts());

        topJoin.getOtherJoinCondition().ifPresent(otherJoinCondition -> allNonHashJoinConjuncts.addAll(
                ExpressionUtils.extractConjunction(otherJoinCondition)));
        leftJoin.getOtherJoinCondition().ifPresent(otherJoinCondition -> allNonHashJoinConjuncts.addAll(
                ExpressionUtils.extractConjunction(otherJoinCondition)));
        rightJoin.getOtherJoinCondition().ifPresent(otherJoinCondition -> allNonHashJoinConjuncts.addAll(
                ExpressionUtils.extractConjunction(otherJoinCondition)));
    }

    @SafeVarargs
    public final void initAllProject(LogicalProject<? extends Plan>... projects) {
        for (LogicalProject<? extends Plan> project : projects) {
            allProjects.addAll(project.getProjects());
        }
    }

    /**
     * Init the condition of join.
     */
    public boolean init() {
        // Ignore join with some OnClause like:
        // Join C = B + A for above example.
        // TODO: also need for otherJoinCondition

        HashSet<SlotReference> newLeftJoinSlots = new HashSet<>(aOutput);
        newLeftJoinSlots.addAll(cOutput);
        HashSet<SlotReference> newRightJoinSlots = new HashSet<>(bOutput);
        newLeftJoinSlots.addAll(dOutput);
        for (Expression hashConjunct : allHashJoinConjuncts) {
            List<SlotReference> slots = hashConjunct.collect(SlotReference.class::isInstance);
            if (newLeftJoinSlots.containsAll(slots)) {
                newLeftHashJoinConjuncts.add(hashConjunct);
            } else if (newRightJoinSlots.containsAll(slots)) {
                newRightHashJoinConjuncts.add(hashConjunct);
            } else {
                newTopHashJoinConjuncts.add(hashConjunct);
            }
        }
        for (Expression nonHashConjunct : allNonHashJoinConjuncts) {
            List<SlotReference> slots = nonHashConjunct.collect(SlotReference.class::isInstance);
            if (newLeftJoinSlots.containsAll(slots)) {
                newLeftHashJoinConjuncts.add(nonHashConjunct);
            } else if (newRightJoinSlots.containsAll(slots)) {
                newRightHashJoinConjuncts.add(nonHashConjunct);
            } else {
                newTopHashJoinConjuncts.add(nonHashConjunct);
            }
        }

        if (newLeftHashJoinConjuncts.isEmpty() || newRightHashJoinConjuncts.isEmpty()
                || newTopHashJoinConjuncts.isEmpty()) {
            return false;
        }

        return true;
    }

    /**
     * Get newTopJoin
     */
    public LogicalJoin<? extends Plan, ? extends Plan> newTopJoin() {

        Pair<List<NamedExpression>, List<NamedExpression>> projectExprsPair = splitProjectExprs();
        List<NamedExpression> newLeftProjectExprs = projectExprsPair.first;
        List<NamedExpression> newRightProjectExprs = projectExprsPair.second;

        LogicalJoin<GroupPlan, GroupPlan> newLeftJoin = new LogicalJoin<>(JoinType.INNER_JOIN, newLeftHashJoinConjuncts,
                ExpressionUtils.andByOptional(newLeftNonHashJoinConjuncts), a, c);
        LogicalJoin<GroupPlan, GroupPlan> newRightJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                newRightHashJoinConjuncts, ExpressionUtils.andByOptional(newRightNonHashJoinConjuncts), b, d);

        Plan left = JoinReorderCommon.project(newLeftProjectExprs, newLeftJoin).orElse(newLeftJoin);
        Plan right = JoinReorderCommon.project(newRightProjectExprs, newRightJoin).orElse(newRightJoin);

        return new LogicalJoin<>(JoinType.INNER_JOIN, newTopHashJoinConjuncts,
                ExpressionUtils.andByOptional(newTopNonHashJoinConjuncts), left, right);
    }

    protected Pair<List<NamedExpression>, List<NamedExpression>> splitProjectExprs() {
        List<NamedExpression> newLeftJoinProjectExprs = Lists.newArrayList();
        List<NamedExpression> newRightJoinProjectExprs = Lists.newArrayList();

        HashSet<SlotReference> newLeftJoinOutputSlotsSet = new HashSet<>(aOutput);
        newLeftJoinOutputSlotsSet.addAll(cOutput);

        for (NamedExpression projectExpr : allProjects) {
            List<SlotReference> usedSlotRefs = projectExpr.collect(SlotReference.class::isInstance);
            if (newLeftJoinOutputSlotsSet.containsAll(usedSlotRefs)) {
                newLeftJoinProjectExprs.add(projectExpr);
            } else {
                newRightJoinProjectExprs.add(projectExpr);
            }
        }
        return Pair.of(newLeftJoinProjectExprs, newRightJoinProjectExprs);
    }

    public static boolean check(LogicalJoin<? extends Plan, ? extends Plan> topJoin) {
        return !topJoin.getJoinReorderContext().hasCommute() && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasRightAssociate() && !topJoin.getJoinReorderContext()
                .hasExchange();
    }
}
