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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * Common function for JoinLAsscom
 */
public class JoinLAsscomHelper {
    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */
    private final LogicalJoin topJoin;
    private final LogicalJoin<GroupPlan, GroupPlan> bottomJoin;
    private final Plan a;
    private final Plan b;
    private final Plan c;

    private final List<Expression> topHashJoinConjuncts;
    private final List<Expression> bottomHashJoinConjuncts;
    private final List<Expression> allNonHashJoinConjuncts = Lists.newArrayList();
    private final List<SlotReference> aOutputSlots;
    private final List<SlotReference> bOutputSlots;
    private final List<SlotReference> cOutputSlots;

    private final List<Expression> newBottomHashJoinConjuncts = Lists.newArrayList();
    private final List<Expression> newBottomNonHashJoinConjuncts = Lists.newArrayList();

    private final List<Expression> newTopHashJoinConjuncts = Lists.newArrayList();
    private final List<Expression> newTopNonHashJoinConjuncts = Lists.newArrayList();


    /**
     * Init plan and output.
     */
    public JoinLAsscomHelper(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        this.topJoin = topJoin;
        this.bottomJoin = bottomJoin;

        a = bottomJoin.left();
        b = bottomJoin.right();
        c = topJoin.right();

        Preconditions.checkArgument(!topJoin.getHashJoinConjuncts().isEmpty(),
                "topJoin hashJoinConjuncts must exist.");
        topHashJoinConjuncts = topJoin.getHashJoinConjuncts();
        if (topJoin.getOtherJoinCondition().isPresent()) {
            allNonHashJoinConjuncts.addAll(
                    ExpressionUtils.extractConjunction(topJoin.getOtherJoinCondition().get()));
        }
        Preconditions.checkArgument(!bottomJoin.getHashJoinConjuncts().isEmpty(),
                "bottomJoin onClause must exist.");
        bottomHashJoinConjuncts = bottomJoin.getHashJoinConjuncts();
        if (bottomJoin.getOtherJoinCondition().isPresent()) {
            allNonHashJoinConjuncts.addAll(
                    ExpressionUtils.extractConjunction(bottomJoin.getOtherJoinCondition().get()));
        }

        aOutputSlots = Utils.getOutputSlotReference(a);
        bOutputSlots = Utils.getOutputSlotReference(b);
        cOutputSlots = Utils.getOutputSlotReference(c);
    }

    public static JoinLAsscomHelper of(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        return new JoinLAsscomHelper(topJoin, bottomJoin);
    }

    /**
     * Get the onCondition of newTopJoin and newBottomJoin.
     */
    public boolean initJoinOnCondition() {
        for (Expression topJoinOnClauseConjunct : topHashJoinConjuncts) {
            // Ignore join with some OnClause like:
            // Join C = B + A for above example.
            List<SlotReference> topJoinUsedSlot = topJoinOnClauseConjunct.collect(SlotReference.class::isInstance);
            if (ExpressionUtils.isIntersecting(topJoinUsedSlot, aOutputSlots)
                    && ExpressionUtils.isIntersecting(topJoinUsedSlot, bOutputSlots)
                    && ExpressionUtils.isIntersecting(topJoinUsedSlot, cOutputSlots)
            ) {
                return false;
            }
        }

        List<Expression> allHashJoinConjuncts = Lists.newArrayList();
        allHashJoinConjuncts.addAll(topHashJoinConjuncts);
        allHashJoinConjuncts.addAll(bottomHashJoinConjuncts);

        HashSet<SlotReference> newBottomJoinSlots = new HashSet<>(aOutputSlots);
        newBottomJoinSlots.addAll(cOutputSlots);

        for (Expression hashConjunct : allHashJoinConjuncts) {
            List<SlotReference> slots = hashConjunct.collect(SlotReference.class::isInstance);
            if (newBottomJoinSlots.containsAll(slots)) {
                newBottomHashJoinConjuncts.add(hashConjunct);
            } else {
                newTopHashJoinConjuncts.add(hashConjunct);
            }
        }
        for (Expression nonHashConjunct : allNonHashJoinConjuncts) {
            List<SlotReference> slots = nonHashConjunct.collect(SlotReference.class::isInstance);
            if (newBottomJoinSlots.containsAll(slots)) {
                newBottomNonHashJoinConjuncts.add(nonHashConjunct);
            } else {
                newTopNonHashJoinConjuncts.add(nonHashConjunct);
            }
        }
        // newBottomJoinOnCondition/newTopJoinOnCondition is empty. They are cross join.
        // Example:
        // A: col1, col2. B: col2, col3. C: col3, col4
        // (A & B on A.col2=B.col2) & C on B.col3=C.col3.
        // (A & B) & C -> (A & C) & B.
        // (A & C) will be cross join (newBottomJoinOnCondition is empty)
        if (newBottomHashJoinConjuncts.isEmpty() || newTopHashJoinConjuncts.isEmpty()) {
            return false;
        }

        return true;
    }


    /**
     * Get projectExpr of left and right.
     * Just for project-inside.
     */
    private Pair<List<NamedExpression>, List<NamedExpression>> getProjectExprs() {
        Preconditions.checkArgument(topJoin.left() instanceof LogicalProject);
        LogicalProject project = (LogicalProject) topJoin.left();

        List<NamedExpression> projectExprs = project.getProjects();
        List<NamedExpression> newRightProjectExprs = Lists.newArrayList();
        List<NamedExpression> newLeftProjectExpr = Lists.newArrayList();

        HashSet<SlotReference> bOutputSlotsSet = new HashSet<>(bOutputSlots);
        for (NamedExpression projectExpr : projectExprs) {
            List<SlotReference> usedSlotRefs = projectExpr.collect(SlotReference.class::isInstance);
            if (bOutputSlotsSet.containsAll(usedSlotRefs)) {
                newRightProjectExprs.add(projectExpr);
            } else {
                newLeftProjectExpr.add(projectExpr);
            }
        }

        return Pair.of(newLeftProjectExpr, newRightProjectExprs);
    }


    private LogicalJoin<GroupPlan, GroupPlan> newBottomJoin() {
        Optional<Expression> bottomNonHashExpr;
        if (newBottomNonHashJoinConjuncts.isEmpty()) {
            bottomNonHashExpr = Optional.empty();
        } else {
            bottomNonHashExpr = Optional.of(ExpressionUtils.and(newBottomNonHashJoinConjuncts));
        }
        return new LogicalJoin(
                bottomJoin.getJoinType(),
                newBottomHashJoinConjuncts,
                bottomNonHashExpr,
                a, c);
    }

    /**
     * Create topJoin for project-inside.
     */
    public LogicalJoin newProjectTopJoin() {
        Plan left;
        Plan right;

        List<NamedExpression> newLeftProjectExpr = getProjectExprs().first;
        List<NamedExpression> newRightProjectExprs = getProjectExprs().second;
        if (!newLeftProjectExpr.isEmpty()) {
            left = new LogicalProject<>(newLeftProjectExpr, newBottomJoin());
        } else {
            left = newBottomJoin();
        }
        if (!newRightProjectExprs.isEmpty()) {
            right = new LogicalProject<>(newRightProjectExprs, b);
        } else {
            right = b;
        }
        Optional<Expression> topNonHashExpr;
        if (newTopNonHashJoinConjuncts.isEmpty()) {
            topNonHashExpr = Optional.empty();
        } else {
            topNonHashExpr = Optional.of(ExpressionUtils.and(newTopNonHashJoinConjuncts));
        }
        return new LogicalJoin<>(
                topJoin.getJoinType(),
                newTopHashJoinConjuncts,
                topNonHashExpr,
                left, right);
    }

    /**
     * Create topJoin for no-project-inside.
     */
    public LogicalJoin newTopJoin() {
        // TODO: add column map (use project)
        // SlotReference bind() may have solved this problem.
        // source: | A       | B | C      |
        // target: | A       | C      | B |
        Optional<Expression> topNonHashExpr;
        if (newTopNonHashJoinConjuncts.isEmpty()) {
            topNonHashExpr = Optional.empty();
        } else {
            topNonHashExpr = Optional.of(ExpressionUtils.and(newTopNonHashJoinConjuncts));
        }
        return new LogicalJoin(
                topJoin.getJoinType(),
                newTopHashJoinConjuncts,
                topNonHashExpr,
                newBottomJoin(), b);
    }

    public static boolean check(LogicalJoin topJoin) {
        if (topJoin.getJoinReorderContext().hasCommute()) {
            return false;
        }
        return true;
    }
}
