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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Common join helper for three-join.
 */
abstract class ThreeJoinHelper {
    protected final LogicalJoin<? extends Plan, ? extends Plan> topJoin;
    protected final LogicalJoin<GroupPlan, GroupPlan> bottomJoin;
    protected final GroupPlan a;
    protected final GroupPlan b;
    protected final GroupPlan c;

    protected final Set<Slot> aOutputSet;
    protected final Set<Slot> bOutputSet;
    protected final Set<Slot> cOutputSet;
    protected final Set<Slot> bottomJoinOutputSet;

    protected final List<Expression> allHashJoinConjuncts = Lists.newArrayList();
    protected final List<Expression> allNonHashJoinConjuncts = Lists.newArrayList();

    protected final List<Expression> newBottomHashJoinConjuncts = Lists.newArrayList();
    protected final List<Expression> newBottomNonHashJoinConjuncts = Lists.newArrayList();

    protected final List<Expression> newTopHashJoinConjuncts = Lists.newArrayList();
    protected final List<Expression> newTopNonHashJoinConjuncts = Lists.newArrayList();

    /**
     * Init plan and output.
     */
    public ThreeJoinHelper(LogicalJoin<? extends Plan, ? extends Plan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin, GroupPlan a, GroupPlan b, GroupPlan c) {
        this.topJoin = topJoin;
        this.bottomJoin = bottomJoin;
        this.a = a;
        this.b = b;
        this.c = c;

        aOutputSet = a.getOutputSet();
        bOutputSet = b.getOutputSet();
        cOutputSet = c.getOutputSet();
        bottomJoinOutputSet = bottomJoin.getOutputSet();

        Preconditions.checkArgument(!topJoin.getHashJoinConjuncts().isEmpty(), "topJoin hashJoinConjuncts must exist.");
        Preconditions.checkArgument(!bottomJoin.getHashJoinConjuncts().isEmpty(),
                "bottomJoin hashJoinConjuncts must exist.");

        allHashJoinConjuncts.addAll(topJoin.getHashJoinConjuncts());
        allHashJoinConjuncts.addAll(bottomJoin.getHashJoinConjuncts());
        topJoin.getOtherJoinCondition().ifPresent(otherJoinCondition -> allNonHashJoinConjuncts.addAll(
                ExpressionUtils.extractConjunction(otherJoinCondition)));
        bottomJoin.getOtherJoinCondition().ifPresent(otherJoinCondition -> allNonHashJoinConjuncts.addAll(
                ExpressionUtils.extractConjunction(otherJoinCondition)));
    }

    /**
     * Get the onCondition of newTopJoin and newBottomJoin.
     */
    public boolean initJoinOnCondition() {
        // Ignore join with some OnClause like:
        // Join C = B + A for above example.
        // TODO: also need for otherJoinCondition
        for (Expression topJoinOnClauseConjunct : topJoin.getHashJoinConjuncts()) {
            Set<Slot> topJoinUsedSlot = topJoinOnClauseConjunct.collect(Slot.class::isInstance);
            if (ExpressionUtils.isIntersecting(topJoinUsedSlot, aOutputSet) && ExpressionUtils.isIntersecting(
                    topJoinUsedSlot, bOutputSet) && ExpressionUtils.isIntersecting(topJoinUsedSlot, cOutputSet)) {
                return false;
            }
        }

        Set<Slot> newBottomJoinSlots = new HashSet<>(aOutputSet);
        newBottomJoinSlots.addAll(cOutputSet);
        for (Expression hashConjunct : allHashJoinConjuncts) {
            Set<Slot> slots = hashConjunct.collect(Slot.class::isInstance);
            if (newBottomJoinSlots.containsAll(slots)) {
                newBottomHashJoinConjuncts.add(hashConjunct);
            } else {
                newTopHashJoinConjuncts.add(hashConjunct);
            }
        }
        for (Expression nonHashConjunct : allNonHashJoinConjuncts) {
            Set<Slot> slots = nonHashConjunct.collect(Slot.class::isInstance);
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
}
