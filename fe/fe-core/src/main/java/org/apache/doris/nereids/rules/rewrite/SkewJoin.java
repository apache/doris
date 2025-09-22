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

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.JoinSkewInfo;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import java.util.ArrayList;
import java.util.List;

/**
 * When encountering a data-skewed join, there are currently two optimization methods:
 * using salt-join or using broadcast join.
 * If we detect data skew during the RBO phase and the right table is relatively large, we will automatically add salt.
 *
 * Depends on InitJoinOrder rule
 */
public class SkewJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getJoinType().isOneSideOuterJoin()
                        || join.getJoinType().isInnerJoin())
                .when(join -> join.getDistributeHint().distributeType == DistributeType.NONE)
                .whenNot(LogicalJoin::isMarkJoin)
                .thenApply(SkewJoin::transform).toRule(RuleType.SALT_JOIN);
    }

    private static Plan transform(MatchingContext<LogicalJoin<Plan, Plan>> ctx) {
        if (ConnectContext.get() == null) {
            return null;
        }
        StatsDerive derive = new StatsDerive(false);

        LogicalJoin<Plan, Plan> join = ctx.root;
        Expression skewExpr = null;
        List<Expression> hotValues = new ArrayList<>();
        if (join.getHashJoinConjuncts().size() != 1) {
            return null;
        }
        AbstractPlan left = (AbstractPlan) join.left();
        if (left.getStats() == null) {
            left.accept(derive, new DeriveContext());
        }
        AbstractPlan right = (AbstractPlan) join.right();
        if (right.getStats() == null) {
            right.accept(derive, new DeriveContext());
        }

        EqualPredicate equal = (EqualPredicate) join.getHashJoinConjuncts().get(0);
        if (join.left().getOutputSet().contains(equal.right())) {
            equal = equal.commute();
        }
        if (join.getJoinType().isInnerJoin() || join.getJoinType().isLeftOuterJoin()) {
            Expression leftEqHand = equal.child(0);
            if (left.getStats().findColumnStatistics(leftEqHand) != null
                    && left.getStats().findColumnStatistics(leftEqHand).getHotValues() != null) {
                skewExpr = leftEqHand;
                hotValues.addAll(left.getStats().findColumnStatistics(leftEqHand).getHotValues().keySet());
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            Expression rightEqHand = equal.child(1);
            if (right.getStats().findColumnStatistics(rightEqHand) != null
                    && right.getStats().findColumnStatistics(rightEqHand).getHotValues() != null) {
                skewExpr = rightEqHand;
                hotValues.addAll(right.getStats().findColumnStatistics(rightEqHand).getHotValues().keySet());
            }
        } else {
            return null;
        }
        if (skewExpr == null || hotValues.isEmpty()) {
            return null;
        }

        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        // broadcast join for small right table
        // salt join for large right table
        if (right.getStats().getRowCount() < sessionVariable.getBroadcastRowCountLimit() / 100) {
            DistributeHint hint = new DistributeHint(DistributeType.BROADCAST_RIGHT);
            join.setHint(hint);
            return join;
        } else {
            DistributeHint hint = new DistributeHint(DistributeType.SHUFFLE_RIGHT,
                    new JoinSkewInfo(skewExpr, hotValues, false));
            join.setHint(hint);
            return SaltJoin.transform(join);
        }
    }
}
