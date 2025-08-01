package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.JoinSkewInfo;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;


import java.util.ArrayList;
import java.util.List;


public class SkewJoin  extends OneRewriteRuleFactory {

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
        LogicalJoin<Plan, Plan> join = ctx.root;
        Expression skewExpr = null;
        List<Expression> hotValues = new ArrayList<>();
        if (join.getHashJoinConjuncts().size() != 1) {
            return null;
        }
        EqualPredicate equal = (EqualPredicate) join.getHashJoinConjuncts().get(0);
        if (join.left().getOutputSet().contains(equal.right())) {
            equal = equal.commute();
        }
        if (join.getJoinType().isInnerJoin() || join.getJoinType().isLeftOuterJoin()) {
            AbstractPlan left = (AbstractPlan) join.left();
            Expression leftEqHand = equal.child(0);
            if (left.getStats().findColumnStatistics(leftEqHand) != null
                    && left.getStats().findColumnStatistics(leftEqHand).getHotValues() != null) {
                skewExpr = leftEqHand;
                hotValues.addAll(left.getStats().findColumnStatistics(leftEqHand).getHotValues().keySet());
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            AbstractPlan right = (AbstractPlan) join.right();
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

        DistributeHint hint = new DistributeHint(DistributeType.SHUFFLE_RIGHT,
                new JoinSkewInfo(skewExpr, hotValues, false));
        join.setHint(hint);
        return SaltJoin.transform(join);
    }
}
