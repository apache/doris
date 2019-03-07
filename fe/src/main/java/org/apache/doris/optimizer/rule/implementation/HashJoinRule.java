package org.apache.doris.optimizer.rule.implementation;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptLogicallJoin;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptPhysicalHashJoin;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class HashJoinRule extends ImplemetationRule {

    public static HashJoinRule INSTANCE = new HashJoinRule();

    private HashJoinRule() {
        super(OptRuleType.RULE_EQ_JOIN_TO_HASH_JOIN,
                new OptExpression(
                        new OptLogicallJoin(),
                        new OptExpression(new OptPatternLeaf()),
                        new OptExpression(new OptPatternLeaf())
                ));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression leftChild = expr.getInput(0);
        final OptExpression rightChild = expr.getInput(1);
        Preconditions.checkNotNull(leftChild);
        Preconditions.checkNotNull(rightChild);
        final OptExpression newExpr = new OptExpression(
                new OptPhysicalHashJoin(),
                leftChild,
                rightChild);
        newExprs.add(newExpr);
    }
}
