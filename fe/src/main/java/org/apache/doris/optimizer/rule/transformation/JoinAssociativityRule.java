package org.apache.doris.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptLogicallJoin;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class JoinAssociativityRule extends ExplorationRule {

    public static JoinAssociativityRule INSTANCE = new JoinAssociativityRule();

    private JoinAssociativityRule() {
        super(OptRuleType.RULE_JOIN_ASSOCIATIVITY,
                new OptExpression(new OptLogicallJoin(),
                        new OptExpression(new OptLogicallJoin(),
                                new OptExpression(new OptPatternLeaf()),
                                new OptExpression(new OptPatternLeaf())),
                        new OptExpression(new OptPatternLeaf())));
    }

    @Override
    public boolean isCompatible(OptRuleType type) {
        if (type == this.type()) {
            return false;
        }
        return true;
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression leftChildJoin = expr.getInput(0);
        final OptExpression rightChild = expr.getInput(1);
        Preconditions.checkNotNull(leftChildJoin);
        Preconditions.checkNotNull(rightChild);
        final OptExpression leftChildJoinLeftChild = leftChildJoin.getInput(0);
        final OptExpression leftChildJoinRightChild = leftChildJoin.getInput(1);
        Preconditions.checkNotNull(leftChildJoinLeftChild);
        Preconditions.checkNotNull(leftChildJoinRightChild);

        //TODO predicates.....
        final OptExpression newLeftChildJoin = new OptExpression(
                new OptLogicallJoin(),
                leftChildJoinLeftChild,
                rightChild);
        final OptExpression newTopJoin = new OptExpression(
                new OptLogicallJoin(),
                newLeftChildJoin,
                leftChildJoinRightChild);
        newExprs.add(newTopJoin);
    }
}
