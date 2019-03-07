package org.apache.doris.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptLogicallJoin;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class JoinCommutativityRule extends ExplorationRule {

    public static JoinCommutativityRule INSTANCE = new JoinCommutativityRule();

    private JoinCommutativityRule() {
        super(OptRuleType.RULE_JOIN_COMMUTATIVITY,
                new OptExpression(new OptLogicallJoin(),
                        new OptExpression(new OptPatternLeaf()),
                        new OptExpression(new OptPatternLeaf())));
    }

    public boolean isCompatible(OptRuleType type) {
        if (type == this.type()) {
            return false;
        }
        return true;
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression leftChild = expr.getInput(0);
        final OptExpression rightChild = expr.getInput(1);
        Preconditions.checkNotNull(leftChild);
        Preconditions.checkNotNull(rightChild);

        // TODO children's tuple need to exchange.
        final OptExpression newJoinExpr = new OptExpression(new OptLogicallJoin(),
                rightChild, leftChild);
        newExprs.add(newJoinExpr);
    }
}
