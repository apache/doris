package org.apache.doris.optimizer;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.transformation.ExplorationRule;

import java.util.List;

public class OptUTInternalCommutativityRule extends ExplorationRule {

    public static OptUTInternalCommutativityRule INSTANCE = new OptUTInternalCommutativityRule();

    private OptUTInternalCommutativityRule() {
        super(OptRuleType.RULE_EXP_UT_COMMUTATIVITY,
                OptExpression.create(new OptLogicalUTInternalNode(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternLeaf())));
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
        final OptExpression newJoinExpr = OptExpression.create(new OptLogicalUTInternalNode(),
                rightChild, leftChild);
        newExprs.add(newJoinExpr);
    }
}
