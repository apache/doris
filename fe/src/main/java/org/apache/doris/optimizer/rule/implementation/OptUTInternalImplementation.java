package org.apache.doris.optimizer.rule.implementation;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptLogicalUTInternalNode;
import org.apache.doris.optimizer.operator.OptPhysicalUTInternalNode;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class OptUTInternalImplementation extends OptRule {

    public static OptUTInternalImplementation INSTANCE = new OptUTInternalImplementation();

    private OptUTInternalImplementation() {
        super(OptRuleType.RULE_IMP_UT_INTERNAL,
                OptExpression.create(
                        new OptLogicalUTInternalNode(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternLeaf())
                ));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression leftChild = expr.getInput(0);
        final OptExpression rightChild = expr.getInput(1);
        Preconditions.checkNotNull(leftChild);
        Preconditions.checkNotNull(rightChild);
        final OptExpression newExpr = OptExpression.create(
                new OptPhysicalUTInternalNode(),
                leftChild,
                rightChild);
        newExprs.add(newExpr);
    }
}
