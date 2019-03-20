package org.apache.doris.optimizer.rule.implementation;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptPhysicalOlapScan;
import org.apache.doris.optimizer.operator.OptLogicalUTLeafNode;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class OptUTLeafImplementation extends ImplemetationRule {

    public static OptUTLeafImplementation INSTANCE = new OptUTLeafImplementation();

    private OptUTLeafImplementation() {
        super(OptRuleType.RULE_IMP_UT_LEAF,
                OptExpression.create(
                        new OptLogicalUTLeafNode()));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression newExpr = OptExpression.create(new OptPhysicalOlapScan());
        newExprs.add(newExpr);
    }
}
