package org.apache.doris.optimizer.rule.implementation;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptLogicalScan;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptPhysicalOlapScan;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class OlapScanRule extends ImplemetationRule {

    public static OlapScanRule INSTANCE = new OlapScanRule();

    private OlapScanRule() {
        super(OptRuleType.RULE_OLAP_LSCAN_TO_PSCAN,
                new OptExpression(
                        new OptLogicalScan()));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression newExpr = new OptExpression(new OptPhysicalOlapScan());
        newExprs.add(newExpr);
    }
}
