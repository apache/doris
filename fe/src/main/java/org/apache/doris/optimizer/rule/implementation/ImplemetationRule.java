package org.apache.doris.optimizer.rule.implementation;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.rule.OptRuleType;

public abstract class ImplemetationRule extends OptRule {

    public ImplemetationRule(OptRuleType type, OptExpression pattern) {
        super(type, pattern);
    }

    @Override
    public boolean isImplementation() {
        return true;
    }
}
