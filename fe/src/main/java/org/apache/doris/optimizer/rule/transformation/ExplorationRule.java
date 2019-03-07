package org.apache.doris.optimizer.rule.transformation;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.rule.OptRuleType;


public abstract class ExplorationRule extends OptRule {

    public ExplorationRule(OptRuleType type, OptExpression pattern) {
        super(type, pattern);
    }

    @Override
    public boolean isExploration() {
        return true;
    }
}
