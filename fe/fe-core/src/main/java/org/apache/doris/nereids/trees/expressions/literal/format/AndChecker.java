package org.apache.doris.nereids.trees.expressions.literal.format;

import java.util.List;

public class AndChecker extends FormatChecker {
    private final List<FormatChecker> checkers;

    public AndChecker(StringInspect stringInspect, List<FormatChecker> checkers) {
        super(stringInspect);
        this.checkers = checkers;
    }

    @Override
    protected boolean doCheck() {
        for (FormatChecker checker : checkers) {
            if (!checker.check()) {
                return false;
            }
        }
        return true;
    }
}
