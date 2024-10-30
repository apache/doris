package org.apache.doris.nereids.trees.expressions.literal.format;

import java.util.List;

public class OrChecker extends FormatChecker {
    private final List<FormatChecker> checkers;

    public OrChecker(StringInspect stringInspect, List<FormatChecker> checkers) {
        super(stringInspect);
        this.checkers = checkers;
    }

    @Override
    protected boolean doCheck() {
        for (FormatChecker checker : checkers) {
            if (!checker.check()) {
                checker.stepBack();
            } else {
                return true;
            }
        }
        return false;
    }
}
