package org.apache.doris.nereids.trees.expressions.literal.format;

public class OptionChecker extends FormatChecker {
    private final FormatChecker checker;

    public OptionChecker(StringInspect stringInspect, FormatChecker checker) {
        super(stringInspect);
        this.checker = checker;
    }

    @Override
    protected boolean doCheck() {
        checker.check();
        return true;
    }
}
