package org.apache.doris.nereids.trees.expressions.literal.format;


import java.util.function.Predicate;

public class CustomCharChecker extends FormatChecker {
    private Predicate<Character> checker;

    public CustomCharChecker(StringInspect stringInspect, Predicate<Character> checker) {
        super(stringInspect);
        this.checker = checker;
    }

    @Override
    protected boolean doCheck() {
        if (stringInspect.eos() || !checker.test(stringInspect.lookAndStep())) {
            return false;
        }
        return true;
    }
}
