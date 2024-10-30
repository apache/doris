package org.apache.doris.nereids.trees.expressions.literal.format;

public class IntegerChecker extends FormatChecker {
    private IntegerChecker(StringInspect stringInspect) {
        super(stringInspect);
    }

    public static boolean isValidInteger(String string) {
        IntegerChecker integerChecker = new IntegerChecker(new StringInspect(string));
        return integerChecker.check() && integerChecker.stringInspect.eos();
    }

    @Override
    protected boolean doCheck() {
        FormatChecker checker = and(
                option(chars(c -> c == '+' || c == '-')),
                number(1)
        );
        return checker.check();
    }
}
