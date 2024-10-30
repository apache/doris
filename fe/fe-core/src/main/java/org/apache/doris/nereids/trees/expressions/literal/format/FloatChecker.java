package org.apache.doris.nereids.trees.expressions.literal.format;

public class FloatChecker extends FormatChecker {
    private FloatChecker(StringInspect stringInspect) {
        super(stringInspect);
    }

    public static boolean isValidFloat(String str) {
        return new FloatChecker(new StringInspect(str.trim())).check();
    }

    @Override
    protected boolean doCheck() {
        FormatChecker floatFormatChecker = and(
            option(chars(c -> c == '+' || c == '-')),
            or(
                // 123 or 123.456
                and(number(1), option(and(ch('.'), number(0)))),
                // .123
                and(ch('.'), number(1))
            ),
            option(
                // E+10 or E-10 or E10
                and(
                    ch('E'),
                    option(chars(c -> c == '+' || c == '-')),
                    number(1)
                )
            )
        );
        return floatFormatChecker.check() && stringInspect.eos();
    }

    public static void main(String[] args) {
        String str = "1.";
        FloatChecker floatChecker = new FloatChecker(new StringInspect(str));
        System.out.println(floatChecker.check());
        System.out.println(floatChecker.getCheckContent());
    }
}
