package org.apache.doris.nereids.trees.expressions.literal.format;

import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;

public class DateTimeChecker extends FormatChecker {
    private DateTimeChecker(StringInspect stringInspect) {
        super(stringInspect);
    }

    public static boolean isValidDateTime(String str) {
        str = str.trim();
        return new DateTimeChecker(new StringInspect(str)).check();
    }

    @Override
    protected boolean doCheck() {
        FormatChecker dateFormatChecker = and(
            or(
                // date
                and(
                    or(
                        // 20241012
                        number(8, 8),
                        // 2024-10-12
                        and(
                            number(4, 4), // year
                            chars(DateLiteral.punctuations::contains),
                            number(2, 2), // month
                            chars(DateLiteral.punctuations::contains),
                            number(2, 2) // day
                        )
                    ),
                    option(ch('Z'))
                ),
                // datetime
                and(
                    or(
                        // 20241012010203
                        number(14, 14),
                        // 2024-01-01 01:02:03
                        and(
                            number(4, 4), // year
                            chars(DateLiteral.punctuations::contains),
                            number(2, 2), // month
                            chars(DateLiteral.punctuations::contains),
                            number(2, 2), // day
                            atLeast(1, c -> c == 'T' || c == ' ' || DateLiteral.punctuations.contains(c)),
                            number(2, 2), // hour
                            chars(DateLiteral.punctuations::contains),
                            number(2, 2), // minute
                            chars(DateLiteral.punctuations::contains),
                            number(2, 2) // second
                        )
                    ),
                    option(nanoSecond()),
                    option(timeZone())
                )
            )
        );
        return dateFormatChecker.check();
    }

    private FormatChecker nanoSecond() {
        return and(
            ch('.'),
            number(1)
        );
    }

    private FormatChecker timeZone() {
        // Z or +08:00 or -01:00
        return or(
            ch('Z'),
            and(
                chars(c -> c == '+' || c == '-'),
                number(2, 2),
                ch(':'),
                number(2, 2)
            )
        );
    }
}
