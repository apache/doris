// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.literal.format;

import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;

/** DateTimeChecker */
public class DateTimeChecker extends FormatChecker {
    private static final DateTimeChecker INSTANCE = new DateTimeChecker();

    private final FormatChecker checker;

    private DateTimeChecker() {
        super("DateTimeChecker");

        this.checker =
                or(
                    // date
                    and("Date",
                        or(
                            // 20241012
                            digit(8, 8),
                            // 2024-10-12
                            and(
                                digit(1, 4), // year
                                chars(DateLiteral.punctuations::contains),
                                digit(1, 2), // month
                                chars(DateLiteral.punctuations::contains),
                                digit(1, 2) // day
                            )
                        )
                    ),
                    // datetime
                    and("DateTime",
                        or("YearToSecond",
                            // 20241012010203
                            and("FullCompactDateTime",
                                digit(8, 8),
                                atLeast(0, c -> c == 'T'),
                                digit(6, 6)
                            ),

                            // 241012010203 or 241012T010203
                            and("ShortCompactDateTime",
                                digit(6, 6),
                                atLeast(0, c -> c == 'T'),
                                digit(6, 6)
                            ),

                            // 2024-01-01 01:02:03
                            and("NormalDateTime",
                                digit(1, 4), // year
                                chars(DateLiteral.punctuations::contains),
                                digit(1, 2), // month
                                chars(DateLiteral.punctuations::contains),
                                digit(1, 2), // day
                                atLeast(1, c -> c == 'T' || c == ' ' || DateLiteral.punctuations.contains(c)),
                                digit(1, 2), // hour
                                option(
                                    and(
                                        chars(DateLiteral.punctuations::contains),
                                        digit(1, 2), // minute
                                        option(
                                            and(
                                                chars(DateLiteral.punctuations::contains),
                                                digit(1, 2) // second
                                            )
                                        )
                                    )
                                )
                            )
                        ),
                        option("NanoSecond", nanoSecond()),
                        option("TimeZone", timeZone())
                    )
               );
    }

    public static boolean isValidDateTime(String str) {
        str = str.trim();
        StringInspect stringInspect = new StringInspect(str.trim());
        return INSTANCE.check(stringInspect).matched && stringInspect.eos();
    }

    @Override
    protected boolean doCheck(StringInspect stringInspect) {
        return checker.check(stringInspect).matched;
    }

    private FormatChecker nanoSecond() {
        return and(
            ch('.'),
            digit(1)
        );
    }

    private FormatChecker timeZone() {
        // Z or +08:00 or UTC-01:00
        return and(
            // timezone: Europe/London, America/New_York
            atLeast(0, c -> ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '/' || c == '_'),
            option(
                and(
                    chars(c -> c == '+' || c == '-'),
                    digit(1, 2),
                    option(
                        and(
                            ch(':'),
                            digit(1, 2),
                            option(
                                and(
                                    ch(':'),
                                    digit(1, 2)
                                )
                            )
                        )
                    )
                )
            )
        );
    }
}
