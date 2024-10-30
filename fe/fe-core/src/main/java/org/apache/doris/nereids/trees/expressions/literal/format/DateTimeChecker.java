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
