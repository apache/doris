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
                                ch('-'),
                                digit(1, 2), // month
                                ch('-'),
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
                                ch('-'),
                                digit(1, 2), // month
                                ch('-'),
                                digit(1, 2), // day
                                atLeast(1, c -> c == 'T' || c == ' ' || DateLiteral.punctuations.contains(c)),
                                digit(1, 2), // hour
                                option(
                                    and(
                                        ch(':'),
                                        digit(1, 2), // minute
                                        option(
                                            and(
                                                ch(':'),
                                                digit(1, 2) // second
                                            )
                                        )
                                    )
                                )
                            )
                        ),
                        option("NanoSecond", nanoSecond()),
                        atLeast(0, c -> c == ' '),
                        option("TimeZone", timeZone())
                    )
               );
    }

    public static boolean isValidDateTime(String str) {
        str = str.trim();
        StringInspect stringInspect = new StringInspect(str.trim());
        return INSTANCE.check(stringInspect).matched && stringInspect.eos();
    }

    /** check weather a datetime has timezone part */
    public static boolean hasTimeZone(String str) {
        str = str.trim();

        if (!isValidDateTime(str)) {
            return false;
        }

        if (str.endsWith("Z")) {
            return true;
        }

        // Find the separator between date and time parts (' ' or 'T')
        int timeSeparatorIndex = -1;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ' ' || str.charAt(i) == 'T') {
                timeSeparatorIndex = i;
                break;
            }
        }

        // no time part found
        if (timeSeparatorIndex == -1) {
            return false;
        }

        // Find the end of time part (after seconds or nanoseconds)
        int timeEndIndex = timeSeparatorIndex + 1;
        // Skip time part
        while (timeEndIndex < str.length() && (Character.isDigit(str.charAt(timeEndIndex))
                || str.charAt(timeEndIndex) == ':' || str.charAt(timeEndIndex) == '.'
                || str.charAt(timeEndIndex) == ' ')) {
            timeEndIndex++;
        }

        // Check if there's any content after the time part
        if (timeEndIndex >= str.length()) {
            return false;
        }

        // Check for offset timezone like +08:00 or -05:00
        for (int i = timeEndIndex; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '+' || c == '-') {
                if (i + 1 < str.length() && Character.isDigit(str.charAt(i + 1))) {
                    return true;
                }
            }
        }

        // Check for named timezone like Europe/London, America/New_York, UTC, GMT
        for (int i = timeEndIndex; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isLetter(c) || c == '/' || c == '_') {
                return true;
            }
        }

        return false;
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
