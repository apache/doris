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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

/**
 * Composition of MySQL Date:
 * 1. Without any delimiter, e.g.: '20220801', '20220801010101'
 *    MySQL supports '20220801T010101' but doesn't support '20220801 010101'
 *    In this scenario, MySQL does not support zone/offset
 * 2. With delimiters (':'/'-'):
 *    The composition is Date + ' '/'T' + Time + Zone + Offset
 *    Both Zone and Offset are Optional
 *    Date needs to be cautious about the two-digit year https://dev.mysql.com/doc/refman/8.0/en/datetime.html
 *      Dates containing 2-digit year values are ambiguous as the century is unknown.
 *      MySQL interprets 2-digit year values using these rules:
 *          Year values in the range 00-69 become 2000-2069.
 *          Year values in the range 70-99 become 1970-1999.
 *    Time needs to be cautious about microseconds:
 *      Note incomplete times 'hh:mm:ss', 'hh:mm', 'D hh:mm', 'D hh', or 'ss'
 */
public class DateTimeFormatterUtils {
    public static final DateTimeFormatter ZONE_FORMATTER = new DateTimeFormatterBuilder()
            .optionalStart()
            .parseCaseInsensitive()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // HH[:mm][:ss][.microsecond]
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            // microsecond maxWidth is 7, we may need 7th digit to judge overflow
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 7, true).toFormatter())
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // Time without delimiter: HHmmss[.microsecond]
    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 7, true).toFormatter())
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // yyyymmdd
    private static final DateTimeFormatter BASIC_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // Date without delimiter
    public static final DateTimeFormatter BASIC_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_FORMATTER)
            .appendLiteral('T')
            .append(BASIC_TIME_FORMATTER)
            .appendOptional(ZONE_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // Date without delimiter
    public static final DateTimeFormatter BASIC_FORMATTER_WITHOUT_T = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_FORMATTER)
            .appendOptional(BASIC_TIME_FORMATTER)
            .appendOptional(ZONE_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);

    // Datetime
    public static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter ZONE_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 2)
            .append(ZONE_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter ZONE_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .append(ZONE_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);

    /**
     * Format TimeV2 literal according to MySQL time_format spec.
     */
    public static String formatTimeLiteral(TimeV2Literal time, String pattern) {
        double value = (double) time.getValue();
        int hour = Math.abs(time.getHour());
        int minute = Math.abs(time.getMinute());
        int second = Math.abs(time.getSecond());
        int microsecond = Math.abs(time.getMicroSecond());

        StringBuilder builder = new StringBuilder(pattern.length() + 8);
        if (value < 0) {
            builder.append('-');
        }

        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c != '%' || i == pattern.length() - 1) {
                builder.append(c);
                continue;
            }
            char spec = pattern.charAt(++i);
            switch (spec) {
                case 'H':
                    if (hour < 100) {
                        appendTwoDigits(builder, hour);
                    } else {
                        appendWithPad(builder, hour, 2, '0');
                    }
                    break;
                case 'h':
                case 'I': {
                    int hour12 = (hour % 24 + 11) % 12 + 1;
                    appendTwoDigits(builder, hour12);
                    break;
                }
                case 'i':
                    appendTwoDigits(builder, minute);
                    break;
                case 'k':
                    appendWithPad(builder, hour, 1, '0');
                    break;
                case 'l': {
                    int hour12 = (hour % 24 + 11) % 12 + 1;
                    appendWithPad(builder, hour12, 1, '0');
                    break;
                }
                case 's':
                case 'S':
                    appendTwoDigits(builder, second);
                    break;
                case 'f':
                    appendWithPad(builder, microsecond, 6, '0');
                    break;
                case 'p':
                    builder.append((hour % 24 >= 12) ? "PM" : "AM");
                    break;
                case 'r': {
                    int hour12 = (hour % 24 + 11) % 12 + 1;
                    appendTwoDigits(builder, hour12);
                    builder.append(':');
                    appendTwoDigits(builder, minute);
                    builder.append(':');
                    appendTwoDigits(builder, second);
                    builder.append(' ');
                    builder.append((hour % 24 >= 12) ? "PM" : "AM");
                    break;
                }
                case 'T':
                    if (hour < 100) {
                        appendTwoDigits(builder, hour);
                    } else {
                        appendWithPad(builder, hour, 2, '0');
                    }
                    builder.append(':');
                    appendTwoDigits(builder, minute);
                    builder.append(':');
                    appendTwoDigits(builder, second);
                    break;
                case 'Y':
                    // Year, 4 digits
                    builder.append("0000");
                    break;
                case 'y':
                case 'm':
                case 'd':
                    // Year (2 digits), Month, Day - insert 2 zeros
                    builder.append("00");
                    break;
                case 'c':
                case 'e':
                    // Month (0..12) or Day without leading zero - insert 1 zero
                    builder.append('0');
                    break;
                case 'M':
                case 'W':
                case 'j':
                case 'D':
                case 'U':
                case 'u':
                case 'V':
                case 'v':
                case 'x':
                case 'X':
                case 'w':
                    // These specifiers are not supported for TIME type
                    return null;
                default:
                    builder.append(spec);
                    break;
            }
        }
        return builder.toString();
    }

    private static void appendTwoDigits(StringBuilder builder, int value) {
        builder.append((char) ('0' + (value / 10) % 10));
        builder.append((char) ('0' + (value % 10)));
    }

    private static void appendWithPad(StringBuilder builder, int value, int targetLength, char padChar) {
        String str = Integer.toString(Math.abs(value));
        for (int i = str.length(); i < targetLength; i++) {
            builder.append(padChar);
        }
        builder.append(str);
    }
}
