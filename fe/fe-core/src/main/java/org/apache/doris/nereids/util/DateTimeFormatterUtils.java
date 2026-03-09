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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
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

    private static final int WEEK_MONDAY_FIRST = 1;
    private static final int WEEK_YEAR = 2;
    private static final int WEEK_FIRST_WEEKDAY = 4;

    private static final int MAX_FORMAT_RESULT_LENGTH = 100;
    private static final int SAFE_FORMAT_STRING_MARGIN = 12;
    private static final int MAX_FORMAT_STRING_LENGTH = 128;

    private static final String[] ABBR_MONTH_NAMES = {
            "", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    };

    private static final String[] MONTH_NAMES = {
            "", "January", "February", "March", "April", "May", "June", "July", "August", "September",
            "October", "November", "December"
    };

    private static final String[] ABBR_DAY_NAMES = {
            "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
    };

    private static final String[] DAY_NAMES = {
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    };

    private static void appendTwoDigits(StringBuilder builder, int value) {
        builder.append((char) ('0' + (value / 10) % 10));
        builder.append((char) ('0' + (value % 10)));
    }

    /**
     * Conservative implementation of DATE_FORMAT/TIME_FORMAT for datetime literals
     * used in constant folding.
     *
     * @param datetime     datetime literal to format
     * @param format       format pattern
     * @param isTimeFormat true when invoked via time_format, false for date_format
     * @return formatted string or null when pattern requires missing date fields
     */
    public static String toFormatStringConservative(DateTimeV2Literal datetime, StringLikeLiteral format,
            boolean isTimeFormat) {
        int year = isTimeFormat ? 0 : (int) datetime.getYear();
        int month = isTimeFormat ? 0 : (int) datetime.getMonth();
        int day = isTimeFormat ? 0 : (int) datetime.getDay();
        int hour = (int) datetime.getHour();
        int minute = (int) datetime.getMinute();
        int second = (int) datetime.getSecond();
        int microsecond = (int) datetime.getMicroSecond();

        String pattern = trimFormat(format.getValue());
        return formatTemporalLiteral(year, month, day, hour, minute, second, microsecond, pattern);
    }

    /**
     * Conservative implementation of TIME_FORMAT for time literals used in constant
     * folding.
     *
     * @param time   time literal to format
     * @param format format pattern
     * @return formatted string with sign preserved; null when pattern requires date
     *         fields
     */
    public static String toFormatStringConservative(TimeV2Literal time, StringLikeLiteral format) {
        String pattern = trimFormat(format.getValue());
        String res = formatTemporalLiteral(0, 0, 0, time.getHour(), time.getMinute(),
                time.getSecond(), time.getMicroSecond(), pattern);
        if (time.isNegative()) {
            res = "-" + res;
        }
        return res;
    }

    private static int calcWeekNumber(int year, int month, int day, int mode) {
        int[] weekYear = new int[1];
        return calcWeekNumberAndYear(year, month, day, mode, weekYear);
    }

    private static int calcWeekNumberAndYear(int year, int month, int day, int mode, int[] toYear) {
        return calcWeekInternal(calcDayNr(year, month, day), year, month, day, mode, toYear);
    }

    private static int calcWeekInternal(long dayNr, int year, int month, int day, int mode, int[] toYear) {
        if (year == 0) {
            toYear[0] = 0;
            return 0;
        }
        boolean mondayFirst = (mode & WEEK_MONDAY_FIRST) != 0;
        boolean weekYear = (mode & WEEK_YEAR) != 0;
        boolean firstWeekday = (mode & WEEK_FIRST_WEEKDAY) != 0;

        long daynrFirstDay = calcDayNr(year, 1, 1);
        int weekdayFirstDay = calcWeekday(daynrFirstDay, !mondayFirst);

        toYear[0] = year;

        if (month == 1 && day <= (7 - weekdayFirstDay)) {
            if (!weekYear && ((firstWeekday && weekdayFirstDay != 0) || (!firstWeekday && weekdayFirstDay > 3))) {
                return 0;
            }
            toYear[0]--;
            weekYear = true;
            int days = calcDaysInYear(toYear[0]);
            daynrFirstDay -= days;
            weekdayFirstDay = (weekdayFirstDay + 53 * 7 - days) % 7;
        }

        int days;
        if ((firstWeekday && weekdayFirstDay != 0) || (!firstWeekday && weekdayFirstDay > 3)) {
            days = (int) (dayNr - (daynrFirstDay + (7 - weekdayFirstDay)));
        } else {
            days = (int) (dayNr - (daynrFirstDay - weekdayFirstDay));
        }

        if (weekYear && days >= 52 * 7) {
            weekdayFirstDay = (weekdayFirstDay + calcDaysInYear(toYear[0])) % 7;
            if ((firstWeekday && weekdayFirstDay == 0) || (!firstWeekday && weekdayFirstDay <= 3)) {
                toYear[0]++;
                return 1;
            }
        }

        return days / 7 + 1;
    }

    private static int mysqlWeekMode(int mode) {
        mode &= 7;
        if ((mode & WEEK_MONDAY_FIRST) == 0) {
            mode ^= WEEK_FIRST_WEEKDAY;
        }
        return mode;
    }

    private static int calcWeekday(long dayNr, boolean sundayFirst) {
        return (int) ((dayNr + 5 + (sundayFirst ? 1 : 0)) % 7);
    }

    private static long calcDayNr(int year, int month, int day) {
        // Align with BE/MySQL: Monday = 0 when sundayFirst=false in calcWeekday.
        if (year == 0 && month == 0) {
            return 0;
        }
        if (year == 0 && month == 1 && day == 1) {
            return 1;
        }

        long y = year;
        long delsum = 365 * y + 31L * (month - 1) + day;
        if (month <= 2) {
            y--;
        } else {
            delsum -= (month * 4 + 23) / 10;
        }
        return delsum + y / 4 - y / 100 + y / 400;
    }

    private static int calcDaysInYear(int year) {
        return isLeap(year) ? 366 : 365;
    }

    private static boolean isLeap(int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    }

    private static void appendFourDigits(StringBuilder builder, int value) {
        if (value >= 1000 && value <= 9999) {
            builder.append(value);
            return;
        }
        appendWithPad(builder, value, 4, '0');
    }

    private static void appendWithPad(StringBuilder builder, int value, int targetLength, char padChar) {
        String str = Integer.toString(Math.abs(value));
        for (int i = str.length(); i < targetLength; i++) {
            builder.append(padChar);
        }
        builder.append(str);
    }

    // MySQL-compatible time_format for TIME/DATE/DATETIME literals.
    private static String formatTemporalLiteral(int year, int month, int day, int hour, int minute,
            int second, int microsecond, String pattern) {
        StringBuilder builder = new StringBuilder(pattern.length() + 16);

        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c != '%' || i == pattern.length() - 1) {
                builder.append(c);
                continue;
            }

            char spec = pattern.charAt(++i);
            switch (spec) {
                case 'y':
                    appendTwoDigits(builder, year % 100);
                    break;
                case 'Y':
                    appendFourDigits(builder, year);
                    break;
                case 'd':
                    appendTwoDigits(builder, day);
                    break;
                case 'H':
                    if (hour < 100) {
                        appendTwoDigits(builder, hour);
                    } else {
                        appendWithPad(builder, hour, 2, '0');
                    }
                    break;
                case 'i':
                    appendTwoDigits(builder, minute);
                    break;
                case 'm':
                    appendTwoDigits(builder, month);
                    break;
                case 'h':
                case 'I': {
                    int hour12 = (hour % 24 + 11) % 12 + 1;
                    appendTwoDigits(builder, hour12);
                    break;
                }
                case 's':
                case 'S':
                    appendTwoDigits(builder, second);
                    break;
                case 'a':
                    if (month == 0 || day == 0 || year == 0) {
                        return null;
                    }
                    builder.append(ABBR_DAY_NAMES[calcWeekday(calcDayNr(year, month, day), false)]);
                    break;
                case 'b':
                    if (month == 0) {
                        return null;
                    }
                    builder.append(ABBR_MONTH_NAMES[month]);
                    break;
                case 'c': {
                    String str = Integer.toString(month);
                    if (str.length() < 1) {
                        builder.append('0');
                    }
                    builder.append(str);
                    break;
                }
                case 'D':
                    if (month == 0) {
                        return null;
                    }
                    builder.append(day);
                    if (day >= 10 && day <= 19) {
                        builder.append("th");
                    } else {
                        switch (day % 10) {
                            case 1:
                                builder.append("st");
                                break;
                            case 2:
                                builder.append("nd");
                                break;
                            case 3:
                                builder.append("rd");
                                break;
                            default:
                                builder.append("th");
                                break;
                        }
                    }
                    break;
                case 'e': {
                    String str = Integer.toString(day);
                    if (str.length() < 1) {
                        builder.append('0');
                    }
                    builder.append(str);
                    break;
                }
                case 'f':
                    appendWithPad(builder, microsecond, 6, '0');
                    break;
                case 'j':
                    if (month == 0 || day == 0) {
                        return null;
                    }
                    int dayOfYear = (int) (calcDayNr(year, month, day) - calcDayNr(year, 1, 1) + 1);
                    appendWithPad(builder, dayOfYear, 3, '0');
                    break;
                case 'k': {
                    String str = Integer.toString(hour);
                    if (str.length() < 1) {
                        builder.append('0');
                    }
                    builder.append(str);
                    break;
                }
                case 'l': {
                    int hour12 = (hour % 24 + 11) % 12 + 1;
                    String str = Integer.toString(hour12);
                    if (str.length() < 1) {
                        builder.append('0');
                    }
                    builder.append(str);
                    break;
                }
                case 'M':
                    if (month == 0) {
                        return null;
                    }
                    builder.append(MONTH_NAMES[month]);
                    break;
                case 'p':
                    builder.append((hour % 24) >= 12 ? "PM" : "AM");
                    break;
                case 'r': {
                    int hour12 = (hour % 24 + 11) % 12 + 1;
                    appendTwoDigits(builder, hour12);
                    builder.append(':');
                    appendTwoDigits(builder, minute);
                    builder.append(':');
                    appendTwoDigits(builder, second);
                    builder.append(' ');
                    builder.append((hour % 24) >= 12 ? "PM" : "AM");
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
                case 'u':
                    if (month == 0) {
                        return null;
                    }
                    appendTwoDigits(builder, calcWeekNumber(year, month, day, mysqlWeekMode(1)));
                    break;
                case 'U':
                    if (month == 0) {
                        return null;
                    }
                    appendTwoDigits(builder, calcWeekNumber(year, month, day, mysqlWeekMode(0)));
                    break;
                case 'v':
                    if (month == 0) {
                        return null;
                    }
                    appendTwoDigits(builder, calcWeekNumber(year, month, day, mysqlWeekMode(3)));
                    break;
                case 'V':
                    if (month == 0) {
                        return null;
                    }
                    appendTwoDigits(builder, calcWeekNumber(year, month, day, mysqlWeekMode(2)));
                    break;
                case 'w':
                    if (month == 0 && year == 0) {
                        return null;
                    }
                    builder.append(calcWeekday(calcDayNr(year, month, day), true));
                    break;
                case 'W':
                    if (year == 0 && month == 0) {
                        return null;
                    }
                    builder.append(DAY_NAMES[calcWeekday(calcDayNr(year, month, day), false)]);
                    break;
                case 'x': {
                    if (month == 0 || day == 0) {
                        return null;
                    }
                    int[] weekYear = new int[1];
                    calcWeekNumberAndYear(year, month, day, mysqlWeekMode(3), weekYear);
                    appendFourDigits(builder, weekYear[0]);
                    break;
                }
                case 'X': {
                    if (month == 0 || day == 0) {
                        return null;
                    }
                    int[] weekYear = new int[1];
                    calcWeekNumberAndYear(year, month, day, mysqlWeekMode(2), weekYear);
                    appendFourDigits(builder, weekYear[0]);
                    break;
                }
                default:
                    builder.append(spec);
                    break;
            }
        }

        if (builder.length() > MAX_FORMAT_RESULT_LENGTH) {
            throw new AnalysisException("Formatted string length exceeds the maximum allowed length");
        }
        return builder.toString();
    }

    private static String trimFormat(String pattern) {
        if (pattern == null) {
            throw new AnalysisException("Format string is null");
        }
        int start = 0;
        int end = pattern.length();
        while (start < end && Character.isWhitespace(pattern.charAt(start))) {
            start++;
        }
        while (end > start && Character.isWhitespace(pattern.charAt(end - 1))) {
            end--;
        }
        String trimmed = pattern.substring(start, end);
        if (trimmed.length() > MAX_FORMAT_STRING_LENGTH) {
            throw new AnalysisException("Format string length exceeds the maximum allowed length");
        }
        return trimmed;
    }
}
