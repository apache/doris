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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateTimeFormatterUtils;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.collect.ImmutableSet;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Date literal in Nereids.
 */
public class DateLiteral extends Literal {
    public static final String JAVA_DATE_FORMAT = "yyyy-MM-dd";

    public static final Set<Character> punctuations = ImmutableSet.of('!', '@', '#', '$', '%', '^', '&', '*', '(', ')',
            '-', '+', '=', '_', '{', '}', '[', ']', '|', '\\', ':', ';', '"', '\'', '<', '>', ',', '.', '?', '/', '~',
            '`');

    // for cast datetime type to date type.
    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
    private static final DateLiteral MIN_DATE = new DateLiteral(0, 1, 1);
    private static final DateLiteral MAX_DATE = new DateLiteral(9999, 12, 31);
    private static final int[] DAYS_IN_MONTH = new int[] {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    protected long year;
    protected long month;
    protected long day;

    public DateLiteral(String s) throws AnalysisException {
        this(DateType.INSTANCE, s);
    }

    protected DateLiteral(DateLikeType dataType, String s) throws AnalysisException {
        super(dataType);
        init(s);
    }

    public DateLiteral(DataType type) throws AnalysisException {
        super(type);
    }

    /**
     * C'tor for date type.
     */
    public DateLiteral(long year, long month, long day) {
        this(DateType.INSTANCE, year, month, day);
    }

    /**
     * C'tor for date type.
     */
    public DateLiteral(DateLikeType dataType, long year, long month, long day) {
        super(dataType);
        this.year = year;
        this.month = month;
        this.day = day;
    }

    /**
     * C'tor for type conversion.
     */
    public DateLiteral(DateLiteral other, DataType type) {
        super(type);
        this.year = other.year;
        this.month = other.month;
        this.day = other.day;
    }

    // normalize yymmdd -> yyyymmdd
    static String normalizeBasic(String s) {
        UnaryOperator<String> normalizeTwoDigit = (input) -> {
            String yy = input.substring(0, 2);
            int year = Integer.parseInt(yy);
            if (year >= 0 && year <= 69) {
                input = "20" + input;
            } else if (year >= 70 && year <= 99) {
                input = "19" + input;
            }
            return input;
        };

        // s.len == 6, assume it is "yymmdd"
        // 'T' exists, assume it is "yymmddT......"
        if (s.length() == 6
                || (s.length() > 6 && s.charAt(6) == 'T')) {
            // check s index 0 - 6 all is digit char
            for (int i = 0; i < 6; i++) {
                if (!Character.isDigit(s.charAt(i))) {
                    return s;
                }
            }
            return normalizeTwoDigit.apply(s);
        }

        // handle yymmddHHMMSS
        if (s.length() >= 12) {
            // check s index 0 - 11 all is digit char
            for (int i = 0; i < 12; i++) {
                if (!Character.isDigit(s.charAt(i))) {
                    return s;
                }
            }
            if (s.length() == 12 || !Character.isDigit(s.charAt(12))) {
                return normalizeTwoDigit.apply(s);
            }
        }

        return s;
    }

    private static boolean isPunctuation(char c) {
        return punctuations.contains(c);
    }

    static Result<String, AnalysisException> normalize(String s) {
        // merge consecutive space
        if (s.contains("  ")) {
            s = s.replaceAll(" +", " ");
        }

        StringBuilder sb = new StringBuilder();

        int i = 0;
        // date and time contains 6 number part at most, so we just need normal 6 number part
        int partNumber = 0;

        // handle two digit year
        if (isPunctuation(s.charAt(2))) {
            String yy = s.substring(0, 2);
            int year = Integer.parseInt(yy);
            if (year >= 0 && year <= 69) {
                sb.append("20");
            } else if (year >= 70 && year <= 99) {
                sb.append("19");
            }
            sb.append(yy);
            i = 2;
            partNumber += 1;
        }

        // normalize leading 0 for date and time
        // The first part is 1-digit x: 000x-month-day
        // The first part is 2-digit xy: 20xy-month-day / 19xy-month-day
        // The first part is 3-digit xyz: 20xy-0z-day / 19xy-0z-day
        // The first part is 4-digit xyzw: xyzw-month-day
        while (i < s.length() && partNumber < 6) {
            char c = s.charAt(i);
            if (Character.isDigit(c)) {
                // find consecutive digit
                int j = i + 1;
                while (j < s.length() && Character.isDigit(s.charAt(j))) {
                    j += 1;
                }
                int len = j - i;
                if (len == 4 || len == 2) {
                    sb.append(s, i, j);
                } else if (len == 3) {
                    if (partNumber == 0) {
                        String yy = s.substring(i, i + 2);
                        int year = Integer.parseInt(yy);
                        if (year >= 0 && year <= 69) {
                            sb.append("20");
                        } else if (year >= 70 && year <= 99) {
                            sb.append("19");
                        }
                        sb.append(yy).append('-');
                    } else {
                        sb.append(s, i, i + 2).append(' ');
                    }
                    j = j - 1;
                } else if (len == 1) {
                    if (partNumber == 0) {
                        sb.append("000").append(c);
                    } else {
                        sb.append('0').append(c);
                    }
                } else {
                    final String currentString = s;
                    return Result.err(
                            () -> new AnalysisException("date/datetime literal [" + currentString + "] is invalid")
                    );
                }
                i = j;
                partNumber += 1;
            } else if (isPunctuation(c) || c == ' ' || c == 'T') {
                i += 1;
                if (partNumber < 3 && isPunctuation(c)) {
                    sb.append('-');
                } else if (partNumber == 3) {
                    while (i < s.length() && (isPunctuation(s.charAt(i)) || s.charAt(i) == ' ' || s.charAt(i) == 'T')) {
                        i += 1;
                    }
                    // avoid add blank before zone-id, for example 2008-08-08 +08:00
                    // should be normalized to 2008-08-08+08:00
                    if (i >= s.length() || Character.isDigit(s.charAt(i))) {
                        sb.append(' ');
                    }
                } else if (partNumber > 3 && isPunctuation(c)) {
                    sb.append(':');
                } else {
                    final String currentString = s;
                    return Result.err(
                            () -> new AnalysisException("date/datetime literal [" + currentString + "] is invalid")
                    );
                }
            } else {
                break;
            }
        }

        // add missing Minute Second in Time part
        if (sb.length() == 13) {
            sb.append(":00:00");
        } else if (sb.length() == 16) {
            sb.append(":00");
        }

        // parse MicroSecond
        // Keep up to 7 digits at most, 7th digit is use for overflow.
        int j = i;
        if (partNumber == 6 && i < s.length() && s.charAt(i) == '.') {
            sb.append(s.charAt(i));
            i += 1;
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                if (i - j <= 7) {
                    sb.append(s.charAt(i));
                }
                i += 1;
            }
        }

        // trim use to remove any blank before zone id or zone offset
        sb.append(s.substring(i).trim());

        return Result.ok(sb.toString());
    }

    /** parseDateLiteral */
    public static Result<DateLiteral, AnalysisException> parseDateLiteral(String s) {
        Result<TemporalAccessor, AnalysisException> parseResult = parseDateTime(s);
        if (parseResult.isError()) {
            return parseResult.cast();
        }
        TemporalAccessor dateTime = parseResult.get();
        int year = DateUtils.getOrDefault(dateTime, ChronoField.YEAR);
        int month = DateUtils.getOrDefault(dateTime, ChronoField.MONTH_OF_YEAR);
        int day = DateUtils.getOrDefault(dateTime, ChronoField.DAY_OF_MONTH);

        if (checkDatetime(dateTime) || checkRange(year, month, day) || checkDate(year, month, day)) {
            return Result.err(() -> new AnalysisException("date/datetime literal [" + s + "] is out of range"));
        }
        return Result.ok(new DateLiteral(year, month, day));
    }

    /** parseDateTime */
    public static Result<TemporalAccessor, AnalysisException> parseDateTime(String s) {
        // fast parse '2022-01-01'
        if (s.length() == 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
            TemporalAccessor date = fastParseDate(s);
            if (date != null) {
                return Result.ok(date);
            }
        }

        String originalString = s;
        try {
            TemporalAccessor dateTime;

            // remove suffix/prefix ' '
            s = s.trim();
            // parse condition without '-' and ':'
            boolean containsPunctuation = false;
            int len = Math.min(s.length(), 11);
            for (int i = 0; i < len; i++) {
                if (isPunctuation(s.charAt(i))) {
                    containsPunctuation = true;
                    break;
                }
            }
            if (!containsPunctuation) {
                s = normalizeBasic(s);
                // mysql reject "20200219 010101" "200219 010101", can't use ' ' spilt basic date time.

                if (!s.contains("T")) {
                    dateTime = DateTimeFormatterUtils.BASIC_FORMATTER_WITHOUT_T.parse(s);
                } else {
                    dateTime = DateTimeFormatterUtils.BASIC_DATE_TIME_FORMATTER.parse(s);
                }
                return Result.ok(dateTime);
            }

            Result<String, AnalysisException> normalizeResult = normalize(s);
            if (normalizeResult.isError()) {
                return normalizeResult.cast();
            }
            s = normalizeResult.get();

            if (!s.contains(" ")) {
                dateTime = DateTimeFormatterUtils.ZONE_DATE_FORMATTER.parse(s);
            } else {
                dateTime = DateTimeFormatterUtils.ZONE_DATE_TIME_FORMATTER.parse(s);
            }

            // if Year is not present, throw exception
            if (!dateTime.isSupported(ChronoField.YEAR)) {
                return Result.err(
                        () -> new AnalysisException("date/datetime literal [" + originalString + "] is invalid")
                );
            }

            return Result.ok(dateTime);
        } catch (Exception ex) {
            return Result.err(() -> new AnalysisException("date/datetime literal [" + originalString + "] is invalid"));
        }
    }

    protected void init(String s) throws AnalysisException {
        TemporalAccessor dateTime = parseDateTime(s).get();
        year = DateUtils.getOrDefault(dateTime, ChronoField.YEAR);
        month = DateUtils.getOrDefault(dateTime, ChronoField.MONTH_OF_YEAR);
        day = DateUtils.getOrDefault(dateTime, ChronoField.DAY_OF_MONTH);

        if (checkDatetime(dateTime) || checkRange(year, month, day) || checkDate(year, month, day)) {
            throw new AnalysisException("date/datetime literal [" + s + "] is out of range");
        }
    }

    protected static boolean checkRange(long year, long month, long day) {
        return year > MAX_DATE.getYear() || month > MAX_DATE.getMonth() || day > MAX_DATE.getDay();
    }

    protected static boolean checkDate(long year, long month, long day) {
        if (month != 0 && day > DAYS_IN_MONTH[(int) month]) {
            if (month == 2 && day == 29 && (Year.isLeap(year) && year > 0)) {
                return false;
            }
            return true;
        }
        return false;
    }

    protected static boolean isDateOutOfRange(LocalDateTime dateTime) {
        return dateTime == null || dateTime.isBefore(START_OF_A_DAY) || dateTime.isAfter(END_OF_A_DAY);
    }

    private static boolean checkDatetime(TemporalAccessor dateTime) {
        return DateUtils.getOrDefault(dateTime, ChronoField.HOUR_OF_DAY) != 0
                || DateUtils.getOrDefault(dateTime, ChronoField.MINUTE_OF_HOUR) != 0
                || DateUtils.getOrDefault(dateTime, ChronoField.SECOND_OF_MINUTE) != 0
                || DateUtils.getOrDefault(dateTime, ChronoField.MICRO_OF_SECOND) != 0;
    }

    @Override
    public Long getValue() {
        return (year * 10000 + month * 100 + day) * 1000000L;
    }

    @Override
    public double getDouble() {
        return (double) getValue();
    }

    @Override
    public String getStringValue() {
        if (0 <= year && year <= 9999 && 0 <= month && month <= 99 && 0 <= day && day <= 99) {
            char[] format = new char[] {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};
            int offset = 3;
            long year = this.year;
            while (year > 0) {
                format[offset--] = (char) ('0' + (year % 10));
                year /= 10;
            }

            offset = 6;
            long month = this.month;
            while (month > 0) {
                format[offset--] = (char) ('0' + (month % 10));
                month /= 10;
            }

            offset = 9;
            long day = this.day;
            while (day > 0) {
                format[offset--] = (char) ('0' + (day % 10));
                day /= 10;
            }
            return String.valueOf(format);
        }
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    @Override
    public String computeToSql() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateLiteral(this, context);
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, Type.DATE);
    }

    public long getYear() {
        return year;
    }

    public long getMonth() {
        return month;
    }

    public long getDay() {
        return day;
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days));
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months));
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks));
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years));
    }

    public LocalDateTime toJavaDateType() {
        return LocalDateTime.of(((int) getYear()), ((int) getMonth()), ((int) getDay()), 0, 0, 0);
    }

    public long getTotalDays() {
        return calculateDays(this.year, this.month, this.day);
    }

    // calculate the number of days from year 0000-00-00 to year-month-day
    private long calculateDays(long year, long month, long day) {
        long totalDays = 0;
        long y = year;

        if (year == 0 && month == 0) {
            return 0;
        }

        /* Cast to int to be able to handle month == 0 */
        totalDays = 365 * y + 31 * (month - 1) + day;
        if (month <= 2) {
            // No leap year
            y--;
        } else {
            // This is great!!!
            // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            // 0, 0, 3, 3, 4, 4, 5, 5, 5,  6,  7,  8
            totalDays -= (month * 4 + 23) / 10;
        }
        // Every 400 year has 97 leap year, 100, 200, 300 are not leap year.
        return totalDays + y / 4 - y / 100 + y / 400;
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return isDateOutOfRange(dateTime)
                ? new NullLiteral(DateType.INSTANCE)
                : new DateLiteral(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
    }

    /**
     * 2020-01-01
     *
     * @return 2020-01-01 00:00:00
     */
    public DateTimeLiteral toBeginOfTheDay() {
        return new DateTimeLiteral(year, month, day, 0, 0, 0);
    }

    /**
     * 2020-01-01
     *
     * @return 2020-01-01 23:59:59
     */
    public DateTimeLiteral toEndOfTheDay() {
        return new DateTimeLiteral(year, month, day, 23, 59, 59);
    }

    /**
     * 2020-01-01
     *
     * @return 2020-01-02 0:0:0
     */
    public DateTimeLiteral toBeginOfTomorrow() {
        Expression tomorrow = plusDays(1);
        if (tomorrow instanceof DateLiteral) {
            return ((DateLiteral) tomorrow).toBeginOfTheDay();
        } else {
            return toEndOfTheDay();
        }
    }

    private static TemporalAccessor fastParseDate(String date) {
        Integer year = readNextInt(date, 0, 4);
        Integer month = readNextInt(date, 5, 2);
        Integer day = readNextInt(date, 8, 2);
        if (year != null && month != null && day != null) {
            return LocalDate.of(year, month, day);
        } else {
            return null;
        }
    }

    private static Integer readNextInt(String str, int offset, int readLength) {
        int value = 0;
        int realReadLength = 0;
        for (int i = offset; i < str.length(); i++) {
            char c = str.charAt(i);
            if ('0' <= c && c <= '9') {
                realReadLength++;
                value = value * 10 + (c - '0');
            } else {
                break;
            }
        }
        return readLength == realReadLength ? value : null;
    }
}
