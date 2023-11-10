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
import org.apache.doris.nereids.util.StandardDateFormat;

import java.time.LocalDateTime;
import java.time.Year;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Date literal in Nereids.
 */
public class DateLiteral extends Literal {
    public static final String JAVA_DATE_FORMAT = "yyyy-MM-dd";

    // for cast datetime type to date type.
    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59);
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
        java.util.function.UnaryOperator<String> normalizeTwoDigit = (input) -> {
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

    static String normalize(String s) {
        StringBuilder sb = new StringBuilder();

        int i = 0;

        // handle two digit year
        if (s.charAt(2) != '-' && s.charAt(4) != '-') {
            throw new AnalysisException("date/datetime literal [" + s + "] is invalid");
        }
        if (s.charAt(2) == '-') {
            String yy = s.substring(0, 2);
            int year = Integer.parseInt(yy);
            if (year >= 0 && year <= 69) {
                sb.append("20");
            } else if (year >= 70 && year <= 99) {
                sb.append("19");
            }
            sb.append(yy);
            i = 2;
        }

        // normalized leading 0
        while (i < s.length()) {
            char c = s.charAt(i);

            if (c == '.') {
                // skip .microsecond, such as .0001 .000001
                sb.append(c);  // Append the dot itself
                i += 1;  // Skip the dot

                // skip the microsecond part
                while (i < s.length() && Character.isDigit(s.charAt(i))) {
                    sb.append(s.charAt(i));
                    i += 1;
                }
            } else if (Character.isDigit(c)) {
                // find consecutive digit
                int j = i + 1;
                while (j < s.length() && Character.isDigit(s.charAt(j))) {
                    j += 1;
                }
                int len = j - i;
                if (len == 4 || len == 2) {
                    for (int k = i; k < j; k++) {
                        sb.append(s.charAt(k));
                    }
                } else if (len == 1) {
                    sb.append('0');
                    sb.append(c);
                } else {
                    throw new AnalysisException("date/datetime literal [" + s + "] is invalid");
                }
                i = j;
            } else {
                sb.append(c);
                i += 1;
            }
        }

        int len = sb.length();
        // Replace delimiter 'T' with ' '
        if (len > 10 && sb.charAt(10) == 'T') {
            sb.setCharAt(10, ' ');
        }

        // add missing Minute Second in Time part
        if (len > 10 && sb.charAt(10) == ' ') {
            if (len == 13 || len > 13 && sb.charAt(13) != ':') {
                sb.insert(13, ":00:00");
            } else if (len == 16 || (len > 16 && sb.charAt(16) != ':')) {
                sb.insert(16, ":00");
            }
        }

        len = sb.length();
        int signIdx = sb.indexOf("+", 10); // from index:10, skip date part (it contains '-')
        signIdx = signIdx == -1 ? sb.indexOf("-", 10) : signIdx;
        if (signIdx != -1 && len - signIdx == 3) {
            sb.append(":00");
        }

        return sb.toString();
    }

    protected static TemporalAccessor parse(String s) {
        String originalString = s;
        try {
            TemporalAccessor dateTime;

            // parse condition without '-' and ':'
            if (!s.contains("-") && !s.contains(":")) {
                s = normalizeBasic(s);
                // mysql reject "20200219 010101" "200219 010101", can't use ' ' spilt basic date time.
                if (!s.contains("T")) {
                    dateTime = DateTimeFormatterUtils.BASIC_FORMATTER_WITHOUT_T.parse(s);
                } else {
                    dateTime = DateTimeFormatterUtils.BASIC_DATE_TIME_FORMATTER.parse(s);
                }
                return dateTime;
            }

            s = normalize(s);

            if (!s.contains(" ")) {
                dateTime = DateTimeFormatterUtils.ZONE_DATE_FORMATTER.parse(s);
            } else {
                dateTime = DateTimeFormatterUtils.ZONE_DATE_TIME_FORMATTER.parse(s);
            }

            // if Year is not present, throw exception
            if (!dateTime.isSupported(ChronoField.YEAR)) {
                throw new AnalysisException("date/datetime literal [" + originalString + "] is invalid");
            }

            return dateTime;
        } catch (Exception ex) {
            throw new AnalysisException("date/datetime literal [" + originalString + "] is invalid");
        }
    }

    protected void init(String s) throws AnalysisException {
        TemporalAccessor dateTime = parse(s);
        year = DateUtils.getOrDefault(dateTime, ChronoField.YEAR);
        month = DateUtils.getOrDefault(dateTime, ChronoField.MONTH_OF_YEAR);
        day = DateUtils.getOrDefault(dateTime, ChronoField.DAY_OF_MONTH);

        if (checkDatetime(dateTime) || checkRange() || checkDate()) {
            throw new AnalysisException("date/datetime literal [" + s + "] is out of range");
        }
    }

    protected boolean checkRange() {
        return year > MAX_DATE.getYear() || month > MAX_DATE.getMonth() || day > MAX_DATE.getDay();
    }

    protected boolean checkDate() {
        if (month != 0 && day > DAYS_IN_MONTH[((int) month)]) {
            if (month == 2 && day == 29 && Year.isLeap(year)) {
                return false;
            }
            return true;
        }
        return false;
    }

    protected static boolean isDateOutOfRange(LocalDateTime dateTime) {
        return dateTime.isBefore(START_OF_A_DAY) || dateTime.isAfter(END_OF_A_DAY);
    }

    private boolean checkDatetime(TemporalAccessor dateTime) {
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
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    @Override
    public String toSql() {
        return String.format("'%s'", toString());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateLiteral(this, context);
    }

    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d", year, month, day);
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
        return fromJavaDateType(DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusDays(days));
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusMonths(months));
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusWeeks(weeks));
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusYears(years));
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
}
