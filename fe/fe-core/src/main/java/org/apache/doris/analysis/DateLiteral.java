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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DateLiteral.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TDateLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class DateLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);
    private static final double[] SCALE_FACTORS;

    static {
        SCALE_FACTORS = new double[7];
        for (int i = 0; i < SCALE_FACTORS.length; i++) {
            SCALE_FACTORS[i] = Math.pow(10, 6 - i);
        }
    }

    private static final DateLiteral MIN_DATE = new DateLiteral(0000, 1, 1);
    private static final DateLiteral MAX_DATE = new DateLiteral(9999, 12, 31);
    private static final DateLiteral MIN_DATETIME = new DateLiteral(0000, 1, 1, 0, 0, 0);
    private static final DateLiteral MAX_DATETIME = new DateLiteral(9999, 12, 31, 23, 59, 59);

    private static final DateLiteral MIN_DATETIMEV2
            = new DateLiteral(0000, 1, 1, 0, 0, 0, 0, Type.DATETIMEV2);
    private static final DateLiteral MAX_DATETIMEV2
            = new DateLiteral(9999, 12, 31, 23, 59, 59, 999999L, Type.DATETIMEV2);
    private static final int DATEKEY_LENGTH = 8;
    private static final int DATETIMEKEY_LENGTH = 14;
    private static final int MAX_MICROSECOND = 999999;

    private static DateTimeFormatter DATE_TIME_FORMATTER = null;
    private static DateTimeFormatter DATE_TIME_FORMATTER_TO_MICRO_SECOND = null;
    private static DateTimeFormatter DATE_FORMATTER = null;
    private static List<DateTimeFormatter> formatterList = null;
    /*
     *  The datekey type is widely used in data warehouses
     *  For example, 20121229 means '2012-12-29'
     *  and data in the form of 'yyyymmdd' is generally called the datekey type.
     */
    private static DateTimeFormatter DATEKEY_FORMATTER = null;
    // 'yyyymmddHHMMss'
    private static DateTimeFormatter DATETIMEKEY_FORMATTER = null;

    private static Map<String, Integer> MONTH_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> MONTH_ABBR_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> WEEK_DAY_NAME_DICT = Maps.newHashMap();
    private static Set<Character> TIME_PART_SET = Sets.newHashSet();
    private static final int[] DAYS_IN_MONTH = new int[]{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    private static final WeekFields weekFields = WeekFields.of(DayOfWeek.SUNDAY, 7);

    static {
        try {
            DATE_TIME_FORMATTER = formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
            DATE_FORMATTER = formatBuilder("%Y-%m-%d").toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
            DATEKEY_FORMATTER = formatBuilder("%Y%m%d").toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
            DATETIMEKEY_FORMATTER = formatBuilder("%Y%m%d%H%i%s").toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
            DATE_TIME_FORMATTER_TO_MICRO_SECOND = new DateTimeFormatterBuilder()
                .appendPattern("uuuu-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
            formatterList = Lists.newArrayList(
                formatBuilder("%Y%m%d").appendLiteral('T').appendPattern("HHmmss")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                formatBuilder("%Y%m%d").appendLiteral('T').appendPattern("HHmmss")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, false)
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                formatBuilder("%Y%m%d%H%i%s")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                formatBuilder("%Y%m%d%H%i%s")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, false)
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                DATETIMEKEY_FORMATTER, DATEKEY_FORMATTER);
            TIME_PART_SET = "HhIiklrSsTp".chars().mapToObj(c -> (char) c).collect(Collectors.toSet());
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }

        MONTH_NAME_DICT.put("january", 1);
        MONTH_NAME_DICT.put("february", 2);
        MONTH_NAME_DICT.put("march", 3);
        MONTH_NAME_DICT.put("april", 4);
        MONTH_NAME_DICT.put("may", 5);
        MONTH_NAME_DICT.put("june", 6);
        MONTH_NAME_DICT.put("july", 7);
        MONTH_NAME_DICT.put("august", 8);
        MONTH_NAME_DICT.put("september", 9);
        MONTH_NAME_DICT.put("october", 10);
        MONTH_NAME_DICT.put("november", 11);
        MONTH_NAME_DICT.put("december", 12);

        MONTH_ABBR_NAME_DICT.put("jan", 1);
        MONTH_ABBR_NAME_DICT.put("feb", 2);
        MONTH_ABBR_NAME_DICT.put("mar", 3);
        MONTH_ABBR_NAME_DICT.put("apr", 4);
        MONTH_ABBR_NAME_DICT.put("may", 5);
        MONTH_ABBR_NAME_DICT.put("jun", 6);
        MONTH_ABBR_NAME_DICT.put("jul", 7);
        MONTH_ABBR_NAME_DICT.put("aug", 8);
        MONTH_ABBR_NAME_DICT.put("sep", 9);
        MONTH_ABBR_NAME_DICT.put("oct", 10);
        MONTH_ABBR_NAME_DICT.put("nov", 11);
        MONTH_ABBR_NAME_DICT.put("dec", 12);

        WEEK_DAY_NAME_DICT.put("monday", 0);
        WEEK_DAY_NAME_DICT.put("tuesday", 1);
        WEEK_DAY_NAME_DICT.put("wednesday", 2);
        WEEK_DAY_NAME_DICT.put("thursday", 3);
        WEEK_DAY_NAME_DICT.put("friday", 4);
        WEEK_DAY_NAME_DICT.put("saturday", 5);
        WEEK_DAY_NAME_DICT.put("sunday", 6);

        MONTH_ABBR_NAME_DICT.put("mon", 0);
        MONTH_ABBR_NAME_DICT.put("tue", 1);
        MONTH_ABBR_NAME_DICT.put("wed", 2);
        MONTH_ABBR_NAME_DICT.put("thu", 3);
        MONTH_ABBR_NAME_DICT.put("fri", 4);
        MONTH_ABBR_NAME_DICT.put("sat", 5);
        MONTH_ABBR_NAME_DICT.put("sun", 6);
    }

    private static final Pattern HAS_OFFSET_PART = Pattern.compile("[\\+\\-]\\d{2}:\\d{2}");

    @Override
    public boolean equals(Object o) {
        if (o instanceof DateLiteral) {
            return compareLiteral((LiteralExpr) o) == 0;
        }
        return super.equals(o);
    }

    // Date Literal persist type in meta
    private enum DateLiteralType {
        DATETIME(0),
        DATE(1),

        DATETIMEV2(2),
        DATEV2(3);

        private final int value;

        DateLiteralType(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    public DateLiteral() {
        super();
    }

    public DateLiteral(Type type, boolean isMax) throws AnalysisException {
        super();
        this.type = type;
        if (type.equals(Type.DATE) || type.equals(Type.DATEV2)) {
            if (isMax) {
                copy(MAX_DATE);
            } else {
                copy(MIN_DATE);
            }
        } else if (type.equals(Type.DATETIME)) {
            if (isMax) {
                copy(MAX_DATETIME);
            } else {
                copy(MIN_DATETIME);
            }
        } else {
            if (isMax) {
                copy(MAX_DATETIMEV2);
            } else {
                copy(MIN_DATETIMEV2);
            }
        }
        analysisDone();
    }

    public DateLiteral(String s, Type type) throws AnalysisException {
        super();
        init(s, type);
        analysisDone();
    }

    public DateLiteral(String s) throws AnalysisException {
        super();
        init(s, null);
        analysisDone();
    }

    public DateLiteral(long unixTimestamp, TimeZone timeZone, Type type) throws AnalysisException {
        Timestamp timestamp = new Timestamp(unixTimestamp);

        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.of(timeZone.getID()));
        year = zonedDateTime.getYear();
        month = zonedDateTime.getMonthValue();
        day = zonedDateTime.getDayOfMonth();
        hour = zonedDateTime.getHour();
        minute = zonedDateTime.getMinute();
        second = zonedDateTime.getSecond();
        microsecond = zonedDateTime.get(ChronoField.MICRO_OF_SECOND);
        if (type.equals(Type.DATE)) {
            hour = 0;
            minute = 0;
            second = 0;
            microsecond = 0;
            this.type = Type.DATE;
        } else if (type.equals(Type.DATETIME)) {
            this.type = Type.DATETIME;
            microsecond = 0;
        } else if (type.equals(Type.DATEV2)) {
            hour = 0;
            minute = 0;
            second = 0;
            microsecond = 0;
            this.type = Type.DATEV2;
        } else if (type.equals(Type.DATETIMEV2)) {
            this.type = Type.DATETIMEV2;
        } else {
            throw new AnalysisException("Error date literal type : " + type);
        }
        analysisDone();
    }

    public DateLiteral(long year, long month, long day) {
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.year = year;
        this.month = month;
        this.day = day;
        this.type = ScalarType.getDefaultDateType(Type.DATE);
        analysisDone();
    }

    public DateLiteral(long year, long month, long day, Type type) {
        this.year = year;
        this.month = month;
        this.day = day;
        Preconditions.checkArgument(type.getPrimitiveType().equals(Type.DATE.getPrimitiveType())
                || type.getPrimitiveType().equals(Type.DATEV2.getPrimitiveType()));
        this.type = type;
        analysisDone();
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
        this.type = ScalarType.getDefaultDateType(Type.DATETIME);
        analysisDone();
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second, long microsecond,
                       Type type) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
        this.microsecond = microsecond;
        Preconditions.checkArgument(type.isDatetimeV2());
        this.type = type;
        analysisDone();
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second, Type type) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
        Preconditions.checkArgument(type.getPrimitiveType().equals(Type.DATETIME.getPrimitiveType())
                || type.getPrimitiveType().equals(Type.DATETIMEV2.getPrimitiveType()));
        this.type = type;
        analysisDone();
    }

    public DateLiteral(LocalDateTime dateTime, Type type) {
        this.year = dateTime.getYear();
        this.month = dateTime.getMonthValue();
        this.day = dateTime.getDayOfMonth();
        this.type = type;
        if (type.isDatetime() || type.isDatetimeV2()) {
            this.hour = dateTime.getHour();
            this.minute = dateTime.getMinute();
            this.second = dateTime.getSecond();
            this.microsecond = dateTime.get(ChronoField.MICRO_OF_SECOND);
        }
        analysisDone();
    }

    public DateLiteral(DateLiteral other) {
        super(other);
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        type = other.type;
    }

    public static DateLiteral createMinValue(Type type) throws AnalysisException {
        return new DateLiteral(type, false);
    }

    private void init(String s, @Nullable Type type) throws AnalysisException {
        try {
            if (type != null) {
                Preconditions.checkArgument(type.isDateType());
            }
            TemporalAccessor dateTime = null;
            boolean parsed = false;
            int offset = 0;

            // parse timezone
            if (haveTimeZoneOffset(s) || haveTimeZoneName(s)) {
                String tzString = new String();
                if (haveTimeZoneName(s)) { // GMT, UTC+8, Z[, CN, Asia/Shanghai]
                    int split = getTimeZoneSplitPos(s);
                    Preconditions.checkArgument(split > 0);
                    tzString = s.substring(split);
                    s = s.substring(0, split);
                } else { // +04:30
                    Preconditions.checkArgument(s.charAt(s.length() - 6) == '-' || s.charAt(s.length() - 6) == '+');
                    tzString = s.substring(s.length() - 6);
                    s = s.substring(0, s.length() - 6);
                }
                ZoneId zone = ZoneId.of(tzString);
                ZoneId dorisZone = DateUtils.getTimeZone();
                offset = dorisZone.getRules().getOffset(java.time.Instant.now()).getTotalSeconds()
                    - zone.getRules().getOffset(java.time.Instant.now()).getTotalSeconds();
            }

            if (!s.contains("-")) {
                // handle format like 20210106, but should not handle 2021-1-6
                for (DateTimeFormatter formatter : formatterList) {
                    try {
                        dateTime = formatter.parse(s);
                        parsed = true;
                        break;
                    } catch (DateTimeParseException ex) {
                        // ignore
                    }
                }
                if (!parsed) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
            } else {
                String[] datePart = s.contains(" ") ? s.split(" ")[0].split("-") : s.split("-");
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
                if (datePart.length != 3) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
                for (int i = 0; i < datePart.length; i++) {
                    switch (i) {
                        case 0:
                            if (datePart[i].length() == 2) {
                                // If year is represented by two digits, number bigger than 70 will be prefixed
                                // with 19 otherwise 20. e.g. 69 -> 2069, 70 -> 1970.
                                builder.appendValueReduced(ChronoField.YEAR, 2, 2, 1970);
                            } else {
                                builder.appendPattern(String.join("", Collections.nCopies(datePart[i].length(), "u")));
                            }
                            break;
                        case 1:
                            builder.appendPattern(String.join("", Collections.nCopies(datePart[i].length(), "M")));
                            break;
                        case 2:
                            builder.appendPattern(String.join("", Collections.nCopies(datePart[i].length(), "d")));
                            break;
                        default:
                            throw new AnalysisException("Two many parts in date format " + s);
                    }
                    if (i < datePart.length - 1) {
                        builder.appendLiteral("-");
                    }
                }
                if (s.contains(" ")) {
                    builder.appendLiteral(" ");
                }
                String[] timePart = s.contains(" ") ? s.split(" ")[1].split(":") : new String[]{};
                if (timePart.length > 0 && type != null && (type.equals(Type.DATE) || type.equals(Type.DATEV2))) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
                if (timePart.length == 0 && type != null && (type.equals(Type.DATETIME) || type.equals(
                        Type.DATETIMEV2))) {
                    throw new AnalysisException("Invalid datetime value: " + s);
                }
                for (int i = 0; i < timePart.length; i++) {
                    switch (i) {
                        case 0:
                            builder.appendPattern(String.join("", Collections.nCopies(timePart[i].length(), "H")));
                            break;
                        case 1:
                            builder.appendPattern(String.join("", Collections.nCopies(timePart[i].length(), "m")));
                            break;
                        case 2:
                            builder.appendPattern(String.join("", Collections.nCopies(timePart[i].contains(".")
                                    ? timePart[i].split("\\.")[0].length() : timePart[i].length(), "s")));
                            if (timePart[i].contains(".")) {
                                builder.appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true);
                            }
                            break;
                        default:
                            throw new AnalysisException("Two many parts in time format " + s);
                    }
                    if (i < timePart.length - 1) {
                        builder.appendLiteral(":");
                    }
                }
                // The default resolver style is 'SMART', which parses "2022-06-31" as "2022-06-30"
                // and does not throw an exception. 'STRICT' is used here.
                DateTimeFormatter formatter = builder.toFormatter().withResolverStyle(ResolverStyle.STRICT);
                dateTime = formatter.parse(s);
                parsed = true;
            }

            Preconditions.checkArgument(parsed);
            year = getOrDefault(dateTime, ChronoField.YEAR, 0);
            month = getOrDefault(dateTime, ChronoField.MONTH_OF_YEAR, 0);
            day = getOrDefault(dateTime, ChronoField.DAY_OF_MONTH, 0);
            hour = getOrDefault(dateTime, ChronoField.HOUR_OF_DAY, 0);
            minute = getOrDefault(dateTime, ChronoField.MINUTE_OF_HOUR, 0);
            second = getOrDefault(dateTime, ChronoField.SECOND_OF_MINUTE, 0);
            microsecond = getOrDefault(dateTime, ChronoField.MICRO_OF_SECOND, 0);

            if (type != null) {
                if (microsecond != 0 && type.isDatetime()) {
                    int dotIndex = s.lastIndexOf(".");
                    int scale = s.length() - dotIndex - 1;
                    type = ScalarType.createDatetimeV2Type(scale);
                }
            } else {
                if (hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
                    type = ScalarType.getDefaultDateType(Type.DATE);
                } else {
                    type = ScalarType.getDefaultDateType(Type.DATETIME);
                    if (type.isDatetimeV2() && microsecond != 0) {
                        int scale = 6;
                        for (int i = 0; i < 6; i++) {
                            if (microsecond % Math.pow(10.0, i + 1) > 0) {
                                break;
                            } else {
                                scale -= 1;
                            }
                        }
                        type = ScalarType.createDatetimeV2Type(scale);
                    }
                }
            }
            this.type = type;

            if (offset != 0) {
                DateLiteral result = this.plusSeconds(offset);
                this.second = result.second;
                this.minute = result.minute;
                this.hour = result.hour;
                this.day = result.day;
                this.month = result.month;
                this.year = result.year;
            }

            if (checkRange() || checkDate()) {
                throw new AnalysisException("Datetime value is out of range");
            }
        } catch (Exception ex) {
            throw new AnalysisException("date literal [" + s + "] is invalid: " + ex.getMessage());
        }
    }

    private void copy(DateLiteral other) {
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        type = other.type;
    }

    @Override
    public Expr clone() {
        return new DateLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case DATE:
            case DATEV2:
                return year == 0 && month == 1 && day == 1
                    && this.getStringValue().compareTo(MIN_DATE.getStringValue()) == 0;
            case DATETIME:
                return year == 0 && month == 1 && day == 1
                    && this.getStringValue().compareTo(MIN_DATETIME.getStringValue()) == 0;
            case DATETIMEV2:
                return year == 0 && month == 1 && day == 1
                    && this.getStringValue().compareTo(MIN_DATETIMEV2.getStringValue()) == 0;
            default:
                return false;
        }
    }

    @Override
    public Object getRealValue() {
        if (type.equals(Type.DATE)) {
            return year * 16 * 32L + month * 32 + day;
        } else if (type.equals(Type.DATETIME)) {
            return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
        } else if (type.equals(Type.DATEV2)) {
            return (year << 9) | (month << 5) | day;
        } else if (type.isDatetimeV2()) {
            return (year << 46) | (month << 42) | (day << 37) | (hour << 32)
                | (minute << 26) | (second << 20) | (microsecond % (1 << 20));
        } else {
            Preconditions.checkState(false, "invalid date type: " + type);
            return -1L;
        }
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        ByteBuffer buffer;
        if (type == PrimitiveType.DATEV2) {
            int value = (int) ((year << 9) | (month << 5) | day);
            buffer = ByteBuffer.allocate(4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(value);
        } else if (type == PrimitiveType.DATETIMEV2) {
            long value = (year << 46) | (month << 42) | (day << 37) | (hour << 32)
                    | (minute << 26) | (second << 20) | (microsecond % (1 << 20));
            buffer = ByteBuffer.allocate(8);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(value);
        } else {
            // This hash value should be computed using new String since precision is introduced to datetime.
            // But it is hard to keep compatibility. So I don't change this function here.
            String value = convertToString(type);
            try {
                buffer = ByteBuffer.wrap(value.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof PlaceHolderExpr) {
            return this.compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 1;
        }

        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (expr instanceof DateLiteral) {
            DateLiteral other = (DateLiteral) expr;
            long yearMonthDay = year * 10000 + month * 100 + day;
            long otherYearMonthDay = other.year * 10000 + other.month * 100 + other.day;
            long diffDay = yearMonthDay - otherYearMonthDay;
            if (diffDay != 0) {
                return diffDay < 0 ? -1 : 1;
            }

            int typeAsInt = isDateType() ? 0 : 1;
            int thatTypeAsInt = other.isDateType() ? 0 : 1;
            int typeDiff = typeAsInt - thatTypeAsInt;
            if (typeDiff != 0) {
                return typeDiff;
            } else if (typeAsInt == 0) {
                // if all is date and equals date, then return
                return 0;
            }

            long hourMinuteSecond = hour * 10000 + minute * 100 + second;
            long otherHourMinuteSecond = other.hour * 10000 + other.minute * 100 + other.second;
            long diffSecond = hourMinuteSecond - otherHourMinuteSecond;
            if (diffSecond != 0) {
                return diffSecond < 0 ? -1 : 1;
            }
            long diff = getMicroPartWithinScale() - other.getMicroPartWithinScale();
            return diff < 0 ? -1 : (diff == 0 ? 0 : 1);
        }
        // date time will not overflow when doing addition and subtraction
        return getStringValue().compareTo(expr.getStringValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    private void fillPaddedValue(char[] buffer, int start, long value, int length) {
        int end = start + length;
        for (int i = end - 1; i >= start; i--) {
            buffer[i] = (char) ('0' + value % 10);
            value /= 10;
        }
    }

    public boolean isDateType() {
        return this.type.isDate() || this.type.isDateV2();
    }

    public boolean isDateTimeType() {
        return this.type.isDatetime() || this.type.isDatetimeV2();
    }

    @Override
    public String getStringValue() {
        char[] dateTimeChars = new char[26]; // Enough to hold "YYYY-MM-DD HH:MM:SS.mmmmmm"

        // Populate the date part
        fillPaddedValue(dateTimeChars, 0, year, 4);
        dateTimeChars[4] = '-';
        fillPaddedValue(dateTimeChars, 5, month, 2);
        dateTimeChars[7] = '-';
        fillPaddedValue(dateTimeChars, 8, day, 2);

        if (type.isDate() || type.isDateV2()) {
            return new String(dateTimeChars, 0, 10);
        }

        // Populate the time part
        dateTimeChars[10] = ' ';
        fillPaddedValue(dateTimeChars, 11, hour, 2);
        dateTimeChars[13] = ':';
        fillPaddedValue(dateTimeChars, 14, minute, 2);
        dateTimeChars[16] = ':';
        fillPaddedValue(dateTimeChars, 17, second, 2);

        if (type.isDatetimeV2()) {
            int scale = ((ScalarType) type).getScalarScale();
            long scaledMicroseconds = (long) (microsecond / SCALE_FACTORS[scale]);

            if (scaledMicroseconds != 0) {
                dateTimeChars[19] = '.';
                fillPaddedValue(dateTimeChars, 20, (int) scaledMicroseconds, scale);
                return new String(dateTimeChars, 0, 20 + scale);
            }
        }

        return new String(dateTimeChars, 0, 19);
    }

    public String getStringValue(Type type) {
        char[] dateTimeChars = new char[26]; // Enough to hold "YYYY-MM-DD HH:MM:SS.mmmmmm"

        // Populate the date part
        fillPaddedValue(dateTimeChars, 0, year, 4);
        dateTimeChars[4] = '-';
        fillPaddedValue(dateTimeChars, 5, month, 2);
        dateTimeChars[7] = '-';
        fillPaddedValue(dateTimeChars, 8, day, 2);

        if (type.isDate() || type.isDateV2()) {
            return new String(dateTimeChars, 0, 10);
        }

        // Populate the time part
        dateTimeChars[10] = ' ';
        fillPaddedValue(dateTimeChars, 11, hour, 2);
        dateTimeChars[13] = ':';
        fillPaddedValue(dateTimeChars, 14, minute, 2);
        dateTimeChars[16] = ':';
        fillPaddedValue(dateTimeChars, 17, second, 2);

        if (type.isDatetimeV2()) {
            int scale = ((ScalarType) type).getScalarScale();
            long scaledMicroseconds = (long) (microsecond / SCALE_FACTORS[scale]);
            dateTimeChars[19] = '.';
            fillPaddedValue(dateTimeChars, 20, (int) scaledMicroseconds, scale);
            return new String(dateTimeChars, 0, 20 + scale);
        }

        return new String(dateTimeChars, 0, 19);
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValue() + options.getNestedStringWrapper();
    }

    public void roundCeiling(int newScale) {
        Preconditions.checkArgument(type.isDatetimeV2());
        long remain = Double.valueOf(microsecond % (Math.pow(10, 6 - newScale))).longValue();
        if (remain != 0) {
            microsecond = Double.valueOf((microsecond + (Math.pow(10, 6 - newScale)))
                / (int) (Math.pow(10, 6 - newScale)) * (Math.pow(10, 6 - newScale))).longValue();
        }
        if (microsecond > MAX_MICROSECOND) {
            microsecond %= microsecond;
            DateLiteral result = this.plusSeconds(1);
            this.second = result.second;
            this.minute = result.minute;
            this.hour = result.hour;
            this.day = result.day;
            this.month = result.month;
            this.year = result.year;
        }
        type = ScalarType.createDatetimeV2Type(newScale);
    }

    public void roundFloor(int newScale) {
        microsecond = Double.valueOf(microsecond / (int) (Math.pow(10, 6 - newScale))
            * (Math.pow(10, 6 - newScale))).longValue();
        type = ScalarType.createDatetimeV2Type(newScale);
    }

    public String convertToString(PrimitiveType type) {
        if (type == PrimitiveType.DATE || type == PrimitiveType.DATEV2) {
            return String.format("%04d-%02d-%02d", year, month, day);
        } else if (type == PrimitiveType.DATETIMEV2) {
            String tmp = String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year, month, day, hour, minute, second);
            if (microsecond == 0) {
                return tmp;
            }
            return tmp + String.format(".%06d", microsecond);
        } else {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        }
    }

    @Override
    public long getLongValue() {
        if (this.getType().isDate() || this.getType().isDateV2()) {
            return year * 10000 + month * 100 + day;
        } else {
            return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
        }
    }

    @Override
    public double getDoubleValue() {
        return getLongValue();
    }

    public double getDoubleValueAsDateTime() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        if (type.isDatetimeV2()) {
            this.roundFloor(((ScalarType) type).getScalarScale());
        }
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(getStringValue());
        try {
            checkValueValid();
        } catch (AnalysisException e) {
            // we must check before here. when we think we are ready to send thrift msg,
            // the invalid value is not acceptable. we can't properly deal with it.
            LOG.warn("meet invalid value when plan to translate " + toString() + " to thrift node");
        }
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDateType()) {
            if (type.equals(targetType)) {
                return this;
            }
            if (targetType.equals(Type.DATE) || targetType.equals(Type.DATEV2)) {
                return new DateLiteral(this.year, this.month, this.day, targetType);
            } else if (targetType.equals(Type.DATETIME)) {
                return new DateLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second,
                    targetType);
            } else if (targetType.isDatetimeV2()) {
                return new DateLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second,
                    this.microsecond, targetType);
            } else {
                throw new AnalysisException("Error date literal type : " + type);
            }
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue());
        } else if (targetType.isBigIntType()) {
            long value = getYear() * 1000 + getMonth() * 100 + getDay();
            return new IntLiteral(value, Type.BIGINT);
        } else {
            if (Type.isImplicitlyCastable(this.type, targetType, true, SessionVariable.getEnableDecimal256())) {
                return new CastExpr(targetType, this);
            }
        }
        Preconditions.checkState(false);
        return this;
    }

    public void castToDate() {
        if (this.type.isDateOrDateTime()) {
            this.type = Type.DATE;
        } else {
            this.type = Type.DATEV2;
        }
        hour = 0;
        minute = 0;
        second = 0;
    }

    public boolean hasTimePart() {
        if (this.type.isDateV2() || this.type.isDate()) {
            return false;
        } else {
            if (hour != 0 || minute != 0 || second != 0) {
                return true;
            } else {
                return !this.type.isDatetime() && microsecond != 0;
            }
        }
    }

    private void fromPackedDatetimeV2(long packedTime) {
        microsecond = (packedTime % (1L << 20));
        long ymdhms = (packedTime >> 20);
        long ymd = ymdhms >> 17;
        day = ymd % (1 << 5);
        long ym = ymd >> 5;
        month = ym % (1 << 4);
        year = ym >> 4;

        long hms = ymdhms % (1 << 17);
        second = hms % (1 << 6);
        minute = (hms >> 6) % (1 << 6);
        hour = (hms >> 12);
        // set default date literal type to DATETIME
        // date literal read from meta will set type by flag bit;
        this.type = Type.DATETIMEV2;
    }

    private void fromPackedDateV2(long packedTime) {
        day = packedTime % (1 << 5);
        long ym = packedTime >> 5;
        month = ym % (1 << 4);
        year = ym >> 4;

        this.type = Type.DATEV2;
    }

    private void fromPackedDatetime(long packedTime) {
        microsecond = (packedTime % (1L << 24));
        long ymdhms = (packedTime >> 24);
        long ymd = ymdhms >> 17;
        day = ymd % (1 << 5);
        long ym = ymd >> 5;
        month = ym % 13;
        year = ym / 13;
        year %= 10000;

        long hms = ymdhms % (1 << 17);
        second = hms % (1 << 6);
        minute = (hms >> 6) % (1 << 6);
        hour = (hms >> 12);
        // set default date literal type to DATETIME
        // date literal read from meta will set type by flag bit;
        this.type = Type.DATETIME;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        short dateLiteralType = in.readShort();
        if (dateLiteralType == DateLiteralType.DATETIME.value()) {
            fromPackedDatetime(in.readLong());
            this.type = Type.DATETIME;
        } else if (dateLiteralType == DateLiteralType.DATE.value()) {
            fromPackedDatetime(in.readLong());
            this.type = Type.DATE;
        } else if (dateLiteralType == DateLiteralType.DATETIMEV2.value()) {
            fromPackedDatetimeV2(in.readLong());
            this.type = ScalarType.createDatetimeV2Type(in.readInt());
        } else if (dateLiteralType == DateLiteralType.DATEV2.value()) {
            fromPackedDateV2(in.readLong());
            this.type = Type.DATEV2;
        } else {
            throw new IOException("Error date literal type : " + type);
        }
    }

    private boolean isLeapYear() {
        return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0 && year > 0));
    }

    // Validation check should be same as DateV2Value<T>::is_invalid in BE
    @Override
    public void checkValueValid() throws AnalysisException {
        if (year < 0 || year > 9999) {
            throw new AnalysisException("DateLiteral has invalid year value: " + year);
        }
        if (month < 1 || month > 12) {
            throw new AnalysisException("DateLiteral has invalid month value: " + month);
        }
        if (day < 1 || day > DAYS_IN_MONTH[(int) month]) {
            if (!(month == 2 && day == 29 && isLeapYear())) {
                throw new AnalysisException("DateLiteral has invalid day value: " + day);
            }
        }
        if (type.isDatetimeV2() || type.isDatetime()) {
            if (hour < 0 || hour > 24) {
                throw new AnalysisException("DateLiteral has invalid hour value: " + hour);
            }
            if (minute < 0 || minute > 60) {
                throw new AnalysisException("DateLiteral has invalid minute value: " + minute);
            }
            if (second < 0 || second > 60) {
                throw new AnalysisException("DateLiteral has invalid second value: " + second);
            }
            if (type.isDatetimeV2() && (microsecond < 0 || microsecond > 999999)) {
                throw new AnalysisException("DateLiteral has invalid microsecond value: " + microsecond);
            }
        }
    }

    public static DateLiteral read(DataInput in) throws IOException {
        DateLiteral literal = new DateLiteral();
        literal.readFields(in);
        return literal;
    }

    public long unixTimestamp(TimeZone timeZone) {
        Timestamp timestamp = getTimestamp(timeZone);
        return timestamp.getTime();
    }

    private Timestamp getTimestamp(TimeZone timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.of((int) year, (int) month, (int) day, (int) hour,
                (int) minute, (int) second, (int) microsecond * 1000, ZoneId.of(timeZone.getID()));
        return Timestamp.from(zonedDateTime.toInstant());
    }

    public long getUnixTimestampWithMillisecond(TimeZone timeZone) {
        return unixTimestamp(timeZone);
    }

    public long getUnixTimestampWithMicroseconds(TimeZone timeZone) {
        Timestamp timestamp = getTimestamp(timeZone);
        return timestamp.getTime() * 1000 + timestamp.getNanos() / 1000 % 1000;
    }

    public static boolean hasTimePart(String format) {
        return format.chars().anyMatch(c -> TIME_PART_SET.contains((char) c));
    }

    // Return the date stored in the dateliteral as pattern format.
    // eg : "%Y-%m-%d" or "%Y-%m-%d %H:%i:%s"
    public String dateFormat(String pattern) throws AnalysisException {
        TemporalAccessor accessor;
        if (type.equals(Type.DATE) || type.equals(Type.DATEV2)) {
            accessor = DATE_FORMATTER.parse(getStringValue());
        } else if (type.isDatetimeV2()) {
            accessor = DATE_TIME_FORMATTER_TO_MICRO_SECOND.parse(getStringValue());
        } else {
            accessor = DATE_TIME_FORMATTER.parse(getStringValue());
        }
        DateTimeFormatter toFormatter = formatBuilder(pattern).toFormatter();
        return toFormatter.format(accessor);
    }

    private static DateTimeFormatterBuilder formatBuilder(String pattern) throws AnalysisException {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);
            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.SHORT);
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.SHORT);
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendValue(ChronoField.MONTH_OF_YEAR);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendValue(ChronoField.DAY_OF_MONTH, 2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendValue(ChronoField.DAY_OF_MONTH);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendValue(ChronoField.HOUR_OF_DAY, 2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendValue(ChronoField.HOUR_OF_AMPM, 2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendValue(ChronoField.MINUTE_OF_HOUR, 2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendValue(ChronoField.DAY_OF_YEAR, 3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendValue(ChronoField.HOUR_OF_DAY);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendValue(ChronoField.HOUR_OF_AMPM);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.FULL);
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendValue(ChronoField.MONTH_OF_YEAR, 2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendText(ChronoField.AMPM_OF_DAY);
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendValue(ChronoField.HOUR_OF_AMPM, 2)
                            .appendPattern(":mm:ss ")
                            .appendText(ChronoField.AMPM_OF_DAY, TextStyle.FULL)
                                .toFormatter();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendValue(ChronoField.SECOND_OF_MINUTE, 2);
                        break;
                    case 'T': // %T Time, 24-hour (HH:mm:ss)
                        builder.appendPattern("HH:mm:ss");
                        break;
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                        builder.appendValue(weekFields.weekOfWeekBasedYear(), 2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.FULL);
                        break;
                    case 'x': // %x Year for the week where Monday is the first day of the week,
                        builder.appendValue(IsoFields.WEEK_BASED_YEAR, 4);
                        break;
                    case 'X':
                        builder.appendValue(weekFields.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        // %X Year for the week, where Sunday is the first day of the week,
                        // numeric, four digits; used with %v
                        builder.appendValue(ChronoField.YEAR, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendValueReduced(ChronoField.YEAR, 2, 2, 1970);
                        break;
                    // TODO(Gabriel): support microseconds in date literal
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                    case 'f': // %f Microseconds (000000..999999)
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        throw new AnalysisException(String.format("%%%s not supported in date format string",
                            character));
                    case '%': // %% A literal "%" character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            } else if (character == '%') {
                escaped = true;
            } else {
                builder.appendLiteral(character);
            }
        }
        return builder;
    }

    private int getOrDefault(final TemporalAccessor accessor, final ChronoField field,
                             final int defaultValue) {
        return accessor.isSupported(field) ? accessor.get(field) : defaultValue;
    }

    public LocalDateTime getTimeFormatter() {
        if (type.equals(Type.DATE) || type.equals(Type.DATEV2)) {
            return LocalDateTime.of((int) this.year, (int) this.month, (int) this.day, 0, 0, 0);
        } else if (type.isDatetimeV2()) {
            return LocalDateTime.of((int) this.year, (int) this.month, (int) this.day, (int) this.hour,
                (int) this.minute,
                (int) this.second, (int) this.microsecond * 1000);
        } else {
            return LocalDateTime.of((int) this.year, (int) this.month, (int) this.day, (int) this.hour,
                (int) this.minute,
                (int) this.second);
        }
    }

    public DateLiteral plusYears(long year) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusYears(year), type);
    }

    public DateLiteral plusMonths(long month) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusMonths(month), type);
    }

    public DateLiteral plusDays(long day) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusDays(day), type);
    }

    public DateLiteral plusHours(long hour) throws AnalysisException {
        if (type.isDate()) {
            return new DateLiteral(getTimeFormatter().plusHours(hour), Type.DATETIME);
        }
        if (type.isDateV2()) {
            return new DateLiteral(getTimeFormatter().plusHours(hour), Type.DATETIMEV2);
        }
        return new DateLiteral(getTimeFormatter().plusHours(hour), type);
    }

    public DateLiteral plusMinutes(long minute) {
        if (type.isDate()) {
            return new DateLiteral(getTimeFormatter().plusMinutes(minute), Type.DATETIME);
        }
        if (type.isDateV2()) {
            return new DateLiteral(getTimeFormatter().plusMinutes(minute), Type.DATETIMEV2);
        }
        return new DateLiteral(getTimeFormatter().plusMinutes(minute), type);
    }

    public DateLiteral plusSeconds(long second) {
        if (type.isDate()) {
            return new DateLiteral(getTimeFormatter().plusSeconds(second), Type.DATETIME);
        }
        if (type.isDateV2()) {
            return new DateLiteral(getTimeFormatter().plusSeconds(second), Type.DATETIMEV2);
        }
        return new DateLiteral(getTimeFormatter().plusSeconds(second), type);
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

    public long getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }

    public long getMicrosecond() {
        return microsecond;
    }

    @SerializedName("y")
    private long year;
    @SerializedName("m")
    private long month;
    @SerializedName("d")
    private long day;
    @SerializedName("h")
    private long hour;
    @SerializedName("M")
    private long minute;
    @SerializedName("s")
    private long second;
    @SerializedName("ms")
    private long microsecond;

    @Override
    public int hashCode() {
        // do not invoke the super.hashCode(), just use the return value of getLongValue()
        // a DateV2 DateLiteral obj may be equal to a Date DateLiteral
        // if the return value of getLongValue() is the same
        return Long.hashCode(getLongValue());
    }

    // parse the date string value in 'value' by 'format' pattern.
    // return the next position to parse if hasSubVal is true.
    // throw InvalidFormatException if encounter errors.
    // this method is exaclty same as from_date_format_str() in be/src/runtime/datetime_value.cpp
    // change this method should also change that.
    public int fromDateFormatStr(String format, String value, boolean hasSubVal) throws InvalidFormatException {
        int pFormat = 0; // pointer to the current format string
        int endFormat = format.length(); // end of format string
        int pValue = 0; // pointer to the date string value
        int endValue = value.length(); // end of date string value

        int partUsed = 0;
        final int yearPart = 1 << 0;
        final int monthPart = 1 << 1;
        final int dayPart = 1 << 2;
        final int weekdayPart = 1 << 3;
        final int yeardayPart = 1 << 4;
        final int weekNumPart = 1 << 5;
        final int normalDatePart = yearPart | monthPart | dayPart;
        final int specialDatePart = weekdayPart | yeardayPart | weekNumPart;
        final int datePart = normalDatePart | specialDatePart;
        final int hourPart = 1 << 6;
        final int minutePart = 1 << 7;
        final int secondPart = 1 << 8;
        final int fracPart = 1 << 9;
        final int timePart = hourPart | minutePart | secondPart | fracPart;

        int halfDay = 0; // 0 for am/none, 12 for pm.
        long weekday = -1;
        long yearday = -1;
        long weekNum = -1;

        boolean strictWeekNumber = false;
        boolean sundayFirst = false;
        boolean strictWeekNumberYearType = false;
        long strictWeekNumberYear = -1;
        boolean hourSystem12 = false; // hour in [0..12] and with am/pm

        char now;
        while (pFormat < endFormat && pValue < endValue) {
            // Skip space character
            while (pValue < endValue && Character.isSpaceChar(value.charAt(pValue))) {
                pValue++;
            }
            if (pValue >= endValue) {
                break;
            }

            // Check switch
            now = format.charAt(pFormat);
            if (now == '%' && pFormat + 1 < endFormat) {
                int tmp = 0;
                long intValue = 0;
                pFormat++;
                now = format.charAt(pFormat);
                pFormat++;
                switch (now) {
                    // Year
                    case 'y':
                        // Year, numeric (two digits)
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        intValue += intValue >= 70 ? 1900 : 2000;
                        this.year = intValue;
                        pValue = tmp;
                        partUsed |= yearPart;
                        break;
                    case 'Y':
                        // Year, numeric, four digits
                        tmp = pValue + Math.min(4, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        if (tmp - pValue <= 2) {
                            intValue += intValue >= 70 ? 1900 : 2000;
                        }
                        this.year = intValue;
                        pValue = tmp;
                        partUsed |= yearPart;
                        break;
                    // Month
                    case 'm':
                    case 'c':
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        this.month = intValue;
                        pValue = tmp;
                        partUsed |= monthPart;
                        break;
                    case 'M': {
                        int nextPos = findWord(value, pValue);
                        intValue = checkWord(MONTH_NAME_DICT, value.substring(pValue, nextPos));
                        this.month = intValue;
                        pValue = nextPos;
                        partUsed |= monthPart;
                        break;
                    }
                    case 'b': {
                        int nextPos = findWord(value, pValue);
                        intValue = checkWord(MONTH_ABBR_NAME_DICT, value.substring(pValue, nextPos));
                        this.month = intValue;
                        pValue = nextPos;
                        partUsed |= monthPart;
                        break;
                    }
                    // Day
                    case 'd':
                    case 'e':
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        this.day = intValue;
                        pValue = tmp;
                        partUsed |= dayPart;
                        break;
                    case 'D':
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        this.day = intValue;
                        pValue = tmp + Math.min(2, endValue - tmp);
                        partUsed |= dayPart;
                        break;
                    // Hour
                    case 'h':
                    case 'I':
                    case 'l':
                        hourSystem12 = true;
                        partUsed |= hourPart;
                    case 'k': // CHECKSTYLE IGNORE THIS LINE: Fall through
                    case 'H':
                        tmp = findNumber(value, pValue, 2);
                        intValue = strToLong(value.substring(pValue, tmp));
                        this.hour = intValue;
                        pValue = tmp;
                        partUsed |= hourPart;
                        break;
                    // Minute
                    case 'i':
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        this.minute = intValue;
                        pValue = tmp;
                        partUsed |= minutePart;
                        break;
                    // Second
                    case 's':
                    case 'S':
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        this.second = intValue;
                        pValue = tmp;
                        partUsed |= secondPart;
                        break;
                    // Micro second
                    case 'f':
                        tmp = pValue;
                        // when there's still something to the end, fix the scale of ms.
                        while (tmp < endValue && Character.isDigit(value.charAt(tmp))) {
                            tmp += 1;
                        }

                        if (tmp - pValue > 6) {
                            int tmp2 = pValue + 6;
                            intValue = strToLong(value.substring(pValue, tmp2));
                        } else {
                            intValue = strToLong(value.substring(pValue, tmp));
                        }
                        this.microsecond = (long) (intValue * Math.pow(10, 6 - Math.min(6, tmp - pValue)));
                        partUsed |= fracPart;
                        pValue = tmp;
                        break;
                    // AM/PM
                    case 'p':
                        if ((endValue - pValue) < 2 || Character.toUpperCase(value.charAt(pValue + 1)) != 'M'
                                || !hourSystem12) {
                            throw new InvalidFormatException("Invalid %p format");
                        }
                        if (Character.toUpperCase(value.charAt(pValue)) == 'P') {
                            // PM
                            halfDay = 12;
                        }
                        pValue += 2;
                        break;
                    // Weekday
                    case 'W': {
                        int nextPos = findWord(value, pValue);
                        intValue = checkWord(WEEK_DAY_NAME_DICT, value.substring(pValue, nextPos));
                        intValue++;
                        weekday = intValue;
                        partUsed |= weekdayPart;
                        break;
                    }
                    case 'a': {
                        int nextPos = findWord(value, pValue);
                        intValue = checkWord(WEEK_DAY_NAME_DICT, value.substring(pValue, nextPos));
                        intValue++;
                        weekday = intValue;
                        partUsed |= weekdayPart;
                        break;
                    }
                    case 'w':
                        tmp = pValue + Math.min(1, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        if (intValue >= 7) {
                            throw new InvalidFormatException("invalid day of week: " + intValue);
                        }
                        if (intValue == 0) {
                            intValue = 7;
                        }
                        weekday = intValue;
                        pValue = tmp;
                        partUsed |= weekdayPart;
                        break;
                    case 'j':
                        tmp = pValue + Math.min(3, endValue - pValue);
                        intValue = strToLong(value.substring(pValue, tmp));
                        yearday = intValue;
                        pValue = tmp;
                        partUsed |= yeardayPart;
                        break;
                    case 'u':
                    case 'v':
                    case 'U':
                    case 'V':
                        sundayFirst = (format.charAt(pFormat - 1) == 'U' || format.charAt(pFormat - 1) == 'V');
                        // Used to check if there is %x or %X
                        strictWeekNumber = (format.charAt(pFormat - 1) == 'V' || format.charAt(pFormat - 1) == 'v');
                        tmp = pValue + Math.min(2, endValue - pValue);
                        intValue = Long.valueOf(value.substring(pValue, tmp));
                        weekNum = intValue;
                        if (weekNum > 53 || (strictWeekNumber && weekNum == 0)) {
                            throw new InvalidFormatException("invalid num of week: " + weekNum);
                        }
                        pValue = tmp;
                        partUsed |= weekNumPart;
                        break;
                    // strict week number, must be used with %V or %v
                    case 'x':
                    case 'X':
                        strictWeekNumberYearType = (format.charAt(pFormat - 1) == 'X');
                        tmp = pValue + Math.min(4, endValue - pValue);
                        intValue = Long.valueOf(value.substring(pValue, tmp));
                        strictWeekNumberYear = intValue;
                        pValue = tmp;
                        partUsed |= weekNumPart;
                        break;
                    case 'r':
                        tmp = fromDateFormatStr("%I:%i:%S %p", value.substring(pValue, endValue), true);
                        pValue = tmp;
                        partUsed |= timePart;
                        break;
                    case 'T':
                        tmp = fromDateFormatStr("%H:%i:%S", value.substring(pValue, endValue), true);
                        pValue = tmp;
                        partUsed |= timePart;
                        break;
                    case '.':
                        while (pValue < endValue && Character.toString(value.charAt(pValue)).matches("\\p{Punct}")) {
                            pValue++;
                        }
                        break;
                    case '@':
                        while (pValue < endValue && Character.isLetter(value.charAt(pValue))) {
                            pValue++;
                        }
                        break;
                    case '#':
                        while (pValue < endValue && Character.isDigit(value.charAt(pValue))) {
                            pValue++;
                        }
                        break;
                    case '%': // %%, escape the %
                        if ('%' != value.charAt(pValue)) {
                            throw new InvalidFormatException("invalid char after %: " + value.charAt(pValue));
                        }
                        pValue++;
                        break;
                    default:
                        throw new InvalidFormatException("Invalid format pattern: " + now);
                }
            } else if (format.charAt(pFormat) != ' ') {
                if (format.charAt(pFormat) != value.charAt(pValue)) {
                    throw new InvalidFormatException("Invalid char: " + value.charAt(pValue) + ", expected: "
                        + format.charAt(pFormat));
                }
                pFormat++;
                pValue++;
            } else {
                pFormat++;
            }
        }

        // continue to iterate pattern if has
        // to find out if it has time part.
        while (pFormat < endFormat) {
            now = format.charAt(pFormat);
            if (now == '%' && pFormat + 1 < endFormat) {
                pFormat++;
                now = format.charAt(pFormat);
                pFormat++;
                switch (now) {
                    case 'H':
                    case 'h':
                    case 'I':
                    case 'i':
                    case 'k':
                    case 'l':
                    case 'r':
                    case 's':
                    case 'S':
                    case 'p':
                    case 'T':
                        partUsed |= timePart;
                        break;
                    default:
                        break;
                }
            } else {
                pFormat++;
            }
        }

        if (partUsed == 0) {
            throw new InvalidFormatException("Nothing for legal Date: " + value);
        }

        if (hourSystem12) {
            if (this.hour > 12 || this.hour < 1) {
                throw new InvalidFormatException("Invalid hour: " + hour);
            }
            this.hour = (this.hour % 12) + halfDay;
        }

        if (hasSubVal) {
            return pValue;
        }

        // Year day
        if (yearday > 0) {
            long days = calcDaynr(this.year, 1, 1) + yearday - 1;
            getDateFromDaynr(days);
        }

        // weekday
        if (weekNum >= 0 && weekday > 0) {
            // Check
            if ((strictWeekNumber && (strictWeekNumberYear < 0
                    || strictWeekNumberYearType != sundayFirst))
                    || (!strictWeekNumber && strictWeekNumberYear >= 0)) {
                throw new InvalidFormatException("invalid week number");
            }
            long days = calcDaynr(strictWeekNumber ? strictWeekNumberYear : this.year, 1, 1);

            long weekdayB = calcWeekday(days, sundayFirst);

            if (sundayFirst) {
                days += ((weekdayB == 0) ? 0 : 7) - weekdayB + (weekNum - 1) * 7 + weekday % 7;
            } else {
                days += ((weekdayB <= 3) ? 0 : 7) - weekdayB + (weekNum - 1) * 7 + weekday - 1;
            }
            getDateFromDaynr(days);
        }

        // complete default month/day
        if ((partUsed & ~normalDatePart) == 0) { // only date here
            if ((partUsed & dayPart) == 0) {
                day = 1;
                if ((partUsed & monthPart) == 0) {
                    month = 1;
                }
            }
        }

        // Compute timestamp type
        if ((partUsed & datePart) != 0) { // Ymd part only
            if ((partUsed & fracPart) != 0) {
                this.type = Type.DATETIMEV2_WITH_MAX_SCALAR;
            } else if ((partUsed & timePart) != 0) {
                this.type = ScalarType.getDefaultDateType(Type.DATETIME);
            } else {
                this.type = ScalarType.getDefaultDateType(Type.DATE);
            }
        }

        if (checkRange() || checkDate()) {
            throw new InvalidFormatException("Invalid format");
        }
        return 0;
    }

    public int fromDateFormatStr(String format, String value, boolean hasSubVal, Type type)
            throws InvalidFormatException {
        switch (type.getPrimitiveType()) {
            case DATETIME:
            case DATE:
                return fromDateFormatStr(format, value, hasSubVal);
            default:
                int val = fromDateFormatStr(format, value, hasSubVal);
                convertTypeToV2();
                return val;
        }
    }

    private void convertTypeToV2() {
        switch (type.getPrimitiveType()) {
            case DATETIME:
                this.type = Type.DATETIMEV2;
                break;
            case DATE:
                this.type = Type.DATEV2;
                break;
            default:
        }
    }

    private boolean checkRange() {
        return year > MAX_DATETIME.year || month > MAX_DATETIME.month || day > MAX_DATETIME.day
            || hour > MAX_DATETIME.hour || minute > MAX_DATETIME.minute || second > MAX_DATETIME.second
            || microsecond > MAX_MICROSECOND;
    }

    private boolean checkDate() {
        if (month != 0 && day > DAYS_IN_MONTH[((int) month)]) {
            if (month == 2 && day == 29 && Year.isLeap(year)) {
                return false;
            }
            return true;
        }
        return false;
    }

    private long strToLong(String l) throws InvalidFormatException {
        try {
            long y = Long.valueOf(l);
            if (y < 0) {
                throw new InvalidFormatException("Invalid format: negative number.");
            }
            return y;
        } catch (NumberFormatException e) {
            throw new InvalidFormatException(e.getMessage());
        }
    }

    public long daynr() {
        return calcDaynr(this.year, this.month, this.day);
    }

    // calculate the number of days from year 0000-00-00 to year-month-day
    private long calcDaynr(long year, long month, long day) {
        long delsum = 0;
        long y = year;

        if (year == 0 && month == 0) {
            return 0;
        }

        /* Cast to int to be able to handle month == 0 */
        delsum = 365 * y + 31 * (month - 1) + day;
        if (month <= 2) {
            // No leap year
            y--;
        } else {
            // This is great!!!
            // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            // 0, 0, 3, 3, 4, 4, 5, 5, 5,  6,  7,  8
            delsum -= (month * 4 + 23) / 10;
        }
        // Every 400 year has 97 leap year, 100, 200, 300 are not leap year.
        return delsum + y / 4 - y / 100 + y / 400;
    }

    private long calcWeekday(long dayNr, boolean isSundayFirstDay) {
        return (dayNr + 5L + (isSundayFirstDay ? 1L : 0L)) % 7;
    }

    private void getDateFromDaynr(long daynr) throws InvalidFormatException {
        if (daynr <= 0 || daynr > 3652424) {
            throw new InvalidFormatException("Invalid days to year: " + daynr);
        }
        this.year = daynr / 365;
        long daysBeforeYear = 0;
        while (daynr < (daysBeforeYear = calcDaynr(this.year, 1, 1))) {
            this.year--;
        }
        long daysOfYear = daynr - daysBeforeYear + 1;
        int leapDay = 0;
        if (Year.isLeap(this.year)) {
            if (daysOfYear > 31 + 28) {
                daysOfYear--;
                if (daysOfYear == 31 + 28) {
                    leapDay = 1;
                }
            }
        }
        this.month = 1;
        while (daysOfYear > DAYS_IN_MONTH[(int) this.month]) {
            daysOfYear -= DAYS_IN_MONTH[(int) this.month];
            this.month++;
        }
        this.day = daysOfYear + leapDay;
    }

    // find a word start from 'start' from value.
    private int findWord(String value, int start) {
        int p = start;
        while (p < value.length() && Character.isLetter(value.charAt(p))) {
            p++;
        }
        return p;
    }

    // find a number start from 'start' from value.
    private int findNumber(String value, int start, int maxLen) {
        int p = start;
        int left = maxLen;
        while (p < value.length() && Character.isDigit(value.charAt(p)) && left > 0) {
            p++;
            left--;
        }
        return p;
    }

    // check if the given value exist in dict, return dict value.
    private int checkWord(Map<String, Integer> dict, String value) throws InvalidFormatException {
        Integer i = dict.get(value.toLowerCase());
        if (i != null) {
            return i;
        }
        throw new InvalidFormatException("'" + value + "' is invalid");
    }

    private long getMicroPartWithinScale() {
        if (type.isDatetimeV2()) {
            int scale = ((ScalarType) type).getScalarScale();
            return (long) (microsecond / SCALE_FACTORS[scale]);
        } else {
            return 0;
        }
    }

    public void setMinValue() {
        year = 0;
        month = 1;
        day = 1;
        hour = 0;
        minute = 0;
        second = 0;
        microsecond = 0;
    }

    @Override
    public void setupParamFromBinary(ByteBuffer data, boolean isUnsigned) {
        int len = getParmLen(data);
        if (type.getPrimitiveType() == PrimitiveType.DATE) {
            if (len >= 4) {
                year = (int) data.getChar();
                month = (int) data.get();
                day = (int) data.get();
                hour = 0;
                minute = 0;
                second = 0;
                microsecond = 0;
            } else {
                copy(MIN_DATE);
            }
            return;
        }
        if (type.getPrimitiveType() == PrimitiveType.DATETIME) {
            if (len >= 4) {
                year = (int) data.getChar();
                month = (int) data.get();
                day = (int) data.get();
                microsecond = 0;
                if (len > 4) {
                    hour = (int) data.get();
                    minute = (int) data.get();
                    second = (int) data.get();
                } else {
                    hour = 0;
                    minute = 0;
                    second = 0;
                    microsecond = 0;
                }
                if (len > 7) {
                    microsecond = data.getInt();
                    // choose highest scale to keep microsecond value
                    type = ScalarType.createDatetimeV2Type(6);
                }
            } else {
                copy(MIN_DATETIME);
            }
            return;
        }
    }

    private static boolean haveTimeZoneOffset(String arg) {
        Preconditions.checkArgument(arg.length() > 6);
        return HAS_OFFSET_PART.matcher(arg.substring(arg.length() - 6)).matches();
    }

    private static boolean haveTimeZoneName(String arg) {
        for (char ch : arg.toCharArray()) {
            if (Character.isUpperCase(ch) && ch != 'T') {
                return true;
            }
        }
        return false;
    }

    private static int getTimeZoneSplitPos(String arg) {
        int split = arg.length() - 1;
        for (; !Character.isAlphabetic(arg.charAt(split)); split--) {
        } // skip +8 of UTC+8
        for (; split >= 0 && (Character.isUpperCase(arg.charAt(split)) || arg.charAt(split) == '/'); split--) {
        }
        return split + 1;
    }
}
