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
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDateLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DateLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);

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
    private static final int[] DAYS_IN_MONTH = new int[] {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    private static final int ALLOW_SPACE_MASK = 4 | 64;
    private static final int MAX_DATE_PARTS = 8;
    private static final int YY_PART_YEAR = 70;

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

    private void init(String s, Type type) throws AnalysisException {
        try {
            Preconditions.checkArgument(type.isDateType());
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
                String[] timePart = s.contains(" ") ? s.split(" ")[1].split(":") : new String[] {};
                if (timePart.length > 0 && (type.equals(Type.DATE) || type.equals(Type.DATEV2))) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
                if (timePart.length == 0 && (type.equals(Type.DATETIME) || type.equals(Type.DATETIMEV2))) {
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
            if (microsecond != 0 && type.isDatetime()) {
                int dotIndex = s.lastIndexOf(".");
                int scale = s.length() - dotIndex - 1;
                type = ScalarType.createDatetimeV2Type(scale);
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
                return this.getStringValue().compareTo(MIN_DATE.getStringValue()) == 0;
            case DATETIME:
                return this.getStringValue().compareTo(MIN_DATETIME.getStringValue()) == 0;
            case DATETIMEV2:
                return this.getStringValue().compareTo(MIN_DATETIMEV2.getStringValue()) == 0;
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
        if (expr instanceof NullLiteral) {
            return 1;
        }

        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        // date time will not overflow when doing addition and subtraction
        return getStringValue().compareTo(expr.getStringValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String getStringValue() {
        if (type.isDate() || type.isDateV2()) {
            return String.format("%04d-%02d-%02d", year, month, day);
        } else if (type.isDatetimeV2()) {
            int scale = ((ScalarType) type).getScalarScale();
            long ms = Double.valueOf(microsecond / (int) (Math.pow(10, 6 - ((ScalarType) type).getScalarScale()))
                    * (Math.pow(10, 6 - ((ScalarType) type).getScalarScale()))).longValue();
            String tmp = String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year, month, day, hour, minute, second);
            if (ms == 0) {
                return tmp;
            }
            return tmp + String.format(".%06d", ms).substring(0, scale + 1);
        } else {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        }
    }

    @Override
    public String getStringValueForArray() {
        return "\"" + getStringValue() + "\"";
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
            if (ConnectContext.get() != null) {
                ConnectContext.get().getState().reset();
            }
            // If date value is invalid, set this to null
            msg.node_type = TExprNodeType.NULL_LITERAL;
            msg.setIsNullable(true);
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
            if (Type.isImplicitlyCastable(this.type, targetType, true)) {
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

    private long makePackedDatetime() {
        long ymd = ((year * 13 + month) << 5) | day;
        long hms = (hour << 12) | (minute << 6) | second;
        long packedDatetime = ((ymd << 17) | hms) << 24 + microsecond;
        return packedDatetime;
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

    private long makePackedDatetimeV2() {
        return (year << 46) | (month << 42) | (day << 37) | (hour << 32)
                | (minute << 26) | (second << 20) | (microsecond % (1 << 20));
    }

    private long makePackedDateV2() {
        return ((year << 9) | (month << 5) | day);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        // set flag bit in meta, 0 is DATETIME and 1 is DATE
        if (this.type.equals(Type.DATETIME)) {
            out.writeShort(DateLiteralType.DATETIME.value());
            out.writeLong(makePackedDatetime());
        } else if (this.type.equals(Type.DATE)) {
            out.writeShort(DateLiteralType.DATE.value());
            out.writeLong(makePackedDatetime());
        } else if (this.type.getPrimitiveType() == PrimitiveType.DATETIMEV2) {
            out.writeShort(DateLiteralType.DATETIMEV2.value());
            out.writeLong(makePackedDatetimeV2());
            out.writeInt(((ScalarType) this.type).getScalarScale());
        } else if (this.type.equals(Type.DATEV2)) {
            out.writeShort(DateLiteralType.DATEV2.value());
            out.writeLong(makePackedDateV2());
        } else {
            throw new IOException("Error date literal type : " + type);
        }
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
        ZonedDateTime zonedDateTime = ZonedDateTime.of((int) year, (int) month, (int) day, (int) hour,
                (int) minute, (int) second, (int) microsecond, ZoneId.of(timeZone.getID()));
        Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
        return timestamp.getTime();
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
                        builder.appendValue(ChronoField.ALIGNED_WEEK_OF_YEAR, 2);
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
        TemporalAccessor accessor;
        if (type.equals(Type.DATE) || type.equals(Type.DATEV2)) {
            accessor = DATE_FORMATTER.parse(getStringValue());
        } else if (type.isDatetimeV2()) {
            accessor = DATE_TIME_FORMATTER_TO_MICRO_SECOND.parse(getStringValue());
        } else {
            accessor = DATE_TIME_FORMATTER.parse(getStringValue());
        }
        final int year = accessor.get(ChronoField.YEAR);
        final int month = accessor.get(ChronoField.MONTH_OF_YEAR);
        final int dayOfMonth = accessor.get(ChronoField.DAY_OF_MONTH);
        final int hour = getOrDefault(accessor, ChronoField.HOUR_OF_DAY, 0);
        final int minute = getOrDefault(accessor, ChronoField.MINUTE_OF_HOUR, 0);
        final int second = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE, 0);
        final int microSeconds = getOrDefault(accessor, ChronoField.MICRO_OF_SECOND, 0);

        // LocalDateTime of(int year, int month, int dayOfMonth, int hour, int minute,
        // int second, int nanoOfSecond)
        return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, microSeconds * 1000);
    }

    public DateLiteral plusYears(int year) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusYears(year), type);
    }

    public DateLiteral plusMonths(int month) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusMonths(month), type);
    }

    public DateLiteral plusDays(int day) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusDays(day), type);
    }

    public DateLiteral plusHours(int hour) throws AnalysisException {
        if (type.isDate()) {
            return new DateLiteral(getTimeFormatter().plusHours(hour), Type.DATETIME);
        }
        if (type.isDateV2()) {
            return new DateLiteral(getTimeFormatter().plusHours(hour), Type.DATETIMEV2);
        }
        return new DateLiteral(getTimeFormatter().plusHours(hour), type);
    }

    public DateLiteral plusMinutes(int minute) {
        if (type.isDate()) {
            return new DateLiteral(getTimeFormatter().plusMinutes(minute), Type.DATETIME);
        }
        if (type.isDateV2()) {
            return new DateLiteral(getTimeFormatter().plusMinutes(minute), Type.DATETIMEV2);
        }
        return new DateLiteral(getTimeFormatter().plusMinutes(minute), type);
    }

    public DateLiteral plusSeconds(int second) {
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

    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;
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
        int fp = 0; // pointer to the current format string
        int fend = format.length(); // end of format string
        int vp = 0; // pointer to the date string value
        int vend = value.length(); // end of date string value

        boolean datePartUsed = false;
        boolean timePartUsed = false;
        boolean microSecondPartUsed = false;

        int dayPart = 0;
        long weekday = -1;
        long yearday = -1;
        long weekNum = -1;

        boolean strictWeekNumber = false;
        boolean sundayFirst = false;
        boolean strictWeekNumberYearType = false;
        long strictWeekNumberYear = -1;
        boolean usaTime = false;

        char f;
        while (fp < fend && vp < vend) {
            // Skip space character
            while (vp < vend && Character.isSpaceChar(value.charAt(vp))) {
                vp++;
            }
            if (vp >= vend) {
                break;
            }

            // Check switch
            f = format.charAt(fp);
            if (f == '%' && fp + 1 < fend) {
                int tmp = 0;
                long intValue = 0;
                fp++;
                f = format.charAt(fp);
                fp++;
                switch (f) {
                    // Year
                    case 'y':
                        // Year, numeric (two digits)
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        intValue += intValue >= 70 ? 1900 : 2000;
                        this.year = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'Y':
                        // Year, numeric, four digits
                        tmp = vp + Math.min(4, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        if (tmp - vp <= 2) {
                            intValue += intValue >= 70 ? 1900 : 2000;
                        }
                        this.year = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    // Month
                    case 'm':
                    case 'c':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.month = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'M': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(MONTH_NAME_DICT, value.substring(vp, nextPos));
                        this.month = intValue;
                        vp = nextPos;
                        break;
                    }
                    case 'b': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(MONTH_ABBR_NAME_DICT, value.substring(vp, nextPos));
                        this.month = intValue;
                        vp = nextPos;
                        break;
                    }
                    // Day
                    case 'd':
                    case 'e':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.day = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'D':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.day = intValue;
                        vp = tmp + Math.min(2, vend - tmp);
                        datePartUsed = true;
                        break;
                    // Hour
                    case 'h':
                    case 'I':
                    case 'l':
                        usaTime = true;
                    case 'k': // CHECKSTYLE IGNORE THIS LINE: Fall through
                    case 'H':
                        tmp = findNumber(value, vp, 2);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.hour = intValue;
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    // Minute
                    case 'i':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.minute = intValue;
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    // Second
                    case 's':
                    case 'S':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.second = intValue;
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    // Micro second
                    case 'f':
                        // FIXME: fix same with BE
                        tmp = vp;
                        // when there's still something to the end, fix the scale of ms.
                        while (tmp < vend && Character.isDigit(value.charAt(tmp))) {
                            tmp += 1;
                        }

                        if (tmp - vp > 6) {
                            int tmp2 = vp + 6;
                            intValue = strToLong(value.substring(vp, tmp2));
                        } else {
                            intValue = strToLong(value.substring(vp, tmp));
                        }
                        this.microsecond = (long) (intValue * Math.pow(10, 6 - Math.min(6, tmp - vp)));
                        timePartUsed = true;
                        microSecondPartUsed = true;
                        vp = tmp;
                        break;
                    // AM/PM
                    case 'p':
                        if ((vend - vp) < 2 || Character.toUpperCase(value.charAt(vp + 1)) != 'M' || !usaTime) {
                            throw new InvalidFormatException("Invalid %p format");
                        }
                        if (Character.toUpperCase(value.charAt(vp)) == 'P') {
                            // PM
                            dayPart = 12;
                        }
                        timePartUsed = true;
                        vp += 2;
                        break;
                    // Weekday
                    case 'W': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(WEEK_DAY_NAME_DICT, value.substring(vp, nextPos));
                        intValue++;
                        weekday = intValue;
                        datePartUsed = true;
                        break;
                    }
                    case 'a': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(WEEK_DAY_NAME_DICT, value.substring(vp, nextPos));
                        intValue++;
                        weekday = intValue;
                        datePartUsed = true;
                        break;
                    }
                    case 'w':
                        tmp = vp + Math.min(1, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        if (intValue >= 7) {
                            throw new InvalidFormatException("invalid day of week: " + intValue);
                        }
                        if (intValue == 0) {
                            intValue = 7;
                        }
                        weekday = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'j':
                        tmp = vp + Math.min(3, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        yearday = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'u':
                    case 'v':
                    case 'U':
                    case 'V':
                        sundayFirst = (format.charAt(fp - 1) == 'U' || format.charAt(fp - 1) == 'V');
                        // Used to check if there is %x or %X
                        strictWeekNumber = (format.charAt(fp - 1) == 'V' || format.charAt(fp - 1) == 'v');
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = Long.valueOf(value.substring(vp, tmp));
                        weekNum = intValue;
                        if (weekNum > 53 || (strictWeekNumber && weekNum == 0)) {
                            throw new InvalidFormatException("invalid num of week: " + weekNum);
                        }
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    // strict week number, must be used with %V or %v
                    case 'x':
                    case 'X':
                        strictWeekNumberYearType = (format.charAt(fp - 1) == 'X');
                        tmp = vp + Math.min(4, vend - vp);
                        intValue = Long.valueOf(value.substring(vp, tmp));
                        strictWeekNumberYear = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'r':
                        tmp = fromDateFormatStr("%I:%i:%S %p", value.substring(vp, vend), true);
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    case 'T':
                        tmp = fromDateFormatStr("%H:%i:%S", value.substring(vp, vend), true);
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    case '.':
                        while (vp < vend && Character.toString(value.charAt(vp)).matches("\\p{Punct}")) {
                            vp++;
                        }
                        break;
                    case '@':
                        while (vp < vend && Character.isLetter(value.charAt(vp))) {
                            vp++;
                        }
                        break;
                    case '#':
                        while (vp < vend && Character.isDigit(value.charAt(vp))) {
                            vp++;
                        }
                        break;
                    case '%': // %%, escape the %
                        if ('%' != value.charAt(vp)) {
                            throw new InvalidFormatException("invalid char after %: " + value.charAt(vp));
                        }
                        vp++;
                        break;
                    default:
                        throw new InvalidFormatException("Invalid format pattern: " + f);
                }
            } else if (format.charAt(fp) != ' ') {
                if (format.charAt(fp) != value.charAt(vp)) {
                    throw new InvalidFormatException("Invalid char: " + value.charAt(vp) + ", expected: "
                            + format.charAt(fp));
                }
                fp++;
                vp++;
            } else {
                fp++;
            }
        }

        // continue to iterate pattern if has
        // to find out if it has time part.
        while (fp < fend) {
            f = format.charAt(fp);
            if (f == '%' && fp + 1 < fend) {
                fp++;
                f = format.charAt(fp);
                fp++;
                switch (f) {
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
                        timePartUsed = true;
                        break;
                    default:
                        break;
                }
            } else {
                fp++;
            }
        }

        if (usaTime) {
            if (this.hour > 12 || this.hour < 1) {
                throw new InvalidFormatException("Invalid hour: " + hour);
            }
            this.hour = (this.hour % 12) + dayPart;
        }

        if (hasSubVal) {
            return vp;
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

        // Compute timestamp type
        // TODO(Gabriel): we still use old version datetime/date and change this to new version when
        //  we think it's stable enough
        if (datePartUsed) {
            if (microSecondPartUsed) {
                this.type = Type.DATETIMEV2_WITH_MAX_SCALAR;
            } else if (timePartUsed) {
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

    // The interval format is that with no delimiters
    // YYYY-MM-DD HH-MM-DD.FFFFFF AM in default format, and now doris will skip part 7
    // 0    1  2  3  4  5  6      7
    public void fromDateStr(String dateStr) throws AnalysisException {
        dateStr = dateStr.trim();
        if (dateStr.isEmpty()) {
            throw new AnalysisException("parse datetime value failed: " + dateStr);
        }
        int[] dateVal = new int[MAX_DATE_PARTS];
        int[] dateLen = new int[MAX_DATE_PARTS];

        // Fix year length
        int pre = 0;
        int pos = 0;
        while (pos < dateStr.length() && (Character.isDigit(dateStr.charAt(pos)) || dateStr.charAt(pos) == 'T')) {
            pos++;
        }
        int yearLen = 4;
        int digits = pos - pre;
        boolean isIntervalFormat = false;
        // For YYYYMMDD/YYYYMMDDHHMMSS is 4 digits years
        if (pos == dateStr.length() || dateStr.charAt(pos) == '.') {
            if (digits == 4 || digits == 8 || digits >= 14) {
                yearLen = 4;
            } else {
                yearLen = 2;
            }
            isIntervalFormat = true;
        }

        int fieldIdx = 0;
        int fieldLen = yearLen;
        while (pre < dateStr.length() && Character.isDigit(dateStr.charAt(pre)) && fieldIdx < MAX_DATE_PARTS - 1) {
            int start = pre;
            int tempVal = 0;
            boolean scanToDelim = (!isIntervalFormat) && (fieldIdx != 6);
            while (pre < dateStr.length() && Character.isDigit(dateStr.charAt(pre))
                    && (scanToDelim || fieldLen-- != 0)) {
                tempVal = tempVal * 10 + (dateStr.charAt(pre++) - '0');
            }
            dateVal[fieldIdx] = tempVal;
            dateLen[fieldIdx] = pre - start;
            fieldLen = 2;

            if (pre == dateStr.length()) {
                fieldIdx++;
                break;
            }

            if (fieldIdx == 2 && dateStr.charAt(pre) == 'T') {
                // YYYYMMDDTHHMMDD, skip 'T' and continue
                pre++;
                fieldIdx++;
                continue;
            }

            // Second part
            if (fieldIdx == 5) {
                if (dateStr.charAt(pre) == '.') {
                    pre++;
                    fieldLen = 6;
                } else if (Character.isDigit(dateStr.charAt(pre))) {
                    fieldIdx++;
                    break;
                }
                fieldIdx++;
                continue;
            }
            // escape separator
            while (pre < dateStr.length() && ((Character.toString(dateStr.charAt(pre)).matches("\\p{Punct}"))
                    || Character.isSpaceChar(dateStr.charAt(pre)))) {
                if (Character.isSpaceChar(dateStr.charAt(pre))) {
                    if (((1 << fieldIdx) & ALLOW_SPACE_MASK) == 0) {
                        throw new AnalysisException("parse datetime value failed: " + dateStr);
                    }
                }
                pre++;
            }
            fieldIdx++;
        }
        int numField = fieldIdx;
        if (!isIntervalFormat) {
            yearLen = dateLen[0];
        }
        for (; fieldIdx < MAX_DATE_PARTS; ++fieldIdx) {
            dateLen[fieldIdx] = 0;
            dateVal[fieldIdx] = 0;
        }
        if (yearLen == 2) {
            if (dateVal[0] < YY_PART_YEAR) {
                dateVal[0] += 2000;
            } else {
                dateVal[0] += 1900;
            }
        }

        if (numField < 3) {
            throw new AnalysisException("parse datetime value failed: " + dateStr);
        }

        year = dateVal[0];
        month = dateVal[1];
        day = dateVal[2];
        hour = dateVal[3];
        minute = dateVal[4];
        second = dateVal[5];
        microsecond = dateVal[6];

        if (numField == 3) {
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

        if (checkRange() || checkDate()) {
            throw new AnalysisException("Datetime value is out of range: " + dateStr);
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
    public void setupParamFromBinary(ByteBuffer data) {
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
