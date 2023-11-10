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

package org.apache.doris.common.util;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO(dhc) add nanosecond timer for coordinator's root profile
public class TimeUtils {
    public static final String UTC_TIME_ZONE = "Europe/London"; // This is just a Country to represent UTC offset +00:00
    public static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";
    public static final ZoneId TIME_ZONE;
    public static final ImmutableMap<String, String> timeZoneAliasMap;
    // NOTICE: Date formats are not synchronized.
    // it must be used as synchronized externally.
    public static final DateTimeFormatter DATE_FORMAT;
    public static final DateTimeFormatter DATETIME_FORMAT;
    public static final DateTimeFormatter TIME_FORMAT;
    public static final Pattern DATETIME_FORMAT_REG =
            Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|("
                    + "\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))"
                    + "[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))"
                    + "(\\s(((0?[0-9])|([1][0-9])|([2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
    public static final DateTimeFormatter DATETIME_MS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());
    public static final DateTimeFormatter DATETIME_NS_FORMAT = DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
            .withZone(ZoneId.systemDefault());
    public static final DateTimeFormatter DATETIME_FORMAT_WITH_HYPHEN = DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd-HH-mm-ss")
            .withZone(ZoneId.systemDefault());
    private static final Logger LOG = LogManager.getLogger(TimeUtils.class);
    private static final Pattern TIMEZONE_OFFSET_FORMAT_REG = Pattern.compile("^[+-]?\\d{1,2}:\\d{2}$");
    public static Date MIN_DATE = null;
    public static Date MAX_DATE = null;
    public static Date MIN_DATETIME = null;
    public static Date MAX_DATETIME = null;

    static {
        TIME_ZONE = ZoneId.of("UTC+8");

        Map<String, String> timeZoneMap = Maps.newHashMap();
        timeZoneMap.putAll(ZoneId.SHORT_IDS);

        // set CST to +08:00 instead of America/Chicago
        timeZoneMap.put("CST", DEFAULT_TIME_ZONE);
        timeZoneMap.put("PRC", DEFAULT_TIME_ZONE);
        timeZoneMap.put("UTC", UTC_TIME_ZONE);
        timeZoneMap.put("GMT", UTC_TIME_ZONE);

        timeZoneAliasMap = ImmutableMap.copyOf(timeZoneMap);

        DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DATE_FORMAT.withZone(TIME_ZONE);

        DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DATETIME_FORMAT.withZone(TIME_ZONE);

        TIME_FORMAT = DateTimeFormatter.ofPattern("HH");
        TIME_FORMAT.withZone(TIME_ZONE);

        try {

            MIN_DATE = Date.from(
                    LocalDate.parse("0001-01-01", DATE_FORMAT).atStartOfDay().atZone(TIME_ZONE).toInstant());
            MAX_DATE = Date.from(
                    LocalDate.parse("9999-12-31", DATE_FORMAT).atStartOfDay().atZone(TIME_ZONE).toInstant());

            MIN_DATETIME = Date.from(
                    LocalDateTime.parse("0001-01-01 00:00:00", DATETIME_FORMAT).atZone(TIME_ZONE).toInstant());
            MAX_DATETIME = Date.from(
                    LocalDateTime.parse("9999-12-31 23:59:59", DATETIME_FORMAT).atZone(TIME_ZONE).toInstant());

        } catch (DateTimeParseException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }

    public static long getStartTimeMs() {
        return System.currentTimeMillis();
    }

    public static long getElapsedTimeMs(long startTime) {
        return System.currentTimeMillis() - startTime;
    }

    public static String getCurrentFormatTime() {
        return LocalDateTime.now().format(DATETIME_FORMAT);
    }

    public static TimeZone getTimeZone() {
        String timezone;
        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        } else {
            timezone = VariableMgr.getDefaultSessionVariable().getTimeZone();
        }
        return TimeZone.getTimeZone(ZoneId.of(timezone, timeZoneAliasMap));
    }

    // return the time zone of current system
    public static TimeZone getSystemTimeZone() {
        return TimeZone.getTimeZone(ZoneId.of(ZoneId.systemDefault().getId(), timeZoneAliasMap));
    }

    // get time zone of given zone name, or return system time zone if name is null.
    public static TimeZone getOrSystemTimeZone(String timeZone) {
        if (timeZone == null) {
            return getSystemTimeZone();
        }
        return TimeZone.getTimeZone(ZoneId.of(timeZone, timeZoneAliasMap));
    }

    public static String longToTimeString(long timeStamp, DateTimeFormatter dateFormat) {
        if (timeStamp <= 0L) {
            return FeConstants.null_string;
        }
        return dateFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneId.systemDefault()));
    }

    public static String longToTimeStringWithFormat(long timeStamp, DateTimeFormatter datetimeFormatTimeZone) {
        TimeZone timeZone = getTimeZone();
        datetimeFormatTimeZone.withZone(timeZone.toZoneId());
        return longToTimeString(timeStamp, datetimeFormatTimeZone);
    }

    public static String longToTimeString(long timeStamp) {
        return longToTimeStringWithFormat(timeStamp, DATETIME_FORMAT);
    }

    public static String longToTimeStringWithms(long timeStamp) {
        return longToTimeStringWithFormat(timeStamp, DATETIME_MS_FORMAT);
    }

    public static Date getHourAsDate(String hour) {
        String fullHour = hour;
        if (fullHour.length() == 1) {
            fullHour = "0" + fullHour;
        }
        try {
            return Date.from(
                    LocalTime.parse(fullHour, TIME_FORMAT).atDate(LocalDate.now()).atZone(TIME_ZONE).toInstant());
        } catch (DateTimeParseException e) {
            LOG.warn("invalid time format: {}", fullHour);
            return null;
        }
    }

    public static Date parseDate(String dateStr, PrimitiveType type) throws AnalysisException {
        Date date = null;
        Matcher matcher = DATETIME_FORMAT_REG.matcher(dateStr);
        if (!matcher.matches()) {
            throw new AnalysisException("Invalid date string: " + dateStr);
        }
        dateStr = formatDateStr(dateStr);
        if (type == PrimitiveType.DATE) {
            ParsePosition pos = new ParsePosition(0);
            date = Date.from(
                    LocalDate.from(DATE_FORMAT.parse(dateStr, pos)).atStartOfDay().atZone(TIME_ZONE).toInstant());
            if (pos.getIndex() != dateStr.length() || date == null) {
                throw new AnalysisException("Invalid date string: " + dateStr);
            }
        } else if (type == PrimitiveType.DATETIME) {
            try {
                date = Date.from(LocalDateTime.parse(dateStr, DATETIME_FORMAT).atZone(TIME_ZONE).toInstant());
            } catch (DateTimeParseException e) {
                throw new AnalysisException("Invalid date string: " + dateStr);
            }
        } else {
            Preconditions.checkState(false, "error type: " + type);
        }

        return date;
    }

    public static Date parseDate(String dateStr, Type type) throws AnalysisException {
        return parseDate(dateStr, type.getPrimitiveType());
    }

    public static String format(Date date, PrimitiveType type) {
        if (type == PrimitiveType.DATE) {
            return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()).format(DATE_FORMAT);
        } else if (type == PrimitiveType.DATETIME) {
            return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()).format(DATETIME_FORMAT);
        } else {
            return "INVALID";
        }
    }

    public static String format(Date date, Type type) {
        return format(date, type.getPrimitiveType());
    }

    public static long timeStringToLong(String timeStr) {
        Date d;
        try {
            d = Date.from(LocalDateTime.parse(timeStr, DATETIME_FORMAT).atZone(TIME_ZONE).toInstant());
        } catch (DateTimeParseException e) {
            return -1;
        }
        return d.getTime();
    }

    public static long timeStringToLong(String timeStr, TimeZone timeZone) {
        DateTimeFormatter dateFormatTimeZone = DATETIME_FORMAT;
        dateFormatTimeZone.withZone(timeZone.toZoneId());
        LocalDateTime d;
        try {
            d = LocalDateTime.parse(timeStr, dateFormatTimeZone);
        } catch (DateTimeParseException e) {
            return -1;
        }
        return d.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
    }

    // Check if the time zone_value is valid
    public static String checkTimeZoneValidAndStandardize(String value) throws DdlException {
        Function<String, String> standardizeValue = s -> {
            boolean positive = s.charAt(0) != '-';
            String[] parts = s.replaceAll("[+-]", "").split(":");
            return (positive ? "+" : "-") + String.format("%02d:%02d",
                    Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        };

        try {
            if (value == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, "null");
            }
            // match offset type, such as +08:00, -07:00
            Matcher matcher = TIMEZONE_OFFSET_FORMAT_REG.matcher(value);
            // it supports offset and region timezone type, and short zone ids
            boolean match = matcher.matches();
            if (!value.contains("/") && !timeZoneAliasMap.containsKey(value) && !match) {
                if ((value.startsWith("GMT") || value.startsWith("UTC"))
                        && TIMEZONE_OFFSET_FORMAT_REG.matcher(value.substring(3)).matches()) {
                    value = value.substring(0, 3) + standardizeValue.apply(value.substring(3));
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
                }
            }
            if (match) {
                value = standardizeValue.apply(value);

                // timezone offsets around the world extended from -12:00 to +14:00
                int tz = Integer.parseInt(value.substring(1, 3)) * 100 + Integer.parseInt(value.substring(4, 6));
                if (value.charAt(0) == '-' && tz > 1200) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
                } else if (value.charAt(0) == '+' && tz > 1400) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
                }
            }
            ZoneId.of(value, timeZoneAliasMap);
            return value;
        } catch (DateTimeException ex) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
        }
        throw new DdlException("Parse time zone " + value + " error");
    }

    // format string DateTime  And Full Zero for hour,minute,second
    public static LocalDateTime formatDateTimeAndFullZero(String datetime, DateTimeFormatter formatter) {
        TemporalAccessor temporal = formatter.parse(datetime);
        int year = temporal.isSupported(ChronoField.YEAR)
                ? temporal.get(ChronoField.YEAR) : 0;
        int month = temporal.isSupported(ChronoField.MONTH_OF_YEAR)
                ? temporal.get(ChronoField.MONTH_OF_YEAR) : 1;
        int day = temporal.isSupported(ChronoField.DAY_OF_MONTH)
                ? temporal.get(ChronoField.DAY_OF_MONTH) : 1;
        int hour = temporal.isSupported(ChronoField.HOUR_OF_DAY)
                ? temporal.get(ChronoField.HOUR_OF_DAY) : 0;
        int minute = temporal.isSupported(ChronoField.MINUTE_OF_HOUR)
                ? temporal.get(ChronoField.MINUTE_OF_HOUR) : 0;
        int second = temporal.isSupported(ChronoField.SECOND_OF_MINUTE)
                ? temporal.get(ChronoField.SECOND_OF_MINUTE) : 0;
        int milliSecond = temporal.isSupported(ChronoField.MILLI_OF_SECOND)
                ? temporal.get(ChronoField.MILLI_OF_SECOND) : 0;
        return LocalDateTime.of(LocalDate.of(year, month, day),
                LocalTime.of(hour, minute, second, milliSecond * 1000000));
    }

    private static String formatDateStr(String dateStr) {
        String[] parts = dateStr.trim().split("[ :-]+");
        return String.format("%s-%02d-%02d%s", parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                parts.length > 3 ? String.format(" %02d:%02d:%02d", Integer.parseInt(parts[3]),
                        Integer.parseInt(parts[4]), Integer.parseInt(parts[5])) : "");
    }

}
