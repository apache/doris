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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO(dhc) add nanosecond timer for coordinator's root profile
public class TimeUtils {
    private static final Logger LOG = LogManager.getLogger(TimeUtils.class);

    public static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";

    private static final TimeZone TIME_ZONE;

    // set CST to +08:00 instead of America/Chicago
    public static final ImmutableMap<String, String> timeZoneAliasMap = ImmutableMap.of(
            "CST", DEFAULT_TIME_ZONE, "PRC", DEFAULT_TIME_ZONE);

    // NOTICE: Date formats are not synchronized.
    // it must be used as synchronized externally.
    private static final SimpleDateFormat DATE_FORMAT;
    private static final SimpleDateFormat DATETIME_FORMAT;
    private static final SimpleDateFormat TIME_FORMAT;
    
    private static final Pattern DATETIME_FORMAT_REG =
            Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|("
                    + "\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))"
                    + "[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))"
                    + "(\\s(((0?[0-9])|([1][0-9])|([2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
    
    private static final Pattern TIMEZONE_OFFSET_FORMAT_REG = Pattern.compile("^[+-]{0,1}\\d{1,2}\\:\\d{2}$");

    public static Date MIN_DATE = null;
    public static Date MAX_DATE = null;
    
    public static Date MIN_DATETIME = null;
    public static Date MAX_DATETIME = null;

    static {
        TIME_ZONE = new SimpleTimeZone(8 * 3600 * 1000, "");
        
        DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
        DATE_FORMAT.setTimeZone(TIME_ZONE);

        DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DATETIME_FORMAT.setTimeZone(TIME_ZONE);

        TIME_FORMAT = new SimpleDateFormat("HH");
        TIME_FORMAT.setTimeZone(TIME_ZONE);

        try {
            MIN_DATE = DATE_FORMAT.parse("0000-01-01");
            MAX_DATE = DATE_FORMAT.parse("9999-12-31");

            MIN_DATETIME = DATETIME_FORMAT.parse("0000-01-01 00:00:00");
            MAX_DATETIME = DATETIME_FORMAT.parse("9999-12-31 23:59:59");

        } catch (ParseException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }
    
    public static long getStartTime() {
        return System.nanoTime();
    }
    
    public static long getEstimatedTime(long startTime) {
        return System.nanoTime() - startTime;
    }
    
    public static synchronized String getCurrentFormatTime() {
        return DATETIME_FORMAT.format(new Date());
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
    
    public static String longToTimeString(long timeStamp, SimpleDateFormat dateFormat) {
        if (timeStamp <= 0L) {
            return FeConstants.null_string;
        }
        return dateFormat.format(new Date(timeStamp));
    }

    public static synchronized String longToTimeString(long timeStamp) {
        TimeZone timeZone = getTimeZone();
        SimpleDateFormat dateFormatTimeZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatTimeZone.setTimeZone(timeZone);
        return longToTimeString(timeStamp, dateFormatTimeZone);
    }
    
    public static synchronized Date getTimeAsDate(String timeString) {
        try {
            Date date = TIME_FORMAT.parse(timeString);
            return date;
        } catch (ParseException e) {
            LOG.warn("invalid time format: {}", timeString);
            return null;
        }
    }

    public static synchronized Date parseDate(String dateStr, PrimitiveType type) throws AnalysisException {
        Date date = null;
        Matcher matcher = DATETIME_FORMAT_REG.matcher(dateStr);
        if (!matcher.matches()) {
            throw new AnalysisException("Invalid date string: " + dateStr);
        }
        if (type == PrimitiveType.DATE) {
            ParsePosition pos = new ParsePosition(0);
            date = DATE_FORMAT.parse(dateStr, pos);
            if (pos.getIndex() != dateStr.length() || date == null) {
                throw new AnalysisException("Invalid date string: " + dateStr);
            }
        } else if (type == PrimitiveType.DATETIME) {
            try {
                date = DATETIME_FORMAT.parse(dateStr);
            } catch (ParseException e) {
                throw new AnalysisException("Invalid date string: " + dateStr);
            }
        } else {
            Preconditions.checkState(false, "error type: " + type);
        }

        return date;
    }

    public static synchronized Date parseDate(String dateStr, Type type) throws AnalysisException {
        return parseDate(dateStr, type.getPrimitiveType());
    }

    public static synchronized String format(Date date, PrimitiveType type) {
        if (type == PrimitiveType.DATE) {
            return DATE_FORMAT.format(date);
        } else if (type == PrimitiveType.DATETIME) {
            return DATETIME_FORMAT.format(date);
        } else {
            return "INVALID";
        }
    }

    public static synchronized String format(Date date, Type type) {
        return format(date, type.getPrimitiveType());
    }

    /*
     * only used for ETL
     */
    public static long dateTransform(long time, PrimitiveType type) {
        Calendar cal = Calendar.getInstance(TIME_ZONE);
        cal.setTimeInMillis(time);

        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);

        if (type == PrimitiveType.DATE) {
            return year * 16 * 32L + month * 32 + day;
        } else if (type == PrimitiveType.DATETIME) {
            // datetime
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            int minute = cal.get(Calendar.MINUTE);
            int second = cal.get(Calendar.SECOND);
            return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
        } else {
            Preconditions.checkState(false, "invalid date type: " + type);
            return -1L;
        }
    }

    public static long dateTransform(long time, Type type) {
        return dateTransform(time, type.getPrimitiveType());
    }

    public static long timeStringToLong(String timeStr) {
        Date d;
        try {
            d = DATETIME_FORMAT.parse(timeStr);
        } catch (ParseException e) {
            return -1;
        }
        return d.getTime();
    }

    // Check if the time zone_value is valid
    public static String checkTimeZoneValidAndStandardize(String value) throws DdlException {
        try {
            if (value == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, "null");
            }
            // match offset type, such as +08:00, -07:00
            Matcher matcher = TIMEZONE_OFFSET_FORMAT_REG.matcher(value);
            // it supports offset and region timezone type, "CST" use here is compatibility purposes.
            boolean match = matcher.matches();
            if (!value.contains("/") && !value.equals("CST") && !match) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
            }
            if (match) {
                boolean postive = value.charAt(0) != '-';
                value = (postive ? "+" : "-") + String.format("%02d:%02d",
                        Integer.parseInt(value.replaceAll("[+-]", "").split(":")[0]),
                        Integer.parseInt(value.replaceAll("[+-]", "").split(":")[1]));

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
}
