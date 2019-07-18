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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimeLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * compute functions in FE.
 *
 * when you add a new function, please ensure the name, argTypes , returnType and compute logic are consistent with BE's function
 */
public class FEFunctions {
    private static final Logger LOG = LogManager.getLogger(FEFunctions.class);
    /**
     * date and time function
     */
    @FEFunction(name = "timediff", argTypes = { "DATETIME", "DATETIME" }, returnType = "TIME")
    public static TimeLiteral timeDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long timediff = getTime(first) - getTime(second);
        if(timediff < TimeUtils.MIN_TIME) {
            timediff = TimeUtils.MIN_TIME;
        } else if(timediff > TimeUtils.MAX_TIME) {
            timediff = TimeUtils.MAX_TIME;
        }
        return new TimeLiteral((int) timediff);
    }

    @FEFunction(name = "datediff", argTypes = { "DATETIME", "DATETIME" }, returnType = "INT")
    public static IntLiteral dateDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            // DATEDIFF function only uses the date part for calculations and ignores the time part
            long diff = sdf.parse(first.getStringValue()).getTime() - sdf.parse(second.getStringValue()).getTime();
            long datediff = diff / 1000 / 60 / 60 / 24;
            return new IntLiteral(datediff, Type.INT);
        } catch (ParseException e) {
            throw new AnalysisException(e.getLocalizedMessage());
        }
    }

    @FEFunction(name = "date_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral dateAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        Date d = new Date(getTime(date));
        d = DateUtils.addDays(d, (int) day.getLongValue());
        return new DateLiteral(DateFormatUtils.format(d, "yyyy-MM-dd HH:mm:ss"), Type.DATETIME);
    }

    @FEFunction(name = "adddate", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral addDate(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return dateAdd(date, day);
    }

    @FEFunction(name = "days_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral daysAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return dateAdd(date, day);
    }

    @FEFunction(name = "date_format", argTypes = { "DATETIME", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral dateFormat(LiteralExpr date, StringLiteral fmtLiteral) throws AnalysisException {
        String result = dateFormat(new Date(getTime(date)), fmtLiteral.getStringValue());
        return new StringLiteral(result);
    }

    @FEFunction(name = "str_to_date", argTypes = { "VARCHAR", "VARCHAR" }, returnType = "DATETIME")
    public static DateLiteral dateParse(StringLiteral date, StringLiteral fmtLiteral) throws AnalysisException {
        boolean hasTimePart = false;
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

        String formatString = fmtLiteral.getStringValue();
        boolean escaped = false;
        for (int i = 0; i < formatString.length(); i++) {
            char character = formatString.charAt(i);

            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendDayOfMonth(1);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendHourOfDay(2);
                        hasTimePart = true;
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendClockhourOfHalfday(2);
                        hasTimePart = true;
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendMinuteOfHour(2);
                        hasTimePart = true;
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendHourOfDay(1);
                        hasTimePart = true;
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendClockhourOfHalfday(1);
                        hasTimePart = true;
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        hasTimePart = true;
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendSecondOfMinute(2);
                        hasTimePart = true;
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        hasTimePart = true;
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendWeekOfWeekyear(2);
                        break;
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                        builder.appendWeekyear(4, 4);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendTwoDigitYear(2020);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new AnalysisException(String.format("%%%s not supported in date format string", character));
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

        Date retDate = new Date(builder.toFormatter().withLocale(Locale.ENGLISH).parseMillis(date.getStringValue()));
        if (hasTimePart) {
            return new DateLiteral(DateFormatUtils.format(retDate, "yyyy-MM-dd HH:mm:ss"), Type.DATETIME);
        } else {
            return new DateLiteral(DateFormatUtils.format(retDate, "yyyy-MM-dd"), Type.DATE);
        }
    }

    @FEFunction(name = "date_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral dateSub(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        Date d = new Date(getTime(date));
        d = DateUtils.addDays(d, -(int) day.getLongValue());
        return new DateLiteral(DateFormatUtils.format(d, "yyyy-MM-dd HH:mm:ss"), Type.DATETIME);
    }

    @FEFunction(name = "year", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral year(LiteralExpr arg) throws AnalysisException {
        long timestamp = getTime(arg);
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(timestamp);
        return new IntLiteral(instance.get(Calendar.YEAR), Type.INT);
    }

    @FEFunction(name = "month", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral month(LiteralExpr arg) throws AnalysisException {
        long timestamp = getTime(arg);
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(timestamp);
        return new IntLiteral(instance.get(Calendar.MONTH) + 1, Type.INT);
    }

    @FEFunction(name = "day", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral day(LiteralExpr arg) throws AnalysisException {
        long timestamp = getTime(arg);
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(timestamp);
        return new IntLiteral(instance.get(Calendar.DAY_OF_MONTH), Type.INT);
    }

    @FEFunction(name = "unix_timestamp", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral unix_timestamp(LiteralExpr arg) throws AnalysisException {
        long timestamp = getTime(arg);
        return new IntLiteral(timestamp / 1000, Type.INT);
    }

    @FEFunction(name = "from_unixtime", argTypes = { "INT" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime) throws AnalysisException {
        //if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        Date date = new Date(unixTime.getLongValue() * 1000);
        return new StringLiteral(dateFormat(date, "%Y-%m-%d %H:%i:%S"));
    }

    @FEFunction(name = "from_unixtime", argTypes = { "INT", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime, StringLiteral fmtLiteral) throws AnalysisException {
        //if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        Date date = new Date(unixTime.getLongValue() * 1000);
        //currently, doris BE only support "yyyy-MM-dd HH:mm:ss" and "yyyy-MM-dd" format
        return new StringLiteral(DateFormatUtils.format(date, fmtLiteral.getStringValue()));
    }

    private static long getTime(LiteralExpr expr) throws AnalysisException {
        try {
            String[] parsePatterns = { "yyyyMMdd", "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss" };
            long time;
            if (expr instanceof DateLiteral) {
                time = expr.getLongValue();
            } else {
                time = DateUtils.parseDate(expr.getStringValue(), parsePatterns).getTime();
            }
            return time;
        } catch (ParseException e) {
            throw new AnalysisException(e.getLocalizedMessage());
        }
    }

    private static int calFirstWeekDay(int year, int firstWeekDay) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, Calendar.JANUARY,1);
        int firstDay = 1;
        calendar.set(Calendar.DAY_OF_MONTH, firstDay);
        while (calendar.get(Calendar.DAY_OF_WEEK) != firstWeekDay) {
            calendar.set(Calendar.DAY_OF_MONTH, ++firstDay);
        }
        return firstDay;
    }

    private  static String dateFormat(Date date,  String pattern) {
        DateTimeFormatterBuilder formatterBuilder = new DateTimeFormatterBuilder();
        Calendar calendar = Calendar.getInstance();
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);
            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        formatterBuilder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        formatterBuilder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        formatterBuilder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        formatterBuilder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        formatterBuilder.appendDayOfMonth(1);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                        formatterBuilder.appendFractionOfSecond(6, 9);
                        break;
                    case 'H': // %H Hour (00..23)
                        formatterBuilder.appendHourOfDay(2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        formatterBuilder.appendClockhourOfHalfday(2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        formatterBuilder.appendMinuteOfHour(2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        formatterBuilder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        formatterBuilder.appendHourOfDay(1);
                        break;
                    case 'l': // %l Hour (1..12)
                        formatterBuilder.appendClockhourOfHalfday(1);
                        break;
                    case 'M': // %M Month name (January..December)
                        formatterBuilder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        formatterBuilder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        formatterBuilder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        formatterBuilder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        formatterBuilder.appendSecondOfMinute(2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        formatterBuilder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        break;
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    {
                        int week;
                        calendar.setTime(date);
                        int firstSunday = calFirstWeekDay(calendar.get(Calendar.YEAR), Calendar.SUNDAY);
                        if (calendar.get(Calendar.DATE) <= 7 && calendar.get(Calendar.MONTH) == Calendar.JANUARY
                                && calendar.get(Calendar.DATE) >= firstSunday) {
                            week = 1;
                        } else {
                            calendar.add(Calendar.DATE, -7);
                            week = calendar.get(Calendar.WEEK_OF_YEAR) +
                                    (calFirstWeekDay(calendar.get(Calendar.YEAR), Calendar.SUNDAY) == 1 ? 1 : 0);
                        }
                        formatterBuilder.appendLiteral(String.format("%02d", week));
                        break;
                    }
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        formatterBuilder.appendWeekOfWeekyear(2);
                        break;
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                        calendar.setTime(date);
                        if(calendar.get(Calendar.MONTH) == Calendar.JANUARY &&
                                calendar.get(Calendar.DATE) < calFirstWeekDay(calendar.get(Calendar.YEAR), Calendar.SUNDAY)) {
                            formatterBuilder.appendLiteral(String.valueOf(calendar.get(Calendar.YEAR) - 1));
                        } else {
                            formatterBuilder.appendLiteral(String.valueOf(calendar.get(Calendar.YEAR)));
                        }
                        break;
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                        formatterBuilder.appendWeekyear(4, 4);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        formatterBuilder.appendDayOfWeekText();
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        calendar.setTime(date);
                        calendar.setFirstDayOfWeek(Calendar.SUNDAY);
                        formatterBuilder.appendLiteral(String.valueOf(calendar.get(Calendar.DAY_OF_WEEK) - 1));
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        int PIVOT_YEAR = 2020;
                        formatterBuilder.appendTwoDigitYear(PIVOT_YEAR);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        formatterBuilder.appendYear(4, 4);
                        break;
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        calendar.setTime(date);
                        int day = calendar.get(Calendar.DAY_OF_MONTH);
                        if (day >= 10 && day <= 19) {
                            formatterBuilder.appendLiteral(String.valueOf(day) + "th");
                        } else {
                            switch (day % 10) {
                                case 1:
                                    formatterBuilder.appendLiteral(String.valueOf(day) + "st");
                                    break;
                                case 2:
                                    formatterBuilder.appendLiteral(String.valueOf(day) + "nd");
                                    break;
                                case 3:
                                    formatterBuilder.appendLiteral(String.valueOf(day) + "rd");
                                    break;
                                default:
                                    formatterBuilder.appendLiteral(String.valueOf(day) + "th");
                                    break;
                            }
                        }
                        break;
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                        calendar.setTime(date);
                        if (calendar.get(Calendar.DATE) <= 7 && calendar.get(Calendar.MONTH) == Calendar.JANUARY) {
                            int firstSunday = calFirstWeekDay(calendar.get(Calendar.YEAR), Calendar.SUNDAY);
                            formatterBuilder.appendLiteral(String.format("%02d",
                                    ((calendar.get(Calendar.DATE) < firstSunday && firstSunday != 1) ? 0 : 1)));
                        } else {
                            calendar.add(Calendar.DATE, -7);
                            calendar.setFirstDayOfWeek(Calendar.SUNDAY);
                            formatterBuilder.appendLiteral(String.format("%02d",
                                    calendar.get(Calendar.WEEK_OF_YEAR)
                                    + (calFirstWeekDay(calendar.get(Calendar.YEAR), Calendar.SUNDAY) == 1 ? 1 : 0)));
                        }
                        break;
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    {
                        calendar.setTime(date);
                        int week;
                        int firstMonday = calFirstWeekDay(calendar.get(Calendar.YEAR), Calendar.MONDAY);
                        if (calendar.get(Calendar.DATE) <= 7 && calendar.get(Calendar.MONTH) == Calendar.JANUARY) {
                            week = (calendar.get(Calendar.DATE) >= firstMonday || firstMonday == 1) ? 1 : 0 ;
                            week += (firstMonday >= 5 ? 1 : 0);
                        } else {
                            calendar.add(Calendar.DATE, -7);
                            calendar.setFirstDayOfWeek(Calendar.MONDAY);
                            week = calendar.get(Calendar.WEEK_OF_YEAR) + ((firstMonday >= 5 || firstMonday == 1) ? 1 : 0);
                        }
                        formatterBuilder.appendLiteral(String.format("%02d", week));
                        break;
                    }
                    case '%': // %% A literal “%” character
                        formatterBuilder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        formatterBuilder.appendLiteral(character);
                        break;
                }
                escaped = false;
            }
            else if (character == '%') {
                escaped = true;
            }
            else {
                formatterBuilder.appendLiteral(character);
            }
        }
        DateTimeFormatter formatter = formatterBuilder.toFormatter();
        return formatter.withLocale(Locale.US).print(date.getTime());
    }

    /**
     ------------------------------------------------------------------------------
     */


    /**
     * Cast
     */

    @FEFunction(name = "casttoint", argTypes = { "VARCHAR"}, returnType = "INT")
    public static IntLiteral castToInt(StringLiteral expr) throws AnalysisException {
        return new IntLiteral(expr.getLongValue(), Type.INT);
    }


    /**
     ------------------------------------------------------------------------------
     */

    /**
     * Math function
     */

    @FEFunction(name = "floor", argTypes = { "DOUBLE"}, returnType = "BIGINT")
    public static IntLiteral floor(LiteralExpr expr) throws AnalysisException {
        long result = (long)Math.floor(expr.getDoubleValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    /**
     ------------------------------------------------------------------------------
     */

    /**
     * Arithmetic function
     */

    @FEFunction(name = "add", argTypes = { "BIGINT", "BIGINT" }, returnType = "BIGINT")
    public static IntLiteral addInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "add", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral addDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() + second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    @FEFunction(name = "add", argTypes = { "DECIMAL", "DECIMAL" }, returnType = "DECIMAL")
    public static DecimalLiteral addDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.add(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "add", argTypes = { "DECIMALV2", "DECIMALV2" }, returnType = "DECIMALV2")
    public static DecimalLiteral addDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());

        BigDecimal result = left.add(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "add", argTypes = { "LARGEINT", "LARGEINT" }, returnType = "LARGEINT")
    public static LargeIntLiteral addBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.add(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "subtract", argTypes = { "BIGINT", "BIGINT" }, returnType = "BIGINT")
    public static IntLiteral subtractInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "subtract", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral subtractDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() - second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    @FEFunction(name = "subtract", argTypes = { "DECIMAL", "DECIMAL" }, returnType = "DECIMAL")
    public static DecimalLiteral subtractDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.subtract(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = { "DECIMALV2", "DECIMALV2" }, returnType = "DECIMALV2")
    public static DecimalLiteral subtractDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());

        BigDecimal result = left.subtract(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = { "LARGEINT", "LARGEINT" }, returnType = "LARGEINT")
    public static LargeIntLiteral subtractBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.subtract(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "multiply", argTypes = { "BIGINT", "BIGINT" }, returnType = "BIGINT")
    public static IntLiteral multiplyInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long left = first.getLongValue();
        long right = second.getLongValue();
        long result = Math.multiplyExact(left, right);
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "multiply", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral multiplyDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() * second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    @FEFunction(name = "multiply", argTypes = { "DECIMAL", "DECIMAL" }, returnType = "DECIMAL")
    public static DecimalLiteral multiplyDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.multiply(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = { "DECIMALV2", "DECIMALV2" }, returnType = "DECIMALV2")
    public static DecimalLiteral multiplyDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());

        BigDecimal result = left.multiply(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = { "LARGEINT", "LARGEINT" }, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.multiply(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "divide", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral divideDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() / second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    @FEFunction(name = "divide", argTypes = { "DECIMAL", "DECIMAL" }, returnType = "DECIMAL")
    public static DecimalLiteral divideDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.divide(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = { "DECIMALV2", "DECIMALV2" }, returnType = "DECIMALV2")
    public static DecimalLiteral divideDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());

        BigDecimal result = left.divide(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "concat", argTypes = { "VARCHAR"}, returnType = "VARCHAR")
    public static StringLiteral concat(StringLiteral... values) throws AnalysisException {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (StringLiteral value : values) {
            resultBuilder.append(value.getStringValue());
        }
        return new StringLiteral(resultBuilder.toString());
    }

    @FEFunction(name = "concat_ws", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static StringLiteral concat_ws(StringLiteral split, StringLiteral... values) throws AnalysisException {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
            resultBuilder.append(values[i].getStringValue()).append(split.getStringValue());
        }
        resultBuilder.append(values[values.length - 1].getStringValue());
        return new StringLiteral(resultBuilder.toString());
    }
}
