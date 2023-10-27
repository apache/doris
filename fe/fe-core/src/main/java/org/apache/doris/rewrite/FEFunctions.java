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

import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;

/**
 * compute functions in FE.
 *
 * when you add a new function, please ensure the name, argTypes,
 * returnType and compute logic are consistent with BE's function
 */
public class FEFunctions {
    private static final Logger LOG = LogManager.getLogger(FEFunctions.class);

    @FEFunction(name = "version", argTypes = {}, returnType = "VARCHAR")
    public static StringLiteral version() throws AnalysisException {
        return new StringLiteral(GlobalVariable.version);
    }

    /**
     * date and time function
     */
    @FEFunction(name = "timediff", argTypes = { "DATETIME", "DATETIME" }, returnType = "TIME")
    public static FloatLiteral timeDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long firstTimestamp = ((DateLiteral) first).unixTimestamp(TimeUtils.getTimeZone());
        long secondTimestamp = ((DateLiteral) second).unixTimestamp(TimeUtils.getTimeZone());
        return new FloatLiteral((double) (firstTimestamp - secondTimestamp) / 1000,
            FloatLiteral.getDefaultTimeType(Type.TIME));
    }

    @FEFunction(name = "datediff", argTypes = { "DATETIME", "DATETIME" }, returnType = "INT")
    public static IntLiteral dateDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        DateLiteral firstDate = ((DateLiteral) first);
        DateLiteral secondDate = ((DateLiteral) second);
        // DATEDIFF function only uses the date part for calculations and ignores the time part
        firstDate.castToDate();
        secondDate.castToDate();
        long datediff = (firstDate.unixTimestamp(TimeUtils.getTimeZone())
                - secondDate.unixTimestamp(TimeUtils.getTimeZone())) / 1000 / 60 / 60 / 24;
        return new IntLiteral(datediff, Type.INT);
    }

    @FEFunction(name = "dayofweek", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static IntLiteral dayOfWeek(LiteralExpr date) throws AnalysisException {
        // use zellar algorithm.
        long year = ((DateLiteral) date).getYear();
        long month = ((DateLiteral) date).getMonth();
        long day = ((DateLiteral) date).getDay();
        if (month < 3) {
            month += 12;
            year -= 1;
        }
        long c = year / 100;
        long y = year % 100;
        long t;
        if (date.compareTo(new DateLiteral(1582, 10, 4)) > 0) {
            t = (y + y / 4 + c / 4 - 2 * c + 26 * (month + 1) / 10 + day - 1) % 7;
        } else {
            t = (y + y / 4 - c + 26 * (month + 1) / 10 + day + 4) % 7;
        }
        return new IntLiteral(t + 1);
    }

    @FEFunction(name = "date_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral dateAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return daysAdd(date, day);
    }

    @FEFunction(name = "adddate", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral addDate(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return daysAdd(date, day);
    }

    @FEFunction(name = "years_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral yearsAdd(LiteralExpr date, LiteralExpr year) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusYears(year.getLongValue());
    }

    @FEFunction(name = "months_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral monthsAdd(LiteralExpr date, LiteralExpr month) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusMonths(month.getLongValue());
    }

    @FEFunction(name = "days_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral daysAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusDays(day.getLongValue());
    }

    @FEFunction(name = "hours_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral hoursAdd(LiteralExpr date, LiteralExpr hour) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusHours(hour.getLongValue());
    }

    @FEFunction(name = "minutes_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral minutesAdd(LiteralExpr date, LiteralExpr minute) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusMinutes(minute.getLongValue());
    }

    @FEFunction(name = "seconds_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
     public static DateLiteral secondsAdd(LiteralExpr date, LiteralExpr second) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusSeconds(second.getLongValue());
    }

    @FEFunction(name = "date_format", argTypes = { "DATETIME", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral dateFormat(LiteralExpr date, StringLiteral fmtLiteral) throws AnalysisException {
        String result = ((DateLiteral) date).dateFormat(fmtLiteral.getStringValue());
        return new StringLiteral(result);
    }

    @FEFunction(name = "str_to_date", argTypes = { "VARCHAR", "VARCHAR" }, returnType = "DATETIMEV2")
    public static DateLiteral dateParse(StringLiteral date, StringLiteral fmtLiteral) throws AnalysisException {
        DateLiteral dateLiteral = new DateLiteral();
        try {
            dateLiteral.fromDateFormatStr(fmtLiteral.getStringValue(), date.getStringValue(), false);
            dateLiteral.setType(dateLiteral.getType());
            return dateLiteral;
        } catch (InvalidFormatException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @FEFunction(name = "date_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral dateSub(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return dateAdd(date, new IntLiteral(-(int) day.getLongValue()));
    }

    @FEFunction(name = "years_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral yearsSub(LiteralExpr date, LiteralExpr year) throws AnalysisException {
        return yearsAdd(date, new IntLiteral(-(int) year.getLongValue()));
    }

    @FEFunction(name = "months_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral monthsSub(LiteralExpr date, LiteralExpr month) throws AnalysisException {
        return monthsAdd(date, new IntLiteral(-(int) month.getLongValue()));
    }

    @FEFunction(name = "days_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral daysSub(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return daysAdd(date, new IntLiteral(-(int) day.getLongValue()));
    }

    @FEFunction(name = "hours_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral hoursSub(LiteralExpr date, LiteralExpr hour) throws AnalysisException {
        return hoursAdd(date, new IntLiteral(-(int) hour.getLongValue()));
    }

    @FEFunction(name = "minutes_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral minutesSub(LiteralExpr date, LiteralExpr minute) throws AnalysisException {
        return minutesAdd(date, new IntLiteral(-(int) minute.getLongValue()));
    }

    @FEFunction(name = "seconds_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral secondsSub(LiteralExpr date, LiteralExpr second) throws AnalysisException {
        return secondsAdd(date, new IntLiteral(-(int) second.getLongValue()));
    }

    @FEFunction(name = "year", argTypes = { "DATETIME" }, returnType = "SMALLINT")
    public static IntLiteral year(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getYear(), Type.INT);
    }

    @FEFunction(name = "month", argTypes = { "DATETIME" }, returnType = "TINYINT")
    public static IntLiteral month(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getMonth(), Type.INT);
    }

    @FEFunction(name = "day", argTypes = { "DATETIME" }, returnType = "TINYINT")
    public static IntLiteral day(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getDay(), Type.INT);
    }

    @FEFunction(name = "unix_timestamp", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral unixTimestamp(LiteralExpr arg) throws AnalysisException {
        long unixTime = ((DateLiteral) arg).unixTimestamp(TimeUtils.getTimeZone()) / 1000;
        // date before 1970-01-01 or after 2038-01-19 03:14:07 should return 0 for unix_timestamp() function
        unixTime = unixTime < 0 ? 0 : unixTime;
        unixTime = unixTime > Integer.MAX_VALUE ? 0 : unixTime;
        return new IntLiteral(unixTime, Type.INT);
    }

    @FEFunction(name = "unix_timestamp", argTypes = { "DATE" }, returnType = "INT")
    public static IntLiteral unixTimestamp2(LiteralExpr arg) throws AnalysisException {
        long unixTime = ((DateLiteral) arg).unixTimestamp(TimeUtils.getTimeZone()) / 1000;
        // date before 1970-01-01 or after 2038-01-19 03:14:07 should return 0 for unix_timestamp() function
        unixTime = unixTime < 0 ? 0 : unixTime;
        unixTime = unixTime > Integer.MAX_VALUE ? 0 : unixTime;
        return new IntLiteral(unixTime, Type.INT);
    }

    @FEFunction(name = "from_unixtime", argTypes = { "BIGINT" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0 || unixTime.getLongValue() > 32536771199L) {
            throw new AnalysisException("unix timestamp out of range");
        }
        DateLiteral dl = new DateLiteral(unixTime.getLongValue() * 1000, TimeUtils.getTimeZone(),
                Type.DATETIME);
        return new StringLiteral(dl.getStringValue());
    }

    @FEFunction(name = "from_unixtime", argTypes = { "BIGINT", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime, StringLiteral fmtLiteral) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0 || unixTime.getLongValue() >= 32536771199L) {
            throw new AnalysisException("unix timestamp out of range");
        }
        DateLiteral dl = new DateLiteral(unixTime.getLongValue() * 1000, TimeUtils.getTimeZone(),
                Type.DATETIME);
        return new StringLiteral(dl.dateFormat(fmtLiteral.getStringValue()));
    }

    @FEFunction(name = "now", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral now() throws AnalysisException {
        return  new DateLiteral(LocalDateTime.now(TimeUtils.getTimeZone().toZoneId()),
                Type.DATETIME);
    }

    @FEFunction(name = "current_timestamp", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral currentTimestamp() throws AnalysisException {
        return now();
    }

    @FEFunction(name = "curdate", argTypes = {}, returnType = "DATE")
    public static DateLiteral curDate() {
        return new DateLiteral(LocalDateTime.now(TimeUtils.getTimeZone().toZoneId()),
                Type.DATE);
    }

    @FEFunction(name = "current_date", argTypes = {}, returnType = "DATE")
    public static DateLiteral currentDate() {
        return curDate();
    }

    @FEFunction(name = "curtime", argTypes = {}, returnType = "TIME")
    public static FloatLiteral curTime() throws AnalysisException {
        DateLiteral now = now();
        return new FloatLiteral((double) (now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond()),
            FloatLiteral.getDefaultTimeType(Type.TIME));
    }

    @FEFunction(name = "current_time", argTypes = {}, returnType = "TIME")
    public static FloatLiteral currentTime() throws AnalysisException {
        return curTime();
    }

    @FEFunction(name = "utc_timestamp", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral utcTimestamp() {
        return new DateLiteral(LocalDateTime.now(TimeUtils.getOrSystemTimeZone("+00:00").toZoneId()),
                Type.DATETIME);
    }

    @FEFunction(name = "hour", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static IntLiteral hour(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof DateLiteral) {
            return new IntLiteral(((DateLiteral) arg).getHour());
        }
        return null;
    }

    @FEFunction(name = "minute", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static IntLiteral minute(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof DateLiteral) {
            return new IntLiteral(((DateLiteral) arg).getMinute());
        }
        return null;
    }

    @FEFunction(name = "second", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static IntLiteral second(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof DateLiteral) {
            return new IntLiteral(((DateLiteral) arg).getSecond());
        }
        return null;
    }

    @FEFunction(name = "timestamp", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static DateLiteral timestamp(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof DateLiteral) {
            return (DateLiteral) arg;
        }
        return null;
    }

    @FEFunction(name = "to_monday", argTypes = {"DATETIME"}, returnType = "DATE")
    public static DateLiteral toMonday(LiteralExpr arg) {
        if (arg instanceof DateLiteral && (arg.getType().isDate() || arg.getType().isDatetime())) {
            DateLiteral dateLiteral = ((DateLiteral) arg);
            LocalDateTime dateTime = LocalDateTime.of(
                    ((int) dateLiteral.getYear()), ((int) dateLiteral.getMonth()), ((int) dateLiteral.getDay()),
                    0, 0, 0);
            dateTime = toMonday(dateTime);
            return new DateLiteral(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(), Type.DATE);
        }
        return null;
    }

    @FEFunction(name = "to_monday", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static DateLiteral toMondayV2(LiteralExpr arg) {
        if (arg instanceof DateLiteral && (arg.getType().isDateV2() || arg.getType().isDatetimeV2())) {
            DateLiteral dateLiteral = ((DateLiteral) arg);
            LocalDateTime dateTime = LocalDateTime.of(
                    ((int) dateLiteral.getYear()), ((int) dateLiteral.getMonth()), ((int) dateLiteral.getDay()),
                    0, 0, 0);
            dateTime = toMonday(dateTime);
            return new DateLiteral(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(), Type.DATEV2);
        }
        return null;
    }

    @FEFunction(name = "second_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral second_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.SECOND);
    }

    @FEFunction(name = "second_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral second_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.SECOND);
    }

    @FEFunction(name = "minute_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral minute_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.MINUTE);
    }

    @FEFunction(name = "minute_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral minute_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.MINUTE);
    }

    @FEFunction(name = "hour_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral hour_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.HOUR);
    }

    @FEFunction(name = "hour_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral hour_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.HOUR);
    }

    @FEFunction(name = "day_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral day_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.DAY);
    }

    @FEFunction(name = "day_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral day_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.DAY);
    }

    // get it's from be/src/vec/functions/function_datetime_floor_ceil.cpp##time_round
    public static DateLiteral getFloorCeilDateLiteral(LiteralExpr datetime, LiteralExpr period,
            LiteralExpr defaultDatetime, boolean isCeil, TimeUnit type) throws AnalysisException {
        DateLiteral dt = ((DateLiteral) datetime);
        DateLiteral start = ((DateLiteral) defaultDatetime);
        long periodValue = ((IntLiteral) period).getValue();
        long diff = 0;
        long trivialPart = 0;

        switch (type) {
            case YEAR: {
                diff = dt.getYear() - start.getYear();
                trivialPart = (dt.getLongValue() % 10000000000L) - (start.getLongValue() % 10000000000L);
                break;
            }
            case MONTH: {
                diff = (dt.getYear() - start.getYear()) * 12 + (dt.getMonth() - start.getMonth());
                trivialPart = (dt.getLongValue() % 100000000L) - (start.getLongValue() % 100000000L);
                break;
            }
            case WEEK: {
                diff = (dt.daynr() / 7) - (start.daynr() / 7);
                long part2 = (dt.daynr() % 7) * 24 * 3600 + dt.getHour() * 3600 + dt.getMinute() * 60 + dt.getSecond();
                long part1 = (start.daynr() % 7) * 24 * 3600 + start.getHour() * 3600 + start.getMinute() * 60
                        + start.getSecond();
                trivialPart = part2 - part1;
                break;
            }
            case DAY: {
                diff = dt.daynr() - start.daynr();
                long part2 = dt.getHour() * 3600 + dt.getMinute() * 60 + dt.getSecond();
                long part1 = start.getHour() * 3600 + start.getMinute() * 60 + start.getSecond();
                trivialPart = part2 - part1;
                break;
            }
            case HOUR: {
                diff = (dt.daynr() - start.daynr()) * 24 + (dt.getHour() - start.getHour());
                trivialPart = (dt.getMinute() * 60 + dt.getSecond()) - (start.getMinute() * 60 + start.getSecond());
                break;
            }
            case MINUTE: {
                diff = (dt.daynr() - start.daynr()) * 24 * 60 + (dt.getHour() - start.getHour()) * 60
                        + (dt.getMinute() - start.getMinute());
                trivialPart = dt.getSecond() - start.getSecond();
                break;
            }
            case SECOND: {
                diff = (dt.daynr() - start.daynr()) * 24 * 60 * 60 + (dt.getHour() - start.getHour()) * 60 * 60
                        + (dt.getMinute() - start.getMinute()) * 60 + (dt.getSecond() - start.getSecond());
                trivialPart = 0;
                break;
            }
            default:
                break;
        }

        if (isCeil) {
            diff = diff + (trivialPart > 0 ? 1 : 0);
        } else {
            diff = diff - (trivialPart < 0 ? 1 : 0);
        }
        long deltaInsidePeriod = (diff % periodValue + periodValue) % periodValue;
        long step = diff - deltaInsidePeriod;
        if (isCeil) {
            step = step + (deltaInsidePeriod == 0 ? 0 : periodValue);
        }
        switch (type) {
            case YEAR:
                return start.plusYears(step);
            case MONTH:
                return start.plusMonths(step);
            case WEEK:
                return start.plusDays(step * 7);
            case DAY:
                return start.plusDays(step);
            case HOUR:
                return start.plusHours(step);
            case MINUTE:
                return start.plusMinutes(step);
            case SECOND:
                return start.plusSeconds(step);
            default:
                break;
        }
        return null;
    }

    @FEFunction(name = "week_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral week_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.WEEK);
    }

    @FEFunction(name = "week_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral week_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.WEEK);
    }

    @FEFunction(name = "month_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral month_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.MONTH);
    }

    @FEFunction(name = "month_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral month_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.MONTH);
    }

    @FEFunction(name = "year_floor", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral year_floor(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, false, TimeUnit.YEAR);
    }

    @FEFunction(name = "year_ceil", argTypes = { "DATETIMEV2", "INT", "DATETIMEV2" }, returnType = "DATETIMEV2")
    public static DateLiteral year_ceil(LiteralExpr datetime, LiteralExpr period, LiteralExpr defaultDatetime)
            throws AnalysisException {
        return getFloorCeilDateLiteral(datetime, period, defaultDatetime, true, TimeUnit.YEAR);
    }

    @FEFunction(name = "date_trunc", argTypes = {"DATETIME", "VARCHAR"}, returnType = "DATETIME")
    public static DateLiteral dateTruncDatetime(LiteralExpr date, LiteralExpr truncate) {
        if (date.getType().isDateType()) {
            DateLiteral dateLiteral = ((DateLiteral) date);
            LocalDateTime localDate = dateTruncHelper(LocalDateTime.of(
                            (int) dateLiteral.getYear(), (int) dateLiteral.getMonth(), (int) dateLiteral.getDay(),
                            (int) dateLiteral.getHour(), (int) dateLiteral.getMinute(), (int) dateLiteral.getSecond()),
                    truncate.getStringValue());

            return new DateLiteral(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                    localDate.getHour(), localDate.getMinute(), localDate.getSecond(), date.getType());
        }
        return null;
    }

    @FEFunction(name = "date_trunc", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "DATETIMEV2")
    public static DateLiteral dateTruncDatetimeV2(LiteralExpr date, LiteralExpr truncate) {
        if (date.getType().isDateType()) {
            DateLiteral dateLiteral = ((DateLiteral) date);
            LocalDateTime localDate = dateTruncHelper(LocalDateTime.of(
                            (int) dateLiteral.getYear(), (int) dateLiteral.getMonth(), (int) dateLiteral.getDay(),
                            (int) dateLiteral.getHour(), (int) dateLiteral.getMinute(), (int) dateLiteral.getSecond()),
                    truncate.getStringValue());

            return new DateLiteral(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                    localDate.getHour(), localDate.getMinute(), localDate.getSecond(), date.getType());
        }
        return null;
    }

    @FEFunction(name = "date_trunc", argTypes = { "DATE", "VARCHAR" }, returnType = "DATE")
    public static DateLiteral dateTruncDate(LiteralExpr date, LiteralExpr truncate) {
        if (date.getType().isDateType()) {
            DateLiteral dateLiteral = ((DateLiteral) date);
            LocalDateTime localDate = dateTruncHelper(LocalDateTime.of(
                    (int) dateLiteral.getYear(), (int) dateLiteral.getMonth(), (int) dateLiteral.getDay(), 0, 0, 0),
                    truncate.getStringValue());

            return new DateLiteral(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                    localDate.getHour(), localDate.getMinute(), localDate.getSecond(), date.getType());
        }
        return null;
    }

    @FEFunction(name = "date_trunc", argTypes = { "DATEV2", "VARCHAR" }, returnType = "DATEV2")
    public static DateLiteral dateTruncDateV2(LiteralExpr date, LiteralExpr truncate) {
        if (date.getType().isDateType()) {
            DateLiteral dateLiteral = ((DateLiteral) date);
            LocalDateTime localDate = dateTruncHelper(LocalDateTime.of(
                    (int) dateLiteral.getYear(), (int) dateLiteral.getMonth(), (int) dateLiteral.getDay(), 0, 0, 0),
                    truncate.getStringValue());

            return new DateLiteral(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                    localDate.getHour(), localDate.getMinute(), localDate.getSecond(), date.getType());
        }
        return null;
    }

    private static LocalDateTime dateTruncHelper(LocalDateTime dateTime, String trunc) {
        int year = dateTime.getYear();
        int month = dateTime.getMonthValue();
        int day = dateTime.getDayOfMonth();
        int hour = dateTime.getHour();
        int minute = dateTime.getMinute();
        int second = dateTime.getSecond();
        switch (trunc.toLowerCase()) {
            case "year":
                month = 0;
            case "quarter": // CHECKSTYLE IGNORE THIS LINE
                month = ((month - 1) / 3) * 3 + 1;
            case "month": // CHECKSTYLE IGNORE THIS LINE
                day = 1;
                break;
            case "week":
                LocalDateTime firstDayOfWeek = firstDayOfWeek(dateTime);
                year = firstDayOfWeek.getYear();
                month = firstDayOfWeek.getMonthValue();
                day = firstDayOfWeek.getDayOfMonth();
            default: // CHECKSTYLE IGNORE THIS LINE
                break;
        }
        switch (trunc.toLowerCase()) {
            case "year":
            case "quarter":
            case "month":
            case "week":
            case "day": // CHECKSTYLE IGNORE THIS LINE
                hour = 0;
            case "hour": // CHECKSTYLE IGNORE THIS LINE
                minute = 0;
            case "minute": // CHECKSTYLE IGNORE THIS LINE
                second = 0;
            default: // CHECKSTYLE IGNORE THIS LINE
        }
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    private static int distanceToFirstDayOfWeek(LocalDateTime dateTime) {
        return dateTime.getDayOfWeek().getValue() - 1;
    }

    private static LocalDateTime firstDayOfWeek(LocalDateTime dateTime) {
        return dateTime.plusDays(-distanceToFirstDayOfWeek(dateTime));
    }

    private static LocalDateTime toMonday(LocalDateTime dateTime) {
        LocalDateTime specialUpperBound = LocalDateTime.of(1970, 1, 4, 0, 0, 0);
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (dateTime.isAfter(specialUpperBound) || dateTime.isBefore(specialLowerBound)) {
            return dateTime.plusDays(-dateTime.getDayOfWeek().getValue() + 1);
        }
        return specialLowerBound;
    }

    /**
     ------------------------------------------------------------------------------
     */

    /**
     * Math function
     */

    @FEFunction(name = "floor", argTypes = { "DOUBLE"}, returnType = "BIGINT")
    public static IntLiteral floor(LiteralExpr expr) throws AnalysisException {
        long result = (long) Math.floor(expr.getDoubleValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    /**
     ------------------------------------------------------------------------------
     */

    /**
     * Arithmetic function
     */

    @FEFunction(name = "add", argTypes = { "TINYINT", "TINYINT" }, returnType = "SMALLINT")
    public static IntLiteral addTinyint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.SMALLINT);
    }

    @FEFunction(name = "add", argTypes = { "SMALLINT", "SMALLINT" }, returnType = "INT")
    public static IntLiteral addSmallint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.INT);
    }

    @FEFunction(name = "add", argTypes = { "INT", "INT" }, returnType = "BIGINT")
    public static IntLiteral addInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "add", argTypes = { "BIGINT", "BIGINT" }, returnType = "BIGINT")
    public static IntLiteral addBigint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "add", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral addDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() + second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
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

    @FEFunction(name = "subtract", argTypes = { "TINYINT", "TINYINT" }, returnType = "SMALLINT")
    public static IntLiteral subtractTinyint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.SMALLINT);
    }

    @FEFunction(name = "subtract", argTypes = { "SMALLINT", "SMALLINT" }, returnType = "INT")
    public static IntLiteral subtractSmallint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.INT);
    }

    @FEFunction(name = "subtract", argTypes = { "INT", "INT" }, returnType = "BIGINT")
    public static IntLiteral subtractInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "subtract", argTypes = { "BIGINT", "BIGINT" }, returnType = "BIGINT")
    public static IntLiteral subtractBigint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "subtract", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral subtractDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() - second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
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

    @FEFunction(name = "multiply", argTypes = { "TINYINT", "TINYINT" }, returnType = "SMALLINT")
    public static IntLiteral multiplyTinyint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.multiplyExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.SMALLINT);
    }

    @FEFunction(name = "multiply", argTypes = { "SMALLINT", "SMALLINT" }, returnType = "INT")
    public static IntLiteral multiplySmallint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.multiplyExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.INT);
    }

    @FEFunction(name = "multiply", argTypes = { "INT", "INT" }, returnType = "BIGINT")
    public static IntLiteral multiplyInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.multiplyExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "multiply", argTypes = { "BIGINT", "BIGINT" }, returnType = "BIGINT")
    public static IntLiteral multiplyBigint(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.multiplyExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "multiply", argTypes = { "DOUBLE", "DOUBLE" }, returnType = "DOUBLE")
    public static FloatLiteral multiplyDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() * second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
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
        if (second.getDoubleValue() == 0.0) {
            return null;
        }
        double result = first.getDoubleValue() / second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    @FEFunction(name = "divide", argTypes = { "DECIMALV2", "DECIMALV2" }, returnType = "DECIMALV2")
    public static DecimalLiteral divideDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        if (right.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
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

    @FEFunctionList({
        @FEFunction(name = "ifnull", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR"),
        @FEFunction(name = "ifnull", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT"),
        @FEFunction(name = "ifnull", argTypes = {"INT", "INT"}, returnType = "INT"),
        @FEFunction(name = "ifnull", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT"),
        @FEFunction(name = "ifnull", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME"),
        @FEFunction(name = "ifnull", argTypes = { "DATE", "DATETIME" }, returnType = "DATETIME"),
        @FEFunction(name = "ifnull", argTypes = { "DATETIME", "DATE" }, returnType = "DATETIME")
    })
    public static LiteralExpr ifNull(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return first instanceof NullLiteral ? second : first;
    }

    // maybe use alias info to reduce redundant code
    @FEFunctionList({
        @FEFunction(name = "nvl", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR"),
        @FEFunction(name = "nvl", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT"),
        @FEFunction(name = "nvl", argTypes = {"INT", "INT"}, returnType = "INT"),
        @FEFunction(name = "nvl", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT"),
        @FEFunction(name = "nvl", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME"),
        @FEFunction(name = "nvl", argTypes = { "DATE", "DATETIME" }, returnType = "DATETIME"),
        @FEFunction(name = "nvl", argTypes = { "DATETIME", "DATE" }, returnType = "DATETIME")
    })
    public static LiteralExpr nvl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return first instanceof NullLiteral ? second : first;
    }

    @FEFunctionList({
        @FEFunction(name = "array", argTypes = {"BOOLEAN"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"TINYINT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"SMALLINT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"INT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"BIGINT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"LARGEINT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"DATETIME"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"DATE"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"FLOAT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"DOUBLE"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"DECIMALV2"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"VARCHAR"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"STRING"}, returnType = "ARRAY")
    })
    public static ArrayLiteral array(LiteralExpr... exprs) throws AnalysisException {
        return new ArrayLiteral(exprs);
    }

}
