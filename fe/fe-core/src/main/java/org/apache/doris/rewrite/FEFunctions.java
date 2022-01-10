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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.GlobalVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * compute functions in FE.
 *
 * when you add a new function, please ensure the name, argTypes , returnType and compute logic are consistent with BE's function
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
        return new FloatLiteral((double) (firstTimestamp - secondTimestamp) / 1000, Type.TIME);
    }

    @FEFunction(name = "datediff", argTypes = { "DATETIME", "DATETIME" }, returnType = "INT")
    public static IntLiteral dateDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        DateLiteral firstDate = ((DateLiteral) first);
        DateLiteral secondDate = ((DateLiteral) second);
        // DATEDIFF function only uses the date part for calculations and ignores the time part
        firstDate.castToDate();
        secondDate.castToDate();
        long datediff = (firstDate.unixTimestamp(TimeUtils.getTimeZone()) - secondDate.unixTimestamp(TimeUtils.getTimeZone())) / 1000 / 60 / 60 / 24;
        return new IntLiteral(datediff, Type.INT);
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
        return dateLiteral.plusYears((int) year.getLongValue());
    }

    @FEFunction(name = "months_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral monthsAdd(LiteralExpr date, LiteralExpr month) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusMonths((int) month.getLongValue());
    }

    @FEFunction(name = "days_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral daysAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusDays((int) day.getLongValue());
    }

    @FEFunction(name = "hours_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral hoursAdd(LiteralExpr date, LiteralExpr hour) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusHours((int) hour.getLongValue());
    }

    @FEFunction(name = "minutes_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral minutesAdd(LiteralExpr date, LiteralExpr minute) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusMinutes((int) minute.getLongValue());
    }

    @FEFunction(name = "seconds_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
     public static DateLiteral secondsAdd(LiteralExpr date, LiteralExpr second) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusSeconds((int) second.getLongValue());
    }

    @FEFunction(name = "date_format", argTypes = { "DATETIME", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral dateFormat(LiteralExpr date, StringLiteral fmtLiteral) throws AnalysisException {
        String result = ((DateLiteral) date).dateFormat(fmtLiteral.getStringValue());
        return new StringLiteral(result);
    }

    @FEFunction(name = "str_to_date", argTypes = { "VARCHAR", "VARCHAR" }, returnType = "DATETIME")
    public static DateLiteral dateParse(StringLiteral date, StringLiteral fmtLiteral) throws AnalysisException {
        DateLiteral dateLiteral = new DateLiteral();
        try {
            dateLiteral.fromDateFormatStr(fmtLiteral.getStringValue(), date.getStringValue(), false);
            return dateLiteral;
        } catch (InvalidFormatException e) {
            e.printStackTrace();
            throw new AnalysisException(e.getMessage());
        }
    }

    @FEFunction(name = "makedate", argTypes = { "INT", "INT" }, returnType = "DATETIME")
    public static DateLiteral makeDate(LiteralExpr date) throws AnalysisException {
        return (DateLiteral) date;
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

    @FEFunction(name = "year", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral year(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getYear(), Type.INT);
    }

    @FEFunction(name = "month", argTypes = { "DATETIME" }, returnType = "INT")
    public static IntLiteral month(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getMonth(), Type.INT);
    }

    @FEFunction(name = "day", argTypes = { "DATETIME" }, returnType = "INT")
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

    @FEFunction(name = "from_unixtime", argTypes = { "INT" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        DateLiteral dl = new DateLiteral(unixTime.getLongValue() * 1000, TimeUtils.getTimeZone(), Type.DATETIME);
        return new StringLiteral(dl.getStringValue());
    }

    @FEFunction(name = "from_unixtime", argTypes = { "INT", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime, StringLiteral fmtLiteral) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        DateLiteral dl = new DateLiteral(unixTime.getLongValue() * 1000, TimeUtils.getTimeZone(), Type.DATETIME);
        return new StringLiteral(dl.dateFormat(fmtLiteral.getStringValue()));
    }

    @FEFunction(name = "now", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral now() throws AnalysisException {
        return  new DateLiteral(LocalDateTime.now(DateTimeZone.forTimeZone(TimeUtils.getTimeZone())), Type.DATETIME);
    }

    @FEFunction(name = "current_timestamp", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral currentTimestamp() throws AnalysisException {
        return now();
    }

    @FEFunction(name = "curdate", argTypes = {}, returnType = "DATE")
    public static DateLiteral curDate() throws AnalysisException {
        return new DateLiteral(LocalDateTime.now(DateTimeZone.forTimeZone(TimeUtils.getTimeZone())), Type.DATE);
    }

    @FEFunction(name = "curtime", argTypes = {}, returnType = "TIME")
    public static FloatLiteral curTime() throws AnalysisException {
        DateLiteral now = now();
        return new FloatLiteral((double) (now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond()), Type.TIME);
    }

    @FEFunction(name = "current_time", argTypes = {}, returnType = "TIME")
    public static FloatLiteral currentTime() throws AnalysisException {
        return curTime();
    }

    @FEFunction(name = "utc_timestamp", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral utcTimestamp() throws AnalysisException {
        return new DateLiteral(LocalDateTime.now(DateTimeZone.forTimeZone(TimeUtils.getOrSystemTimeZone("+00:00"))),
                Type.DATETIME);
    }

    @FEFunction(name = "yearweek", argTypes = { "DATE" }, returnType = "INT")
    public static IntLiteral yearWeek(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof IntLiteral) {
            return (IntLiteral) arg;
        }
        return null;
    }

    @FEFunction(name = "yearweek", argTypes = { "DATE", "INT" }, returnType = "INT")
    public static IntLiteral yearWeekMod(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof IntLiteral) {
            return (IntLiteral) arg;
        }
        return null;
    }

    @FEFunction(name = "week", argTypes = { "DATE" }, returnType = "INT")
    public static IntLiteral week(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof IntLiteral) {
            return (IntLiteral) arg;
        }
        return null;
    }

    @FEFunction(name = "week", argTypes = { "DATE", "INT" }, returnType = "INT")
    public static IntLiteral weekMode(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof IntLiteral) {
            return (IntLiteral) arg;
        }
        return null;
    }

    @FEFunction(name = "hour", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntLiteral hour(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof DateLiteral) {
            return new IntLiteral(((DateLiteral) arg).getHour());
        }
        return null;
    }

    @FEFunction(name = "minute", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntLiteral minute(LiteralExpr arg) throws AnalysisException {
        if (arg instanceof DateLiteral) {
            return new IntLiteral(((DateLiteral) arg).getMinute());
        }
        return null;
    }

    @FEFunction(name = "second", argTypes = {"DATETIME"}, returnType = "INT")
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

    @FEFunctionList({
        @FEFunction(name = "array", argTypes = {"INT"}, returnType = "ARRAY"),
        @FEFunction(name = "array", argTypes = {"VARCHAR"}, returnType = "ARRAY")
    })
    public static ArrayLiteral array(LiteralExpr... exprs) throws AnalysisException {
        return new ArrayLiteral(exprs);
    }

}
