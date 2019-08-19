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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import org.apache.doris.common.util.TimeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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
    public static FloatLiteral timeDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            long timediff = sdf.parse(first.getStringValue()).getTime() - sdf.parse(second.getStringValue()).getTime();
            return new FloatLiteral((double) timediff, Type.TIME);
        } catch (ParseException e) {
            throw new AnalysisException(e.getLocalizedMessage());
        }
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
        DateLiteral dateLiteral = (DateLiteral) date;

        DateLiteral result = new DateLiteral(dateLiteral);
        result.setDay(dateLiteral.getDay() + day.getLongValue());
        return result;
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
        String result = ((DateLiteral) date).dateFormat(fmtLiteral.getStringValue());
        return new StringLiteral(result);
    }

    @FEFunction(name = "str_to_date", argTypes = { "VARCHAR", "VARCHAR" }, returnType = "DATETIME")
    public static DateLiteral dateParse(StringLiteral date, StringLiteral fmtLiteral) throws AnalysisException {
        return DateLiteral.dateParser(date.getStringValue(), fmtLiteral.getStringValue());
    }

    @FEFunction(name = "date_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateLiteral dateSub(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return dateAdd(date, new IntLiteral(-(int) day.getLongValue()));
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
    public static IntLiteral unix_timestamp(LiteralExpr arg) throws AnalysisException {
        try {
            System.out.println("unix " + ((DateLiteral) arg).unixTime(TimeUtils.getTimeZone()));
            return new IntLiteral(((DateLiteral) arg).unixTime(TimeUtils.getTimeZone()) / 1000, Type.INT);
        } catch (ParseException e) {
            throw new AnalysisException(e.getLocalizedMessage());
        }
    }

    @FEFunction(name = "from_unixtime", argTypes = { "INT" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime) throws AnalysisException {
        //if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeUtils.getTimeZone());
        String dateLiteral = dateFormat.format(new Date(unixTime.getLongValue() * 1000));
        return new StringLiteral(dateLiteral);
    }
    @FEFunction(name = "from_unixtime", argTypes = { "INT", "VARCHAR" }, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime, StringLiteral fmtLiteral) throws AnalysisException {
        //if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeUtils.getTimeZone());
        String dateLiteral = dateFormat.format(new Date(unixTime.getLongValue() * 1000));

        DateLiteral dl = new DateLiteral(dateLiteral, Type.DATETIME);
        return new StringLiteral(dl.dateFormat(fmtLiteral.getStringValue()));
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
