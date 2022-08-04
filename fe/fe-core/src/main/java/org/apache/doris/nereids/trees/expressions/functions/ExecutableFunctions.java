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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.rewrite.FEFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

/**
 * functions that can be executed in FE.
 */
public class ExecutableFunctions {
    public static final ExecutableFunctions INSTANCE = new ExecutableFunctions();

    private static final Logger LOG = LogManager.getLogger(ExecutableFunctions.class);

    /**
     * Executable arithmetic functions
     */

    @FEFunction(name = "add", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static TinyIntLiteral addTinyint(TinyIntLiteral first, TinyIntLiteral second) {
        byte result = (byte) Math.addExact(first.getValue(), second.getValue());
        return new TinyIntLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral addSmallint(SmallIntLiteral first, SmallIntLiteral second) {
        short result = (short) Math.addExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"INT", "INT"}, returnType = "INT")
    public static IntegerLiteral addInt(IntegerLiteral first, IntegerLiteral second) {
        int result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral addBigint(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral addDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() + second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral addDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static TinyIntLiteral subtractTinyint(TinyIntLiteral first, TinyIntLiteral second) {
        byte result = (byte) Math.subtractExact(first.getValue(), second.getValue());
        return new TinyIntLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral subtractSmallint(SmallIntLiteral first, SmallIntLiteral second) {
        short result = (short) Math.subtractExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"INT", "INT"}, returnType = "INT")
    public static IntegerLiteral subtractInt(IntegerLiteral first, IntegerLiteral second) {
        int result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral subtractBigint(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral subtractDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() - second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral subtractDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static TinyIntLiteral multiplyTinyint(TinyIntLiteral first, TinyIntLiteral second) {
        byte result = (byte) Math.multiplyExact(first.getValue(), second.getValue());
        return new TinyIntLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral multiplySmallint(SmallIntLiteral first, SmallIntLiteral second) {
        short result = (short) Math.multiplyExact(first.getValue(), second.getValue());
        return new SmallIntLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"INT", "INT"}, returnType = "INT")
    public static IntegerLiteral multiplyInt(IntegerLiteral first, IntegerLiteral second) {
        int result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral multiplyBigint(BigIntLiteral first, BigIntLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new BigIntLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral multiplyDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() * second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral multiplyDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral divideDouble(DoubleLiteral first, DoubleLiteral second) {
        if (second.getValue() == 0.0) {
            return null;
        }
        double result = first.getValue() / second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral divideDecimal(DecimalLiteral first, DecimalLiteral second) {
        if (first.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal result = first.getValue().divide(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "date_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral dateSub(DateTimeLiteral date, IntegerLiteral day) throws AnalysisException {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @FEFunction(name = "date_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral dateAdd(DateTimeLiteral date, IntegerLiteral day) throws AnalysisException {
        return daysAdd(date, day);
    }

    @FEFunction(name = "years_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral yearsAdd(DateTimeLiteral date, IntegerLiteral year) throws AnalysisException {
        return date.plusYears(year.getValue());
    }

    @FEFunction(name = "months_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral monthsAdd(DateTimeLiteral date, IntegerLiteral month) throws AnalysisException {
        return date.plusMonths(month.getValue());
    }

    @FEFunction(name = "days_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral daysAdd(DateTimeLiteral date, IntegerLiteral day) throws AnalysisException {
        return date.plusDays(day.getValue());
    }

    @FEFunction(name = "hours_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral hoursAdd(DateTimeLiteral date, IntegerLiteral hour) throws AnalysisException {
        return date.plusHours(hour.getValue());
    }

    @FEFunction(name = "minutes_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral minutesAdd(DateTimeLiteral date, IntegerLiteral minute) throws AnalysisException {
        return date.plusMinutes(minute.getValue());
    }

    @FEFunction(name = "seconds_add", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral secondsAdd(DateTimeLiteral date, IntegerLiteral second) throws AnalysisException {
        return date.plusSeconds(second.getValue());
    }

    @FEFunction(name = "years_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral yearsSub(DateTimeLiteral date, IntegerLiteral year) throws AnalysisException {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    @FEFunction(name = "months_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral monthsSub(DateTimeLiteral date, IntegerLiteral month) throws AnalysisException {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    @FEFunction(name = "days_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral daysSub(DateTimeLiteral date, IntegerLiteral day) throws AnalysisException {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @FEFunction(name = "hours_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral hoursSub(DateTimeLiteral date, IntegerLiteral hour) throws AnalysisException {
        return hoursAdd(date, new IntegerLiteral(-hour.getValue()));
    }

    @FEFunction(name = "minutes_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral minutesSub(DateTimeLiteral date, IntegerLiteral minute) throws AnalysisException {
        return minutesAdd(date, new IntegerLiteral(-minute.getValue()));
    }

    @FEFunction(name = "seconds_sub", argTypes = { "DATETIME", "INT" }, returnType = "DATETIME")
    public static DateTimeLiteral secondsSub(DateTimeLiteral date, IntegerLiteral second) throws AnalysisException {
        return secondsAdd(date, new IntegerLiteral(-second.getValue()));
    }

}
