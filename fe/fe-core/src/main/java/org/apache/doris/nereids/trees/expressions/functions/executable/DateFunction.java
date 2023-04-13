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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDate;
import java.util.Calendar;

/**
 * executable functions:
 * unclassified date function
 */
public class DateFunction {
    /**
     * datetime arithmetic function date
     */
    @ExecFunction(name = "date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static DateLiteral date(DateTimeLiteral dateTime) throws AnalysisException {
        return new DateLiteral(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    @ExecFunction(name = "date", argTypes = {"DATETIMEV2"}, returnType = "DATE")
    public static DateLiteral date(DateTimeV2Literal dateTime) throws AnalysisException {
        return new DateLiteral(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * datetime arithmetic function datediff
     */
    @ExecFunction(name = "datediff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dateDiff(DateTimeV2Literal date1, DateTimeV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.getYear(), date1.getMonth(), date1.getDay(),
                date2.getYear(), date2.getMonth(), date2.getDay()));
    }

    @ExecFunction(name = "datediff", argTypes = {"DATEV2", "DATEV2"}, returnType = "INT")
    public static IntegerLiteral dateDiff(DateV2Literal date1, DateV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.getYear(), date1.getMonth(), date1.getDay(),
                date2.getYear(), date2.getMonth(), date2.getDay()));
    }

    @ExecFunction(name = "datediff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "INT")
    public static IntegerLiteral dateDiff(DateTimeV2Literal date1, DateV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.getYear(), date1.getMonth(), date1.getDay(),
                date2.getYear(), date2.getMonth(), date2.getDay()));
    }

    @ExecFunction(name = "datediff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dateDiff(DateV2Literal date1, DateTimeV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.getYear(), date1.getMonth(), date1.getDay(),
                date2.getYear(), date2.getMonth(), date2.getDay()));
    }

    @ExecFunction(name = "datediff", argTypes = {"DATETIME", "DATETIME"}, returnType = "INT")
    public static IntegerLiteral dateDiff(DateTimeLiteral date1, DateTimeLiteral date2) {
        return new IntegerLiteral(dateDiff(date1.getYear(), date1.getMonth(), date1.getDay(),
                date2.getYear(), date2.getMonth(), date2.getDay()));
    }

    private static int dateDiff(long year1, long month1, long day1, long year2, long month2, long day2) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(((int) year1), ((int) month1), ((int) day1));
        long time1 = calendar.getTimeInMillis();
        calendar.set(((int) year2), ((int) month2), ((int) day2));
        long time2 = calendar.getTimeInMillis();
        return ((int) ((time1 - time2) / (1000 * 3600 * 24)));
    }

    /**
     * datetime arithmetic function date-format
     */
    @ExecFunction(name = "date_format", argTypes = {"DATE", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateLiteral date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateTimeLiteral date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateV2Literal date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateTimeV2Literal date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    /**
     * datetime arithmetic function day-of-week
     */
    @ExecFunction(name = "dayofweek", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateLiteral date) {
        return new IntegerLiteral(distanceToFirstDayOfWeek(date.getYear(), date.getMonth(), date.getDay()));
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateTimeLiteral date) {
        return new IntegerLiteral(distanceToFirstDayOfWeek(date.getYear(), date.getMonth(), date.getDay()));
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateV2Literal date) {
        return new IntegerLiteral(distanceToFirstDayOfWeek(date.getYear(), date.getMonth(), date.getDay()));
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateTimeV2Literal date) {
        return new IntegerLiteral(distanceToFirstDayOfWeek(date.getYear(), date.getMonth(), date.getDay()));
    }

    private static int distanceToFirstDayOfWeek(long year, long month, long day) {
        return LocalDate.of((int) year, (int) month, (int) day).getDayOfWeek().getValue() - 1;
    }

    private static long[] firstDayOfWeek(long year, long month, long day) {
        int distance = distanceToFirstDayOfWeek(year, month, day);
        DateLiteral temp = new DateLiteral(year, month, day).plusDays(-distance);
        return new long[] {temp.getYear(), temp.getMonth(), temp.getDay()};
    }

    /**
     * datetime arithmetic function date-trunc
     */
    @ExecFunction(name = "date_trunc", argTypes = {"DATETIME", "VARCHAR"}, returnType = "DATETIME")
    public static DateTimeLiteral dateTrunc(DateTimeLiteral date, VarcharLiteral trunc) {
        long[] timeTags = dateTruncHelper(date.getYear(), date.getMonth(), date.getDay(),
                date.getHour(), date.getMinute(), date.getSecond(), trunc.getValue());
        return new DateTimeLiteral(timeTags[0], timeTags[1], timeTags[2], timeTags[3], timeTags[4], timeTags[5]);
    }

    @ExecFunction(name = "date_trunc", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "DATETIMEV2")
    public static DateTimeV2Literal dateTrunc(DateTimeV2Literal date, VarcharLiteral trunc) {
        long[] timeTags = dateTruncHelper(date.getYear(), date.getMonth(), date.getDay(),
                date.getHour(), date.getMinute(), date.getSecond(), trunc.getValue());
        return new DateTimeV2Literal(timeTags[0], timeTags[1], timeTags[2], timeTags[3], timeTags[4], timeTags[5], 0);
    }

    private static long[] dateTruncHelper(long year, long month, long day, long hour, long minute, long second,
            String trunc) {
        switch (trunc.toLowerCase()) {
            case "year":
                month = 0;
            case "quarter": // CHECKSTYLE IGNORE THIS LINE
                month = ((month - 1) / 4) * 4 + 1;
            case "month": // CHECKSTYLE IGNORE THIS LINE
                day = 1;
                break;
            case "week":
                long[] ymd = firstDayOfWeek(year, month, day);
                year = ymd[0];
                month = ymd[1];
                day = ymd[2];
            default: // CHECKSTYLE IGNORE THIS LINE
                break;
        }
        switch (trunc.toLowerCase()) {
            default: // CHECKSTYLE IGNORE THIS LINE
            case "day": // CHECKSTYLE IGNORE THIS LINE
                hour = 0;
            case "hour": // CHECKSTYLE IGNORE THIS LINE
                minute = 0;
            case "minute": // CHECKSTYLE IGNORE THIS LINE
                second = 0;
        }
        return new long[] {year, month, day, hour, minute, second};
    }

    /**
     * datetime arithmetic function date-v2
     */
    @ExecFunction(name = "datev2", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static DateV2Literal dateV2(DateTimeV2Literal dateTime) {
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

}
