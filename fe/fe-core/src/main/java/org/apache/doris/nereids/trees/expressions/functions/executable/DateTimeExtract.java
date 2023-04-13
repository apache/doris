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

import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;

/**
 * executable function:
 * year, quarter, month, week, dayOfYear, dayOfweek, dayOfMonth, hour, minute, second
 */
public class DateTimeExtract {
    /**
     * Executable datetime extract year
     */
    @ExecFunction(name = "year", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral year(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral year(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral year(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral year(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract quarter
     */
    @ExecFunction(name = "quarter", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral quarter(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral quarter(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral quarter(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral quarter(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract month
     */
    @ExecFunction(name = "month", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral month(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "month", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral month(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "month", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral month(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "month", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral month(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract day
     */
    @ExecFunction(name = "day", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral day(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "day", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral day(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "day", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral day(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "day", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral day(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract dayofyear
     */
    @ExecFunction(name = "dayofyear", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    /**
     * Executable datetime extract dayofmonth
     */
    @ExecFunction(name = "dayofmonth", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    /**
     * Executable datetime extract dayofweek
     */
    @ExecFunction(name = "dayofweek", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    /**
     * Executable datetime extract week
     */
    @ExecFunction(name = "week", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral week(DateLiteral date) {
        return weekOfYear(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATE", "INT"}, returnType = "INT")
    public static IntegerLiteral week(DateLiteral date, IntegerLiteral mode) {
        return weekOfYear(date.toJavaDateType(), mode.getValue());
    }

    @ExecFunction(name = "week", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral week(DateTimeLiteral date) {
        return weekOfYear(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATETIME", "INT"}, returnType = "INT")
    public static IntegerLiteral week(DateTimeLiteral date, IntegerLiteral mode) {
        return weekOfYear(date.toJavaDateType(), mode.getValue());
    }

    @ExecFunction(name = "week", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral week(DateV2Literal date) {
        return weekOfYear(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATEV2", "INT"}, returnType = "INT")
    public static IntegerLiteral week(DateV2Literal date, IntegerLiteral mode) {
        return weekOfYear(date.toJavaDateType(), mode.getValue());
    }

    @ExecFunction(name = "week", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral week(DateTimeV2Literal date) {
        return weekOfYear(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATETIMEV2", "INT"}, returnType = "INT")
    public static IntegerLiteral week(DateTimeV2Literal date, IntegerLiteral mode) {
        return weekOfYear(date.toJavaDateType(), mode.getValue());
    }

    private static IntegerLiteral weekOfYear(LocalDateTime date, int mode) {
        if (mode < 0 || mode > 7) {
            return null;
        }
        WeekFields fields;
        if (mode == 0) {
            fields = WeekFields.of(DayOfWeek.SUNDAY, 7);
        } else if (mode == 1) {
            fields = WeekFields.of(DayOfWeek.MONDAY, 4);
        } else if (mode == 2) {
            fields = WeekFields.of(DayOfWeek.SUNDAY, 0);
        } else if (mode == 3) {
            fields = WeekFields.of(DayOfWeek.MONDAY, 4);
        } else if (mode == 4) {
            fields = WeekFields.of(DayOfWeek.SUNDAY, 4);
        } else if (mode == 5) {
            fields = WeekFields.of(DayOfWeek.MONDAY, 0);
        } else if (mode == 6) {
            fields = WeekFields.of(DayOfWeek.SUNDAY, 4);
        } else {
            fields = WeekFields.of(DayOfWeek.MONDAY, 7);
        }
        return new IntegerLiteral(((int) fields.weekOfYear().getFrom(date)));
    }
}
