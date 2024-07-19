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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import java.time.LocalDateTime;

/**
 * executable functions:
 * year/month/day/hour/minute/second_ceil/floor
 */
@Developing
public class TimeRoundSeries {
    private static final LocalDateTime START_ORIGINAL_DAY = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private static final LocalDateTime START_ORIGINAL_WEEK = LocalDateTime.of(1970, 1, 4, 0, 0, 0);

    enum DATE {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    // get it's from be/src/vec/functions/function_datetime_floor_ceil.cpp##time_round
    private static LocalDateTime getDateCeilOrFloor(DATE tag, LocalDateTime date, int period, LocalDateTime origin,
            boolean getCeil) {
        DateTimeV2Literal dt = (DateTimeV2Literal) DateTimeV2Literal.fromJavaDateType(date);
        DateTimeV2Literal start = (DateTimeV2Literal) DateTimeV2Literal.fromJavaDateType(origin);
        long diff = 0;
        long trivialPart = 0;
        switch (tag) {
            case YEAR: {
                diff = dt.getYear() - start.getYear();
                trivialPart = (dt.getValue() % 10000000000L) - (start.getValue() % 10000000000L);
                break;
            }
            case MONTH: {
                diff = (dt.getYear() - start.getYear()) * 12 + (dt.getMonth() - start.getMonth());
                trivialPart = (dt.getValue() % 100000000L) - (start.getValue() % 100000000L);
                break;
            }
            case DAY: {
                diff = dt.getTotalDays() - start.getTotalDays();
                long part2 = dt.getHour() * 3600 + dt.getMinute() * 60 + dt.getSecond();
                long part1 = start.getHour() * 3600 + start.getMinute() * 60 + start.getSecond();
                trivialPart = part2 - part1;
                break;
            }
            case HOUR: {
                diff = (dt.getTotalDays() - start.getTotalDays()) * 24 + (dt.getHour() - start.getHour());
                trivialPart = (dt.getMinute() * 60 + dt.getSecond())
                        - (start.getMinute() * 60 + start.getSecond());
                break;
            }
            case MINUTE: {
                diff = (dt.getTotalDays() - start.getTotalDays()) * 24 * 60 + (dt.getHour() - start.getHour()) * 60
                        + (dt.getMinute() - start.getMinute());
                trivialPart = dt.getSecond() - start.getSecond();
                break;
            }
            case SECOND: {
                diff = (dt.getTotalDays() - start.getTotalDays()) * 24 * 60 * 60
                        + (dt.getHour() - start.getHour()) * 60 * 60
                        + (dt.getMinute() - start.getMinute()) * 60
                        + (dt.getSecond() - start.getSecond());
                trivialPart = dt.getMicroSecond() - start.getMicroSecond();
                break;
            }
            default: {
                return null;
            }
        }
        if (getCeil) {
            diff = diff + (trivialPart > 0 ? 1 : 0);
        } else {
            diff = diff - (trivialPart < 0 ? 1 : 0);
        }
        long deltaInsidePeriod = (diff % period + period) % period;
        long step = diff - deltaInsidePeriod;
        if (getCeil) {
            step = step + (deltaInsidePeriod == 0 ? 0 : period);
        }
        switch (tag) {
            case YEAR:
                return ((DateTimeLiteral) start.plusYears(step)).toJavaDateType();
            case MONTH:
                return ((DateTimeLiteral) start.plusMonths(step)).toJavaDateType();
            case DAY:
                return ((DateTimeLiteral) start.plusDays(step)).toJavaDateType();
            case HOUR:
                return ((DateTimeLiteral) start.plusHours(step)).toJavaDateType();
            case MINUTE:
                return ((DateTimeLiteral) start.plusMinutes(step)).toJavaDateType();
            case SECOND:
                return ((DateTimeLiteral) start.plusSeconds(step)).toJavaDateType();
            default:
                break;
        }
        return null;
    }

    /**
     * datetime arithmetic function year-ceil
     */
    @ExecFunction(name = "year_ceil", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression yearCeil(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression yearCeil(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression yearCeil(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression yearCeil(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression yearCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression yearCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression yearCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression yearCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression yearCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression yearCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression yearCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression yearCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    /**
     * datetime arithmetic function month-ceil
     */
    @ExecFunction(name = "month_ceil", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression monthCeil(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression monthCeil(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression monthCeil(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression monthCeil(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression monthCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression monthCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression monthCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression monthCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression monthCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression monthCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression monthCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression monthCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    /**
     * datetime arithmetic function day-ceil
     */
    @ExecFunction(name = "day_ceil", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression dayCeil(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression dayCeil(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression dayCeil(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression dayCeil(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression dayCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression dayCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression dayCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression dayCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression dayCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression dayCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression dayCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression dayCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    /**
     * datetime arithmetic function hour-ceil
     */
    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression hourCeil(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression hourCeil(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression hourCeil(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression hourCeil(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression hourCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression hourCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression hourCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression hourCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression hourCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression hourCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression hourCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression hourCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    /**
     * datetime arithmetic function minute-ceil
     */
    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression minuteCeil(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression minuteCeil(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression minuteCeil(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression minuteCeil(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression minuteCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression minuteCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression minuteCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression minuteCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression minuteCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression minuteCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression minuteCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression minuteCeil(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    /**
     * datetime arithmetic function SECOND-ceil
     */
    @ExecFunction(name = "second_ceil", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression secondCeil(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression secondCeil(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression secondCeil(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression secondCeil(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression secondCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression secondCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression secondCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression secondCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression secondCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression secondCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression secondCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression secondCeil(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    /**
     * datetime arithmetic function year-floor
     */
    @ExecFunction(name = "year_floor", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression yearFloor(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression yearFloor(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression yearFloor(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression yearFloor(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression yearFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression yearFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression yearFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression yearFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression yearFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression yearFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression yearFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression yearFloor(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    /**
     * datetime arithmetic function month-floor
     */
    @ExecFunction(name = "month_floor", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression monthFloor(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression monthFloor(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression monthFloor(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression monthFloor(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression monthFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression monthFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression monthFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression monthFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression monthFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression monthFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression monthFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression monthFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    /**
     * datetime arithmetic function day-floor
     */
    @ExecFunction(name = "day_floor", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression dayFloor(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression dayFloor(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression dayFloor(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression dayFloor(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression dayFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression dayFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression dayFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression dayFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression dayFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression dayFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression dayFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression dayFloor(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    /**
     * datetime arithmetic function hour-floor
     */
    @ExecFunction(name = "hour_floor", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression hourFloor(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression hourFloor(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression hourFloor(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression hourFloor(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression hourFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression hourFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression hourFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression hourFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression hourFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression hourFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression hourFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression hourFloor(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    /**
     * datetime arithmetic function minute-floor
     */
    @ExecFunction(name = "minute_floor", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression minuteFloor(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression minuteFloor(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression minuteFloor(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression minuteFloor(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression minuteFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression minuteFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression minuteFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression minuteFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression minuteFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression minuteFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression minuteFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression minuteFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    /**
     * datetime arithmetic function SECOND-floor
     */
    @ExecFunction(name = "second_floor", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression secondFloor(DateTimeLiteral date) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static Expression secondFloor(DateTimeLiteral date, IntegerLiteral period) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME")
    public static Expression secondFloor(DateTimeLiteral date, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIME", "INT", "DATETIME"}, returnType = "DATETIME")
    public static Expression secondFloor(DateTimeLiteral date, IntegerLiteral period, DateTimeLiteral origin) {
        return DateTimeLiteral.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression secondFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATEV2", "INT"}, returnType = "DATEV2")
    public static Expression secondFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATEV2", "DATEV2"}, returnType = "DATEV2")
    public static Expression secondFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATEV2", "INT", "DATEV2"}, returnType = "DATEV2")
    public static Expression secondFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression secondFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIMEV2", "INT"}, returnType = "DATETIMEV2")
    public static Expression secondFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression secondFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor", argTypes = {"DATETIMEV2", "INT", "DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression secondFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }
}
