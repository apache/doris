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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeDayUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeHourUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeMinuteUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeMonthUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeSecondUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeWeekUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeYearUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsSub;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Interval;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

/**
 * bind arithmetic function
 */
public class DatetimeFunctionBinder {

    public static final DatetimeFunctionBinder INSTANCE = new DatetimeFunctionBinder();

    private static final String DATEDIFF = "DATEDIFF";

    private static final ImmutableSet<String> TIMESTAMP_DIFF_FUNCTION_NAMES
            = ImmutableSet.of("TIMESTAMPDIFF", DATEDIFF);
    private static final ImmutableSet<String> TIMESTAMP_ADD_FUNCTION_NAMES
            = ImmutableSet.of("TIMESTAMPADD", "DATEADD");
    public static final ImmutableSet<String> TIMESTAMP_SERIES_FUNCTION_NAMES
            = ImmutableSet.<String>builder()
            .addAll(TIMESTAMP_DIFF_FUNCTION_NAMES)
            .addAll(TIMESTAMP_ADD_FUNCTION_NAMES)
            .build();

    private static final ImmutableSet<String> ADD_DATE_FUNCTION_NAMES
            = ImmutableSet.of("ADDDATE", "DAYS_ADD", "DATE_ADD");
    private static final ImmutableSet<String> SUB_DATE_FUNCTION_NAMES
            = ImmutableSet.of("SUBDATE", "DAYS_SUB", "DATE_SUB");
    private static final ImmutableSet<String> DATE_ADD_SUB_SERIES_FUNCTION_NAMES
            = ImmutableSet.<String>builder()
            .addAll(ADD_DATE_FUNCTION_NAMES)
            .addAll(SUB_DATE_FUNCTION_NAMES)
            .build();
    private static final ImmutableSet<String> DATE_FLOOR_FUNCTION_NAMES
            = ImmutableSet.of("DATE_FLOOR");
    private static final ImmutableSet<String> DATE_CEIL_FUNCTION_NAMES
            = ImmutableSet.of("DATE_CEIL");
    private static final ImmutableSet<String> DATE_FLOOR_CEIL_SERIES_FUNCTION_NAMES
            = ImmutableSet.<String>builder()
            .addAll(DATE_FLOOR_FUNCTION_NAMES)
            .addAll(DATE_CEIL_FUNCTION_NAMES)
            .build();
    private static final ImmutableSet<String> DATE_SERIES_FUNCTION_NAMES
            = ImmutableSet.<String>builder()
            .addAll(DATE_ADD_SUB_SERIES_FUNCTION_NAMES)
            .addAll(DATE_FLOOR_CEIL_SERIES_FUNCTION_NAMES)
            .build();

    private static final ImmutableSet<String> ARRAY_RANGE_FUNCTION_NAMES
            = ImmutableSet.of("ARRAY_RANGE", "SEQUENCE");

    static final ImmutableSet<String> SUPPORT_DATETIME_ARITHMETIC_FUNCTION_NAMES
            = ImmutableSet.<String>builder()
            .addAll(TIMESTAMP_SERIES_FUNCTION_NAMES)
            .addAll(DATE_SERIES_FUNCTION_NAMES)
            .build();

    @VisibleForTesting
    static final ImmutableSet<String> SUPPORT_FUNCTION_NAMES
            = ImmutableSet.<String>builder()
            .addAll(SUPPORT_DATETIME_ARITHMETIC_FUNCTION_NAMES)
            .addAll(ARRAY_RANGE_FUNCTION_NAMES)
            .build();

    public static boolean isDatetimeFunction(String functionName) {
        return SUPPORT_FUNCTION_NAMES.contains(functionName.toUpperCase());
    }

    public static boolean isDatetimeArithmeticFunction(String functionName) {
        return SUPPORT_DATETIME_ARITHMETIC_FUNCTION_NAMES.contains(functionName.toUpperCase());
    }

    /**
     * bind datetime functions that have non-expression arguments.
     *
     * @param unboundFunction unbound datetime function
     *
     * @return bound function
     */
    public Expression bind(UnboundFunction unboundFunction) {
        String functionName = unboundFunction.getName().toUpperCase();
        if (TIMESTAMP_SERIES_FUNCTION_NAMES.contains(functionName)) {
            if (unboundFunction.arity() == 2 && functionName.equals(DATEDIFF)) {
                return new DateDiff(unboundFunction.child(0), unboundFunction.child(1));
            } else if (unboundFunction.arity() != 3
                    || !(unboundFunction.child(0) instanceof SlotReference)) {
                throw new AnalysisException("Can not found function '" + functionName
                        + "' with " + unboundFunction.arity() + " arguments");
            }
            String unitName = ((SlotReference) unboundFunction.child(0)).getName().toUpperCase();
            TimeUnit unit;
            try {
                unit = TimeUnit.valueOf(unitName);
            } catch (IllegalArgumentException e) {
                throw new AnalysisException("Unsupported time stamp diff time unit: " + unitName
                        + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
            }
            if (TIMESTAMP_DIFF_FUNCTION_NAMES.contains(functionName)) {
                // timestampdiff(unit, start, end)
                return processTimestampDiff(unit, unboundFunction.child(1), unboundFunction.child(2));
            } else if (TIMESTAMP_ADD_FUNCTION_NAMES.contains(functionName)) {
                // timestampadd(unit, amount, base_time)
                return processDateAdd(unit, unboundFunction.child(2), unboundFunction.child(1));
            } else {
                throw new AnalysisException("Can not found function '" + functionName
                        + "' with " + unboundFunction.arity() + " arguments");
            }
        } else if (DATE_SERIES_FUNCTION_NAMES.contains(functionName)) {
            TimeUnit unit = TimeUnit.DAY;
            // date_add and date_sub's default unit is DAY, date_ceil and date_floor's default unit is SECOND
            if (DATE_FLOOR_CEIL_SERIES_FUNCTION_NAMES.contains(functionName)) {
                unit = TimeUnit.SECOND;
            }
            Expression base;
            Expression amount;
            switch (unboundFunction.arity()) {
                case 2:
                    // function_name(base_time, amount / interval)
                    amount = unboundFunction.child(1);
                    if (unboundFunction.child(1) instanceof Interval) {
                        Interval interval = (Interval) unboundFunction.child(1);
                        unit = interval.timeUnit();
                        amount = interval.value();
                    }
                    base = unboundFunction.child(0);
                    break;
                case 3:
                    // function_name(unit, amount, base_time)
                    if (!(unboundFunction.child(0) instanceof SlotReference)) {
                        throw new AnalysisException("Can not found function '" + functionName
                                + "' with " + unboundFunction.arity() + " arguments");
                    }
                    amount = unboundFunction.child(1);
                    base = unboundFunction.child(2);
                    String unitName = ((SlotReference) unboundFunction.child(0)).getName().toUpperCase();
                    try {
                        unit = TimeUnit.valueOf(unitName);
                    } catch (IllegalArgumentException e) {
                        throw new AnalysisException("Unsupported time stamp diff time unit: " + unitName
                                + ", supported time unit: YEAR/QUARTER/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
                    }
                    break;
                default:
                    throw new AnalysisException("Can not found function '" + functionName
                            + "' with " + unboundFunction.arity() + " arguments");
            }
            if (ADD_DATE_FUNCTION_NAMES.contains(functionName)) {
                // date_add(date, interval amount unit | amount)
                return processDateAdd(unit, base, amount);
            } else if (SUB_DATE_FUNCTION_NAMES.contains(functionName)) {
                // date_add(date, interval amount unit | amount)
                return processDateSub(unit, base, amount);
            } else if (DATE_FLOOR_FUNCTION_NAMES.contains(functionName)) {
                // date_floor(date, interval amount unit | amount)
                return processDateFloor(unit, base, amount);
            } else if (DATE_CEIL_FUNCTION_NAMES.contains(functionName)) {
                // date_ceil(date, interval amount unit | amount)
                return processDateCeil(unit, base, amount);
            } else {
                throw new AnalysisException("Can not found function '" + functionName
                        + "' with " + unboundFunction.arity() + " arguments");
            }
        } else if (ARRAY_RANGE_FUNCTION_NAMES.contains(functionName)) {
            switch (unboundFunction.arity()) {
                case 1:
                    return new ArrayRange(unboundFunction.child(0));
                case 2:
                    return new ArrayRange(unboundFunction.child(0), unboundFunction.child(1));
                case 3:
                    if (unboundFunction.child(2) instanceof Interval) {
                        Interval interval = (Interval) unboundFunction.child(2);
                        TimeUnit unit = interval.timeUnit();
                        Expression step = interval.value();
                        return processArrayRange(unit, unboundFunction.child(0), unboundFunction.child(1), step);
                    }
                    return new ArrayRange(unboundFunction.child(0),
                            unboundFunction.child(1), unboundFunction.child(2));
                default:
                    throw new AnalysisException("Can not found function '" + functionName + "'");
            }
        }
        throw new AnalysisException("Can not found function '" + functionName + "'");
    }

    private Expression processTimestampDiff(TimeUnit unit, Expression start, Expression end) {
        switch (unit) {
            case YEAR:
                return new YearsDiff(end, start);
            case MONTH:
                return new MonthsDiff(end, start);
            case WEEK:
                return new WeeksDiff(end, start);
            case DAY:
                return new DaysDiff(end, start);
            case HOUR:
                return new HoursDiff(end, start);
            case MINUTE:
                return new MinutesDiff(end, start);
            case SECOND:
                return new SecondsDiff(end, start);
            default:
                throw new AnalysisException("Unsupported time stamp diff time unit: " + unit
                        + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
        }
    }

    private Expression processDateAdd(TimeUnit unit, Expression timestamp, Expression amount) {
        switch (unit) {
            case YEAR:
                return new YearsAdd(timestamp, amount);
            case QUARTER:
                return new QuartersAdd(timestamp, amount);
            case MONTH:
                return new MonthsAdd(timestamp, amount);
            case WEEK:
                return new WeeksAdd(timestamp, amount);
            case DAY:
                return new DaysAdd(timestamp, amount);
            case HOUR:
                return new HoursAdd(timestamp, amount);
            case MINUTE:
                return new MinutesAdd(timestamp, amount);
            case SECOND:
                return new SecondsAdd(timestamp, amount);
            default:
                throw new AnalysisException("Unsupported time stamp add time unit: " + unit
                        + ", supported time unit: YEAR/QUARTER/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
        }
    }

    private Expression processDateSub(TimeUnit unit, Expression timeStamp, Expression amount) {
        switch (unit) {
            case YEAR:
                return new YearsSub(timeStamp, amount);
            case QUARTER:
                return new QuartersSub(timeStamp, amount);
            case MONTH:
                return new MonthsSub(timeStamp, amount);
            case WEEK:
                return new WeeksSub(timeStamp, amount);
            case DAY:
                return new DaysSub(timeStamp, amount);
            case HOUR:
                return new HoursSub(timeStamp, amount);
            case MINUTE:
                return new MinutesSub(timeStamp, amount);
            case SECOND:
                return new SecondsSub(timeStamp, amount);
            default:
                throw new AnalysisException("Unsupported time stamp sub time unit: " + unit
                        + ", supported time unit: YEAR/QUARTER/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
        }
    }

    private Expression processDateFloor(TimeUnit unit, Expression timeStamp, Expression amount) {
        DateTimeV2Literal e = DateTimeV2Literal.USE_IN_FLOOR_CEIL;
        switch (unit) {
            case YEAR:
                return new YearFloor(timeStamp, amount, e);
            case MONTH:
                return new MonthFloor(timeStamp, amount, e);
            case WEEK:
                return new WeekFloor(timeStamp, amount, e);
            case DAY:
                return new DayFloor(timeStamp, amount, e);
            case HOUR:
                return new HourFloor(timeStamp, amount, e);
            case MINUTE:
                return new MinuteFloor(timeStamp, amount, e);
            case SECOND:
                return new SecondFloor(timeStamp, amount, e);
            default:
                throw new AnalysisException("Unsupported time stamp floor time unit: " + unit
                        + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
        }
    }

    private Expression processDateCeil(TimeUnit unit, Expression timeStamp, Expression amount) {
        DateTimeV2Literal e = DateTimeV2Literal.USE_IN_FLOOR_CEIL;
        switch (unit) {
            case YEAR:
                return new YearCeil(timeStamp, amount, e);
            case MONTH:
                return new MonthCeil(timeStamp, amount, e);
            case WEEK:
                return new WeekCeil(timeStamp, amount, e);
            case DAY:
                return new DayCeil(timeStamp, amount, e);
            case HOUR:
                return new HourCeil(timeStamp, amount, e);
            case MINUTE:
                return new MinuteCeil(timeStamp, amount, e);
            case SECOND:
                return new SecondCeil(timeStamp, amount, e);
            default:
                throw new AnalysisException("Unsupported time stamp ceil time unit: " + unit
                        + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
        }
    }

    private Expression processArrayRange(TimeUnit unit, Expression start, Expression end, Expression step) {
        switch (unit) {
            case YEAR:
                return new ArrayRangeYearUnit(start, end, step);
            case MONTH:
                return new ArrayRangeMonthUnit(start, end, step);
            case WEEK:
                return new ArrayRangeWeekUnit(start, end, step);
            case DAY:
                return new ArrayRangeDayUnit(start, end, step);
            case HOUR:
                return new ArrayRangeHourUnit(start, end, step);
            case MINUTE:
                return new ArrayRangeMinuteUnit(start, end, step);
            case SECOND:
                return new ArrayRangeSecondUnit(start, end, step);
            default:
                throw new AnalysisException("Unsupported array range time unit: " + unit
                        + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND");
        }
    }

}
