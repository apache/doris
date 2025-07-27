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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExprId;
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
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuarterCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuarterFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersDiff;
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
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatetimeFunctionBinderTest {

    private final TinyIntLiteral tinyIntLiteral = new TinyIntLiteral((byte) 1);

    private final Interval yearInterval = new Interval(tinyIntLiteral, "YEAR");
    private final Interval quarterInterval = new Interval(tinyIntLiteral, "QUARTER");
    private final Interval monthInterval = new Interval(tinyIntLiteral, "MONTH");
    private final Interval weekInterval = new Interval(tinyIntLiteral, "WEEK");
    private final Interval dayInterval = new Interval(tinyIntLiteral, "DAY");
    private final Interval hourInterval = new Interval(tinyIntLiteral, "HOUR");
    private final Interval minuteInterval = new Interval(tinyIntLiteral, "MINUTE");
    private final Interval secondInterval = new Interval(tinyIntLiteral, "SECOND");

    private final SlotReference yearUnit = new SlotReference(new ExprId(-1), "YEAR",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference quarterUnit = new SlotReference(new ExprId(-1), "QUARTER",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference monthUnit = new SlotReference(new ExprId(-1), "MONTH",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference weekUnit = new SlotReference(new ExprId(-1), "WEEK",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference dayUnit = new SlotReference(new ExprId(-1), "DAY",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference hourUnit = new SlotReference(new ExprId(-1), "HOUR",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference minuteUnit = new SlotReference(new ExprId(-1), "MINUTE",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference secondUnit = new SlotReference(new ExprId(-1), "SECOND",
            TinyIntType.INSTANCE, false, ImmutableList.of());
    private final SlotReference invalidUnit = new SlotReference(new ExprId(-1), "INVALID",
            TinyIntType.INSTANCE, false, ImmutableList.of());

    private final DateTimeV2Literal dateTimeV2Literal1 = new DateTimeV2Literal("2024-12-01");
    private final DateTimeV2Literal dateTimeV2Literal2 = new DateTimeV2Literal("2024-12-26");

    @Test
    void testTimestampDiff() {
        Expression result;
        UnboundFunction timeDiff;
        ImmutableList<String> functionNames = ImmutableList.of("timestampdiff", "datediff");

        for (String functionName : functionNames) {
            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    yearUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(YearsDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    monthUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(MonthsDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    weekUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(WeeksDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    dayUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(DaysDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    hourUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(HoursDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    minuteUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(MinutesDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            timeDiff = new UnboundFunction(functionName, ImmutableList.of(
                    secondUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(SecondsDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(invalidUnit,
                                    dateTimeV2Literal1, dateTimeV2Literal2))));

            timeDiff = new UnboundFunction(functionName,
                    ImmutableList.of(quarterUnit, dateTimeV2Literal1, dateTimeV2Literal2));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
            Assertions.assertInstanceOf(QuartersDiff.class, result);
            Assertions.assertEquals(dateTimeV2Literal2, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal1, result.child(1));

            if (functionName.equalsIgnoreCase("datediff")) {
                timeDiff = new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1, dateTimeV2Literal2));
                result = DatetimeFunctionBinder.INSTANCE.bind(timeDiff);
                Assertions.assertInstanceOf(DateDiff.class, result);
                Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
                Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            } else {
                Assertions.assertThrowsExactly(AnalysisException.class,
                        () -> DatetimeFunctionBinder.INSTANCE.bind(
                                new UnboundFunction(functionName, ImmutableList.of(yearUnit,
                                        dateTimeV2Literal1))));
            }

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(new UnboundSlot("unbound"),
                                    dateTimeV2Literal1, dateTimeV2Literal2))));
        }
    }

    @Test
    void testTimestampAdd() {
        Expression result;
        UnboundFunction timeAdd;
        ImmutableList<String> functionNames = ImmutableList.of("timestampadd", "dateadd");

        for (String functionName : functionNames) {
            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    yearUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(YearsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    monthUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(MonthsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    weekUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(WeeksAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dayUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(DaysAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    hourUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(HoursAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    minuteUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(MinutesAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            timeAdd = new UnboundFunction(functionName, ImmutableList.of(
                    secondUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(timeAdd);
            Assertions.assertInstanceOf(SecondsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    invalidUnit, tinyIntLiteral, dateTimeV2Literal1))));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(yearUnit,
                                    dateTimeV2Literal1))));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(new UnboundSlot("unbound"),
                                    tinyIntLiteral, dateTimeV2Literal1))));
        }
    }

    @Test
    void testTwoArgsDateAdd() {
        Expression result;
        UnboundFunction dateAdd;
        ImmutableList<String> functionNames = ImmutableList.of("adddate", "days_add", "date_add");

        for (String functionName : functionNames) {
            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, yearInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(YearsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, quarterInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(QuartersAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, monthInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(MonthsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, weekInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(WeeksAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dayInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(DaysAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, hourInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(HoursAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, minuteInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(MinutesAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, secondInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(SecondsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(DaysAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1))));
        }
    }

    @Test
    void testThreeArgsDateAdd() {
        Expression result;
        UnboundFunction dateAdd;
        ImmutableList<String> functionNames = ImmutableList.of("adddate", "days_add", "date_add");

        for (String functionName : functionNames) {
            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    yearUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(YearsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    quarterUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(QuartersAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    monthUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(MonthsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    weekUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(WeeksAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    dayUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(DaysAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    hourUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(HoursAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    minuteUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(MinutesAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateAdd = new UnboundFunction(functionName, ImmutableList.of(
                    secondUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateAdd);
            Assertions.assertInstanceOf(SecondsAdd.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    invalidUnit, tinyIntLiteral, dateTimeV2Literal1))));

            // invalid expression type for first arg
            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    dateTimeV2Literal2, tinyIntLiteral, dateTimeV2Literal1))));
        }
    }

    @Test
    void testTwoArgsDateSub() {
        Expression result;
        UnboundFunction dateSub;
        ImmutableList<String> functionNames = ImmutableList.of("subdate", "days_sub", "date_sub");

        for (String functionName : functionNames) {
            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, yearInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(YearsSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, quarterInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(QuartersSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, monthInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(MonthsSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, weekInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(WeeksSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dayInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(DaysSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, hourInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(HoursSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, minuteInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(MinutesSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, secondInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(SecondsSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(DaysSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1))));
        }
    }

    @Test
    void testThreeArgsDateSub() {
        Expression result;
        UnboundFunction dateSub;
        ImmutableList<String> functionNames = ImmutableList.of("subdate", "days_sub", "date_sub");

        for (String functionName : functionNames) {
            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    yearUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(YearsSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    quarterUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(QuartersSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    monthUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(MonthsSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    weekUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(WeeksSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    dayUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(DaysSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    hourUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(HoursSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    minuteUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(MinutesSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateSub = new UnboundFunction(functionName, ImmutableList.of(
                    secondUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateSub);
            Assertions.assertInstanceOf(SecondsSub.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    invalidUnit, tinyIntLiteral, dateTimeV2Literal1))));

            // invalid expression type for first arg
            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    dateTimeV2Literal2, tinyIntLiteral, dateTimeV2Literal1))));
        }
    }

    @Test
    void testTwoArgsDateCeil() {
        Expression result;
        UnboundFunction dateCeil;
        ImmutableList<String> functionNames = ImmutableList.of("date_ceil");

        for (String functionName : functionNames) {
            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, yearInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(YearCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, monthInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(MonthCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, weekInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(WeekCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dayInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(DayCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, hourInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(HourCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, minuteInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(MinuteCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, secondInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(SecondCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(SecondCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1))));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1, quarterInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(QuarterCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));
        }
    }

    @Test
    void testThreeArgsDateCeil() {
        Expression result;
        UnboundFunction dateCeil;
        ImmutableList<String> functionNames = ImmutableList.of("date_ceil");

        for (String functionName : functionNames) {
            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    yearUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(YearCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    monthUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(MonthCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    weekUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(WeekCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    dayUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(DayCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    hourUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(HourCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    minuteUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(MinuteCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateCeil = new UnboundFunction(functionName, ImmutableList.of(
                    secondUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(SecondCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1))));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    invalidUnit, tinyIntLiteral, dateTimeV2Literal1))));

            dateCeil = new UnboundFunction(functionName,
                    ImmutableList.of(quarterUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateCeil);
            Assertions.assertInstanceOf(QuarterCeil.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            // invalid expression type for first arg
            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    dateTimeV2Literal2, tinyIntLiteral, dateTimeV2Literal1))));
        }
    }

    @Test
    void testTwoArgsDateFloor() {
        Expression result;
        UnboundFunction dateFloor;
        ImmutableList<String> functionNames = ImmutableList.of("date_floor");

        for (String functionName : functionNames) {
            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, yearInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(YearFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, monthInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(MonthFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, weekInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(WeekFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dayInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(DayFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, hourInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(HourFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, minuteInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(MinuteFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, secondInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(SecondFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(SecondFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1))));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(dateTimeV2Literal1, quarterInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(QuarterFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));
        }
    }

    @Test
    void testThreeArgsDateFloor() {
        Expression result;
        UnboundFunction dateFloor;
        ImmutableList<String> functionNames = ImmutableList.of("date_floor");

        for (String functionName : functionNames) {
            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    yearUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(YearFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    monthUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(MonthFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    weekUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(WeekFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    dayUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(DayFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    hourUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(HourFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    minuteUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(MinuteFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            dateFloor = new UnboundFunction(functionName, ImmutableList.of(
                    secondUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(SecondFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    invalidUnit, tinyIntLiteral, dateTimeV2Literal1))));

            dateFloor = new UnboundFunction(functionName,
                    ImmutableList.of(quarterUnit, tinyIntLiteral, dateTimeV2Literal1));
            result = DatetimeFunctionBinder.INSTANCE.bind(dateFloor);
            Assertions.assertInstanceOf(QuarterFloor.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            // invalid expression type for first arg
            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    dateTimeV2Literal2, tinyIntLiteral, dateTimeV2Literal1))));
        }
    }

    @Test
    void testArrayRange() {
        Expression result;
        UnboundFunction arrayRange;
        ImmutableList<String> functionNames = ImmutableList.of("array_range", "sequence");

        for (String functionName : functionNames) {
            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, yearInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeYearUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, monthInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeMonthUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, weekInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeWeekUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, dayInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeDayUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, hourInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeHourUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, minuteInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeMinuteUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    dateTimeV2Literal1, dateTimeV2Literal2, secondInterval));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRangeSecondUnit.class, result);
            Assertions.assertEquals(dateTimeV2Literal1, result.child(0));
            Assertions.assertEquals(dateTimeV2Literal2, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    tinyIntLiteral, tinyIntLiteral, tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRange.class, result);
            Assertions.assertEquals(tinyIntLiteral, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));
            Assertions.assertEquals(tinyIntLiteral, result.child(2));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(
                    tinyIntLiteral, tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRange.class, result);
            Assertions.assertEquals(tinyIntLiteral, result.child(0));
            Assertions.assertEquals(tinyIntLiteral, result.child(1));

            arrayRange = new UnboundFunction(functionName, ImmutableList.of(tinyIntLiteral));
            result = DatetimeFunctionBinder.INSTANCE.bind(arrayRange);
            Assertions.assertInstanceOf(ArrayRange.class, result);
            Assertions.assertEquals(tinyIntLiteral, result.child(0));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of())));

            Assertions.assertThrowsExactly(AnalysisException.class,
                    () -> DatetimeFunctionBinder.INSTANCE.bind(
                            new UnboundFunction(functionName, ImmutableList.of(
                                    tinyIntLiteral, tinyIntLiteral, tinyIntLiteral, tinyIntLiteral))));
        }
    }
}
