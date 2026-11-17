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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.ColumnRange;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.CalendarAddSubMonotonic;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

class CalendarAddSubMonotonicTest {
    private ConnectContext previousContext;
    private ExpressionRewriteContext context;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setTimeZone("+00:00");
        connectContext.setThreadLocalInfo();
        context = new ExpressionRewriteContext(CascadesContext.initTempContext());
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void testClampingCanResetTimeAcrossDays() {
        List<DataType> types = Arrays.asList(DateTimeV2Type.SYSTEM_DEFAULT,
                TimeStampNsType.INSTANCE, TimeStampTzType.SYSTEM_DEFAULT);
        List<Literal> lowerBounds = Arrays.asList(new DateTimeV2Literal("2023-01-28 10:00:00"),
                new TimeStampNsLiteral("2023-01-28 10:00:00"), new TimestampTzLiteral("2023-01-28 10:00:00"));
        List<Literal> upperBounds = Arrays.asList(new DateTimeV2Literal("2023-01-31 23:59:59"),
                new TimeStampNsLiteral("2023-01-31 23:59:59"), new TimestampTzLiteral("2023-01-31 23:59:59"));
        for (int i = 0; i < types.size(); i++) {
            for (CalendarAddSubMonotonic function : calendarFunctions(new SlotReference("dt", types.get(i)))) {
                Literal lower = lowerBounds.get(i);
                Assertions.assertFalse(function.isMonotonic(lower, upperBounds.get(i)));
                Assertions.assertFalse(function.isMonotonic(lower, null));
                Assertions.assertFalse(function.isMonotonic(null, upperBounds.get(i)));
                Assertions.assertTrue(function.isMonotonic(lower, lower));
            }
        }
    }

    @Test
    void testDateOnlyAndSingleDayRemainMonotonic() {
        for (CalendarAddSubMonotonic function : calendarFunctions(new SlotReference("d", DateV2Type.INSTANCE))) {
            Assertions.assertTrue(function.isMonotonic(
                    new DateV2Literal("2023-01-28"), new DateV2Literal("2023-01-31")));
            Assertions.assertTrue(function.isMonotonic(null, null));
        }
        for (CalendarAddSubMonotonic function : calendarFunctions(
                new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT))) {
            Assertions.assertTrue(function.isMonotonic(new DateTimeV2Literal("2023-01-29 00:00:00"),
                    new DateTimeV2Literal("2023-01-29 23:59:59")));
        }
    }

    @Test
    void testVariableIntervalsAreNotMonotonic() {
        SlotReference date = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        Assertions.assertFalse(new MonthsAdd(date, new SlotReference("months", IntegerType.INSTANCE)).isMonotonic(
                new DateTimeV2Literal("2023-01-29 00:00:00"), new DateTimeV2Literal("2023-01-29 23:59:59")));
    }

    @Test
    void testClampedRangeBoundsPreserveTypesAndPrecision() {
        List<DateLiteral> inputs = calendarLiterals("2023-01-28 10:00:00.123456");
        List<DateLiteral> outputs = calendarLiterals("2023-02-28 10:00:00.123456");
        List<DateLiteral> clampedInputs = calendarLiterals("2023-01-31 10:00:00.123456");
        for (int i = 0; i < inputs.size(); i++) {
            CalendarAddSubMonotonic function = new MonthsAdd(
                    new SlotReference("dt", inputs.get(i).getDataType()), new IntegerLiteral(1));
            DateLiteral output = outputs.get(i);
            Literal lower = function.getRangeLowerBound(inputs.get(i), output);
            Literal upper = function.getRangeUpperBound(clampedInputs.get(i), output);
            Assertions.assertEquals(output.getDataType(), lower.getDataType());
            Assertions.assertEquals(output.getDataType(), upper.getDataType());
            Assertions.assertEquals(output.toJavaDateType().with(LocalTime.MIN),
                    ((DateLiteral) lower).toJavaDateType());
            int maxNano = output instanceof TimeStampNsLiteral ? 999999999 : 999999000;
            Assertions.assertEquals(output.toJavaDateType().with(LocalTime.of(23, 59, 59, maxNano)),
                    ((DateLiteral) upper).toJavaDateType());
            // The last source day has no later day that can reset its lower bound's clock.
            Assertions.assertSame(output, function.getRangeLowerBound(clampedInputs.get(i), output));
            // The first day mapping to the last destination day has no earlier collapsed day.
            Assertions.assertSame(output, function.getRangeUpperBound(inputs.get(i), output));
        }
    }

    @Test
    void testOrdinaryMonthBoundariesKeepExactBounds() {
        List<DateLiteral> inputs = calendarLiterals("2023-01-01 10:00:00.123456");
        List<DateLiteral> outputs = calendarLiterals("2023-02-01 10:00:00.123456");
        for (int i = 0; i < inputs.size(); i++) {
            CalendarAddSubMonotonic function = new MonthsAdd(
                    new SlotReference("dt", inputs.get(i).getDataType()), new IntegerLiteral(1));
            Assertions.assertTrue(function.canEvaluateRange());
            Assertions.assertSame(outputs.get(i), function.getRangeLowerBound(inputs.get(i), outputs.get(i)));
            Assertions.assertSame(outputs.get(i), function.getRangeUpperBound(inputs.get(i), outputs.get(i)));
        }
    }

    @Test
    void testRangeInferenceDoesNotImplyMonotonicity() {
        List<DateLiteral> lowerBounds = calendarLiterals("2023-01-28 10:00:00.123456");
        List<DateLiteral> upperBounds = calendarLiterals("2023-01-31 00:00:00.123456");
        for (int i = 0; i < lowerBounds.size(); i++) {
            DateLiteral lower = lowerBounds.get(i);
            DateLiteral upper = upperBounds.get(i);
            CalendarAddSubMonotonic function = new MonthsAdd(
                    new SlotReference("dt", lower.getDataType()), new IntegerLiteral(1));
            Assertions.assertFalse(function.isMonotonic(lower, upper));
            ColumnRange range = function.inferRange(lower, upper, context).get();
            Assertions.assertEquals(lower.getDataType(), range.getLowerBound().getValue().getDataType());
            Assertions.assertEquals(upper.getDataType(), range.getUpperBound().getValue().getDataType());
            Assertions.assertEquals(LocalDateTime.of(2023, 2, 28, 0, 0),
                    ((DateLiteral) range.getLowerBound().getValue()).toJavaDateType());
            int maxNano = lower instanceof TimeStampNsLiteral ? 999999999 : 999999000;
            Assertions.assertEquals(LocalDateTime.of(2023, 2, 28, 23, 59, 59, maxNano),
                    ((DateLiteral) range.getUpperBound().getValue()).toJavaDateType());
            Assertions.assertFalse(function.inferRange(lower, null, context).get().span().hasUpperBound());
            Assertions.assertFalse(function.inferRange(null, upper, context).get().span().hasLowerBound());
        }
    }

    @Test
    void testDateOnlyAndSameDayInferenceKeepsEndpointTimes() {
        CalendarAddSubMonotonic dateOnly = new MonthsAdd(
                new SlotReference("d", DateV2Type.INSTANCE), new IntegerLiteral(1));
        ColumnRange dateRange = dateOnly.inferRange(new DateV2Literal("2023-01-28"),
                new DateV2Literal("2023-01-31"), context).get();
        Assertions.assertTrue(dateRange.isSingleton());
        Assertions.assertEquals(new DateV2Literal("2023-02-28"), dateRange.getLowerBound().getValue());

        DateLiteral lower = new DateTimeV2Literal("2023-01-29 10:00:00");
        DateLiteral upper = new DateTimeV2Literal("2023-01-29 11:00:00");
        CalendarAddSubMonotonic sameDay = new MonthsAdd(
                new SlotReference("dt", lower.getDataType()), new IntegerLiteral(1));
        ColumnRange range = sameDay.inferRange(lower, upper, context).get();
        Assertions.assertEquals(new DateTimeV2Literal("2023-02-28 10:00:00"), range.getLowerBound().getValue());
        Assertions.assertEquals(new DateTimeV2Literal("2023-02-28 11:00:00"), range.getUpperBound().getValue());
    }

    @Test
    void testRangeEnclosesMonthEndAndLeapDayValues() {
        String[][] ranges = {
                {"2023-01-28T10:00:00", "2023-01-31T23:00:00"},
                {"2023-01-27T00:00:00", "2023-01-31T00:00:00"},
                {"2023-03-28T10:00:00", "2023-03-31T23:00:00"},
                {"2024-02-28T10:00:00", "2024-02-29T23:00:00"},
                {"2023-01-01T00:00:00", "2024-01-01T00:00:00"}
        };
        for (String[] range : ranges) {
            LocalDateTime start = LocalDateTime.parse(range[0]);
            LocalDateTime end = LocalDateTime.parse(range[1]);
            for (int months : new int[] {-12, -3, -1, 0, 1, 3, 12}) {
                for (int type = 0; type < 3; type++) {
                    DateLiteral inputLower = calendarLiterals(start.toString()).get(type);
                    DateLiteral inputUpper = calendarLiterals(end.toString()).get(type);
                    CalendarAddSubMonotonic function = new MonthsAdd(
                            new SlotReference("dt", inputLower.getDataType()), new IntegerLiteral(months));
                    ColumnRange rangeResult = function.inferRange(inputLower, inputUpper, context).get();
                    LocalDateTime lower = ((DateLiteral) rangeResult.getLowerBound().getValue()).toJavaDateType();
                    LocalDateTime upper = ((DateLiteral) rangeResult.getUpperBound().getValue()).toJavaDateType();
                    for (LocalDateTime value = start; !value.isAfter(end); value = value.plusHours(1)) {
                        LocalDateTime shifted = value.plusMonths(months);
                        Assertions.assertFalse(shifted.isBefore(lower));
                        Assertions.assertFalse(shifted.isAfter(upper));
                    }
                }
            }
        }
    }

    private List<DateLiteral> calendarLiterals(String value) {
        return Arrays.asList(new DateTimeV2Literal(DateTimeV2Type.MAX, value),
                new TimeStampNsLiteral(value), new TimestampTzLiteral(TimeStampTzType.of(6), value));
    }

    private List<CalendarAddSubMonotonic> calendarFunctions(SlotReference date) {
        IntegerLiteral one = new IntegerLiteral(1);
        return Arrays.asList(new MonthsAdd(date, one), new MonthsSub(date, one),
                new QuartersAdd(date, one), new QuartersSub(date, one),
                new YearsAdd(date, one), new YearsSub(date, one),
                new YearMonthAdd(date, new StringLiteral("0-1")),
                new YearMonthSub(date, new StringLiteral("0-1")));
    }
}
