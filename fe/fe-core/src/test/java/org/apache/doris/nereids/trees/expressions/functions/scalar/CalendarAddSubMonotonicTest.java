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

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.CalendarAddSubMonotonic;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class CalendarAddSubMonotonicTest {
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

    private List<CalendarAddSubMonotonic> calendarFunctions(SlotReference date) {
        IntegerLiteral one = new IntegerLiteral(1);
        return Arrays.asList(new MonthsAdd(date, one), new MonthsSub(date, one),
                new QuartersAdd(date, one), new QuartersSub(date, one),
                new YearsAdd(date, one), new YearsSub(date, one),
                new YearMonthAdd(date, new StringLiteral("0-1")),
                new YearMonthSub(date, new StringLiteral("0-1")));
    }
}
