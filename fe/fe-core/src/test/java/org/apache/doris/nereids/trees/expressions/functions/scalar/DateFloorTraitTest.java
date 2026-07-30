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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class DateFloorTraitTest {
    private final SlotReference dt = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);

    @Test
    void dateIsFloor() {
        Assertions.assertTrue(new Date(dt).isFloor());
    }

    @Test
    void toDateIsFloor() {
        Assertions.assertTrue(new ToDate(dt).isFloor());
    }

    @Test
    void dateFloorUpperBoundIsPlusOneDay() {
        Optional<Expression> ub = new Date(dt).floorUpperBound(new DateTimeV2Literal("2026-03-26 00:00:00"));
        Assertions.assertTrue(ub.isPresent() && ub.get() instanceof DaysAdd);
    }

    @Test
    void toDateFloorUpperBoundIsPlusOneDay() {
        Optional<Expression> ub = new ToDate(dt).floorUpperBound(new DateTimeV2Literal("2026-03-26 00:00:00"));
        Assertions.assertTrue(ub.isPresent() && ub.get() instanceof DaysAdd);
    }

    @Test
    void dateTruncFloorUnitIsFloor() {
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("month")).isFloor());
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("day")).isFloor());
    }

    @Test
    void dateTruncUnknownUnitIsNotFloor() {
        // a unit outside the floor whitelist must not be treated as floor
        Assertions.assertFalse(new DateTrunc(dt, new VarcharLiteral("nanosecond")).isFloor());
    }

    // per-unit preimage upper bound: each date_trunc unit must map to +1 of the SAME granularity.
    // A wrong mapping here would prune rows that must be kept, so every unit is asserted.
    @Test
    void dateTruncUpperBoundPerUnit() {
        DateTimeV2Literal c = new DateTimeV2Literal("2026-03-01 00:00:00");
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("year")).floorUpperBound(c).get() instanceof YearsAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("quarter"))
                .floorUpperBound(c).get() instanceof QuartersAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("month"))
                .floorUpperBound(c).get() instanceof MonthsAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("week")).floorUpperBound(c).get() instanceof WeeksAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("day")).floorUpperBound(c).get() instanceof DaysAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("hour")).floorUpperBound(c).get() instanceof HoursAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("minute"))
                .floorUpperBound(c).get() instanceof MinutesAdd);
        Assertions.assertTrue(new DateTrunc(dt, new VarcharLiteral("second"))
                .floorUpperBound(c).get() instanceof SecondsAdd);
    }

    @Test
    void dateTruncUnknownUnitNoUpperBound() {
        DateTimeV2Literal c = new DateTimeV2Literal("2026-03-01 00:00:00");
        Assertions.assertFalse(new DateTrunc(dt, new VarcharLiteral("nanosecond")).floorUpperBound(c).isPresent());
    }

    // ---- *_floor family + to_monday: isFloor + per-granularity upper bound ----

    @Test
    void floorFunctionsAreFloor() {
        Assertions.assertTrue(new SecondFloor(dt).isFloor());
        Assertions.assertTrue(new MinuteFloor(dt).isFloor());
        Assertions.assertTrue(new HourFloor(dt).isFloor());
        Assertions.assertTrue(new DayFloor(dt).isFloor());
        Assertions.assertTrue(new MonthFloor(dt).isFloor());
        Assertions.assertTrue(new YearFloor(dt).isFloor());
        Assertions.assertTrue(new QuarterFloor(dt).isFloor());
        Assertions.assertTrue(new ToMonday(dt).isFloor());
    }

    // one-arg floor: upper bound is +1 of that granularity
    @Test
    void floorUpperBoundPerGranularity() {
        DateTimeV2Literal c = new DateTimeV2Literal("2026-03-01 00:00:00");
        Assertions.assertTrue(new SecondFloor(dt).floorUpperBound(c).get() instanceof SecondsAdd);
        Assertions.assertTrue(new MinuteFloor(dt).floorUpperBound(c).get() instanceof MinutesAdd);
        Assertions.assertTrue(new HourFloor(dt).floorUpperBound(c).get() instanceof HoursAdd);
        Assertions.assertTrue(new DayFloor(dt).floorUpperBound(c).get() instanceof DaysAdd);
        Assertions.assertTrue(new MonthFloor(dt).floorUpperBound(c).get() instanceof MonthsAdd);
        Assertions.assertTrue(new YearFloor(dt).floorUpperBound(c).get() instanceof YearsAdd);
        Assertions.assertTrue(new QuarterFloor(dt).floorUpperBound(c).get() instanceof QuartersAdd);
        Assertions.assertTrue(new ToMonday(dt).floorUpperBound(c).get() instanceof WeeksAdd);
    }

    // two-arg floor with a literal period: upper bound = +period of that granularity (present)
    @Test
    void floorWithLiteralPeriodHasUpperBound() {
        DateTimeV2Literal c = new DateTimeV2Literal("2026-03-01 00:00:00");
        Assertions.assertTrue(new DayFloor(dt, new IntegerLiteral(5)).floorUpperBound(c).get() instanceof DaysAdd);
    }

    // three-arg floor with origin: bucket alignment shifts, no engine reverse-solves it -> fail open
    // (lower bound stays sound, upper bound not emitted)
    @Test
    void floorWithOriginNoUpperBound() {
        DateTimeV2Literal c = new DateTimeV2Literal("2026-03-01 00:00:00");
        DateTimeV2Literal origin = new DateTimeV2Literal("2020-01-01 00:00:00");
        Assertions.assertFalse(new DayFloor(dt, new IntegerLiteral(5), origin).floorUpperBound(c).isPresent());
    }
}
