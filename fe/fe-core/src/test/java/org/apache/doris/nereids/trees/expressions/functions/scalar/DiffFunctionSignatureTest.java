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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignature;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Regression tests for apache/doris#66120.
 *
 * <p>All 12 Nereids {@code *_diff} scalar functions used to list their TIMESTAMPTZ signature
 * FIRST in SIGNATURES. {@code SearchSignature}'s timezone-coercion tie-break only fires for
 * inspectable literals ({@code ExpressionUtils.getLiteralAfterUnwrapNullable}), so a
 * non-literal VARCHAR-typed argument (varchar column, subquery slot, UNION slot) ties across
 * every candidate signature and the tie-break falls through to "keep the first-listed
 * candidate" - which was always TIMESTAMPTZ. Casting a plain varchar through timestamptz(6)
 * silently UTC-shifts the value whenever the session {@code time_zone} is not UTC.
 *
 * <p>The fix moves each file's TimeStampTz signature to LAST in SIGNATURES, so the tie
 * resolves to DATETIMEV2 instead. Non-literal TIMESTAMPTZ-typed arguments must still bind to
 * the TimeStampTzType signature - this class also guards against that regressing.
 */
public class DiffFunctionSignatureTest {

    private static final List<NamedDiffConstructor> DIFF_CONSTRUCTORS = ImmutableList.of(
            new NamedDiffConstructor("DateDiff", DateDiff::new),
            new NamedDiffConstructor("DaysDiff", DaysDiff::new),
            new NamedDiffConstructor("HoursDiff", HoursDiff::new),
            new NamedDiffConstructor("MicroSecondsDiff", MicroSecondsDiff::new),
            new NamedDiffConstructor("MilliSecondsDiff", MilliSecondsDiff::new),
            new NamedDiffConstructor("MinutesDiff", MinutesDiff::new),
            new NamedDiffConstructor("MonthsDiff", MonthsDiff::new),
            new NamedDiffConstructor("QuartersDiff", QuartersDiff::new),
            new NamedDiffConstructor("SecondsDiff", SecondsDiff::new),
            new NamedDiffConstructor("TimeDiff", TimeDiff::new),
            new NamedDiffConstructor("WeeksDiff", WeeksDiff::new),
            new NamedDiffConstructor("YearsDiff", YearsDiff::new));

    @Test
    public void testVarcharSlotsBindToDateTimeV2NotTimeStampTz() {
        SlotReference left = SlotReference.of("a", VarcharType.SYSTEM_DEFAULT);
        SlotReference right = SlotReference.of("b", VarcharType.SYSTEM_DEFAULT);
        for (NamedDiffConstructor c : DIFF_CONSTRUCTORS) {
            FunctionSignature signature = c.constructor.apply(left, right).getSignature();
            Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(0),
                    c.name + ": varchar arg0 should bind to DateTimeV2Type, not TimeStampTzType (issue #66120)");
            Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(1),
                    c.name + ": varchar arg1 should bind to DateTimeV2Type, not TimeStampTzType (issue #66120)");
        }
    }

    @Test
    public void testTimeStampTzSlotsStillBindToTimeStampTz() {
        SlotReference left = SlotReference.of("a", TimeStampTzType.of(6));
        SlotReference right = SlotReference.of("b", TimeStampTzType.of(6));
        for (NamedDiffConstructor c : DIFF_CONSTRUCTORS) {
            FunctionSignature signature = c.constructor.apply(left, right).getSignature();
            Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(0),
                    c.name + ": timestamptz arg0 should still bind to TimeStampTzType");
            Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(1),
                    c.name + ": timestamptz arg1 should still bind to TimeStampTzType");
        }
    }

    private static final class NamedDiffConstructor {
        private final String name;
        private final BiFunction<Expression, Expression, ComputeSignature> constructor;

        private NamedDiffConstructor(String name, BiFunction<Expression, Expression, ComputeSignature> constructor) {
            this.name = name;
            this.constructor = constructor;
        }
    }
}
