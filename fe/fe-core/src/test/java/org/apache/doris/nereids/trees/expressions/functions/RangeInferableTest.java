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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.ColumnRange;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsDiff;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class RangeInferableTest {
    private final SlotReference date = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
    private ConnectContext previousContext;
    private ExpressionRewriteContext context;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
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
    void testIncreasingAndSingletonRanges() {
        DaysAdd function = new DaysAdd(date, new IntegerLiteral(2));
        DateTimeV2Literal lower = new DateTimeV2Literal("2023-01-01 10:00:00");
        DateTimeV2Literal upper = new DateTimeV2Literal("2023-01-03 11:00:00");
        DateTimeV2Literal outputLower = new DateTimeV2Literal("2023-01-03 10:00:00");
        DateTimeV2Literal outputUpper = new DateTimeV2Literal("2023-01-05 11:00:00");
        assertRange(ColumnRange.between(outputLower, outputUpper), function.inferRange(lower, upper, context));
        assertRange(ColumnRange.singleton(outputLower), function.inferRange(lower, lower, context));
        assertRange(ColumnRange.atLeast(outputLower), function.inferRange(lower, null, context));
        assertRange(ColumnRange.atMost(outputUpper), function.inferRange(null, upper, context));
        Assertions.assertFalse(function.inferRange(null, null, context).isPresent());
    }

    @Test
    void testDecreasingRangesSwapBoundedAndUnboundedSides() {
        MonthsDiff function = new MonthsDiff(new DateTimeV2Literal("2023-04-01 00:00:00"), date);
        DateTimeV2Literal lower = new DateTimeV2Literal("2023-01-01 00:00:00");
        DateTimeV2Literal upper = new DateTimeV2Literal("2023-03-01 00:00:00");
        Assertions.assertEquals(1, function.getRangeInputChildIndex());
        Assertions.assertFalse(function.isPositive());
        assertRange(ColumnRange.between(new BigIntLiteral(1), new BigIntLiteral(3)),
                function.inferRange(lower, upper, context));
        assertRange(ColumnRange.atMost(new BigIntLiteral(3)), function.inferRange(lower, null, context));
        assertRange(ColumnRange.atLeast(new BigIntLiteral(1)), function.inferRange(null, upper, context));
    }

    @Test
    void testUnknownIntervalsAndInvalidEndpointResultsHaveNoRange() {
        DateTimeV2Literal lower = new DateTimeV2Literal("2023-01-01 00:00:00");
        DateTimeV2Literal upper = new DateTimeV2Literal("2023-01-02 00:00:00");
        DaysAdd variable = new DaysAdd(date, new SlotReference("days", IntegerType.INSTANCE));
        Assertions.assertFalse(variable.inferRange(lower, upper, context).isPresent());
        MonthsAdd calendarVariable = new MonthsAdd(date, new SlotReference("months", IntegerType.INSTANCE));
        Assertions.assertFalse(calendarVariable.inferRange(lower, upper, context).isPresent());

        DaysAdd nullInterval = new DaysAdd(date, new NullLiteral(IntegerType.INSTANCE));
        Assertions.assertFalse(nullInterval.inferRange(lower, upper, context).isPresent());

        DaysAdd overflow = new DaysAdd(date, new IntegerLiteral(1));
        DateTimeV2Literal lastDay = new DateTimeV2Literal("9999-12-31 00:00:00");
        Assertions.assertFalse(overflow.inferRange(lastDay, lastDay, context).isPresent());
        MonthsAdd calendarOverflow = new MonthsAdd(date, new IntegerLiteral(1));
        Assertions.assertFalse(calendarOverflow.inferRange(lower, lastDay, context).isPresent());
    }

    private void assertRange(ColumnRange expected, Optional<ColumnRange> actual) {
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals(expected.rangeSet, actual.get().rangeSet);
    }
}
