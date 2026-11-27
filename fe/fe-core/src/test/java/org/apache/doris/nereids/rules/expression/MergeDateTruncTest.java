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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.rules.expression.rules.MergeDateTrunc;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateV2Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * merge date_trunc test
 */
public class MergeDateTruncTest extends ExpressionRewriteTestHelper {

    @ParameterizedTest
    @CsvSource({"DAY,false,false", "DAY,false,true", "DAY,true,false", "DAY,true,true",
            "WEEK,false,false", "WEEK,false,true", "WEEK,true,false", "WEEK,true,true",
            "QUARTER,false,false", "QUARTER,false,true", "QUARTER,true,false", "QUARTER,true,true"})
    void testMergeIdenticalCalendarUnits(String timeUnit, boolean childUnitFirst, boolean parentUnitFirst) {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp(MergeDateTrunc.INSTANCE)));
        Slot dataSlot = new SlotReference("data_slot", DateV2Type.INSTANCE);
        assertRewrite(dateTrunc(dateTrunc(dataSlot, timeUnit, childUnitFirst),
                        timeUnit.toLowerCase(), parentUnitFirst),
                new DateTrunc(dataSlot, new VarcharLiteral(timeUnit)));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testMergeCompatibleUnits(boolean childUnitFirst, boolean parentUnitFirst) {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp(MergeDateTrunc.INSTANCE)));
        Slot dataSlot = new SlotReference("data_slot", DateV2Type.INSTANCE);
        assertRewrite(dateTrunc(dateTrunc(dataSlot, "HOUR", childUnitFirst), "DAY", parentUnitFirst),
                new DateTrunc(dataSlot, new VarcharLiteral("DAY")));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testKeepMixedCalendarUnits(boolean childUnitFirst, boolean parentUnitFirst) {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp(MergeDateTrunc.INSTANCE)));
        Slot dataSlot = new SlotReference("data_slot", DateV2Type.INSTANCE);
        assertNotRewrite(dateTrunc(dateTrunc(dataSlot, "HOUR", childUnitFirst), "WEEK", parentUnitFirst),
                new DateTrunc(dataSlot, new VarcharLiteral("WEEK")));
        assertNotRewrite(dateTrunc(dateTrunc(dataSlot, "DAY", childUnitFirst), "QUARTER", parentUnitFirst),
                new DateTrunc(dataSlot, new VarcharLiteral("QUARTER")));
    }

    private DateTrunc dateTrunc(Expression date, String timeUnit, boolean unitFirst) {
        VarcharLiteral unit = new VarcharLiteral(timeUnit);
        return unitFirst ? new DateTrunc(unit, date) : new DateTrunc(date, unit);
    }

    @Test
    public void testMergeDateTrunc() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(MergeDateTrunc.INSTANCE)
        ));
        Slot dataSlot = new SlotReference("data_slot", DateV2Type.INSTANCE);
        assertRewrite(new DateTrunc(new DateTrunc(dataSlot, new VarcharLiteral("HOUR")),
                        new VarcharLiteral("DAY")),
                new DateTrunc(dataSlot, new VarcharLiteral("DAY")));

        assertRewrite(new DateTrunc(new DateTrunc(dataSlot, new VarcharLiteral("DAY")),
                        new VarcharLiteral("YEAR")),
                new DateTrunc(dataSlot, new VarcharLiteral("YEAR")));

        assertNotRewrite(new DateTrunc(new DateTrunc(dataSlot, new VarcharLiteral("HOUR")),
                        new VarcharLiteral("WEEK")),
                new DateTrunc(dataSlot, new VarcharLiteral("WEEK")));

        assertNotRewrite(new DateTrunc(new DateTrunc(dataSlot, new VarcharLiteral("DAY")),
                        new VarcharLiteral("QUARTER")),
                new DateTrunc(dataSlot, new VarcharLiteral("QUARTER")));
    }
}
