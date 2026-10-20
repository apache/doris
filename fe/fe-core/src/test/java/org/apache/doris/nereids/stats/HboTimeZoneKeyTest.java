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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourFromUnixtime;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the session time zone in the key of a time zone dependent expression:
 * {@code hour_from_unixtime(t.ts) = 0} reads the epoch value in the session time zone, so the same
 * predicate keeps a different number of rows in two sessions although its arguments are equal.
 */
public class HboTimeZoneKeyTest {

    @Test
    public void testTheSessionTimeZoneIsPartOfTheKey() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        String previous = context.getSessionVariable().getTimeZone();
        try {
            // the description renders its canonical form lazily, so it is built and rendered under one
            // session time zone before the session is switched
            context.getSessionVariable().setTimeZone("+00:00");
            GroupStructInfo utc = hourFromUnixtimeFilter();
            utc.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL);
            utc.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL);
            context.getSessionVariable().setTimeZone("+08:00");
            GroupStructInfo shanghai = hourFromUnixtimeFilter();
            shanghai.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL);
            shanghai.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL);
            Assertions.assertNotEquals(utc.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL),
                    shanghai.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL),
                    "the same predicate keeps one key in two time zones");
            Assertions.assertTrue(
                    utc.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL).contains("HourFromUnixtime<+00%3A00>"),
                    "the time zone is not in the key: " + utc.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL));
            Assertions.assertTrue(
                    shanghai.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL)
                            .contains("HourFromUnixtime<+08%3A00>"),
                    "the time zone is not in the key: "
                            + shanghai.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL));
        } finally {
            context.getSessionVariable().setTimeZone(previous);
            ConnectContext.remove();
        }
    }

    @Test
    public void testAPredicateWithoutATimeZoneDependentFunctionKeepsItsKey() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        String previous = context.getSessionVariable().getTimeZone();
        try {
            context.getSessionVariable().setTimeZone("+00:00");
            GroupStructInfo utc = plainFilter();
            utc.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL);
            utc.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL);
            context.getSessionVariable().setTimeZone("+08:00");
            GroupStructInfo shanghai = plainFilter();
            shanghai.getCanonical(GroupStructInfo.LiteralMode.WITH_LITERAL);
            shanghai.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL);
            Assertions.assertEquals(utc.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL),
                    shanghai.getFingerprint(GroupStructInfo.LiteralMode.WITH_LITERAL),
                    "a predicate without a time zone dependent expression moved");
        } finally {
            context.getSessionVariable().setTimeZone(previous);
            ConnectContext.remove();
        }
    }

    private static GroupStructInfo hourFromUnixtimeFilter() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        // the analysis casts the argument to the bigint the function declares
        HourFromUnixtime hour = new HourFromUnixtime(new Cast(slot, BigIntType.INSTANCE));
        return GroupStructInfo.describeGroup(new Memo(null, new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(hour, new IntegerLiteral(0))), scan)).getRoot());
    }

    private static GroupStructInfo plainFilter() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        return GroupStructInfo.describeGroup(new Memo(null, new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(0))), scan)).getRoot());
    }
}
