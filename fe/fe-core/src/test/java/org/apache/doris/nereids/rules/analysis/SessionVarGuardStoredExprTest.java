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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.ScalarType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SessionVarGuardRewriter#isTimeZoneSensitiveStoredExpr} over the legacy expression
 * tree of a stored (generated column / synchronous materialized view) expression. The classifier must look
 * at the operation plus source and result types, not only at child types: casts into TIMESTAMPTZ are
 * zone-sensitive even though their only child is VARCHAR, while comparing two TIMESTAMPTZ instants is
 * zone-invariant even though both children are TIMESTAMPTZ.
 */
public class SessionVarGuardStoredExprTest {

    private SlotRef tzSlot(String name) {
        SlotRef slot = new SlotRef(null, name);
        slot.setType(ScalarType.createTimeStampTzType(6));
        return slot;
    }

    @Test
    public void testBareTimestampTzSlotIsZoneInvariant() {
        Assertions.assertFalse(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(tzSlot("ts")));
    }

    /**
     * CAST(varchar_col AS TIMESTAMPTZ) has a VARCHAR-only child but interprets the offset-free string in
     * the write/load session zone, so it must be classified as zone-sensitive.
     */
    @Test
    public void testCastIntoTimestampTzIsSensitive() {
        SlotRef varcharSlot = new SlotRef(null, "vc");
        varcharSlot.setType(ScalarType.createVarcharType(20));
        CastExpr castIntoTz = new CastExpr(ScalarType.createTimeStampTzType(6), varcharSlot, true);
        Assertions.assertTrue(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(castIntoTz));
    }

    /**
     * CAST(ts AS STRING) renders the instant in the session zone but ALWAYS embeds the session offset
     * (BE's TimestampTzValue::to_string() appends e.g. "+08:00"), so the stored string is self-describing
     * and never silently misrepresents the instant. Such stored expressions are therefore allowed.
     */
    @Test
    public void testCastFromTimestampTzToStringIsZoneInvariant() {
        CastExpr castToString = new CastExpr(ScalarType.createVarcharType(100), tzSlot("ts"), true);
        Assertions.assertFalse(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(castToString));
    }

    /**
     * CAST(ts AS DATETIME) renders the instant into a zone-free DATETIME with no offset, so the stored
     * value is ambiguous across write sessions and must stay rejected.
     */
    @Test
    public void testCastFromTimestampTzToDatetimeIsSensitive() {
        CastExpr castToDatetime = new CastExpr(ScalarType.createDatetimeV2Type(6), tzSlot("ts"), true);
        Assertions.assertTrue(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(castToDatetime));
    }

    /**
     * CAST(ts AS TIMESTAMPTZ(0)) only changes the scale of the stored instant and is zone-invariant.
     */
    @Test
    public void testCastBetweenTimestampTzIsZoneInvariant() {
        CastExpr scaleDown = new CastExpr(ScalarType.createTimeStampTzType(0), tzSlot("ts"), true);
        Assertions.assertFalse(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(scaleDown));
    }

    /**
     * ts1 = ts2 compares two instants and does not depend on the session zone.
     */
    @Test
    public void testComparisonOfTwoTimestampTzIsZoneInvariant() {
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, tzSlot("ts1"), tzSlot("ts2"));
        Assertions.assertFalse(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(eq));
    }

    /**
     * ts = '2024-01-01 00:00:00' interprets the offset-free literal in the session zone, so it is
     * zone-sensitive even though one operand is a TIMESTAMPTZ slot.
     */
    @Test
    public void testComparisonWithOffsetFreeLiteralIsSensitive() {
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, tzSlot("ts"),
                new StringLiteral("2024-01-01 00:00:00"));
        Assertions.assertTrue(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(eq));
    }

    /**
     * A comparison with two TIMESTAMPTZ-typed operands is still zone-sensitive when one operand is itself a
     * zone-sensitive expression (a cast into TIMESTAMPTZ here): the exemption for direct instant
     * comparisons must not mask a sensitive operand, so the operands are still recursed into.
     */
    @Test
    public void testComparisonOfZoneSensitiveOperandIsSensitive() {
        SlotRef varcharSlot = new SlotRef(null, "vc");
        varcharSlot.setType(ScalarType.createVarcharType(20));
        CastExpr castIntoTz = new CastExpr(ScalarType.createTimeStampTzType(6), varcharSlot, true);
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, castIntoTz, tzSlot("ts2"));
        Assertions.assertTrue(SessionVarGuardRewriter.isTimeZoneSensitiveStoredExpr(eq));
    }
}
