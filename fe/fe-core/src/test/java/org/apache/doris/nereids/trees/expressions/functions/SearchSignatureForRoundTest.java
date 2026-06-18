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

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ceil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RoundBankers;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.math.BigInteger;

public class SearchSignatureForRoundTest {

    private static final DoubleLiteral DOUBLE_VAL = new DoubleLiteral(81.56996587030717);

    /** Run {@code body} with a fresh ConnectContext whose new opt-in var is set to {@code optIn}. */
    private static void withOptIn(boolean optIn, Runnable body) {
        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            ConnectContext ctx = new ConnectContext();
            ctx.getSessionVariable().roundDoubleReturnsDecimalForConstScale = optIn;
            mockedContext.when(ConnectContext::get).thenReturn(ctx);
            body.run();
        }
    }

    private static void assertDecimalReturn(int expectedScale, Expression expr) {
        Assertions.assertTrue(expr.getDataType() instanceof DecimalV3Type,
                () -> "expected DecimalV3Type, got " + expr.getDataType());
        DecimalV3Type t = (DecimalV3Type) expr.getDataType();
        Assertions.assertEquals(expectedScale, t.getScale(),
                () -> "expected scale=" + expectedScale + ", got " + t);
    }

    private static void assertDoubleReturn(Expression expr) {
        Assertions.assertTrue(expr.getDataType() instanceof DoubleType,
                () -> "expected DoubleType, got " + expr.getDataType());
    }

    // ---- opt-in ON: (DOUBLE, non-negative int literal <= 15) routes to DECIMAL ----

    @Test
    void roundDoubleWithConstScaleReturnsDecimal() {
        withOptIn(true, () ->
                assertDecimalReturn(2, new Round(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void roundBankersDoubleWithConstScaleReturnsDecimal() {
        withOptIn(true, () ->
                assertDecimalReturn(2, new RoundBankers(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void ceilDoubleWithConstScaleReturnsDecimal() {
        withOptIn(true, () ->
                assertDecimalReturn(2, new Ceil(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void floorDoubleWithConstScaleReturnsDecimal() {
        withOptIn(true, () ->
                assertDecimalReturn(2, new Floor(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void truncateDoubleWithConstScaleReturnsDecimal() {
        withOptIn(true, () ->
                assertDecimalReturn(2, new Truncate(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void zeroScaleAlsoReturnsDecimal() {
        withOptIn(true, () ->
                assertDecimalReturn(0, new Round(DOUBLE_VAL, new IntegerLiteral(0))));
    }

    @Test
    void roundDoubleAtMaxPreservableScaleReturnsDecimal() {
        // scale 15 == DOUBLE_DECIMAL.scale.
        withOptIn(true, () ->
                assertDecimalReturn(15, new Round(DOUBLE_VAL, new IntegerLiteral(15))));
    }

    @Test
    void roundDoubleWithCastIntLiteralReturnsDecimal() {
        withOptIn(true, () -> {
            Cast wrapped = new Cast(new IntegerLiteral(3), IntegerType.INSTANCE);
            assertDecimalReturn(3, new Round(DOUBLE_VAL, wrapped));
        });
    }

    // ---- opt-in ON but shape doesn't match: stays DOUBLE ----

    @Test
    void roundDoubleSingleArgStaysDouble() {
        withOptIn(true, () -> assertDoubleReturn(new Round(DOUBLE_VAL)));
    }

    @Test
    void roundDoubleNegativeScaleStaysDouble() {
        withOptIn(true, () ->
                assertDoubleReturn(new Round(DOUBLE_VAL, new IntegerLiteral(-1))));
    }

    @Test
    void roundDoubleScaleFromColumnStaysDouble() {
        // When the scale comes from a column (non-literal), we cannot pick a
        // fixed decimal scale at plan time, so we keep the original behavior.
        withOptIn(true, () -> {
            SlotReference scaleCol = new SlotReference("n", IntegerType.INSTANCE);
            assertDoubleReturn(new Round(DOUBLE_VAL, scaleCol));
        });
    }

    @Test
    void roundFloatWithConstScaleStaysDouble() {
        // FLOAT input keeps the original DOUBLE return path.
        withOptIn(true, () -> {
            SlotReference floatCol = new SlotReference("f", FloatType.INSTANCE);
            assertDoubleReturn(new Round(floatCol, new IntegerLiteral(2)));
        });
    }

    @Test
    void roundDoubleScaleAboveMaxPreservableStaysDouble() {
        // scale 17 exceeds DOUBLE_DECIMAL.scale (15).
        withOptIn(true, () ->
                assertDoubleReturn(new Round(DOUBLE_VAL, new IntegerLiteral(17))));
    }

    @Test
    void roundDecimalKeepsExistingDecimalSignature() {
        // The decimal-input path is independent of the new var.
        withOptIn(true, () -> {
            DecimalV3Type t = DecimalV3Type.createDecimalV3Type(10, 5);
            SlotReference dec = new SlotReference("d", t);
            assertDecimalReturn(2, new Round(dec, new IntegerLiteral(2)));
        });
    }

    @Test
    void roundDoubleBigIntScaleAboveIntRangeStaysDouble() {
        withOptIn(true, () ->
                assertDoubleReturn(new Round(DOUBLE_VAL, new BigIntLiteral(4294967298L))));
    }

    @Test
    void roundDoubleBigIntScaleAtIntMaxPlusOneStaysDouble() {
        withOptIn(true, () ->
                assertDoubleReturn(new Round(DOUBLE_VAL, new BigIntLiteral(2147483648L))));
    }

    @Test
    void roundDoubleLargeIntScaleAboveLongRangeStaysDouble() {
        withOptIn(true, () -> {
            BigInteger huge = new BigInteger("99999999999999999999"); // 20 digits
            assertDoubleReturn(new Round(DOUBLE_VAL, new LargeIntLiteral(huge)));
        });
    }

    @Test
    void roundDoubleBigIntNegativeScaleStaysDouble() {
        withOptIn(true, () ->
                assertDoubleReturn(new Round(DOUBLE_VAL, new BigIntLiteral(-1L))));
    }

    // ---- opt-in OFF: DOUBLE shape that would otherwise reroute stays DOUBLE ----

    @Test
    void roundDoubleStaysDoubleWhenOptInIsOff() {
        withOptIn(false, () ->
                assertDoubleReturn(new Round(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void truncateDoubleStaysDoubleWhenOptInIsOff() {
        withOptIn(false, () ->
                assertDoubleReturn(new Truncate(DOUBLE_VAL, new IntegerLiteral(2))));
    }

    @Test
    void roundDecimalIsUnaffectedByOptInOff() {
        // Decimal input is independent of the new var.
        withOptIn(false, () -> {
            DecimalV3Type t = DecimalV3Type.createDecimalV3Type(10, 5);
            SlotReference dec = new SlotReference("d", t);
            assertDecimalReturn(2, new Round(dec, new IntegerLiteral(2)));
        });
    }
}
