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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ceil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RoundBankers;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SearchSignatureForRoundTest {

    private static final DoubleLiteral DOUBLE_VAL = new DoubleLiteral(81.56996587030717);

    private static void assertDecimalReturn(int expectedScale, org.apache.doris.nereids.trees.expressions.Expression expr) {
        Assertions.assertTrue(expr.getDataType() instanceof DecimalV3Type,
                () -> "expected DecimalV3Type, got " + expr.getDataType());
        DecimalV3Type t = (DecimalV3Type) expr.getDataType();
        Assertions.assertEquals(expectedScale, t.getScale(),
                () -> "expected scale=" + expectedScale + ", got " + t);
    }

    private static void assertDoubleReturn(org.apache.doris.nereids.trees.expressions.Expression expr) {
        Assertions.assertTrue(expr.getDataType() instanceof DoubleType,
                () -> "expected DoubleType, got " + expr.getDataType());
    }

    @Test
    void roundDoubleWithConstScaleReturnsDecimal() {
        assertDecimalReturn(2, new Round(DOUBLE_VAL, new IntegerLiteral(2)));
    }

    @Test
    void roundBankersDoubleWithConstScaleReturnsDecimal() {
        assertDecimalReturn(2, new RoundBankers(DOUBLE_VAL, new IntegerLiteral(2)));
    }

    @Test
    void ceilDoubleWithConstScaleReturnsDecimal() {
        assertDecimalReturn(2, new Ceil(DOUBLE_VAL, new IntegerLiteral(2)));
    }

    @Test
    void floorDoubleWithConstScaleReturnsDecimal() {
        assertDecimalReturn(2, new Floor(DOUBLE_VAL, new IntegerLiteral(2)));
    }

    @Test
    void truncateDoubleWithConstScaleReturnsDecimal() {
        assertDecimalReturn(2, new Truncate(DOUBLE_VAL, new IntegerLiteral(2)));
    }

    @Test
    void zeroScaleAlsoReturnsDecimal() {
        assertDecimalReturn(0, new Round(DOUBLE_VAL, new IntegerLiteral(0)));
    }

    @Test
    void roundDoubleSingleArgStaysDouble() {
        assertDoubleReturn(new Round(DOUBLE_VAL));
    }

    @Test
    void roundDoubleNegativeScaleStaysDouble() {
        assertDoubleReturn(new Round(DOUBLE_VAL, new IntegerLiteral(-1)));
    }

    @Test
    void roundDoubleScaleFromColumnStaysDouble() {
        // When the scale comes from a column (non-literal), we cannot pick a
        // fixed decimal scale at plan time, so we keep the original behavior.
        SlotReference scaleCol = new SlotReference("n", IntegerType.INSTANCE);
        assertDoubleReturn(new Round(DOUBLE_VAL, scaleCol));
    }

    @Test
    void roundDecimalKeepsExistingDecimalSignature() {
        DecimalV3Type t = DecimalV3Type.createDecimalV3Type(10, 5);
        SlotReference dec = new SlotReference("d", t);
        assertDecimalReturn(2, new Round(dec, new IntegerLiteral(2)));
    }

    @Test
    void roundDoubleWithCastIntLiteralReturnsDecimal() {
        Cast wrapped = new Cast(new IntegerLiteral(3), IntegerType.INSTANCE);
        assertDecimalReturn(3, new Round(DOUBLE_VAL, wrapped));
    }
}
