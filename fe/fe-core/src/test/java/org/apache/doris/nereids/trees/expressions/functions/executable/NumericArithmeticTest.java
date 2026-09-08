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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class NumericArithmeticTest {

    @Test
    public void testDecimalV3Abs() {
        DecimalV3Literal decimalV3Literal = new DecimalV3Literal(
                DecimalV3Type.createDecimalV3Type(10, 0), new BigDecimal(1));
        DecimalV3Literal result = (DecimalV3Literal) NumericArithmetic.abs(decimalV3Literal);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(10, 0), result.getDataType());
    }

    @Test
    public void testDecimalV2Abs() {
        DecimalLiteral decimalV3Literal = new DecimalLiteral(
                DecimalV2Type.createDecimalV2Type(10, 0), new BigDecimal(1));
        DecimalLiteral result = (DecimalLiteral) NumericArithmetic.abs(decimalV3Literal);
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(10, 0), result.getDataType());
    }

    @Test
    public void testDivideDecimalZeroNumerator() {
        // 0 / 5 must fold to decimal 0, not NULL. The zero-guard must check the
        // denominator (second), not the numerator (first).
        DecimalLiteral zero = new DecimalLiteral(DecimalV2Type.createDecimalV2Type(10, 0), new BigDecimal(0));
        DecimalLiteral five = new DecimalLiteral(DecimalV2Type.createDecimalV2Type(10, 0), new BigDecimal(5));
        Expression result = NumericArithmetic.divideDecimal(zero, five);
        Assertions.assertTrue(result instanceof DecimalLiteral,
                "0 / 5 should fold to a decimal, but got " + result);
        Assertions.assertEquals(0, ((DecimalLiteral) result).getValue().compareTo(BigDecimal.ZERO),
                "0 / 5 should fold to 0");
    }

    @Test
    public void testDivideDecimalZeroDenominator() {
        // 5 / 0 must fold to NULL (SQL divide-by-zero semantics), not throw.
        DecimalLiteral five = new DecimalLiteral(DecimalV2Type.createDecimalV2Type(10, 0), new BigDecimal(5));
        DecimalLiteral zero = new DecimalLiteral(DecimalV2Type.createDecimalV2Type(10, 0), new BigDecimal(0));
        Expression result = NumericArithmetic.divideDecimal(five, zero);
        Assertions.assertTrue(result instanceof NullLiteral,
                "5 / 0 should fold to NULL, but got " + result);
    }

    @Test
    public void testSignBit() {
        Assertions.assertEquals(BooleanLiteral.FALSE, NumericArithmetic.signbit(new DoubleLiteral(0.0)));
        Assertions.assertEquals(BooleanLiteral.TRUE, NumericArithmetic.signbit(new DoubleLiteral(-0.0)));
        Assertions.assertEquals(BooleanLiteral.FALSE, NumericArithmetic.signbit(new DoubleLiteral(1.0)));
        Assertions.assertEquals(BooleanLiteral.TRUE, NumericArithmetic.signbit(new DoubleLiteral(-1.0)));
    }
}
