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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringLikeLiteralTest {
    @Test
    public void testStrToDouble() {
        // fix bug: maxStr.length = 4, maxStr.getBytes().length=12
        // when converting str to double, bytes length is used instead of string length
        String minStr = "商家+店长+场地+设备类型维度";
        String maxStr = "商家维度";
        double d1 = StringLikeLiteral.getDouble(minStr);
        double d2 = StringLikeLiteral.getDouble(maxStr);
        Assertions.assertTrue(d1 < d2);
    }

    @Test
    public void testUtf8() {
        System.setProperty("file.encoding", "ANSI_X3.4-1968");
        double d1 = StringLikeLiteral.getDouble("一般风险准备");
        Assertions.assertEquals(d1, 6.4379158486625512E16);
    }

    @Test
    void testUncheckedCastTo() {
        // To boolean
        StringLiteral s = new StringLiteral("tRuE");
        Assertions.assertTrue(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("1");
        Assertions.assertTrue(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("YeS");
        Assertions.assertTrue(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("t");
        Assertions.assertTrue(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("oN");
        Assertions.assertTrue(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("FaLSE");
        Assertions.assertFalse(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("0");
        Assertions.assertFalse(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("NO");
        Assertions.assertFalse(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("F");
        Assertions.assertFalse(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        s = new StringLiteral("OfF");
        Assertions.assertFalse(((BooleanLiteral) s.uncheckedCastTo(BooleanType.INSTANCE)).getValue());

        // To integer
        s = new StringLiteral("123456");
        Expression expression = s.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(123456, ((IntegerLiteral) expression).getValue().intValue());

        s = new StringLiteral("+123456");
        expression = s.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(123456, ((IntegerLiteral) expression).getValue().intValue());

        s = new StringLiteral("-123456");
        expression = s.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(-123456, ((IntegerLiteral) expression).getValue().intValue());

        s = new StringLiteral("1234567891011");
        StringLiteral finalS = s;
        Assertions.assertThrows(CastException.class, () -> finalS.uncheckedCastTo(IntegerType.INSTANCE));

        // To float
        s = new StringLiteral("123.456");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals((float) 123.456, ((FloatLiteral) expression).getValue().floatValue());

        s = new StringLiteral("1.3E5");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(130000, ((FloatLiteral) expression).getValue().floatValue());

        s = new StringLiteral("-.99");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals((float) -0.99, ((FloatLiteral) expression).getValue().floatValue());

        s = new StringLiteral("-1e400");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((FloatLiteral) expression).getValue().floatValue());

        s = new StringLiteral("InF");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(Float.POSITIVE_INFINITY, ((FloatLiteral) expression).getValue().floatValue());

        s = new StringLiteral("-InfiNIty");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((FloatLiteral) expression).getValue().floatValue());

        s = new StringLiteral("nAN");
        expression = s.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(Float.NaN, ((FloatLiteral) expression).getValue().floatValue());

        // To double
        s = new StringLiteral("123.456");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(123.456, ((DoubleLiteral) expression).getValue().doubleValue());

        s = new StringLiteral("1.3E5");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(130000, ((DoubleLiteral) expression).getValue().doubleValue());

        s = new StringLiteral("-.99");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(-0.99, ((DoubleLiteral) expression).getValue().doubleValue());

        s = new StringLiteral("-1e400");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((DoubleLiteral) expression).getValue().doubleValue());

        s = new StringLiteral("InF");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(Float.POSITIVE_INFINITY, ((DoubleLiteral) expression).getValue().doubleValue());

        s = new StringLiteral("-InfiNIty");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((DoubleLiteral) expression).getValue().doubleValue());

        s = new StringLiteral("nAN");
        expression = s.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertTrue(expression instanceof DoubleLiteral);
        Assertions.assertEquals(Float.NaN, ((DoubleLiteral) expression).getValue().doubleValue());

        // To decimal
        s = new StringLiteral("1234.5678");
        expression = s.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(10, 4));
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals("1234.5678", ((DecimalV3Literal) expression).getValue().toString());

        s = new StringLiteral("1234.5678");
        expression = s.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(6, 2));
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals("1234.57", ((DecimalV3Literal) expression).getValue().toString());

        s = new StringLiteral("1.33e1");
        expression = s.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(6, 2));
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals("13.30", ((DecimalV3Literal) expression).getValue().toString());

        s = new StringLiteral("1234.5678");
        StringLiteral finalS1 = s;
        Assertions.assertThrows(CastException.class, () -> finalS1.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(2, 1)));

        s = new StringLiteral("9999.999");
        expression = s.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(7, 3));
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals("9999.999", ((DecimalV3Literal) expression).getValue().toString());

        s = new StringLiteral("9999.999");
        expression = s.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(7, 2));
        Assertions.assertInstanceOf(DecimalV3Literal.class, expression);
        Assertions.assertEquals("10000.00", ((DecimalV3Literal) expression).getValue().toString());

        s = new StringLiteral("9999.999");
        StringLiteral finalS2 = s;
        Assertions.assertThrows(CastException.class, () -> finalS2.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(6, 2)));

    }
}
