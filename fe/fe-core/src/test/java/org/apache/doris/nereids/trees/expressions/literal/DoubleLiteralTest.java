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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DoubleLiteralTest {

    @Test
    public void testGetStringValue() {
        Assertions.assertEquals("0", new DoubleLiteral(0).getStringValue());
        Assertions.assertEquals("0", new DoubleLiteral(0.0).getStringValue());
        Assertions.assertEquals("0", new DoubleLiteral(-0).getStringValue());
        Assertions.assertEquals("1", new DoubleLiteral(1).getStringValue());
        Assertions.assertEquals("1", new DoubleLiteral(1.0).getStringValue());
        Assertions.assertEquals("-1", new DoubleLiteral(-1).getStringValue());
        Assertions.assertEquals("1.554", new DoubleLiteral(1.554).getStringValue());
        Assertions.assertEquals("0.338", new DoubleLiteral(0.338).getStringValue());
        Assertions.assertEquals("-1", new DoubleLiteral(-1.0).getStringValue());
        Assertions.assertEquals("1e+100", new DoubleLiteral(1e100).getStringValue());
        Assertions.assertEquals("1e-100", new DoubleLiteral(1e-100).getStringValue());
        Assertions.assertEquals("10000000000000000", new DoubleLiteral(1.0E16).getStringValue());
        Assertions.assertEquals("-10000000000000000", new DoubleLiteral(-1.0E16).getStringValue());
        Assertions.assertEquals("1e+17", new DoubleLiteral(1.0E17).getStringValue());
        Assertions.assertEquals("-1e+17", new DoubleLiteral(-1.0E17).getStringValue());
        Assertions.assertEquals("0.0001", new DoubleLiteral(0.0001).getStringValue());
        Assertions.assertEquals("1e+308", new DoubleLiteral(1e308).getStringValue());
        Assertions.assertEquals("-1e+308", new DoubleLiteral(-1e308).getStringValue());
        Assertions.assertEquals("Infinity", new DoubleLiteral(Double.POSITIVE_INFINITY).getStringValue());
        Assertions.assertEquals("-Infinity", new DoubleLiteral(Double.NEGATIVE_INFINITY).getStringValue());
        Assertions.assertEquals("NaN", new DoubleLiteral(Double.NaN).getStringValue());
    }

    @Test
    void testUncheckedCastTo() {
        // To boolean
        DoubleLiteral f1 = new DoubleLiteral(-0.0);
        Assertions.assertFalse(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(0.0);
        Assertions.assertFalse(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(+0.0);
        Assertions.assertFalse(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(1.0);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(-1.0);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(Double.NaN);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(Double.NEGATIVE_INFINITY);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new DoubleLiteral(Double.POSITIVE_INFINITY);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());

        // To integral
        f1 = new DoubleLiteral(12.999);
        Expression expression = f1.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof TinyIntLiteral);
        Assertions.assertEquals(12, ((TinyIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof SmallIntLiteral);
        Assertions.assertEquals(12, ((SmallIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertTrue(expression instanceof IntegerLiteral);
        Assertions.assertEquals(12, ((IntegerLiteral) expression).getValue().intValue());

        f1 = new DoubleLiteral(-12.999);
        expression = f1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof BigIntLiteral);
        Assertions.assertEquals(-12, ((BigIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof LargeIntLiteral);
        Assertions.assertEquals(-12, ((LargeIntLiteral) expression).getValue().intValue());

        f1 = new DoubleLiteral(0.0);
        expression = f1.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof TinyIntLiteral);
        Assertions.assertEquals(0, ((TinyIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof SmallIntLiteral);
        Assertions.assertEquals(0, ((SmallIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertTrue(expression instanceof IntegerLiteral);
        Assertions.assertEquals(0, ((IntegerLiteral) expression).getValue().intValue());

        f1 = new DoubleLiteral(-0.0);
        expression = f1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof BigIntLiteral);
        Assertions.assertEquals(0, ((BigIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof LargeIntLiteral);
        Assertions.assertEquals(0, ((LargeIntLiteral) expression).getValue().intValue());

        f1 = new DoubleLiteral(Double.NaN);
        try {
            f1.uncheckedCastTo(TinyIntType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }

        f1 = new DoubleLiteral(Double.NEGATIVE_INFINITY);
        try {
            f1.uncheckedCastTo(IntegerType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }

        f1 = new DoubleLiteral(Double.POSITIVE_INFINITY);
        try {
            f1.uncheckedCastTo(LargeIntType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }
        f1 = new DoubleLiteral(1e30);
        try {
            f1.uncheckedCastTo(IntegerType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }

        // To float
        f1 = new DoubleLiteral(234.567);
        expression = f1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals((float) 234.567, ((FloatLiteral) expression).getValue());

        f1 = new DoubleLiteral(Double.NaN);
        expression = f1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals(Float.NaN, ((FloatLiteral) expression).getValue());

        f1 = new DoubleLiteral(Double.NEGATIVE_INFINITY);
        expression = f1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((FloatLiteral) expression).getValue());

        f1 = new DoubleLiteral(Double.POSITIVE_INFINITY);
        expression = f1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals(Float.POSITIVE_INFINITY, ((FloatLiteral) expression).getValue());

        f1 = new DoubleLiteral(1e300);
        expression = f1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals(Float.POSITIVE_INFINITY, ((FloatLiteral) expression).getValue());

        f1 = new DoubleLiteral(-1e300);
        expression = f1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((FloatLiteral) expression).getValue());

        // To decimal
        f1 = new DoubleLiteral(234.999);
        expression = f1.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(9, 3));
        Assertions.assertTrue(expression instanceof DecimalLiteral);
        Assertions.assertEquals("234.999", ((DecimalLiteral) expression).getValue().toString());

        f1 = new DoubleLiteral(234.999);
        expression = f1.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(9, 2));
        Assertions.assertTrue(expression instanceof DecimalLiteral);
        Assertions.assertEquals("235.00", ((DecimalLiteral) expression).getValue().toString());
        try {
            f1.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(2, 1));
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }
        f1 = new DoubleLiteral(Double.NaN);
        try {
            f1.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }
        f1 = new DoubleLiteral(Double.POSITIVE_INFINITY);
        try {
            f1.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }
        f1 = new DoubleLiteral(Double.NEGATIVE_INFINITY);
        try {
            f1.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }

        // to date
        f1 = new DoubleLiteral(123.245);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(2000, ((DateLiteral) expression).year);
        Assertions.assertEquals(1, ((DateLiteral) expression).month);
        Assertions.assertEquals(23, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(1231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(2000, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(91231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(2009, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(701231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(1970, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(691231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(2069, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(33691231.777);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(3369, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(33691231121133.777);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertTrue(expression instanceof DateLiteral);
        Assertions.assertEquals(3369, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new DoubleLiteral(1);
        try {
            f1.uncheckedCastTo(DateType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }

        // to datetime
        f1 = new DoubleLiteral(123.245);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(5));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2000, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(23, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(245000, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(1231.9999);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2000, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(12, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(31, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(31231.9999);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2003, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(12, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(31, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(701231.00);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(1970, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(12, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(31, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(691231.00);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2069, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(12, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(31, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(20701231.00);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2070, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(12, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(31, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(20250723182403.3);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2025, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(7, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(23, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(18, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(24, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(3, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(300000, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(19991231235959.99);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(1));
        Assertions.assertTrue(expression instanceof DateTimeV2Literal);
        Assertions.assertEquals(2000, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new DoubleLiteral(1000);
        try {
            f1.uncheckedCastTo(DateType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }
    }
}
