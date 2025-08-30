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
import org.apache.doris.nereids.trees.expressions.literal.format.FloatChecker;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.function.Function;

public class FloatLiteralTest {
    @Test
    public void testChecker() {
        assertValid(
                "123.45",
                "-34.56",
                "0",
                "0.01",
                "10000",
                "+1",
                "-1",
                "+1",
                "1.0",
                "-1.0",
                "-1.0e3",
                ".1",
                "1.",
                "1e3"
        );

        assertInValid(
                "e3",
                "abc",
                "12.34.56",
                "1,234.56"
        );

        Assertions.assertThrows(
                Throwable.class,
                () -> check("e3", s -> new FloatLiteral(new BigDecimal(s).floatValue()))
        );
    }

    private void assertValid(String...validString) {
        for (String str : validString) {
            check(str, s -> new FloatLiteral(new BigDecimal(s).floatValue()));
        }
    }

    private void assertInValid(String...validString) {
        for (String str : validString) {
            Assertions.assertThrows(
                    Throwable.class,
                    () -> check(str, s -> new FloatLiteral(new BigDecimal(s).floatValue()))
            );
        }
    }

    private <T extends FractionalLiteral> T check(String str, Function<String, T> literalBuilder) {
        Assertions.assertTrue(FloatChecker.isValidFloat(str), "Invalid fractional: " + str);
        return literalBuilder.apply(str);
    }

    @Test
    void testUncheckedCastTo() {
        // To boolean
        FloatLiteral f1 = new FloatLiteral((float) -0.0);
        Assertions.assertFalse(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral((float) 0.0);
        Assertions.assertFalse(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral((float) +0.0);
        Assertions.assertFalse(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral((float) 1.0);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral((float) -1.0);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral(Float.NaN);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral(Float.NEGATIVE_INFINITY);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        f1 = new FloatLiteral(Float.POSITIVE_INFINITY);
        Assertions.assertTrue(((BooleanLiteral) f1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());

        // To integral
        f1 = new FloatLiteral((float) 12.999);
        Expression expression = f1.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertInstanceOf(TinyIntLiteral.class, expression);
        Assertions.assertEquals(12, ((TinyIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertInstanceOf(SmallIntLiteral.class, expression);
        Assertions.assertEquals(12, ((SmallIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(12, ((IntegerLiteral) expression).getValue().intValue());

        f1 = new FloatLiteral((float) -12.999);
        expression = f1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertInstanceOf(BigIntLiteral.class, expression);
        Assertions.assertEquals(-12, ((BigIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertInstanceOf(LargeIntLiteral.class, expression);
        Assertions.assertEquals(-12, ((LargeIntLiteral) expression).getValue().intValue());

        f1 = new FloatLiteral((float) 0);
        expression = f1.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertInstanceOf(TinyIntLiteral.class, expression);
        Assertions.assertEquals(0, ((TinyIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertInstanceOf(SmallIntLiteral.class, expression);
        Assertions.assertEquals(0, ((SmallIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(0, ((IntegerLiteral) expression).getValue().intValue());

        f1 = new FloatLiteral((float) -0);
        expression = f1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertInstanceOf(BigIntLiteral.class, expression);
        Assertions.assertEquals(0, ((BigIntLiteral) expression).getValue().intValue());

        expression = f1.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertInstanceOf(LargeIntLiteral.class, expression);
        Assertions.assertEquals(0, ((LargeIntLiteral) expression).getValue().intValue());

        f1 = new FloatLiteral(Float.NaN);
        FloatLiteral finalF = f1;
        Assertions.assertThrows(CastException.class, () -> finalF.uncheckedCastTo(TinyIntType.INSTANCE));

        f1 = new FloatLiteral(Float.NEGATIVE_INFINITY);
        FloatLiteral finalF1 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF1.uncheckedCastTo(IntegerType.INSTANCE));

        f1 = new FloatLiteral(Float.POSITIVE_INFINITY);
        FloatLiteral finalF2 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF2.uncheckedCastTo(LargeIntType.INSTANCE));

        f1 = new FloatLiteral((float) 1e30);
        FloatLiteral finalF3 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF3.uncheckedCastTo(IntegerType.INSTANCE));

        // To double
        f1 = new FloatLiteral((float) 234.567);
        expression = f1.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(234.567, ((DoubleLiteral) expression).getValue());

        f1 = new FloatLiteral(Float.NaN);
        expression = f1.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(Double.NaN, ((DoubleLiteral) expression).getValue());

        f1 = new FloatLiteral(Float.NEGATIVE_INFINITY);
        expression = f1.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, ((DoubleLiteral) expression).getValue());

        f1 = new FloatLiteral(Float.POSITIVE_INFINITY);
        expression = f1.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, ((DoubleLiteral) expression).getValue());

        // To decimal
        f1 = new FloatLiteral((float) 234.999);
        expression = f1.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(9, 3));
        Assertions.assertInstanceOf(DecimalLiteral.class, expression);
        Assertions.assertEquals("234.999", ((DecimalLiteral) expression).getValue().toString());

        f1 = new FloatLiteral((float) 234.999);
        expression = f1.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(9, 2));
        Assertions.assertInstanceOf(DecimalLiteral.class, expression);
        Assertions.assertEquals("235.00", ((DecimalLiteral) expression).getValue().toString());
        FloatLiteral finalF4 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF4.uncheckedCastTo(DecimalV2Type.createDecimalV2Type(2, 1)));

        f1 = new FloatLiteral(Float.NaN);
        FloatLiteral finalF5 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF5.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT));

        f1 = new FloatLiteral(Float.POSITIVE_INFINITY);
        FloatLiteral finalF6 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF6.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT));

        f1 = new FloatLiteral(Float.NEGATIVE_INFINITY);
        FloatLiteral finalF7 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF7.uncheckedCastTo(DecimalV2Type.SYSTEM_DEFAULT));

        // to date
        f1 = new FloatLiteral((float) 123.245);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertInstanceOf(DateLiteral.class, expression);
        Assertions.assertEquals(2000, ((DateLiteral) expression).year);
        Assertions.assertEquals(1, ((DateLiteral) expression).month);
        Assertions.assertEquals(23, ((DateLiteral) expression).day);

        f1 = new FloatLiteral(1231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertInstanceOf(DateLiteral.class, expression);
        Assertions.assertEquals(2000, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new FloatLiteral(91231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertInstanceOf(DateLiteral.class, expression);
        Assertions.assertEquals(2009, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new FloatLiteral(701231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertInstanceOf(DateLiteral.class, expression);
        Assertions.assertEquals(1970, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new FloatLiteral(691231);
        expression = f1.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertInstanceOf(DateLiteral.class, expression);
        Assertions.assertEquals(2069, ((DateLiteral) expression).year);
        Assertions.assertEquals(12, ((DateLiteral) expression).month);
        Assertions.assertEquals(31, ((DateLiteral) expression).day);

        f1 = new FloatLiteral(1);
        FloatLiteral finalF8 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF8.uncheckedCastTo(DateType.INSTANCE));

        // to datetime
        f1 = new FloatLiteral((float) 123.245);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(5));
        Assertions.assertInstanceOf(DateTimeV2Literal.class, expression);
        Assertions.assertEquals(2000, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(23, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(245000, ((DateTimeV2Literal) expression).microSecond);

        f1 = new FloatLiteral((float) 1231.9999);
        expression = f1.uncheckedCastTo(DateTimeV2Type.of(3));
        Assertions.assertInstanceOf(DateTimeV2Literal.class, expression);
        Assertions.assertEquals(2000, ((DateTimeV2Literal) expression).year);
        Assertions.assertEquals(12, ((DateTimeV2Literal) expression).month);
        Assertions.assertEquals(31, ((DateTimeV2Literal) expression).day);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).hour);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).minute);
        Assertions.assertEquals(1, ((DateTimeV2Literal) expression).second);
        Assertions.assertEquals(0, ((DateTimeV2Literal) expression).microSecond);

        f1 = new FloatLiteral(1000);
        FloatLiteral finalF9 = f1;
        Assertions.assertThrows(CastException.class, () -> finalF9.uncheckedCastTo(DateType.INSTANCE));

    }

    @Test
    public void testFloatGetStringValue() {
        Assertions.assertEquals("0", ((StringLiteral) (new FloatLiteral(0f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("0", ((StringLiteral) (new FloatLiteral(0.0f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-0", ((StringLiteral) (new FloatLiteral(-0f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1", ((StringLiteral) (new FloatLiteral(1f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1", ((StringLiteral) (new FloatLiteral(1.0f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1", ((StringLiteral) (new FloatLiteral(-1f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1.554", ((StringLiteral) (new FloatLiteral(1.554f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("0.338", ((StringLiteral) (new FloatLiteral(0.338f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1", ((StringLiteral) (new FloatLiteral(-1.0f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1e+38", ((StringLiteral) (new FloatLiteral((float) 1e38).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1e+38", ((StringLiteral) (new FloatLiteral((float) -1e38).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1000000", ((StringLiteral) (new FloatLiteral((float) 1.0E6).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1000000", ((StringLiteral) (new FloatLiteral((float) -1.0E6).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1e+07", ((StringLiteral) (new FloatLiteral((float) 1.0E7).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1e+07", ((StringLiteral) (new FloatLiteral((float) -1.0E7).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("0.0001", ((StringLiteral) (new FloatLiteral(0.0001f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1e-05", ((StringLiteral) (new FloatLiteral(0.00001f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("Infinity", ((StringLiteral) (new FloatLiteral(Float.POSITIVE_INFINITY).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-Infinity", ((StringLiteral) (new FloatLiteral(Float.NEGATIVE_INFINITY).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("NaN", ((StringLiteral) (new FloatLiteral(Float.NaN).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1234567", ((StringLiteral) (new FloatLiteral(1234567.12345f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1.234568e+07", ((StringLiteral) (new FloatLiteral(12345678.12345f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("0.0001234568", ((StringLiteral) (new FloatLiteral(0.0001234567890123456789f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("1.234568e-15", ((StringLiteral) (new FloatLiteral(0.000000000000001234567890123456f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("123.456", ((StringLiteral) (new FloatLiteral(123.456000f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("123", ((StringLiteral) (new FloatLiteral(123.000f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1234567", ((StringLiteral) (new FloatLiteral(-1234567.12345f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1.234568e+07", ((StringLiteral) (new FloatLiteral(-12345678.12345f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-0.0001234568", ((StringLiteral) (new FloatLiteral(-0.0001234567890123456789f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-1.234568e-15", ((StringLiteral) (new FloatLiteral(-0.000000000000001234567890123456f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-123.456", ((StringLiteral) (new FloatLiteral(-123.456000f).uncheckedCastTo(StringType.INSTANCE))).getValue());
        Assertions.assertEquals("-123", ((StringLiteral) (new FloatLiteral(-123.000f).uncheckedCastTo(StringType.INSTANCE))).getValue());
    }
}
