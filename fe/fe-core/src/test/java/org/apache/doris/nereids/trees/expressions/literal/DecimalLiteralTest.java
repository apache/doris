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
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.math.BigDecimal;

public class DecimalLiteralTest {
    @Test
    void testUncheckedCastTo() {
        // To boolean
        DecimalV3Literal d1 = new DecimalV3Literal(new BigDecimal("0"));
        Assertions.assertFalse(((BooleanLiteral) d1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        d1 = new DecimalV3Literal(new BigDecimal("1.3"));
        Assertions.assertTrue(((BooleanLiteral) d1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());
        d1 = new DecimalV3Literal(new BigDecimal("-2"));
        Assertions.assertTrue(((BooleanLiteral) d1.uncheckedCastTo(BooleanType.INSTANCE)).getValue());

        // To integral
        d1 = new DecimalV3Literal(new BigDecimal("12.999"));
        Expression expression = d1.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof TinyIntLiteral);
        Assertions.assertEquals(12, ((TinyIntLiteral) expression).getValue().intValue());

        expression = d1.uncheckedCastTo(SmallIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof SmallIntLiteral);
        Assertions.assertEquals(12, ((SmallIntLiteral) expression).getValue().intValue());

        expression = d1.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertTrue(expression instanceof IntegerLiteral);
        Assertions.assertEquals(12, ((IntegerLiteral) expression).getValue().intValue());

        d1 = new DecimalV3Literal(new BigDecimal("0"));
        expression = d1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof BigIntLiteral);
        Assertions.assertEquals(0, ((BigIntLiteral) expression).getValue().intValue());

        d1 = new DecimalV3Literal(new BigDecimal("-1000.99234"));
        expression = d1.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertTrue(expression instanceof LargeIntLiteral);
        Assertions.assertEquals(-1000, ((LargeIntLiteral) expression).getValue().intValue());

        d1 = new DecimalV3Literal(new BigDecimal("1e30"));
        try {
            d1.uncheckedCastTo(IntegerType.INSTANCE);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }

        // To float
        d1 = new DecimalV3Literal(new BigDecimal("234.567"));
        expression = d1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertTrue(expression instanceof FloatLiteral);
        Assertions.assertEquals((float) 234.567, ((FloatLiteral) expression).getValue());

        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);

            ConnectContext context = new ConnectContext();
            context.getSessionVariable().enableDecimal256 = true;
            mockedContext.when(ConnectContext::get).thenReturn(context);

            d1 = new DecimalV3Literal(new BigDecimal("1e40"));
            expression = d1.uncheckedCastTo(FloatType.INSTANCE);
            Assertions.assertTrue(expression instanceof FloatLiteral);
            Assertions.assertEquals(Float.POSITIVE_INFINITY, ((FloatLiteral) expression).getValue());

            d1 = new DecimalV3Literal(new BigDecimal("-1e40"));
            expression = d1.uncheckedCastTo(FloatType.INSTANCE);
            Assertions.assertTrue(expression instanceof FloatLiteral);
            Assertions.assertEquals(Float.NEGATIVE_INFINITY, ((FloatLiteral) expression).getValue());
        }

        // To double
        d1 = new DecimalV3Literal(new BigDecimal("234.567"));
        expression = d1.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertTrue(expression instanceof DoubleLiteral);
        Assertions.assertEquals(234.567, ((DoubleLiteral) expression).getValue());

        // To decimal
        expression = d1.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(6, 3));
        Assertions.assertTrue(expression instanceof DecimalV3Literal);
        Assertions.assertEquals("234.567", ((DecimalV3Literal) expression).getValue().toString());

        expression = d1.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(5, 2));
        Assertions.assertTrue(expression instanceof DecimalV3Literal);
        Assertions.assertEquals("234.57", ((DecimalV3Literal) expression).getValue().toString());

        try {
            d1.uncheckedCastTo(DecimalV3Type.createDecimalV3Type(2, 1));
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof CastException);
        }
    }
}
