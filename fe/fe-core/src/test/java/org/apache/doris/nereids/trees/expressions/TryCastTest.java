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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TryCastTest {

    @Test
    public void testTryCastFromBoolean() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", BooleanType.INSTANCE, false);
            TryCast cast = new TryCast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", BooleanType.INSTANCE, true);
            cast = new TryCast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", BooleanType.INSTANCE, false);
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(2, 1));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromTinyInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(4, 1));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromSmallInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", SmallIntType.INSTANCE, true);
            cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(6, 1));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(6, 2));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromInteger() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", IntegerType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", IntegerType.INSTANCE, true);
            cast = new TryCast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(11, 1));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(11, 2));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromBigInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", BigIntType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", BigIntType.INSTANCE, true);
            cast = new TryCast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(20, 1));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            cast = new TryCast(child, DecimalV2Type.createDecimalV2Type(20, 2));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromLargeInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", LargeIntType.INSTANCE, true);
            cast = new TryCast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().enableDecimal256 = true;
            mockedContext.when(ConnectContext::get).thenReturn(context);
            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new TryCast(child, DecimalV3Type.createDecimalV3Type(40, 1));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            cast = new TryCast(child, DecimalV3Type.createDecimalV3Type(40, 2));
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromFloat() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", FloatType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", FloatType.INSTANCE, true);
            cast = new TryCast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            // To date is always nullable
            cast = new TryCast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromDouble() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", DoubleType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, true);
            cast = new TryCast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            // To date is always nullable
            cast = new TryCast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromDecimal() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", DecimalV2Type.SYSTEM_DEFAULT, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", DecimalV2Type.SYSTEM_DEFAULT, true);
            cast = new TryCast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().enableDecimal256 = true;
            mockedContext.when(ConnectContext::get).thenReturn(context);
            // To integer
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(4, 2), false);
            cast = new TryCast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromDatetime() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", DateTimeType.INSTANCE, false);
            TryCast cast = new TryCast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromTime() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", TimeV2Type.SYSTEM_DEFAULT, false);
            TryCast cast = new TryCast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromString() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false, all nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", StringType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromChar() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false, all nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", CharType.SYSTEM_DEFAULT, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromVarchar() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false, all nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", VarcharType.MAX_VARCHAR_TYPE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromJson() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, always nullable. to Json is PN
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", JsonType.INSTANCE, false);
            TryCast cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, always nullable.  to Json is PN
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new TryCast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }

    @Test
    public void testTryCastFromArray() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, false);
            TryCast cast = new TryCast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, true);
            cast = new TryCast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());

            // When strict mode is false, always nullable.  to Json is PN
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, false);
            cast = new TryCast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertFalse(cast.originCastNullable());
            child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, true);
            cast = new TryCast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            Assertions.assertTrue(cast.originCastNullable());
        }
    }
}
