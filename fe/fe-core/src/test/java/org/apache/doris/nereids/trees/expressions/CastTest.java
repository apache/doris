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
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
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

public class CastTest {

    @Test
    public void testCastFromBoolean() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", BooleanType.INSTANCE, false);
            Cast cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", BooleanType.INSTANCE, true);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", BooleanType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(2, 1));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(2, 2));
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromTinyInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", TinyIntType.INSTANCE, true);
            cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(4, 1));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(4, 2));
            Assertions.assertTrue(cast.nullable());

            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromSmallInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", SmallIntType.INSTANCE, true);
            cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(6, 1));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(6, 2));
            Assertions.assertTrue(cast.nullable());

            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // To tinyint is always nullable
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromInteger() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", IntegerType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", IntegerType.INSTANCE, true);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(11, 1));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(11, 2));
            Assertions.assertTrue(cast.nullable());

            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // To tinyint and smallint is always nullable
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromBigInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", BigIntType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", BigIntType.INSTANCE, true);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(20, 1));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(20, 2));
            Assertions.assertTrue(cast.nullable());

            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // To tinyint, smallint and int is always nullable
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromLargeInt() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", LargeIntType.INSTANCE, true);
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().enableDecimal256 = true;
            mockedContext.when(ConnectContext::get).thenReturn(context);
            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(40, 1));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(40, 2));
            Assertions.assertTrue(cast.nullable());

            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // To tinyint, smallint, int and bigint is always nullable
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromFloat() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", FloatType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, true);
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // To integral and decimal is always nullable
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromDouble() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", DoubleType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, true);
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // To integral and decimal is always nullable
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromDecimal() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", DecimalV2Type.SYSTEM_DEFAULT, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.SYSTEM_DEFAULT, true);
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, return nullable when decimal range < 1
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().enableDecimal256 = true;
            mockedContext.when(ConnectContext::get).thenReturn(context);
            // To integer
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(4, 2), false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(5, 2), false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(6, 2), false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(7, 2), false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(11, 2), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(12, 2), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(20, 2), false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(21, 2), false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(40, 2), false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(41, 2), false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            // To decimal
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(41, 2), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(41, 2));
            Assertions.assertFalse(cast.nullable());
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(40, 2));
            Assertions.assertTrue(cast.nullable());
            // To date is always nullable
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromDatetime() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", DateTimeType.INSTANCE, false);
            Cast cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DateTimeV2Type.SYSTEM_DEFAULT, false);
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromTime() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", TimeV2Type.INSTANCE, false);
            Cast cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromString() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false, all nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", StringType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, FloatType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV3Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IPv4Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IPv6Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromChar() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false, all nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", CharType.SYSTEM_DEFAULT, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, FloatType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV3Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IPv4Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IPv6Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromVarchar() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is false, all nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            SlotReference child = new SlotReference("slot", VarcharType.MAX_VARCHAR_TYPE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, FloatType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV3Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DecimalV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, DateTimeV2Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, TimeV2Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IPv4Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            cast = new Cast(child, IPv6Type.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromJson() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            // When strict mode is true, always nullable. to Json is PN
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", JsonType.INSTANCE, false);
            Cast cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, VarcharType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, StringType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, JsonType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, true);
            cast = new Cast(child, JsonType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, always nullable.  to Json is PN
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, BooleanType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, DoubleType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, VarcharType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, StringType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, false);
            cast = new Cast(child, JsonType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", JsonType.INSTANCE, true);
            cast = new Cast(child, JsonType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
        }
    }

    @Test
    public void testCastFromArray() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            SlotReference child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, false);
            Cast cast = new Cast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, true);
            cast = new Cast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, always nullable.  to Json is PN
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, false);
            cast = new Cast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", ArrayType.SYSTEM_DEFAULT, true);
            cast = new Cast(child, ArrayType.SYSTEM_DEFAULT);
            Assertions.assertTrue(cast.nullable());
        }
    }
}
