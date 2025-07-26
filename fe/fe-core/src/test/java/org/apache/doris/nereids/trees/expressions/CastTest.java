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

import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
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
    public void testNullable() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            // When strict mode is true, return child.nullable.
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);

            ConnectContext context = new ConnectContext();
            context.getSessionVariable().enableDecimal256 = true;
            mockedContext.when(ConnectContext::get).thenReturn(context);

            SlotReference child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(5, 3), false);
            Cast cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(5, 3), true);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, child is nullable
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(5, 3), true);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // When strict mode is false, child is not null, all types test.
            // 1. StringLike to other, always nullable.
            child = new SlotReference("slot", StringType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", VarcharType.MAX_VARCHAR_TYPE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", CharType.createCharType(10), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            // 2. Datetime to datetime is always nullable
            child = new SlotReference("slot", DateTimeType.INSTANCE, false);
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DateTimeType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DateTimeV2Type.MAX, false);
            cast = new Cast(child, DateTimeType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DateTimeV2Type.MAX, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            // 3. Time to time is always nullable
            child = new SlotReference("slot", TimeV2Type.MAX, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            // 4. Time to tiny int, small int and int is always nullable
            child = new SlotReference("slot", TimeV2Type.MAX, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", TimeV2Type.MAX, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", TimeV2Type.MAX, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", TimeV2Type.MAX, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", TimeV2Type.MAX, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            // 5. Integral to Integral
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            // 6. Integral to decimal
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(2));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(3));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(4));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(5));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(9));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(10));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(18));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(19));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(38));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(39));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(2, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(3, 0));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(4, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(5, 0));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(9, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(10, 0));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(18, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(19, 0));
            Assertions.assertFalse(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(38, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(39, 0));
            // Maximum precision of decimal v2 is 27.
            Assertions.assertTrue(cast.nullable());

            // 7. Integral to date like and time
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", TinyIntType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", SmallIntType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", IntegerType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", BigIntType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", LargeIntType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            // 8. Float and double to integral, decimal, date like and time.
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(76));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(76, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", FloatType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(76));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(76, 0));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DoubleType.INSTANCE, false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            // 9. Decimal to integral
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(3, 1), false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(5, 2), false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(5, 1), false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(7, 2), false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 1), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(12, 2), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(19, 1), false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(21, 2), false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(39, 1), false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(41, 2), false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(3, 1), false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(5, 2), false);
            cast = new Cast(child, TinyIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(5, 1), false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(7, 2), false);
            cast = new Cast(child, SmallIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 1), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(12, 2), false);
            cast = new Cast(child, IntegerType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(19, 1), false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(21, 2), false);
            cast = new Cast(child, BigIntType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(39, 1), false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(41, 2), false);
            cast = new Cast(child, LargeIntType.INSTANCE);
            Assertions.assertFalse(cast.nullable());

            // 10. decimal to decimal
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(11, 3));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(9, 3));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(11, 4));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(9, 2));
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(11, 3));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(9, 3));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(11, 4));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(9, 2));
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(11, 3));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(9, 3));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(11, 4));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(9, 2));
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(11, 3));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(9, 3));
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(11, 4));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(10, 3), false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(9, 2));
            Assertions.assertTrue(cast.nullable());

            // 11. decimal to time and date like
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(3, 0), false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(3, 0), false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(3, 0), false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(3, 0), false);
            cast = new Cast(child, TimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(3, 0), false);
            cast = new Cast(child, DateType.INSTANCE);
            Assertions.assertTrue(cast.nullable());
            child = new SlotReference("slot", DecimalV2Type.createDecimalV2Type(3, 0), false);
            cast = new Cast(child, DateTimeV2Type.MAX);
            Assertions.assertTrue(cast.nullable());

            // 12. boolean to decimal
            child = new SlotReference("slot", BooleanType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(1, 0));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", BooleanType.INSTANCE, false);
            cast = new Cast(child, DecimalV3Type.createDecimalV3Type(1, 1));
            Assertions.assertTrue(cast.nullable());

            child = new SlotReference("slot", BooleanType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(1, 0));
            Assertions.assertFalse(cast.nullable());
            child = new SlotReference("slot", BooleanType.INSTANCE, false);
            cast = new Cast(child, DecimalV2Type.createDecimalV2Type(1, 1));
            Assertions.assertTrue(cast.nullable());
        }
    }
}
