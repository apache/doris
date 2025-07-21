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

package org.apache.doris.nereids.rules.expression.check;

import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class CheckCastTest {
    @Test
    public void testCommonCheckCast() {
        // 1. Variant type.
        DataType originType = VariantType.INSTANCE;
        DataType targetType = ArrayType.of(IntegerType.INSTANCE);
        Assertions.assertTrue(CheckCast.check(originType, targetType));
        targetType = IntegerType.INSTANCE;
        Assertions.assertTrue(CheckCast.check(originType, targetType));
        targetType = MapType.SYSTEM_DEFAULT;
        Assertions.assertFalse(CheckCast.check(originType, targetType));

        // 2. Null type.
        originType = NullType.INSTANCE;
        Assertions.assertTrue(CheckCast.check(originType, targetType));

        // 3. Same type.
        originType = MapType.SYSTEM_DEFAULT;
        Assertions.assertTrue(CheckCast.check(originType, targetType));

        // 4. ipv4 can't cast to boolean, numeric, date like and time
        checkCastToBasicType(IPv4Type.INSTANCE);

        // 5. ipv6 can't cast to boolean, numeric, date like and time
        checkCastToBasicType(IPv6Type.INSTANCE);

        // 6. bitmap can't cast to boolean, numeric, date like and time
        checkCastToBasicType(BitmapType.INSTANCE);
        checkCastToIpType(BitmapType.INSTANCE);

        // 7. bitmap can't cast to boolean, numeric, date like and time
        checkCastToBasicType(HllType.INSTANCE);
        checkCastToIpType(HllType.INSTANCE);

        // 8. array can't cast to boolean, numeric, date like and time
        checkCastToBasicType(ArrayType.SYSTEM_DEFAULT);
        checkCastToIpType(ArrayType.SYSTEM_DEFAULT);

        // 9. array can't cast to boolean, numeric, date like and time
        checkCastToBasicType(MapType.SYSTEM_DEFAULT);
        checkCastToIpType(MapType.SYSTEM_DEFAULT);

        // 10. array can't cast to boolean, numeric, date like and time
        checkCastToBasicType(StructType.SYSTEM_DEFAULT);
        checkCastToIpType(StructType.SYSTEM_DEFAULT);

        // 11. boolean can't cast to date like and time
        originType = BooleanType.INSTANCE;
        targetType = DateType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = DateTimeV2Type.MAX;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = TimeV2Type.MAX;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        checkCastToIpType(BooleanType.INSTANCE);

        // 12. date like, time and ip can't cast to boolean
        originType = DateType.INSTANCE;
        targetType = BooleanType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        originType = DateTimeV2Type.MAX;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        originType = TimeV2Type.MAX;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        originType = IPv4Type.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        originType = IPv6Type.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
    }

    private void checkCastToBasicType(DataType originType) {
        DataType targetType = BooleanType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = TinyIntType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = SmallIntType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = IntegerType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = BigIntType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = LargeIntType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = FloatType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = DoubleType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = DecimalV2Type.createDecimalV2Type(15, 0);
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = DecimalV3Type.createDecimalV3Type(15, 0);
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = DateType.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = DateTimeV2Type.MAX;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = TimeV2Type.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
    }

    private void checkCastToIpType(DataType originType) {
        DataType targetType = IPv4Type.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
        targetType = IPv6Type.INSTANCE;
        Assertions.assertFalse(CheckCast.check(originType, targetType));
    }

    @Test
    public void testStrictCheckCast() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            // Date type
            DataType originType = DateType.INSTANCE;
            DataType targetType = TinyIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = SmallIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = FloatType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DoubleType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV2Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV3Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = TimeV2Type.MAX;
            Assertions.assertFalse(CheckCast.check(originType, targetType));

            // DateTime type
            originType = DateTimeV2Type.MAX;
            targetType = TinyIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = SmallIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = IntegerType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = FloatType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DoubleType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV2Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV3Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));

            // Time type
            originType = TimeV2Type.MAX;
            targetType = FloatType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DoubleType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV2Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV3Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
        }
    }

    @Test
    public void testUnStrictCheckCast() {
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            // Date type
            DataType originType = DateType.INSTANCE;
            DataType targetType = TinyIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = SmallIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV2Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV3Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = TimeV2Type.MAX;
            Assertions.assertFalse(CheckCast.check(originType, targetType));

            // DateTime type
            originType = DateTimeV2Type.MAX;
            targetType = TinyIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = SmallIntType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = IntegerType.INSTANCE;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV2Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV3Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));

            // Time type
            originType = TimeV2Type.MAX;
            targetType = DecimalV2Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
            targetType = DecimalV3Type.SYSTEM_DEFAULT;
            Assertions.assertFalse(CheckCast.check(originType, targetType));
        }
    }
}
