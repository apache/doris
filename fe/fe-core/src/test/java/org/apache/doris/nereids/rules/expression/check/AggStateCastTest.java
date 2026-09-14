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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AggStateCastTest {
    private final AggStateType stateType = new AggStateType("sum_map",
            ImmutableList.of(MapType.of(StringType.INSTANCE, IntegerType.INSTANCE)),
            ImmutableList.of(true), false);

    @Test
    void testRejectNonStateInputs() {
        for (DataType source : ImmutableList.of(CharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT,
                StringType.INSTANCE, VariantType.INSTANCE, VarBinaryType.INSTANCE,
                IntegerType.INSTANCE, BigIntType.INSTANCE, DoubleType.INSTANCE, BooleanType.INSTANCE,
                DateV2Type.INSTANCE, JsonType.INSTANCE, ArrayType.of(IntegerType.INSTANCE),
                MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), StructType.SYSTEM_DEFAULT,
                BitmapType.INSTANCE, HllType.INSTANCE, QuantileStateType.INSTANCE)) {
            for (boolean strict : new boolean[] {false, true}) {
                Assertions.assertFalse(CheckCast.check(source, stateType, strict), source.toSql());
                Assertions.assertFalse(CheckCast.checkWithLooseAggState(source, stateType, strict), source.toSql());
            }
            Assertions.assertThrows(AnalysisException.class,
                    () -> TypeCoercionUtils.checkCanCastTo(source, stateType), source.toSql());
        }
    }

    @Test
    void testPreserveNullAndIdenticalState() {
        for (boolean strict : new boolean[] {false, true}) {
            Assertions.assertTrue(CheckCast.check(NullType.INSTANCE, stateType, strict));
            Assertions.assertTrue(CheckCast.check(stateType, stateType, strict));
        }
    }

    @Test
    void testRejectNonStateToNestedState() {
        for (boolean strict : new boolean[] {false, true}) {
            for (DataType source : ImmutableList.of(StringType.INSTANCE, VariantType.INSTANCE, JsonType.INSTANCE)) {
                Assertions.assertFalse(CheckCast.check(source, ArrayType.of(stateType), strict));
                Assertions.assertFalse(CheckCast.check(source,
                        MapType.of(StringType.INSTANCE, stateType), strict));
            }
            Assertions.assertFalse(CheckCast.check(ArrayType.of(StringType.INSTANCE),
                    ArrayType.of(stateType), strict));
            Assertions.assertTrue(CheckCast.check(ArrayType.of(stateType), ArrayType.of(stateType), strict));
        }
    }

    @Test
    void testRejectRawStateSql() {
        for (String input : ImmutableList.of(
                "unhex('00020101016101010000000000000001010161010300000000000000')",
                "cast('invalid state' as variant)", "cast('invalid state' as varbinary)", "1")) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                            "select cast(" + input + " as agg_state<sum_map(map<string,int>)>)"));
        }
    }

    @Test
    void testPreserveStateConstructionAndCoercion() {
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select avg_merge(cast(avg_state(cast(1 as int)) as agg_state<avg(bigint)>))");
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select cast(null as agg_state<avg(int)>)");
        PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(
                "select sum_map_merge(sum_map_state(map('a', 1)))");
    }
}
