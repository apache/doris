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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class IvmUtilTest {

    private SlotReference intSlot(String name, boolean nullable) {
        return new SlotReference(name, IntegerType.INSTANCE, nullable);
    }

    private SlotReference varcharSlot(String name, boolean nullable) {
        return new SlotReference(name, VarcharType.SYSTEM_DEFAULT, nullable);
    }

    private MurmurHash3128 rowIdHash(Expression result) {
        Assertions.assertInstanceOf(MurmurHash3128.class, result);
        return (MurmurHash3128) result;
    }

    @Test
    void testBuildRowIdHashEmptyReturnsZero() {
        Expression result = IvmUtil.buildRowIdHash(Collections.emptyList());
        Assertions.assertInstanceOf(LargeIntLiteral.class, result);
        Assertions.assertEquals(0L, ((LargeIntLiteral) result).getValue().longValue());
    }

    @Test
    void testBuildRowIdHashSingleKeyStructure() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", false)));
        Assertions.assertEquals(LargeIntType.INSTANCE, result.getDataType());
        MurmurHash3128 hash = rowIdHash(result);
        Assertions.assertEquals(2, hash.arity());
    }

    @Test
    void testBuildRowIdHashMultipleKeysStructure() {
        List<Expression> keys = ImmutableList.of(intSlot("k1", false), intSlot("k2", true));
        Expression result = IvmUtil.buildRowIdHash(keys);
        MurmurHash3128 hash = rowIdHash(result);
        // 2 keys * 2 args each = 4 hash arguments
        Assertions.assertEquals(4, hash.arity());
    }

    @Test
    void testBuildRowIdHashContainsNvlAndIsNull() {
        List<Expression> keys = ImmutableList.of(intSlot("k1", true), intSlot("k2", true));
        Expression result = IvmUtil.buildRowIdHash(keys);
        MurmurHash3128 hash = rowIdHash(result);
        // Even-indexed args (0, 2) should be Nvl; odd-indexed (1, 3) should be Cast(IsNull)
        Assertions.assertInstanceOf(Nvl.class, hash.child(0));
        Assertions.assertInstanceOf(Cast.class, hash.child(1));
        Assertions.assertInstanceOf(IsNull.class, ((Cast) hash.child(1)).child());
        Assertions.assertInstanceOf(Nvl.class, hash.child(2));
        Assertions.assertInstanceOf(Cast.class, hash.child(3));
        Assertions.assertInstanceOf(IsNull.class, ((Cast) hash.child(3)).child());
    }

    @Test
    void testBuildRowIdHashVarcharKeySkipsInnerCast() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(varcharSlot("k1", false)));
        MurmurHash3128 hash = rowIdHash(result);
        Nvl nvl = (Nvl) hash.child(0);
        // VARCHAR key should not have inner Cast — Nvl wraps the slot directly
        Assertions.assertInstanceOf(SlotReference.class, nvl.child(0));
    }

    @Test
    void testBuildRowIdHashNonVarcharKeyHasInnerCast() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", false)));
        MurmurHash3128 hash = rowIdHash(result);
        Nvl nvl = (Nvl) hash.child(0);
        // INT key should have Cast(slot, VARCHAR) inside Nvl
        Assertions.assertInstanceOf(Cast.class, nvl.child(0));
    }

    @Test
    void testBuildRowIdHashResultNotNullable() {
        // With non-nullable keys
        Expression result1 = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", false)));
        Assertions.assertFalse(result1.nullable());
        // With nullable keys — result should still be non-nullable due to ifnull/isnull wrapping
        Expression result2 = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", true)));
        Assertions.assertFalse(result2.nullable());
    }
}
