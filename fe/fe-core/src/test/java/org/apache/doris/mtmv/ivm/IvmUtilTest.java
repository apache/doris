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

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class IvmUtilTest {

    @Test
    void testCommonHiddenSlots() {
        Assertions.assertTrue(IvmUtil.isCommonHiddenSlot(Column.DELETE_SIGN));
        Assertions.assertTrue(IvmUtil.isCommonHiddenSlot(Column.VERSION_COL));
        Assertions.assertTrue(IvmUtil.isCommonHiddenSlot(Column.SEQUENCE_COL));
        Assertions.assertTrue(IvmUtil.isCommonHiddenSlot(Column.COMMIT_TSO_COL));
        Assertions.assertFalse(IvmUtil.isCommonHiddenSlot(Column.IVM_ROW_ID_COL));
        Assertions.assertTrue(IvmUtil.isCommonHiddenSlot(Column.ROW_STORE_COL));
        Assertions.assertFalse(IvmUtil.isCommonHiddenSlot(Column.IVM_AGG_COUNT_COL));
        Assertions.assertFalse(IvmUtil.isCommonHiddenSlot("not_hidden"));
        TinyIntLiteral deleteSign = (TinyIntLiteral) IvmUtil.getCommonHiddenSlotDefault(
                Column.DELETE_SIGN, TinyIntType.INSTANCE);
        BigIntLiteral version = (BigIntLiteral) IvmUtil.getCommonHiddenSlotDefault(
                Column.VERSION_COL, BigIntType.INSTANCE);
        BigIntLiteral sequence = (BigIntLiteral) IvmUtil.getCommonHiddenSlotDefault(
                Column.SEQUENCE_COL, BigIntType.INSTANCE);
        LargeIntLiteral largeIntSequence = (LargeIntLiteral) IvmUtil.getCommonHiddenSlotDefault(
                Column.SEQUENCE_COL, LargeIntType.INSTANCE);
        BigIntLiteral commitTso = (BigIntLiteral) IvmUtil.getCommonHiddenSlotDefault(
                Column.COMMIT_TSO_COL, BigIntType.INSTANCE);
        VarcharLiteral rowStore = (VarcharLiteral) IvmUtil.getCommonHiddenSlotDefault(
                Column.ROW_STORE_COL, VarcharType.SYSTEM_DEFAULT);
        Expression skipBitmap = IvmUtil.getCommonHiddenSlotDefault(Column.SKIP_BITMAP_COL, BitmapType.INSTANCE);
        Assertions.assertEquals((byte) 0, deleteSign.getValue());
        Assertions.assertEquals(0L, version.getValue());
        Assertions.assertEquals(0L, sequence.getValue());
        Assertions.assertEquals(0L, largeIntSequence.getValue().longValue());
        Assertions.assertEquals(0L, commitTso.getValue());
        Assertions.assertEquals("", rowStore.getStringValue());
        Assertions.assertInstanceOf(BitmapEmpty.class, skipBitmap);
    }

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
    void testBuildRowIdEmptyReturnsZero() {
        Expression result = IvmUtil.buildRowIdHash(Collections.emptyList());
        Assertions.assertInstanceOf(LargeIntLiteral.class, result);
        Assertions.assertEquals(0L, ((LargeIntLiteral) result).getValue().longValue());
    }

    @Test
    void testBuildRowIdSingleLosslessKeyUsesDirectCast() {
        SlotReference key = intSlot("k1", false);
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(key));
        Assertions.assertEquals(LargeIntType.INSTANCE, result.getDataType());
        // INT widens losslessly to largeint, so the key is cast directly instead of hashed
        Assertions.assertInstanceOf(Cast.class, result);
        Assertions.assertEquals(key, ((Cast) result).child());
    }

    @Test
    void testBuildRowIdSingleNullableKeyIsNullable() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", true)));
        Assertions.assertTrue(result.nullable(), "direct-cast row-id is NULL exactly when the key is NULL");
    }

    @Test
    void testBuildRowIdSingleLargeIntKeyPassesThrough() {
        SlotReference key = new SlotReference("k1", LargeIntType.INSTANCE, true);
        Assertions.assertEquals(key, IvmUtil.buildRowIdHash(ImmutableList.of(key)),
                "a largeint key (e.g. a chained MV's stored row-id) is used directly without a Cast");
    }

    @Test
    void testBuildRowIdSingleNonLosslessKeyStillHashes() {
        // VARCHAR cannot widen losslessly to largeint, so a single varchar key is still hashed
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(varcharSlot("k1", false)));
        Assertions.assertInstanceOf(MurmurHash3128.class, result);
    }

    @Test
    void testBuildRowIdMultipleKeysStructure() {
        List<Expression> keys = ImmutableList.of(intSlot("k1", false), intSlot("k2", true));
        Expression result = IvmUtil.buildRowIdHash(keys);
        MurmurHash3128 hash = rowIdHash(result);
        // 2 keys * 2 args each = 4 hash arguments
        Assertions.assertEquals(4, hash.arity());
    }

    @Test
    void testBuildRowIdContainsNvlAndIsNull() {
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
    void testBuildRowIdVarcharKeySkipsInnerCast() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(varcharSlot("k1", false)));
        MurmurHash3128 hash = rowIdHash(result);
        Nvl nvl = (Nvl) hash.child(0);
        // VARCHAR key should not have inner Cast — Nvl wraps the slot directly
        Assertions.assertInstanceOf(SlotReference.class, nvl.child(0));
    }

    @Test
    void testBuildRowIdNonVarcharKeyHasInnerCast() {
        // A single INT key now widens losslessly to largeint and skips the hash; use two keys
        // to force the hash path and verify INT is still cast to VARCHAR inside Nvl there.
        List<Expression> keys = ImmutableList.of(intSlot("k1", false), varcharSlot("k2", false));
        Expression result = IvmUtil.buildRowIdHash(keys);
        MurmurHash3128 hash = rowIdHash(result);
        Nvl nvl = (Nvl) hash.child(0);
        // INT key should have Cast(slot, VARCHAR) inside Nvl
        Assertions.assertInstanceOf(Cast.class, nvl.child(0));
    }

    // ==================== streamName tests ====================

    @Test
    void testStreamName() {
        List<String> qualifiers = ImmutableList.of("internal", "test", "t1");
        Assertions.assertEquals("__doris_ivm_stream_123_782al5xuzxh3qcgg9apwm95wt",
                IvmUtil.streamName(123L, qualifiers));
        Assertions.assertEquals(IvmUtil.streamName(123L, qualifiers), IvmUtil.streamName(123L, qualifiers));
    }

    @Test
    void testStreamNamePrefixConsistency() {
        String name = IvmUtil.streamName(1L, ImmutableList.of("internal", "db", "t"));
        Assertions.assertTrue(name.startsWith(IvmUtil.IVM_STREAM_PREFIX));
        Assertions.assertTrue(name.matches("__doris_ivm_stream_1_[0-9a-z]{25}"));
    }

    @Test
    void testStreamNameUsesFullQualifiers() {
        String left = IvmUtil.streamName(1L, ImmutableList.of("internal", "left_db", "same_name"));
        String right = IvmUtil.streamName(1L, ImmutableList.of("internal", "right_db", "same_name"));
        String external = IvmUtil.streamName(1L, ImmutableList.of("external", "left_db", "same_name"));
        Assertions.assertNotEquals(left, right);
        Assertions.assertNotEquals(left, external);
    }

    @Test
    void testStreamNameUsesUtf8ByteLength() {
        Assertions.assertEquals("__doris_ivm_stream_1_2eudmi94v9xn1pg8fjaozmag8",
                IvmUtil.streamName(1L, ImmutableList.of("internal", "数据库", "表")));
    }

    @Test
    void testStreamNameMaximumLength() {
        String name = IvmUtil.streamName(Long.MAX_VALUE,
                ImmutableList.of("internal", "database", "table"));
        Assertions.assertEquals(64, name.length());
    }

    @Test
    void testStreamNameRequiresFullQualifiers() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IvmUtil.streamName(1L, ImmutableList.of("database", "table")));
    }

    // ==================== buildRowIdHash tests ====================

    @Test
    void testBuildRowIdMultiKeyHashNotNullable() {
        // Multi-key hash path stays non-nullable even with nullable keys (ifnull/isnull wrapping)
        Expression result = IvmUtil.buildRowIdHash(
                ImmutableList.of(intSlot("k1", true), intSlot("k2", true)));
        Assertions.assertFalse(result.nullable());
    }
}
