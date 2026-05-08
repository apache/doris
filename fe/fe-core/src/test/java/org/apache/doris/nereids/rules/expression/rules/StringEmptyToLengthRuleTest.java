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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringEmptyToLengthRuleTest extends ExpressionRewriteTestHelper {

    @BeforeEach
    public void setup() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(StringEmptyToLengthRule.INSTANCE)
        ));
    }

    // ─── Helper: rewrite without type coercion ───────────────────────────────────
    // We bypass ExpressionRewriteTestHelper.assertRewrite() because that method
    // applies typeCoercion() to the input, which may wrap string literals in Cast
    // nodes and prevent our pattern from matching.

    private void assertRuleRewrite(Expression before, Expression expected) {
        Expression result = executor.rewrite(before, context);
        Assertions.assertEquals(expected, result);
    }

    private void assertRuleNoRewrite(Expression before) {
        Expression result = executor.rewrite(before, context);
        Assertions.assertEquals(before, result);
    }

    // ─── Rewrite cases ───────────────────────────────────────────────────────────

    @Test
    public void testStringSlotEqualEmptyRewrite() {
        // str_col = '' → length(str_col) = 0
        SlotReference slot = new SlotReference("str_col", StringType.INSTANCE, true);
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new EqualTo(slot, empty),
                new EqualTo(new Length(slot), new IntegerLiteral(0))
        );
    }

    @Test
    public void testVarcharSlotEqualEmptyRewrite() {
        // varchar_col = '' → length(varchar_col) = 0
        SlotReference slot = new SlotReference("vc_col", VarcharType.SYSTEM_DEFAULT, true);
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new EqualTo(slot, empty),
                new EqualTo(new Length(slot), new IntegerLiteral(0))
        );
    }

    @Test
    public void testReversedOperandsRewrite() {
        // '' = str_col (literal on left) → length(str_col) = 0
        SlotReference slot = new SlotReference("str_col", StringType.INSTANCE, true);
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new EqualTo(empty, slot),
                new EqualTo(new Length(slot), new IntegerLiteral(0))
        );
    }

    @Test
    public void testNotEqualToEmptyRewrite() {
        // NOT(str_col = '') → NOT(length(str_col) = 0)  (i.e. str_col <> '')
        SlotReference slot = new SlotReference("str_col", StringType.INSTANCE, true);
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new Not(new EqualTo(slot, empty)),
                new Not(new EqualTo(new Length(slot), new IntegerLiteral(0)))
        );
    }

    @Test
    public void testNotEqualToEmptyReversedRewrite() {
        // NOT('' = str_col) → NOT(length(str_col) = 0)
        SlotReference slot = new SlotReference("str_col", StringType.INSTANCE, true);
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new Not(new EqualTo(empty, slot)),
                new Not(new EqualTo(new Length(slot), new IntegerLiteral(0)))
        );
    }

    // ─── No-op cases ─────────────────────────────────────────────────────────────

    @Test
    public void testNonEmptyLiteralNotRewritten() {
        // str_col = 'abc' — literal is non-empty, must not be rewritten
        SlotReference slot = new SlotReference("str_col", StringType.INSTANCE, true);
        assertRuleNoRewrite(new EqualTo(slot, new VarcharLiteral("abc")));
    }

    @Test
    public void testNonStringSlotNotRewritten() {
        // int_col = 0 — slot is not string-like, must not be rewritten
        SlotReference intSlot = new SlotReference("int_col", IntegerType.INSTANCE, true);
        assertRuleNoRewrite(new EqualTo(intSlot, new IntegerLiteral(0)));
    }

    @Test
    public void testNotWithNonEmptyLiteralNotRewritten() {
        // NOT(str_col = 'abc') — non-empty literal, must not be rewritten
        SlotReference slot = new SlotReference("str_col", StringType.INSTANCE, true);
        assertRuleNoRewrite(new Not(new EqualTo(slot, new VarcharLiteral("abc"))));
    }

    // ─── Struct element (non-SlotReference expression) cases ─────────────────────

    @Test
    public void testStructElementEqualEmptyRewrite() {
        // struct_element(struct_col, 'f3') = '' → length(struct_element(struct_col, 'f3')) = 0
        SlotReference structSlot = new SlotReference("struct_col",
                new StructType(ImmutableList.of(
                        new StructField("f1", IntegerType.INSTANCE, true, ""),
                        new StructField("f3", StringType.INSTANCE, true, "")
                )), true);
        StructElement structElement = new StructElement(structSlot, new VarcharLiteral("f3"));
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new EqualTo(structElement, empty),
                new EqualTo(new Length(structElement), new IntegerLiteral(0))
        );
    }

    @Test
    public void testStructElementReversedOperandsRewrite() {
        // '' = struct_element(struct_col, 'f3') → length(struct_element(struct_col, 'f3')) = 0
        SlotReference structSlot = new SlotReference("struct_col",
                new StructType(ImmutableList.of(
                        new StructField("f1", IntegerType.INSTANCE, true, ""),
                        new StructField("f3", StringType.INSTANCE, true, "")
                )), true);
        StructElement structElement = new StructElement(structSlot, new VarcharLiteral("f3"));
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new EqualTo(empty, structElement),
                new EqualTo(new Length(structElement), new IntegerLiteral(0))
        );
    }

    @Test
    public void testNotStructElementEqualEmptyRewrite() {
        // NOT(struct_element(struct_col, 'f3') = '') → NOT(length(struct_element(struct_col, 'f3')) = 0)
        SlotReference structSlot = new SlotReference("struct_col",
                new StructType(ImmutableList.of(
                        new StructField("f1", IntegerType.INSTANCE, true, ""),
                        new StructField("f3", StringType.INSTANCE, true, "")
                )), true);
        StructElement structElement = new StructElement(structSlot, new VarcharLiteral("f3"));
        VarcharLiteral empty = new VarcharLiteral("");
        assertRuleRewrite(
                new Not(new EqualTo(structElement, empty)),
                new Not(new EqualTo(new Length(structElement), new IntegerLiteral(0)))
        );
    }
}
