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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class SimplifyConditionalFunctionTest extends ExpressionRewriteTestHelper {
    @Test
    public void testCoalesce() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp((SimplifyConditionalFunction.INSTANCE))));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        SlotReference nonNullableSlot = new SlotReference("b", StringType.INSTANCE, false);

        // coalesce(null, null, nullable_slot) -> nullable_slot
        assertRewrite(new Coalesce(NullLiteral.INSTANCE, NullLiteral.INSTANCE, slot), slot);

        // coalesce(null, null, nullable_slot, slot) -> coalesce(nullable_slot, slot)
        assertRewrite(new Coalesce(NullLiteral.INSTANCE, NullLiteral.INSTANCE, slot, slot),
                new Coalesce(slot, slot));

        // coalesce(null, null, non-nullable_slot, slot) -> non-nullable_slot
        assertRewrite(new Coalesce(NullLiteral.INSTANCE, NullLiteral.INSTANCE, nonNullableSlot, slot),
                nonNullableSlot);

        // coalesce(non-nullable_slot, ...) -> non-nullable_slot
        assertRewrite(new Coalesce(nonNullableSlot, NullLiteral.INSTANCE, nonNullableSlot, slot),
                nonNullableSlot);

        // coalesce(nullable_slot, slot) -> coalesce(nullable_slot, slot)
        assertRewrite(new Coalesce(slot, nonNullableSlot), new Coalesce(slot, nonNullableSlot));

        // coalesce(null, null) -> null
        assertRewrite(new Coalesce(NullLiteral.INSTANCE, NullLiteral.INSTANCE), new NullLiteral(BooleanType.INSTANCE));

        // coalesce(null) -> null
        assertRewrite(new Coalesce(NullLiteral.INSTANCE), new NullLiteral(BooleanType.INSTANCE));

        // coalesce(non-nullable_slot) -> non-nullable_slot
        assertRewrite(new Coalesce(nonNullableSlot), nonNullableSlot);

        // coalesce(non-nullable_slot) -> non-nullable_slot
        assertRewrite(new Coalesce(slot), slot);

        // coalesce(null, nullable_slot, literal) -> coalesce(nullable_slot, slot, literal)
        assertRewrite(new Coalesce(slot, nonNullableSlot), new Coalesce(slot, nonNullableSlot));
    }

    @Test
    public void testNvl() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp((SimplifyConditionalFunction.INSTANCE))));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        SlotReference nonNullableSlot = new SlotReference("b", StringType.INSTANCE, false);
        // nvl(null, nullable_slot) -> nullable_slot
        assertRewrite(new Nvl(NullLiteral.INSTANCE, slot), slot);

        // nvl(null, non-nullable_slot) -> non-nullable_slot
        assertRewrite(new Nvl(NullLiteral.INSTANCE, nonNullableSlot), nonNullableSlot);

        // nvl(nullable_slot, nullable_slot) -> nvl(nullable_slot, nullable_slot)
        assertRewrite(new Nvl(slot, nonNullableSlot), new Nvl(slot, nonNullableSlot));

        // nvl(non-nullable_slot, null) -> non-nullable_slot
        assertRewrite(new Nvl(nonNullableSlot, NullLiteral.INSTANCE), nonNullableSlot);

        // nvl(null, null) -> null
        assertRewrite(new Nvl(NullLiteral.INSTANCE, NullLiteral.INSTANCE), new NullLiteral(BooleanType.INSTANCE));
    }

    @Test
    public void testNullIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp((SimplifyConditionalFunction.INSTANCE))));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        SlotReference nonNullableSlot = new SlotReference("b", StringType.INSTANCE, false);
        // nullif(null, slot) -> null
        assertRewrite(new NullIf(NullLiteral.INSTANCE, slot),
                new Nullable(new NullLiteral(VarcharType.SYSTEM_DEFAULT)));

        // nullif(nullable_slot, null) -> slot
        assertRewrite(new NullIf(slot, NullLiteral.INSTANCE), new Nullable(slot));

        // nullif(non-nullable_slot, null) -> non-nullable_slot
        assertRewrite(new NullIf(nonNullableSlot, NullLiteral.INSTANCE), new Nullable(nonNullableSlot));
    }

}
