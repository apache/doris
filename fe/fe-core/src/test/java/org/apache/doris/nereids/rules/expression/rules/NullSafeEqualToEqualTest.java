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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class NullSafeEqualToEqualTest extends ExpressionRewriteTestHelper {

    // "A <=> Null" to "A is null"
    @Test
    void testNullSafeEqualToIsNull() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        assertRewrite(new NullSafeEqual(slot, NullLiteral.INSTANCE), new IsNull(slot));
        slot = new SlotReference("a", StringType.INSTANCE, false);
        assertRewrite(new NullSafeEqual(slot, NullLiteral.INSTANCE), new IsNull(slot));
    }

    // "0 <=> Null" to false
    @Test
    void testNullSafeEqualToFalse() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        assertRewrite(new NullSafeEqual(new IntegerLiteral(0), NullLiteral.INSTANCE), BooleanLiteral.FALSE);
    }

    // "NULL <=> Null" to false
    @Test
    void testNullSafeEqualToTrue() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        assertRewrite(new NullSafeEqual(NullLiteral.INSTANCE, NullLiteral.INSTANCE), BooleanLiteral.TRUE);
    }

    // "A(nullable)<=>B" not changed
    @Test
    void testNullSafeEqualNotChangedLeftNullable() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference a = new SlotReference("a", StringType.INSTANCE, true);
        SlotReference b = new SlotReference("b", StringType.INSTANCE, false);
        assertRewrite(new NullSafeEqual(a, b), new NullSafeEqual(a, b));
    }

    // "A<=>B(nullable)" not changed
    @Test
    void testNullSafeEqualNotChangedRightNullable() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference a = new SlotReference("a", StringType.INSTANCE, false);
        SlotReference b = new SlotReference("b", StringType.INSTANCE, true);
        assertRewrite(new NullSafeEqual(a, b), new NullSafeEqual(a, b));
    }

    // "A<=>B" not changed
    @Test
    void testNullSafeEqualNotChangedBothNullable() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference a = new SlotReference("a", StringType.INSTANCE, false);
        SlotReference b = new SlotReference("b", StringType.INSTANCE, false);
        assertRewrite(new NullSafeEqual(a, b), new NullSafeEqual(a, b));
    }

    // "1 <=> 0" to "1 = 0"
    @Test
    void testNullSafeEqualToEqual() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        IntegerLiteral a = new IntegerLiteral(0);
        IntegerLiteral b = new IntegerLiteral(1);
        assertRewrite(new NullSafeEqual(a, b), new EqualTo(a, b));
    }
}
