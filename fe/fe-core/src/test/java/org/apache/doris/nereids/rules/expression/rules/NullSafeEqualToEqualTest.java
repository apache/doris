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
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class NullSafeEqualToEqualTest extends ExpressionRewriteTestHelper {

    // "A<=> Null" to "A is null"
    @Test
    void testNullSafeEqualToIsNull() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        assertRewrite(new NullSafeEqual(slot, NullLiteral.INSTANCE), new IsNull(slot));
    }

    // "A<=> Null" to "False", when A is not nullable
    @Test
    void testNullSafeEqualToFalse() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, false);
        assertRewrite(new NullSafeEqual(slot, NullLiteral.INSTANCE), BooleanLiteral.FALSE);
    }

    // "A(nullable)<=>B" not changed
    @Test
    void testNullSafeEqualNotChangedLeft() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference a = new SlotReference("a", StringType.INSTANCE, true);
        SlotReference b = new SlotReference("b", StringType.INSTANCE, false);
        assertRewrite(new NullSafeEqual(a, b), new NullSafeEqual(a, b));
    }

    // "A<=>B(nullable)" not changed
    @Test
    void testNullSafeEqualNotChangedRight() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference a = new SlotReference("a", StringType.INSTANCE, false);
        SlotReference b = new SlotReference("b", StringType.INSTANCE, true);
        assertRewrite(new NullSafeEqual(a, b), new NullSafeEqual(a, b));
    }

    // "A<=>B" changed
    @Test
    void testNullSafeEqualToEqual() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NullSafeEqualToEqual.INSTANCE)
        ));
        SlotReference a = new SlotReference("a", StringType.INSTANCE, false);
        SlotReference b = new SlotReference("b", StringType.INSTANCE, false);
        assertRewrite(new NullSafeEqual(a, b), new EqualTo(a, b));
    }
}
